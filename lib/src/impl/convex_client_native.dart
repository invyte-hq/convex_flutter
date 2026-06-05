import 'dart:async';

import 'package:convex_flutter/src/impl/convex_client_interface.dart';
import 'package:convex_flutter/src/rust/lib.dart';
import 'package:convex_flutter/src/rust/frb_generated.dart';
import 'package:convex_flutter/src/utils.dart';
import 'package:convex_flutter/src/connection_status.dart';
import 'package:convex_flutter/src/convex_config.dart';
import 'package:convex_flutter/src/convex_logger.dart';
import 'package:convex_flutter/src/app_lifecycle_event.dart';
import 'package:convex_flutter/src/app_lifecycle_observer.dart';

/// Native (FFI-based) implementation of Convex client.
///
/// This implementation uses Flutter Rust Bridge to call into the official
/// Convex Rust SDK for mobile and desktop platforms (Android, iOS, macOS,
/// Windows, Linux).
///
/// For web platform, use [WebConvexClient] instead.
class NativeConvexClient implements IConvexClient {
  /// The underlying Rust FFI client
  final MobileConvexClient _rustClient;

  /// Configuration for this client
  @override
  final ConvexConfig config;

  /// Stream controller for auth state changes
  final StreamController<bool> _authStateController =
      StreamController<bool>.broadcast();

  /// Stream controller for lifecycle events
  final StreamController<AppLifecycleEvent> _lifecycleController =
      StreamController<AppLifecycleEvent>.broadcast();

  /// Stream controller for WebSocket connection state changes
  final StreamController<WebSocketConnectionState> _connectionStateController =
      StreamController<WebSocketConnectionState>.broadcast();

  /// Current connection state (cached for sync access)
  WebSocketConnectionState _currentConnectionState =
      WebSocketConnectionState.connecting;

  /// Current auth handle (if using refresh-based auth)
  AuthHandle? _currentAuthHandle;

  /// Lifecycle observer for app state changes
  late final AppLifecycleObserver _lifecycleObserver;

  /// Hard ceiling on a single reconnect attempt. The reconnect body re-fires
  /// subscriptions, whose underlying FFI calls park until the new socket is
  /// connected (the SDK acks Subscribe only after a live-socket flush). Bounding
  /// the whole body guarantees the single-flight guard is always released — even
  /// if the network never comes back — so a later resume can retry instead of
  /// being silently dropped forever. (The native layer also bounds each
  /// subscribe; this is the Dart-side belt for the whole orchestration.)
  static const Duration _reconnectTimeout = Duration(seconds: 20);

  /// Backoff + cap for retrying a reconnect that failed or timed out (e.g. the
  /// network was briefly unavailable right after resume). Bounded so we don't
  /// spin forever; convex's own ~35s inactivity heartbeat is the ultimate
  /// backstop if every retry is exhausted.
  static const Duration _reconnectRetryDelay = Duration(seconds: 3);
  static const int _maxReconnectRetries = 4;

  /// Active subscriptions, keyed by an internal monotonic id. Tracked so we
  /// can re-fire them after [reconnect] tears down the inner Rust client.
  final Map<int, _TrackedSubscription> _activeSubs = {};
  int _nextSubId = 1;

  /// Auth state remembered for replay across [reconnect]. Native auth lives
  /// inside the Rust client, so a force-drop invalidates it; we stash the
  /// caller's fetcher/callback to re-issue [setAuthWithRefresh] after the new
  /// client is built.
  Future<String?> Function()? _lastTokenFetcher;
  void Function(bool)? _lastOnAuthChange;

  /// Tracks an in-flight initial auth setup: set on entry to
  /// [setAuthWithRefresh] and completed after the Rust SDK pushes the first
  /// token to the WS, then nulled out. Steady-state subscribes pay nothing —
  /// `await null?.future` is a no-op.
  ///
  /// During initial auth, [subscribe] awaits this future so the server
  /// processes Authenticate before the Subscribe message. Without it,
  /// subscribes that race the caller's setAuthWithRefresh land at the server
  /// unauthenticated and fail with NOT_AUTHENTICATED on auth-required queries.
  ///
  /// Only the *first* setAuth roundtrip is gated; subsequent refresh-loop
  /// rotations (handled inside the Rust SDK) don't toggle this.
  Completer<void>? _authInFlight;

  /// Single-flight guard: only one reconnect body runs at a time (resumed fires
  /// multiple times on iOS, and a URL switch can race a resume). Always cleared
  /// in [reconnect]'s finally — bounded by [_reconnectTimeout] so it can never
  /// latch forever.
  bool _reconnectInProgress = false;

  /// Coalesces reconnect requests that arrive while one is in flight, so they're
  /// not dropped. A URL switch ([_queuedUrl] non-null) takes precedence over a
  /// plain resume-reconnect when the in-flight attempt drains the queue.
  bool _reconnectQueued = false;
  String? _queuedUrl;

  /// Monotonic token identifying the live reconnect attempt. A body that times
  /// out keeps running in the background; this lets a newer attempt supersede it
  /// so a late-finishing orphan can't clobber the newer attempt's subscription
  /// handles.
  int _reconnectGeneration = 0;

  /// Bounded retry for a reconnect that failed/timed out (network not yet up).
  Timer? _reconnectRetryTimer;
  int _reconnectRetryCount = 0;

  /// True once a real background (`paused`) occurred since the last `resumed`.
  /// This — not wall-clock activity timing — is the signal that the OS may have
  /// suspended the process and reclaimed the socket, so a resume needs a
  /// reconnect. A mere `inactive` peek (Control Center, notification shade,
  /// app-switcher) never sets it, so healthy sockets aren't churned.
  bool _didBackgroundSinceResume = false;

  /// Set once [dispose] runs, so late-firing lifecycle callbacks are ignored.
  bool _disposed = false;

  /// Private constructor
  NativeConvexClient._(this._rustClient, this.config);

  /// Factory method to create and initialize a native client.
  ///
  /// This handles:
  /// - Rust FFI library initialization
  /// - WebSocket state listener setup
  /// - Lifecycle observer setup
  static Future<NativeConvexClient> create(ConvexConfig config) async {
    // Initialize Rust FFI library
    await RustLib.init();

    // Create Rust client instance
    final rustClient = MobileConvexClient(
      deploymentUrl: config.deploymentUrl,
      clientId: config.clientId ?? 'flutter-client',
      verboseLogging: config.verboseNativeLogs,
    );

    // Create native client wrapper
    final client = NativeConvexClient._(rustClient, config);

    // Setup connection state listener BEFORE any operations
    // This prevents race conditions where state changes are missed
    await client._setupConnectionStateListener();

    // Setup lifecycle observer. We both forward events to the public stream
    // AND act on resume to recover stale subscriptions after iOS suspension.
    // `paused`/`detached` mark a real background (process may be suspended and
    // the socket reclaimed); `resumed` then triggers a reconnect only if such a
    // background actually happened — see [_didBackgroundSinceResume].
    client._lifecycleObserver = AppLifecycleObserver(
      onLifecycleChange: (event) {
        client._lifecycleController.add(event);
        switch (event) {
          case AppLifecycleEvent.paused:
          case AppLifecycleEvent.detached:
            client._didBackgroundSinceResume = true;
          case AppLifecycleEvent.resumed:
            client._maybeReconnectOnResume();
          case AppLifecycleEvent.inactive:
            break;
        }
      },
    );

    return client;
  }

  /// Sets up the WebSocket connection state listener.
  ///
  /// This must be called before any queries/mutations to capture all state changes.
  Future<void> _setupConnectionStateListener() async {
    config.logger(
      ConvexLogLevel.debug,
      'native',
      'Setting up WebSocket state listener',
    );

    try {
      await _rustClient.onWebsocketStateChange(
        onStateChange: (state) async {
          config.logger(
            ConvexLogLevel.debug,
            'native',
            'WS state changed: ${state.name}',
          );
          if (_disposed) return;
          _currentConnectionState = state;
          if (!_connectionStateController.isClosed) {
            _connectionStateController.add(state);
          }
          if (state == WebSocketConnectionState.connected) {
            _restoreOrphanedSubscriptionsIfAny();
          }
        },
      );
    } catch (e) {
      config.logger(
        ConvexLogLevel.error,
        'native',
        'WS state listener setup failed: $e',
      );
      rethrow;
    }
  }

  // ============================================================================
  // IConvexClient Implementation - Core Operations
  // ============================================================================

  /// Optionally normalises whole-number doubles to ints in result JSON.
  /// No-op when `config.convertWholeNumberDoublesToInts` is false.
  String _normalize(String json) => config.convertWholeNumberDoublesToInts
      ? normalizeJsonNumbers(json)
      : json;

  @override
  Future<String> query(String name, Map<String, dynamic> args) async {
    final formattedArgs = buildArgs(args);
    final result = await _rustClient
        .query(name: name, args: formattedArgs)
        .timeout(config.operationTimeout);
    return _normalize(result);
  }

  @override
  Future<String> mutation({
    required String name,
    required Map<String, dynamic> args,
  }) async {
    final formattedArgs = buildArgs(args);
    final result = await _rustClient
        .mutation(name: name, args: formattedArgs)
        .timeout(config.operationTimeout);
    return _normalize(result);
  }

  @override
  Future<String> action({
    required String name,
    required Map<String, dynamic> args,
  }) async {
    final formattedArgs = buildArgs(args);
    final result = await _rustClient
        .action(name: name, args: formattedArgs)
        .timeout(config.operationTimeout);
    return _normalize(result);
  }

  @override
  Future<SubscriptionHandle> subscribe({
    required String name,
    required Map<String, dynamic> args,
    required void Function(String) onUpdate,
    required void Function(String, String?) onError,
  }) async {
    // Wait if setAuthWithRefresh is mid-flight, so Authenticate reaches the
    // server before this Subscribe. Outside of initial auth setup this is a
    // no-op (`_authInFlight` is null). See the [_authInFlight] field docs.
    await _authInFlight?.future;

    final formattedArgs = buildArgs(args);
    final id = _nextSubId++;
    final tracked = _TrackedSubscription(
      id: id,
      name: name,
      formattedArgs: formattedArgs,
      userOnUpdate: onUpdate,
      userOnError: onError,
    );
    _activeSubs[id] = tracked;

    try {
      tracked.currentHandle = await _rustClient.subscribe(
        name: name,
        args: formattedArgs,
        onUpdate: (value) => _dispatchOnUpdate(tracked, value),
        onError: (message, value) => _dispatchOnError(tracked, message, value),
      );
    } catch (e) {
      _activeSubs.remove(id);
      rethrow;
    }

    return _NativeSubscriptionHandle(onCancel: () => _cancelTracked(id));
  }

  void _dispatchOnUpdate(_TrackedSubscription tracked, String value) {
    if (tracked.canceled) return;
    try {
      tracked.userOnUpdate(_normalize(value));
    } catch (e, st) {
      config.logger(
        ConvexLogLevel.error,
        'native',
        'onUpdate callback threw for "${tracked.name}": $e\n$st',
      );
    }
  }

  void _dispatchOnError(
    _TrackedSubscription tracked,
    String message,
    String? value,
  ) {
    if (tracked.canceled) return;
    try {
      tracked.userOnError(message, value);
    } catch (e, st) {
      config.logger(
        ConvexLogLevel.error,
        'native',
        'onError callback threw for "${tracked.name}": $e\n$st',
      );
    }
  }

  /// Emits an auth-state change, guarded against a closed controller — a Rust
  /// refresh-loop callback can fire after [dispose] has closed the stream.
  void _emitAuthState(bool isAuthenticated) {
    if (_disposed || _authStateController.isClosed) return;
    _authStateController.add(isAuthenticated);
  }

  void _cancelTracked(int id) {
    final tracked = _activeSubs.remove(id);
    if (tracked == null) return;
    tracked.canceled = true;
    try {
      tracked.currentHandle?.cancel();
    } catch (_) {
      // Best-effort: handle may already be invalidated by a reconnect.
    }
  }

  // ============================================================================
  // IConvexClient Implementation - Authentication
  // ============================================================================

  @override
  Future<void> setAuth({required String? token}) async {
    // Clear any existing refresh-based auth
    _currentAuthHandle?.dispose();
    _currentAuthHandle = null;
    _lastTokenFetcher = null;
    _lastOnAuthChange = null;

    await _rustClient.setAuth(token: token);
    _emitAuthState(token != null);
  }

  @override
  Future<AuthHandle> setAuthWithRefresh({
    required Future<String?> Function() tokenFetcher,
    void Function(bool isAuthenticated)? onAuthChange,
  }) async {
    // Dispose any existing auth handle
    _currentAuthHandle?.dispose();

    // Remember the fetcher/callback so we can replay them after a force
    // reconnect — the Rust client gets dropped, taking the refresh loop with
    // it, and we don't want to push that responsibility onto every caller.
    _lastTokenFetcher = tokenFetcher;
    _lastOnAuthChange = onAuthChange;

    // Open the in-flight gate so subscribes issued before the first
    // `setAuth(token)` returns from the WS wait for Authenticate to land.
    // Defensive: if a previous call left one open (caller didn't dispose),
    // complete it so existing waiters proceed against the new auth setup.
    final completer = Completer<void>();
    _authInFlight?.complete();
    _authInFlight = completer;

    // The Rust SDK manages the JWT-aware refresh loop and only fires
    // on_auth_change on transitions — which matches our public contract.
    // Use try/finally so the in-flight gate is always released, even if
    // the Rust side throws — otherwise subscribes waiting on the completer
    // would hang forever.
    try {
      final handle = await _rustClient.setAuthWithRefresh(
        fetchToken: () async => await tokenFetcher(),
        onAuthChange: (bool isAuth) async {
          onAuthChange?.call(isAuth);
          _emitAuthState(isAuth);
        },
      );

      _currentAuthHandle = handle;
      return handle;
    } finally {
      if (!completer.isCompleted) completer.complete();
      if (identical(_authInFlight, completer)) _authInFlight = null;
    }
  }

  @override
  Future<void> clearAuth() async {
    _currentAuthHandle?.dispose();
    _currentAuthHandle = null;
    _lastTokenFetcher = null;
    _lastOnAuthChange = null;
    await _rustClient.setAuth(token: null);
    _emitAuthState(false);
  }

  @override
  Stream<bool> get authState => _authStateController.stream;

  @override
  bool get isAuthenticated => _currentAuthHandle?.isAuthenticated() ?? false;

  // ============================================================================
  // IConvexClient Implementation - Connection Management
  // ============================================================================

  @override
  Stream<WebSocketConnectionState> get connectionState =>
      _connectionStateController.stream;

  @override
  WebSocketConnectionState get currentConnectionState =>
      _currentConnectionState;

  @override
  bool get isConnected =>
      _currentConnectionState == WebSocketConnectionState.connected;

  @override
  @Deprecated('Use connectionState stream for real-time monitoring')
  Future<ConnectionStatus> checkConnection() async {
    if (config.healthCheckQuery == null) {
      throw StateError(
        'No health check query configured. '
        'Set healthCheckQuery in ConvexConfig or use a real query.',
      );
    }

    try {
      await _rustClient
          .query(name: config.healthCheckQuery!, args: {})
          .timeout(config.operationTimeout);
      return ConnectionStatus.connected;
    } on TimeoutException {
      return ConnectionStatus.timeout;
    } catch (e) {
      return ConnectionStatus.error;
    }
  }

  /// Reconnects the underlying transport and replays auth + every tracked
  /// subscription. With [url], repoints the live client at a *different*
  /// deployment (the W5 in-app deployment switcher); without it, rebuilds the
  /// connection against the current deployment (the resume-recovery path).
  ///
  /// Single-flight and self-healing: a call that arrives while one is already
  /// running is coalesced rather than run concurrently (a URL switch is never
  /// dropped behind a resume-reconnect); the body is bounded by
  /// [_reconnectTimeout] so the guard can never latch forever (the bug this
  /// fixes: an FFI re-subscribe parking on an unready socket used to wedge every
  /// future resume); and a failed/timed-out attempt schedules a bounded retry.
  ///
  /// Returns true if the attempt wired auth and re-fired all subscriptions
  /// without error. "Connected" still flows asynchronously via [connectionState].
  @override
  Future<bool> reconnect({String? url}) async {
    if (_disposed) return false;

    // Coalesce: if a reconnect is already running, remember that another is
    // wanted rather than running two bodies concurrently (which would race on
    // the inner client and _activeSubs). A URL switch wins over a plain resume.
    if (_reconnectInProgress) {
      _reconnectQueued = true;
      if (url != null) _queuedUrl = url;
      return false;
    }

    _reconnectInProgress = true;
    final gen = ++_reconnectGeneration;
    var result = false;
    try {
      result = await _performReconnect(url, gen).timeout(
        _reconnectTimeout,
        onTimeout: () {
          config.logger(
            ConvexLogLevel.warn,
            'native',
            'Reconnect timed out after ${_reconnectTimeout.inSeconds}s; '
                'releasing guard so a later trigger can retry',
          );
          return false;
        },
      );
    } catch (e, st) {
      config.logger(
        ConvexLogLevel.error,
        'native',
        'Reconnect failed: $e\n$st',
      );
      result = false;
    } finally {
      _reconnectInProgress = false;
    }

    if (_disposed) return result;

    // Drain a coalesced request (e.g. a URL switch that arrived mid-reconnect).
    if (_reconnectQueued) {
      _reconnectQueued = false;
      final next = _queuedUrl;
      _queuedUrl = null;
      return reconnect(url: next);
    }

    // Otherwise settle the retry budget: clear it on success, or schedule a
    // bounded retry if the attempt failed/timed out (network still settling).
    if (result) {
      _reconnectRetryCount = 0;
      _reconnectRetryTimer?.cancel();
    } else {
      _scheduleReconnectRetry();
    }
    return result;
  }

  /// The reconnect body. [gen] tags this attempt; because a body that exceeds
  /// [_reconnectTimeout] is abandoned by [reconnect] but keeps running, every
  /// shared-state write first checks it is still the live attempt
  /// (`gen == _reconnectGeneration`) so a late orphan can't clobber a newer
  /// attempt's auth handle or subscription handles.
  Future<bool> _performReconnect(String? url, int gen) async {
    config.logger(
      ConvexLogLevel.debug,
      'native',
      'Reconnect (gen=$gen, target=${url ?? 'same deployment'}, '
          'activeSubs=${_activeSubs.length}, hasAuth=${_lastTokenFetcher != null})',
    );

    // Snapshot what we need to restore. Cancelling a tracked sub mid-loop
    // removes it from _activeSubs, so iterate over a copy.
    final subsToReplay = _activeSubs.values.toList(growable: false);
    final authFetcher = _lastTokenFetcher;
    final authOnChange = _lastOnAuthChange;

    // 1) Tear down the existing Rust subscription tasks. Their cancel sender
    //    flips the spawned task into clean exit; the user's callback refs are
    //    dropped on the Rust side.
    for (final sub in subsToReplay) {
      try {
        sub.currentHandle?.cancel();
      } catch (_) {}
      sub.currentHandle = null;
    }

    // 2) Drop the auth refresh loop — it holds a clone of the Convex client and
    //    would keep poking the dead instance otherwise.
    _currentAuthHandle?.dispose();
    _currentAuthHandle = null;

    // 3) Rebuild the transport. With a url, repoint at the new deployment;
    //    otherwise drop the inner client so the next op builds a fresh socket
    //    against the current deployment. Both leave the client null so the auth
    //    step lazily rebuilds it.
    if (url != null) {
      await _rustClient.setDeploymentUrl(url: url);
    } else {
      await _rustClient.forceReconnect();
    }
    if (gen != _reconnectGeneration) return false;

    // 4) Replay auth BEFORE re-subscribing. Push the initial token directly via
    //    setAuth first so Authenticate is enqueued on the worker's FIFO ahead of
    //    the re-Subscribes below — without this, re-Subscribes can reach the
    //    server before Authenticate and briefly fail auth-required queries
    //    (NOT_AUTHENTICATED). Then start the refresh loop for ongoing rotation.
    //    Skip entirely if the consumer never used refresh-based auth (they own
    //    their setAuth).
    //
    //    NOTE: the refresh loop's own first push (a second Authenticate) may
    //    trail the re-Subscribes — that's intentionally benign. The server
    //    treats a re-sent valid token at a higher monotonic identity version as
    //    a legitimate update, not an AuthError. Don't try to "fix" it by
    //    awaiting the loop's first push: that push happens inside the spawned
    //    Rust task and isn't awaitable from here — which is exactly why the
    //    explicit setAuth above is what guarantees the ordering.
    if (authFetcher != null) {
      String? initialToken;
      try {
        initialToken = await authFetcher();
      } catch (e) {
        config.logger(
          ConvexLogLevel.warn,
          'native',
          'Token fetch during reconnect threw: $e',
        );
        initialToken = null;
      }
      if (gen != _reconnectGeneration) return false;

      await _rustClient.setAuth(token: initialToken);

      final handle = await _rustClient.setAuthWithRefresh(
        fetchToken: () async => await authFetcher(),
        onAuthChange: (bool isAuth) async {
          authOnChange?.call(isAuth);
          _emitAuthState(isAuth);
        },
      );
      if (gen != _reconnectGeneration) {
        handle.dispose();
        return false;
      }
      _currentAuthHandle = handle;
    }

    // 5) Re-fire every still-tracked subscription. A failure here marks the sub
    //    [_TrackedSubscription.needsRestore] and fails the attempt, so it's
    //    re-tried (by the bounded retry, or when the socket next reconnects via
    //    [_restoreOrphanedSubscriptionsIfAny]) rather than silently stranded.
    var allOk = true;
    for (final sub in subsToReplay) {
      if (gen != _reconnectGeneration) return false;
      if (!_activeSubs.containsKey(sub.id)) continue;
      try {
        final handle = await _rustClient.subscribe(
          name: sub.name,
          args: sub.formattedArgs,
          onUpdate: (value) => _dispatchOnUpdate(sub, value),
          onError: (message, value) => _dispatchOnError(sub, message, value),
        );
        // A newer attempt may have superseded us mid-await; don't clobber its
        // fresh handle with this now-stale one.
        if (gen != _reconnectGeneration) {
          try {
            handle.cancel();
          } catch (_) {}
          return false;
        }
        sub.currentHandle = handle;
        sub.needsRestore = false;
      } catch (e, st) {
        allOk = false;
        sub.needsRestore = true;
        config.logger(
          ConvexLogLevel.error,
          'native',
          'Failed to re-subscribe "${sub.name}" after reconnect: $e\n$st',
        );
      }
    }

    return allOk;
  }

  /// Schedules a bounded, delayed retry after a failed/timed-out reconnect (the
  /// network may still be coming up right after resume). Capped by
  /// [_maxReconnectRetries]; convex's own inactivity heartbeat is the backstop
  /// once retries are exhausted.
  void _scheduleReconnectRetry() {
    if (_disposed) return;
    if (_reconnectRetryCount >= _maxReconnectRetries) {
      config.logger(
        ConvexLogLevel.warn,
        'native',
        'Reconnect retries exhausted ($_maxReconnectRetries); '
            'relying on SDK heartbeat to recover',
      );
      return;
    }
    _reconnectRetryCount++;
    _reconnectRetryTimer?.cancel();
    _reconnectRetryTimer = Timer(_reconnectRetryDelay, () {
      if (_disposed) return;
      config.logger(
        ConvexLogLevel.debug,
        'native',
        'Retrying reconnect (attempt $_reconnectRetryCount/$_maxReconnectRetries)',
      );
      unawaited(reconnect());
    });
  }

  /// Restores subscriptions stranded by a prior reconnect whose re-subscribe
  /// failed while the socket was down (e.g. the network only returned after the
  /// bounded retries were exhausted). The freshly-`connected` socket is the
  /// trigger: re-run reconnect to re-fire the still-stranded subs. Keyed off the
  /// explicit [_TrackedSubscription.needsRestore] flag, NOT a null handle —
  /// handles are also transiently null during a normal initial subscribe, which
  /// must not provoke a reconnect. Skipped mid-reconnect (the single-flight
  /// coalesce would queue it anyway; skipping avoids needless churn).
  void _restoreOrphanedSubscriptionsIfAny() {
    if (_disposed || _reconnectInProgress) return;
    final hasStranded = _activeSubs.values.any((s) => s.needsRestore);
    if (!hasStranded) return;
    config.logger(
      ConvexLogLevel.debug,
      'native',
      'Socket connected with stranded subscriptions — re-firing to restore',
    );
    unawaited(reconnect());
  }

  /// Lifecycle hook: reconnect on resume only if a real background actually
  /// happened since the last resume. `paused`/`detached` set
  /// [_didBackgroundSinceResume] (the OS may have suspended us and reclaimed the
  /// socket); a mere `inactive` peek (Control Center, notification shade,
  /// app-switcher) does not, so healthy sockets aren't churned on quick
  /// app-switches.
  void _maybeReconnectOnResume() {
    if (_disposed) return;
    if (!_didBackgroundSinceResume) {
      config.logger(
        ConvexLogLevel.debug,
        'native',
        'Resume without a preceding background — skipping reconnect',
      );
      return;
    }
    _didBackgroundSinceResume = false;
    // Fresh user-driven foregrounding: reset the retry budget.
    _reconnectRetryCount = 0;
    // Fire-and-forget; reconnect() is single-flight + self-healing internally.
    unawaited(reconnect());
  }

  // ============================================================================
  // IConvexClient Implementation - Lifecycle Management
  // ============================================================================

  @override
  Stream<AppLifecycleEvent> get lifecycleEvents => _lifecycleController.stream;

  // ============================================================================
  // IConvexClient Implementation - Resource Management
  // ============================================================================

  @override
  void dispose() {
    _disposed = true;
    // Supersede any in-flight reconnect body so it bails at its next gen-check
    // instead of writing to _currentAuthHandle / the closed controllers below.
    _reconnectGeneration++;
    _reconnectRetryTimer?.cancel();
    for (final sub in _activeSubs.values) {
      sub.canceled = true;
      try {
        sub.currentHandle?.cancel();
      } catch (_) {}
    }
    _activeSubs.clear();
    _currentAuthHandle?.dispose();
    _lifecycleObserver.dispose();
    _authStateController.close();
    _lifecycleController.close();
    _connectionStateController.close();
  }
}

/// Internal bookkeeping for an active subscription so [NativeConvexClient]
/// can re-fire it after [NativeConvexClient.reconnect] tears down the inner
/// Rust client. User callbacks (`userOnUpdate`/`userOnError`) survive across
/// reconnects; only the underlying [SubscriptionHandle] is swapped.
class _TrackedSubscription {
  final int id;
  final String name;
  final Map<String, String> formattedArgs;
  final void Function(String) userOnUpdate;
  final void Function(String, String?) userOnError;

  SubscriptionHandle? currentHandle;
  bool canceled = false;

  /// Set when a reconnect's re-subscribe for this sub failed (socket down), so
  /// it's known to need restoring once the socket is back — see
  /// [NativeConvexClient._restoreOrphanedSubscriptionsIfAny]. Distinct from a
  /// transiently-null [currentHandle] during a normal initial subscribe.
  bool needsRestore = false;

  _TrackedSubscription({
    required this.id,
    required this.name,
    required this.formattedArgs,
    required this.userOnUpdate,
    required this.userOnError,
  });
}

/// Dart-side [SubscriptionHandle] returned from [NativeConvexClient.subscribe].
///
/// Wraps the underlying Rust handle so cancellation also removes the entry
/// from the tracking map (preventing it from being re-fired on the next
/// reconnect) and so the visible handle stays stable across reconnects, even
/// as the inner Rust handle is swapped out.
class _NativeSubscriptionHandle implements SubscriptionHandle {
  final void Function() onCancel;
  bool _canceled = false;

  _NativeSubscriptionHandle({required this.onCancel});

  @override
  void cancel() {
    if (_canceled) return;
    _canceled = true;
    onCancel();
  }

  @override
  void dispose() {
    cancel();
  }

  @override
  bool get isDisposed => _canceled;
}
