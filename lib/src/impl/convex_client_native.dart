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

/// If a subscription has not delivered an update within this window, treat the
/// underlying WebSocket as potentially stale on app resume and force-reconnect.
///
/// iOS suspends the entire process within ~30s of backgrounding, which leaves
/// the WebSocket in a zombie state. The Rust SDK's 5s heartbeat / 30s
/// inactivity threshold (see web_socket_manager.rs) eventually detects this,
/// but users perceive the staleness immediately on resume — so we short-circuit.
const Duration _staleActivityThreshold = Duration(seconds: 10);

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

  /// Active subscriptions, keyed by an internal monotonic id. Tracked so we
  /// can re-fire them after [reconnect] tears down the inner Rust client.
  final Map<int, _TrackedSubscription> _activeSubs = {};
  int _nextSubId = 1;

  /// Wall-clock time of the most recent server activity (subscription update,
  /// query result, etc.) — used to decide whether a resume needs a reconnect.
  DateTime? _lastServerActivity;

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

  /// Guard against re-entrant reconnects (resumed fires multiple times on iOS).
  bool _reconnectInProgress = false;

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
    client._lifecycleObserver = AppLifecycleObserver(
      onLifecycleChange: (event) {
        client._lifecycleController.add(event);
        if (event == AppLifecycleEvent.resumed) {
          client._maybeReconnectOnResume();
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
          _currentConnectionState = state;
          _connectionStateController.add(state);
          if (state == WebSocketConnectionState.connected) {
            _lastServerActivity = DateTime.now();
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
    _lastServerActivity = DateTime.now();
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
    _lastServerActivity = DateTime.now();
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
    _lastServerActivity = DateTime.now();
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
    _lastServerActivity = DateTime.now();
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
    _authStateController.add(token != null);
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
          _authStateController.add(isAuth);
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
    _authStateController.add(false);
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

  /// Tears down the underlying Rust client, replays auth, and re-fires every
  /// tracked subscription. Idempotent — concurrent calls deduplicate.
  ///
  /// Returns true once the new connection is wired and at least the auth/sub
  /// replay completed without throwing. Note that "connected" still flows
  /// asynchronously through [connectionState].
  @override
  Future<bool> reconnect() async {
    if (_disposed) return false;
    if (_reconnectInProgress) return false;
    _reconnectInProgress = true;

    config.logger(
      ConvexLogLevel.debug,
      'native',
      'Forcing reconnect (activeSubs=${_activeSubs.length}, hasAuth=${_currentAuthHandle != null})',
    );

    try {
      // Snapshot what we need to restore. Cancelling a tracked sub mid-loop
      // removes it from _activeSubs, so iterate over a copy.
      final subsToReplay = _activeSubs.values.toList(growable: false);
      final authFetcher = _lastTokenFetcher;
      final authOnChange = _lastOnAuthChange;

      // 1) Tear down the existing Rust subscription tasks. Their cancel
      //    sender flips the spawned task into clean exit; the user's
      //    callback refs are dropped on the Rust side.
      for (final sub in subsToReplay) {
        try {
          sub.currentHandle?.cancel();
        } catch (_) {}
        sub.currentHandle = null;
      }

      // 2) Drop the auth refresh loop — it holds a clone of the Convex
      //    client and would keep poking the dead instance otherwise.
      _currentAuthHandle?.dispose();
      _currentAuthHandle = null;

      // 3) Drop the inner Rust ConvexClient. Next call to query/subscribe
      //    will lazily rebuild a fresh WebSocket.
      await _rustClient.forceReconnect();

      // 4) Replay auth so the new client is authenticated before any
      //    subscriptions go out. Skip if the consumer never used the
      //    refresh-based auth (they're responsible for their own setAuth).
      if (authFetcher != null) {
        final handle = await _rustClient.setAuthWithRefresh(
          fetchToken: () async => await authFetcher(),
          onAuthChange: (bool isAuth) async {
            authOnChange?.call(isAuth);
            _authStateController.add(isAuth);
          },
        );
        _currentAuthHandle = handle;
      }

      // 5) Re-fire every still-tracked subscription. Skip entries that
      //    were cancelled mid-reconnect.
      for (final sub in subsToReplay) {
        if (!_activeSubs.containsKey(sub.id)) continue;
        try {
          sub.currentHandle = await _rustClient.subscribe(
            name: sub.name,
            args: sub.formattedArgs,
            onUpdate: (value) => _dispatchOnUpdate(sub, value),
            onError: (message, value) => _dispatchOnError(sub, message, value),
          );
        } catch (e, st) {
          config.logger(
            ConvexLogLevel.error,
            'native',
            'Failed to re-subscribe "${sub.name}" after reconnect: $e\n$st',
          );
        }
      }

      return true;
    } catch (e, st) {
      config.logger(
        ConvexLogLevel.error,
        'native',
        'Reconnect failed: $e\n$st',
      );
      return false;
    } finally {
      _reconnectInProgress = false;
    }
  }

  /// Lifecycle hook: skip the reconnect if the socket has seen recent traffic.
  /// On iOS, [AppLifecycleEvent.resumed] also fires during quick app-switches
  /// where the connection is still healthy — no point churning the socket.
  void _maybeReconnectOnResume() {
    if (_disposed) return;
    final last = _lastServerActivity;
    final isStale =
        last == null ||
        DateTime.now().difference(last) >= _staleActivityThreshold;
    if (!isStale) {
      config.logger(
        ConvexLogLevel.debug,
        'native',
        'Resume detected, skipping reconnect (last activity ${DateTime.now().difference(last).inMilliseconds}ms ago)',
      );
      return;
    }
    // Fire-and-forget; reconnect() guards re-entrancy internally.
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
