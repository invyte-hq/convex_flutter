// Invyte fork — divergent from upstream convex_flutter. See VENDOR_CHANGES.md
// for the catalogue of patches. Highlights, by area:
//
// Protocol correctness:
//   - _sendAuthMessage: correct Authenticate format (tokenType/value/baseVersion)
//   - setAuth: pass null (not '') on sign-out so tokenType:"None" is sent
//   - _handleMutationResponse / _handleActionResponse: check success flag before
//     treating null result as error (void-returning functions return null)
//   - _handleMutationResponse / _handleActionResponse: emit ClientError
//     (convexError / serverError) — matches native FFI error types
//   - _sendAuthMessage: separate _identityVersion counter (not _querySetVersion)
//   - _handleTransition: deliver null QueryUpdated values to subscribers
//   - _handleTransition: parse and forward subscription errors (errorMessage /
//     error_message / errorData / error)
//
// Connection lifecycle:
//   - _sendMessage: queue while CONNECTING, flush on open
//   - onopen: sync _querySetVersion after flushing queued ModifyQuerySet
//   - onopen: skip queued Authenticate (already sent in onopen handler)
//   - onopen: re-register active subscriptions after reconnect (excluding
//     queryIds already in the flushed queue)
//   - _handleFatalError: generate fresh sessionId + reset state before reconnect
//   - _sendConnectMessage: use monotonic _connectionCount (not _reconnectAttempts)
//   - _WebSubscription stores udfPath + args for re-registration on reconnect
//
// Auth + observability:
//   - setAuthWithRefresh: JWT-aware refresh loop (decode exp, schedule refresh
//     ~60s before expiry); onAuthChange receives the current token
//   - setAuthWithRefresh + subscribe: _authInFlight Completer buffers Subscribe
//     messages during the initial auth roundtrip so Authenticate reaches the
//     server first (no NOT_AUTHENTICATED warnings on auth-required queries
//     issued at sign-in time)
//   - All logs go through config.logger (ConvexLogger callback) — no debugPrint

import 'dart:async';
import 'dart:convert';
import 'dart:js_interop';
import 'dart:js_interop_unsafe';
import 'dart:math' as math;

import 'package:web/web.dart' as web;
import 'package:convex_flutter/src/impl/convex_client_interface.dart';
import 'package:convex_flutter/src/rust/lib.dart'
    show AuthHandle, ClientError, SubscriptionHandle, WebSocketConnectionState;
import 'package:convex_flutter/src/connection_status.dart';
import 'package:convex_flutter/src/convex_config.dart';
import 'package:convex_flutter/src/convex_logger.dart';
import 'package:convex_flutter/src/app_lifecycle_event.dart';
import 'package:convex_flutter/src/app_lifecycle_observer.dart';

// Hot-restart safety stash.
//
// Flutter web hot-restart re-evaluates Dart but does not reset the JS engine.
// The `.toJS` WebSocket event handlers we register in [_setupWebSocketListeners]
// live in JS and hold strong references back into the Dart heap. After hot
// restart they keep firing into now-orphaned Dart objects: parser closures
// whose class tables got swapped (NoSuchMethodError on `json[$_get]`), and
// StreamController listeners that try to setState on widgets attached to a
// disposed EngineFlutterView.
//
// We stash a dispose callback on `globalThis` — which DOES survive hot
// restart — so each new [WebConvexClient.create] can tear down its
// predecessor before opening a fresh WebSocket.
//
// IMPORTANT: we cannot use `@JS('globalThis.foo') external` field bindings
// here. DDC's hot-restart sequence clears those module-scoped externals back
// to null (presumably to drop references to dead Dart isolates' closures).
// Going through `globalContext.getProperty/setProperty` at the JS-property
// level bypasses that reset — DDC has no reason to touch a property it
// doesn't know Dart wrote.
const String _stashKey = '__convexFlutterActiveDispose';

JSFunction? _readStashedDispose() {
  return globalContext.getProperty<JSFunction?>(_stashKey.toJS);
}

void _writeStashedDispose(JSFunction? value) {
  globalContext.setProperty(_stashKey.toJS, value);
}

/// Web (pure Dart) implementation of Convex client.
///
/// This implementation uses the browser's native WebSocket API for web platform,
/// avoiding the need for Rust toolchain or FFI. It implements the same
/// [IConvexClient] interface as [NativeConvexClient], ensuring API compatibility
/// across all platforms.
///
/// For mobile/desktop platforms, use [NativeConvexClient] instead.
class WebConvexClient implements IConvexClient {
  /// Configuration for this client
  @override
  final ConvexConfig config;

  /// WebSocket connection to Convex backend
  web.WebSocket? _ws;

  /// The deployment URL this client is currently connected to. Seeded from
  /// [ConvexConfig.deploymentUrl]; overridable via [reconnect]'s `url` param
  /// (the in-app deployment switcher). [_connect] reads this rather than the
  /// immutable config so a switch takes effect on the next connection.
  late String _activeDeploymentUrl = config.deploymentUrl;

  /// Stream controller for auth state changes (public via [authState]).
  final StreamController<bool> _authStateController =
      StreamController<bool>.broadcast();

  /// Private signal — server-side AuthError received. The refresh loop in
  /// [setAuthWithRefresh] listens here so it can react to server-initiated
  /// auth failures without being confused by programmatic state changes
  /// emitted on [_authStateController].
  final StreamController<void> _authRefreshRequested =
      StreamController<void>.broadcast();

  /// Stream controller for lifecycle events
  final StreamController<AppLifecycleEvent> _lifecycleController =
      StreamController<AppLifecycleEvent>.broadcast();

  /// Stream controller for WebSocket connection state changes
  final StreamController<WebSocketConnectionState> _connectionStateController =
      StreamController<WebSocketConnectionState>.broadcast();

  /// Current connection state (cached for sync access)
  WebSocketConnectionState _currentConnectionState =
      WebSocketConnectionState.connecting;

  /// Current auth token
  String? _currentAuthToken;

  /// Tracks an in-flight initial auth setup: set on entry to
  /// [setAuthWithRefresh] and completed after the first `setAuth(token)` has
  /// been pushed to the WS, then nulled out. Steady-state subscribes pay
  /// nothing — `await null?.future` is a no-op.
  ///
  /// During initial auth, [subscribe] awaits this future so the server
  /// processes Authenticate before the Subscribe message. Without it,
  /// subscribes that race the bridge's setAuthWithRefresh land at the server
  /// unauthenticated and fail with NOT_AUTHENTICATED on auth-required queries.
  ///
  /// Only the *first* setAuth roundtrip is gated; subsequent refresh-loop
  /// rotations don't toggle this (the old token is valid until the new one
  /// arrives, so subscribes never see an unauthenticated WS during a refresh).
  Completer<void>? _authInFlight;

  /// Lifecycle observer for app state changes
  late final AppLifecycleObserver _lifecycleObserver;

  /// Message ID counter for generating unique request IDs
  int _messageIdCounter = 0;

  /// Session ID for Convex sync protocol
  String? _sessionId;

  /// Query ID counter for subscriptions
  int _queryIdCounter = 0;

  /// Query set version counter for ModifyQuerySet messages
  int _querySetVersion = 0;

  /// Identity version counter for Authenticate messages (separate from querySetVersion)
  int _identityVersion = 0;

  /// Pending requests waiting for responses (query, mutation, action)
  final Map<int, Completer<String>> _pendingRequests = {};

  /// Active subscriptions
  final Map<String, _WebSubscription> _subscriptions = {};

  /// Messages queued while WebSocket is still connecting.
  final List<Map<String, dynamic>> _messageQueue = [];

  /// Reconnection attempt counter
  int _reconnectAttempts = 0;

  /// Monotonic connection counter (never reset). Used as the Convex protocol
  /// `connectionCount` field so the server can distinguish first connection (0)
  /// from reconnections (1, 2, …).
  int _connectionCount = 0;

  /// Maximum reconnection attempts
  static const int _maxReconnectAttempts = 10;

  /// Base reconnection delay
  static const Duration _baseReconnectDelay = Duration(seconds: 1);

  /// Timer for reconnection
  Timer? _reconnectTimer;

  /// Whether client is disposed
  bool _isDisposed = false;

  /// Private constructor
  WebConvexClient._(this.config);

  /// Factory method to create and initialize a web client.
  ///
  /// This handles:
  /// - WebSocket connection setup
  /// - Event listener registration
  /// - Lifecycle observer setup
  static Future<WebConvexClient> create(ConvexConfig config) async {
    config.logger(ConvexLogLevel.debug, 'web', 'Creating web client');

    // Hot-restart safety: if a prior Dart run left a live WebConvexClient
    // wired up in JS, tear it down before we open a new WebSocket. Without
    // this, the orphaned client keeps receiving Transition frames and
    // pushing them into a dead Dart heap (parse errors, "render a disposed
    // EngineFlutterView" assertions). See the stash declaration above.
    final priorDispose = _readStashedDispose();
    if (priorDispose != null) {
      try {
        priorDispose.callAsFunction();
        config.logger(
          ConvexLogLevel.info,
          'web',
          'Disposed prior WebConvexClient from previous hot-restart cycle',
        );
      } catch (e) {
        config.logger(
          ConvexLogLevel.warn,
          'web',
          'Failed to dispose prior WebConvexClient: $e',
        );
      }
      _writeStashedDispose(null);
    }

    final client = WebConvexClient._(config);

    // Stash a dispose callback in JS so the NEXT Dart run (after the next
    // hot restart) can find and tear us down. The closure captures `client`
    // strongly — that's intentional; the JS-side reference keeps the client
    // reachable until the next create() invokes & clears the stash, which
    // is exactly when we want the old client gone.
    _writeStashedDispose(
      (() {
        try {
          client.dispose();
        } catch (_) {
          // Swallow — caller's logger may itself be defunct after hot restart.
        }
      }).toJS,
    );

    // Setup lifecycle observer
    // Note: On web, we don't reconnect on lifecycle events because:
    // 1. Page navigation triggers lifecycle events but doesn't disconnect WebSocket
    // 2. WebSocket onclose handler already manages reconnection
    // 3. Browser tab visibility changes are the only real "background" events
    client._lifecycleObserver = AppLifecycleObserver(
      onLifecycleChange: (event) {
        client._lifecycleController.add(event);
        // Do NOT trigger reconnection on web - let WebSocket manage itself
        config.logger(
          ConvexLogLevel.debug,
          'web',
          'Lifecycle event: ${event.name} (no action on web)',
        );
      },
    );

    // Establish WebSocket connection
    await client._connect();

    config.logger(ConvexLogLevel.debug, 'web', 'Client created successfully');
    return client;
  }

  /// Establishes WebSocket connection to Convex backend.
  Future<void> _connect() async {
    if (_isDisposed) return;

    config.logger(ConvexLogLevel.debug, 'web', 'Connecting to Convex');

    try {
      // Convert HTTPS to WSS URL with correct Convex sync endpoint
      // Format: wss://deployment.convex.cloud/api/{version}/sync
      final wsUrl = _activeDeploymentUrl.replaceFirst('https', 'wss');
      final fullUrl = '$wsUrl/api/sync';

      config.logger(ConvexLogLevel.debug, 'web', 'WebSocket URL: $fullUrl');

      // Update state to connecting
      _updateConnectionState(WebSocketConnectionState.connecting);

      // Create WebSocket connection
      _ws = web.WebSocket(fullUrl);

      // Setup event listeners
      _setupWebSocketListeners();

      config.logger(
        ConvexLogLevel.debug,
        'web',
        'WebSocket connection initiated',
      );
    } catch (e) {
      config.logger(ConvexLogLevel.error, 'web', 'Connection failed: $e');
      _scheduleReconnect();
    }
  }

  /// Sets up WebSocket event listeners.
  void _setupWebSocketListeners() {
    final ws = _ws;
    if (ws == null) return;

    // Connection opened
    ws.onopen = (web.Event event) {
      config.logger(ConvexLogLevel.debug, 'web', 'WebSocket opened');
      config.logger(
        ConvexLogLevel.info,
        'web',
        'WS connected (session=$_sessionId, connCount=$_connectionCount, qsVersion=$_querySetVersion→0, idVersion=$_identityVersion→0, queueLen=${_messageQueue.length}, activeSubs=${_subscriptions.length}, hasAuth=${_currentAuthToken != null})',
      );
      _reconnectAttempts = 0; // Reset reconnection counter
      _querySetVersion = 0; // Reset query set version for new connection
      _identityVersion = 0; // Reset identity version for new connection
      _updateConnectionState(WebSocketConnectionState.connected);

      // Send Connect handshake (required by Convex protocol)
      _sendConnectMessage();

      // Send auth token if available
      if (_currentAuthToken != null) {
        _sendAuthMessage(_currentAuthToken!);
      }

      // Flush messages that were queued while WebSocket was connecting.
      // Skip queued Authenticate messages — onopen already sent auth above.
      // Sending a duplicate would cause a protocol version mismatch error.
      //
      // Track which queryIds were sent via queue flush so _resubscribeAll()
      // doesn't duplicate them (duplicate Add → server InternalServerError).
      final flushedQueryIds = <int>{};
      if (_messageQueue.isNotEmpty) {
        config.logger(
          ConvexLogLevel.debug,
          'web',
          'Flushing ${_messageQueue.length} queued message(s)',
        );
        final queued = List<Map<String, dynamic>>.from(_messageQueue);
        _messageQueue.clear();
        for (final msg in queued) {
          if (msg['type'] == 'Authenticate') {
            config.logger(
              ConvexLogLevel.debug,
              'web',
              'Skipping queued Authenticate (already sent in onopen)',
            );
            continue;
          }
          // Track version changes from queued ModifyQuerySet messages so
          // _querySetVersion stays in sync with what the server processes.
          if (msg['type'] == 'ModifyQuerySet') {
            _querySetVersion = msg['newVersion'] as int;
            // Record queryIds so _resubscribeAll can skip them.
            final mods = msg['modifications'] as List?;
            if (mods != null) {
              for (final mod in mods) {
                if (mod is Map && mod['queryId'] is int) {
                  flushedQueryIds.add(mod['queryId'] as int);
                }
              }
            }
          }
          _sendMessage(msg);
        }
      }

      // Re-register active subscriptions that survived a reconnect.
      // After a WS drop + reconnect, the server has a fresh session with no
      // subscriptions. The _subscriptions map still holds Dart-side callbacks,
      // but the server doesn't know about them. Re-send a single
      // ModifyQuerySet with all active subscriptions so live data flows again.
      //
      // Skip queryIds that were already sent during queue flush above —
      // duplicate Add for the same queryId causes server InternalServerError.
      _resubscribeAll(excludeQueryIds: flushedQueryIds);
    }.toJS;

    // Connection closed
    ws.onclose = (web.CloseEvent event) {
      final code = event.code;
      final reason = event.reason;
      final wasClean = event.wasClean;
      config.logger(
        ConvexLogLevel.info,
        'web',
        'WS closed (code=$code, reason="$reason", wasClean=$wasClean, session=$_sessionId, qsVersion=$_querySetVersion, activeSubs=${_subscriptions.length})',
      );
      _updateConnectionState(WebSocketConnectionState.connecting);

      // Attempt reconnection if not disposed
      if (!_isDisposed) {
        _scheduleReconnect();
      }
    }.toJS;

    // Connection error
    ws.onerror = (web.Event event) {
      config.logger(ConvexLogLevel.error, 'web', 'WebSocket error occurred');
      config.logger(ConvexLogLevel.error, 'web', 'Event type: ${event.type}');
      _updateConnectionState(WebSocketConnectionState.connecting);
    }.toJS;

    // Message received
    ws.onmessage = (web.MessageEvent event) {
      final data = event.data;

      // Convert JSAny? to String
      final dataString = (data as JSString?)?.toDart;
      if (dataString != null) {
        _handleMessage(dataString);
      } else {
        config.logger(
          ConvexLogLevel.warn,
          'web',
          'Received non-string message',
        );
      }
    }.toJS;
  }

  /// Handles incoming WebSocket messages.
  void _handleMessage(String data) {
    try {
      final message = jsonDecode(data) as Map<String, dynamic>;
      final type = message['type'] as String?;
      final id = message['id'] as String?;

      // Skip Ping logging — server heartbeats every ~15s, pure protocol
      // noise. Same treatment in _sendMessage / _sendPong below.
      if (type != 'Ping') {
        config.logger(ConvexLogLevel.debug, 'web', 'RAW MESSAGE: $data');
        config.logger(
          ConvexLogLevel.debug,
          'web',
          'Received message type: $type, id: $id',
        );
      }

      switch (type) {
        case 'Transition':
          // Query subscription updates
          _handleTransition(message);
          break;

        case 'MutationResponse':
          _handleMutationResponse(message);
          break;

        case 'ActionResponse':
          _handleActionResponse(message);
          break;

        case 'Ping':
          // Respond to server ping
          _sendPong();
          break;

        case 'FatalError':
          _handleFatalError(message);
          break;

        case 'AuthError':
          _handleAuthError(message);
          break;

        default:
          config.logger(
            ConvexLogLevel.warn,
            'web',
            'Unknown message type: $type',
          );
      }
    } catch (e) {
      config.logger(ConvexLogLevel.error, 'web', 'Failed to parse message: $e');
    }
  }

  /// Handles Transition messages (query subscription updates).
  void _handleTransition(Map<String, dynamic> message) {
    final modifications = message['modifications'] as List?;
    if (modifications == null) return;

    for (final mod in modifications) {
      final queryId = mod['queryId']?.toString();
      if (queryId == null) continue;

      final subscription = _subscriptions[queryId];
      if (subscription == null) continue;

      // Check for error — Convex sends error information when the query
      // fails (e.g. ArgumentValidationError, application-level ConvexError).
      // The field name varies by Convex protocol version; check all known
      // variants: errorMessage, error_message, errorData, error.
      //
      // `errorData` carries the structured `ConvexError.data` payload and
      // arrives as whatever JS shape the backend threw — a Map for object
      // payloads, a primitive for primitive payloads, or absent for plain
      // (non-ConvexError) failures. We JSON-encode it before forwarding so
      // the consumer always sees a String?. Casting raw to `as String?`
      // throws a TypeError when the payload is a Map and silently breaks
      // the subscription (the catch upstream swallows it and onError never
      // fires, leaving the query stuck in a loading state).
      final errorMessage = _extractTransitionErrorMessage(mod);
      final errorData = _extractTransitionErrorData(mod);

      if (errorMessage != null) {
        config.logger(
          ConvexLogLevel.warn,
          'web',
          'Subscription error for queryId=$queryId: $errorMessage (data: $errorData)',
        );
        subscription.onError(errorMessage, errorData);
        continue;
      }

      // If there's no value key at all AND no error, log for diagnostics
      // (this shouldn't happen in normal protocol flow).
      if (!mod.containsKey('value')) {
        config.logger(
          ConvexLogLevel.warn,
          'web',
          'Transition mod with no value or error for queryId=$queryId. Keys: ${(mod as Map).keys.toList()}',
        );
        continue;
      }

      // Forward all values including null (e.g. query returning null for
      // an unauthenticated user). The subscriber decodes via jsonDecode.
      final valueJson = jsonEncode(mod['value']);
      subscription.onUpdate(valueJson);
    }
  }

  /// Extracts a human-readable error message from a Transition modification.
  /// Coerces non-string payloads with `.toString()` so a Map-valued
  /// `errorMessage` (seen on some Convex protocol versions) doesn't TypeError.
  String? _extractTransitionErrorMessage(Map mod) {
    final direct = mod['errorMessage'] ?? mod['error_message'];
    if (direct is String) return direct;
    if (direct != null) return direct.toString();
    final err = mod['error'];
    if (err is String) return err;
    if (err is Map) {
      final m = err['message'];
      if (m is String) return m;
      if (m != null) return m.toString();
    }
    return null;
  }

  /// Extracts the structured `ConvexError.data` payload from a Transition
  /// modification and returns it as a JSON-encoded String? for the
  /// downstream `ClientError.convexError(data: ...)` contract. Accepts any
  /// JS shape the backend threw (Map, primitive, or absent).
  String? _extractTransitionErrorData(Map mod) {
    final direct = mod['errorData'];
    if (direct is String) return direct;
    if (direct != null) return jsonEncode(direct);
    final err = mod['error'];
    if (err is Map) return jsonEncode(err);
    return null;
  }

  /// Handles MutationResponse messages.
  void _handleMutationResponse(Map<String, dynamic> message) {
    final requestId = message['requestId'] as int?;
    if (requestId == null) return;

    final completer = _pendingRequests.remove(requestId);
    if (completer == null) return;

    final success = message['success'] as bool? ?? false;
    final result = message['result'];
    if (!success) {
      completer.completeError(_buildClientError(result, 'Mutation failed'));
    } else {
      completer.complete(result != null ? jsonEncode(result) : 'null');
    }
  }

  /// Handles ActionResponse messages.
  void _handleActionResponse(Map<String, dynamic> message) {
    final requestId = message['requestId'] as int?;
    if (requestId == null) return;

    final completer = _pendingRequests.remove(requestId);
    if (completer == null) return;

    final success = message['success'] as bool? ?? false;
    final result = message['result'];
    if (!success) {
      completer.completeError(_buildClientError(result, 'Action failed'));
    } else {
      completer.complete(result != null ? jsonEncode(result) : 'null');
    }
  }

  /// Builds a [ClientError] from an error response, matching the native FFI
  /// client's error types.
  ///
  /// If `errorData` is present (from a Convex `ConvexError`), emits
  /// [ClientError.convexError]. Otherwise emits [ClientError.serverError].
  ClientError _buildClientError(dynamic result, String fallback) {
    final errorData = result is Map ? result['data'] : null;
    if (errorData != null) {
      return ClientError.convexError(data: jsonEncode(errorData));
    }
    final message = result is Map
        ? (result['message']?.toString() ?? fallback)
        : (result?.toString() ?? fallback);
    return ClientError.serverError(msg: message);
  }

  /// Handles FatalError messages.
  void _handleFatalError(Map<String, dynamic> message) {
    final error = message['error'] as String? ?? 'Unknown fatal error';
    config.logger(
      ConvexLogLevel.error,
      'web',
      '$error (session=$_sessionId, qsVersion=$_querySetVersion, idVersion=$_identityVersion, activeSubs=${_subscriptions.length})',
    );

    // Force a brand-new server session on the next reconnect by discarding
    // the current sessionId. Without this, the reconnect reuses the same
    // sessionId and the server tries to resume the broken session — which
    // immediately triggers another FatalError (version mismatch).
    _sessionId = null;
    _connectionCount = 0;

    // Close connection — the onclose handler will trigger _scheduleReconnect,
    // which creates a fresh WS. The new onopen will generate a new sessionId
    // and re-register all active subscriptions via _resubscribeAll().
    _ws?.close();
  }

  /// Handles AuthError messages.
  void _handleAuthError(Map<String, dynamic> message) {
    final error = message['error'] as String? ?? 'Authentication error';
    final code = message['errorCode'] as String?;
    config.logger(
      ConvexLogLevel.error,
      'web',
      'AuthError: $error (code=$code, hasToken=${_currentAuthToken != null}, session=$_sessionId)',
    );

    // Public state stream: notify consumers that we lost auth.
    _authStateController.add(false);
    // Private signal: kick the refresh loop in setAuthWithRefresh.
    if (!_authRefreshRequested.isClosed) _authRefreshRequested.add(null);
  }

  /// Sends Pong response to server Ping. Heartbeat housekeeping — no
  /// logging on the happy path; failures still surface as errors.
  void _sendPong() {
    try {
      _sendMessage({
        'type': 'Event',
        'eventType': 'Pong', // Required field
        'event': null, // Required field (can be null)
      });
    } catch (e) {
      config.logger(ConvexLogLevel.error, 'web', 'Failed to send Pong: $e');
    }
  }

  /// Sends Connect handshake message.
  void _sendConnectMessage() {
    try {
      // Generate or reuse session ID (must be valid UUID format).
      // _sessionId is reset to null in _handleFatalError so that a fresh
      // session is created after protocol-level failures.
      _sessionId ??= _generateUuid();

      _sendMessage({
        'type': 'Connect',
        'sessionId': _sessionId,
        'maxObservedTimestamp': null,
        'connectionCount': _connectionCount++,
        'lastCloseReason': null, // Required field
        'clientTs': DateTime.now().millisecondsSinceEpoch, // Required field
      });
      config.logger(
        ConvexLogLevel.debug,
        'web',
        'Sent Connect handshake (session=$_sessionId, count=${_connectionCount - 1})',
      );
    } catch (e) {
      config.logger(ConvexLogLevel.error, 'web', 'Failed to send Connect: $e');
    }
  }

  /// Generates a RFC 4122 compliant UUID v4 string.
  String _generateUuid() {
    // UUID v4 format: xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx
    // Where 4 = version 4, y = variant bits (8, 9, A, or B)
    final random = math.Random();

    // Generate random values for each segment
    final segment1 = random.nextInt(0x100000000); // 32 bits = 8 hex chars
    final segment2 = random.nextInt(0x10000); // 16 bits = 4 hex chars
    final segment3 = random.nextInt(
      0x10000,
    ); // 16 bits = 4 hex chars (we'll set version)
    final segment4 = random.nextInt(
      0x10000,
    ); // 16 bits = 4 hex chars (we'll set variant)
    final segment5a = random.nextInt(0x100000000); // 32 bits = 8 hex chars
    final segment5b = random.nextInt(0x10000); // 16 bits = 4 hex chars

    // Set version 4 (bits 12-15 of segment3 = 0100)
    final version4 = (segment3 & 0x0FFF) | 0x4000;

    // Set variant bits (bits 14-15 of segment4 = 10)
    final variant = (segment4 & 0x3FFF) | 0x8000;

    // Combine segment5 parts into 12 hex digits
    final segment5 =
        '${segment5a.toRadixString(16).padLeft(8, '0')}${segment5b.toRadixString(16).padLeft(4, '0')}';

    return '${segment1.toRadixString(16).padLeft(8, '0')}-'
        '${segment2.toRadixString(16).padLeft(4, '0')}-'
        '${version4.toRadixString(16).padLeft(4, '0')}-'
        '${variant.toRadixString(16).padLeft(4, '0')}-'
        '$segment5';
  }

  /// Updates connection state and emits to stream.
  void _updateConnectionState(WebSocketConnectionState newState) {
    // Bail if dispose has run — the WebSocket's async close/error/open
    // events can still fire after we close the underlying stream
    // controller, and `.add()` on a closed broadcast controller throws.
    // Without this guard we get "Bad state: Cannot add new events after
    // calling close" out of every disposed client's onclose handler.
    if (_isDisposed || _connectionStateController.isClosed) return;
    if (_currentConnectionState != newState) {
      config.logger(
        ConvexLogLevel.debug,
        'web',
        'State transition: ${_currentConnectionState.name} → ${newState.name}',
      );
      _currentConnectionState = newState;
      _connectionStateController.add(newState);
    }
  }

  /// Schedules a reconnection attempt with exponential backoff.
  void _scheduleReconnect() {
    if (_isDisposed) return;

    _reconnectTimer?.cancel();

    if (_reconnectAttempts >= _maxReconnectAttempts) {
      config.logger(
        ConvexLogLevel.error,
        'web',
        'Max reconnection attempts reached (activeSubs=${_subscriptions.length}, pendingRequests=${_pendingRequests.length})',
      );
      return;
    }

    // Exponential backoff: 1s, 2s, 4s, 8s, 16s, 32s (max)
    final delay = _baseReconnectDelay * (1 << _reconnectAttempts.clamp(0, 5));
    _reconnectAttempts++;

    config.logger(
      ConvexLogLevel.debug,
      'web',
      'Scheduling reconnect attempt $_reconnectAttempts in ${delay.inSeconds}s',
    );

    _reconnectTimer = Timer(delay, () {
      config.logger(
        ConvexLogLevel.debug,
        'web',
        'Executing reconnect attempt $_reconnectAttempts',
      );
      _connect();
    });
  }

  /// Generates a unique message ID.
  int _generateMessageId() {
    return _messageIdCounter++;
  }

  /// Sends a message over WebSocket.
  /// If the WebSocket is still connecting, the message is queued and sent
  /// automatically once the connection opens.
  void _sendMessage(Map<String, dynamic> message) {
    final ws = _ws;
    if (ws == null || ws.readyState != web.WebSocket.OPEN) {
      _messageQueue.add(message);
      config.logger(
        ConvexLogLevel.debug,
        'web',
        'Queued message (WS not ready): ${message['type']}',
      );
      return;
    }

    final messageJson = jsonEncode(message);
    // Skip Pong logging (paired with the Ping suppression in onmessage).
    final isPong = message['type'] == 'Event' && message['eventType'] == 'Pong';
    if (!isPong) {
      config.logger(ConvexLogLevel.debug, 'web', 'SENDING: $messageJson');
    }
    ws.send(messageJson.toJS);

    if (!isPong) {
      config.logger(
        ConvexLogLevel.debug,
        'web',
        'Sent message: ${message['type']} (id: ${message['id']})',
      );
    }
  }

  /// Sends authentication message.
  ///
  /// Uses [_identityVersion] for baseVersion — this is a separate counter
  /// from [_querySetVersion] (used by ModifyQuerySet). The Convex protocol
  /// tracks query set and identity versions independently.
  void _sendAuthMessage(String? token) {
    try {
      final baseVersion = _identityVersion;
      _identityVersion++;

      // Send Authenticate message (Convex protocol >=1.20)
      if (token != null && token.isNotEmpty) {
        _sendMessage({
          'type': 'Authenticate',
          'tokenType': 'User',
          'value': token,
          'baseVersion': baseVersion,
        });
      } else {
        _sendMessage({
          'type': 'Authenticate',
          'tokenType': 'None',
          'baseVersion': baseVersion,
        });
      }
      config.logger(ConvexLogLevel.debug, 'web', 'Auth token sent');
    } catch (e) {
      config.logger(ConvexLogLevel.error, 'web', 'Failed to send auth: $e');
    }
  }

  // ============================================================================
  // IConvexClient Implementation - Core Operations
  // ============================================================================

  @override
  Future<String> query(String name, Map<String, dynamic> args) async {
    // Queries in Convex protocol use ModifyQuerySet (like subscriptions)
    // We subscribe, wait for first result, then unsubscribe
    final queryId = _queryIdCounter++;
    final queryIdStr = queryId.toString();
    final completer = Completer<String>();

    // Create temporary subscription for one-shot query
    final subscription = _WebSubscription(
      id: queryIdStr,
      onUpdate: (value) {
        if (!completer.isCompleted) {
          completer.complete(value);
          // Auto-unsubscribe after getting result
          _unsubscribe(queryIdStr);
        }
      },
      onError: (message, value) {
        if (!completer.isCompleted) {
          completer.completeError(Exception(message));
          _subscriptions.remove(queryIdStr);
        }
      },
    );
    _subscriptions[queryIdStr] = subscription;

    try {
      // Send ModifyQuerySet with Add (Convex protocol for queries)
      final baseVersion = _querySetVersion;
      final newVersion = ++_querySetVersion;

      _sendMessage({
        'type': 'ModifyQuerySet',
        'baseVersion': baseVersion,
        'newVersion': newVersion,
        'modifications': [
          {
            'type': 'Add',
            'queryId': queryId,
            'udfPath': name,
            'args': [args], // Args must be array
          },
        ],
      });

      return await completer.future.timeout(
        config.operationTimeout,
        onTimeout: () {
          _subscriptions.remove(queryIdStr);
          throw TimeoutException('Query timeout: $name');
        },
      );
    } catch (e) {
      _subscriptions.remove(queryIdStr);
      rethrow;
    }
  }

  @override
  Future<String> mutation({
    required String name,
    required Map<String, dynamic> args,
  }) async {
    final requestId = _generateMessageId();
    final completer = Completer<String>();
    _pendingRequests[requestId] = completer;

    try {
      // Send Mutation message (Convex protocol)
      _sendMessage({
        'type': 'Mutation',
        'requestId': requestId,
        'udfPath': name, // Use udfPath instead of name
        'args': [args], // Args must be array, not object
      });

      return await completer.future.timeout(
        config.operationTimeout,
        onTimeout: () {
          _pendingRequests.remove(requestId);
          throw TimeoutException('Mutation timeout: $name');
        },
      );
    } catch (e) {
      _pendingRequests.remove(requestId);
      rethrow;
    }
  }

  @override
  Future<String> action({
    required String name,
    required Map<String, dynamic> args,
  }) async {
    final requestId = _generateMessageId();
    final completer = Completer<String>();
    _pendingRequests[requestId] = completer;

    try {
      // Send Action message (Convex protocol)
      _sendMessage({
        'type': 'Action',
        'requestId': requestId,
        'udfPath': name, // Use udfPath instead of name
        'args': [args], // Args must be array, not object
      });

      return await completer.future.timeout(
        config.operationTimeout,
        onTimeout: () {
          _pendingRequests.remove(requestId);
          throw TimeoutException('Action timeout: $name');
        },
      );
    } catch (e) {
      _pendingRequests.remove(requestId);
      rethrow;
    }
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

    // Use incrementing query ID (Convex protocol requirement)
    final queryId = _queryIdCounter++;
    final queryIdStr = queryId.toString();

    // Create subscription record — store udfPath and args for re-registration
    // after a WebSocket reconnect (the server loses all subscriptions on drop).
    final serializedArgs = [args];
    final subscription = _WebSubscription(
      id: queryIdStr,
      onUpdate: onUpdate,
      onError: onError,
      udfPath: name,
      args: serializedArgs,
      isSubscription: true,
    );
    _subscriptions[queryIdStr] = subscription;

    try {
      // Send ModifyQuerySet with Add modification (Convex protocol)
      final baseVersion = _querySetVersion;
      final newVersion = ++_querySetVersion;

      _sendMessage({
        'type': 'ModifyQuerySet',
        'baseVersion': baseVersion,
        'newVersion': newVersion,
        'modifications': [
          {
            'type': 'Add',
            'queryId': queryId,
            'udfPath': name, // Use udfPath instead of name
            'args': serializedArgs, // Args must be array, not object
          },
        ],
      });

      config.logger(
        ConvexLogLevel.debug,
        'web',
        'Subscription created: queryId=$queryId',
      );

      // Return handle for cancellation
      return _WebSubscriptionHandle(
        onCancel: () {
          _unsubscribe(queryIdStr);
        },
      );
    } catch (e) {
      _subscriptions.remove(queryIdStr);
      rethrow;
    }
  }

  /// Unsubscribes from a subscription.
  void _unsubscribe(String queryIdStr) {
    final subscription = _subscriptions.remove(queryIdStr);
    if (subscription == null) return;

    config.logger(
      ConvexLogLevel.debug,
      'web',
      'Unsubscribing: queryId=$queryIdStr',
    );

    try {
      final queryId = int.tryParse(queryIdStr);
      if (queryId == null) return;

      // Send ModifyQuerySet with Remove modification (Convex protocol)
      final baseVersion = _querySetVersion;
      final newVersion = ++_querySetVersion;

      _sendMessage({
        'type': 'ModifyQuerySet',
        'baseVersion': baseVersion,
        'newVersion': newVersion,
        'modifications': [
          {'type': 'Remove', 'queryId': queryId},
        ],
      });
    } catch (e) {
      config.logger(
        ConvexLogLevel.error,
        'web',
        'Failed to send unsubscribe: $e',
      );
    }
  }

  /// Re-registers all active long-lived subscriptions with the server.
  ///
  /// Called from onopen after a reconnect. The server session is fresh (version
  /// 0, no subscriptions) but the Dart-side _subscriptions map still holds
  /// callbacks from the previous connection. We send a single ModifyQuerySet
  /// containing Add entries for every subscription so live data starts flowing
  /// again without the UI needing to know about the reconnect.
  void _resubscribeAll({Set<int> excludeQueryIds = const {}}) {
    // Collect subscriptions that can be re-registered (have stored query info
    // and are long-lived subscriptions, not one-shot queries).
    // Skip any queryIds that were already sent during queue flush.
    final resubs = _subscriptions.values
        .where(
          (s) =>
              s.isSubscription &&
              s.udfPath != null &&
              !excludeQueryIds.contains(int.tryParse(s.id)),
        )
        .toList();

    if (resubs.isEmpty) return;

    config.logger(
      ConvexLogLevel.debug,
      'web',
      'Re-registering ${resubs.length} subscription(s) after reconnect',
    );

    final modifications = <Map<String, dynamic>>[];
    for (final sub in resubs) {
      final queryId = int.tryParse(sub.id);
      if (queryId == null) continue;

      modifications.add({
        'type': 'Add',
        'queryId': queryId,
        'udfPath': sub.udfPath,
        'args': sub.args ?? [{}],
      });
    }

    if (modifications.isEmpty) return;

    final baseVersion = _querySetVersion;
    final newVersion = ++_querySetVersion;

    _sendMessage({
      'type': 'ModifyQuerySet',
      'baseVersion': baseVersion,
      'newVersion': newVersion,
      'modifications': modifications,
    });

    config.logger(
      ConvexLogLevel.debug,
      'web',
      'Re-registered ${modifications.length} subscription(s) (version $baseVersion → $newVersion)',
    );
  }

  // ============================================================================
  // IConvexClient Implementation - Authentication
  // ============================================================================

  @override
  Future<void> setAuth({required String? token}) async {
    final hadToken = _currentAuthToken != null;
    _currentAuthToken = token;

    config.logger(
      ConvexLogLevel.info,
      'web',
      'setAuth: ${hadToken ? "replacing" : "setting"} token → ${token != null ? "present" : "null"} (wsReady=${_ws?.readyState == web.WebSocket.OPEN})',
    );

    if (token != null) {
      _sendAuthMessage(token);
      _authStateController.add(true);
    } else {
      _sendAuthMessage(null); // Clear auth
      _authStateController.add(false);
    }
  }

  @override
  Future<AuthHandle> setAuthWithRefresh({
    required Future<String?> Function() tokenFetcher,
    void Function(bool isAuthenticated)? onAuthChange,
  }) async {
    // Open the in-flight gate: any subscribe issued before the first
    // `setAuth(token)` returns from the WS will wait on this completer.
    // Defensive: if a previous call left one open (shouldn't normally
    // happen — caller is expected to dispose first), complete it so
    // existing waiters proceed against the new auth setup.
    final completer = Completer<void>();
    _authInFlight?.complete();
    _authInFlight = completer;

    // Buffer time before token expiry to trigger refresh.
    const refreshBuffer = Duration(seconds: 60);
    // Minimum refresh interval to prevent tight loops on errors.
    const minRefreshInterval = Duration(seconds: 5);
    // Default refresh interval when JWT can't be decoded.
    const defaultRefreshInterval = Duration(minutes: 5);

    Timer? refreshTimer;
    StreamSubscription<void>? authErrorSub;
    var disposed = false;
    // Track auth state to fire onAuthChange only on transitions, matching
    // the Rust SDK's contract.
    var wasAuthenticated = false;

    void notifyTransition(bool isAuth) {
      if (isAuth == wasAuthenticated) return;
      wasAuthenticated = isAuth;
      onAuthChange?.call(isAuth);
    }

    // Fetch a token, push to WS, and schedule the next refresh based on the
    // new token's exp claim. Called on initial setup, on scheduled refresh,
    // and on server-initiated AuthError.
    Future<void> refresh() async {
      if (disposed) return;
      refreshTimer?.cancel();

      String? token;
      try {
        token = await tokenFetcher();
      } catch (e) {
        if (disposed) return;
        config.logger(ConvexLogLevel.error, 'web', 'tokenFetcher threw: $e');
        notifyTransition(false);
        // Back off and retry — don't tight-loop on persistent errors.
        refreshTimer = Timer(minRefreshInterval, refresh);
        return;
      }
      if (disposed) return;

      await setAuth(token: token);

      if (token == null) {
        // Caller signalled sign-out. Stop scheduling — disposal will come
        // through `handle.dispose()` from the consumer.
        notifyTransition(false);
        return;
      }

      notifyTransition(true);

      // Schedule the next refresh from the JWT's exp.
      final ttl = _decodeJwtTtl(token);
      final Duration delay;
      if (ttl != null) {
        final adjusted = ttl - refreshBuffer;
        delay = adjusted < minRefreshInterval ? minRefreshInterval : adjusted;
        config.logger(
          ConvexLogLevel.debug,
          'web',
          'Next token refresh in ${delay.inSeconds}s (JWT exp in ${ttl.inSeconds}s)',
        );
      } else {
        delay = defaultRefreshInterval;
        config.logger(
          ConvexLogLevel.warn,
          'web',
          'JWT exp not decodable; defaulting to ${delay.inMinutes}-minute refresh interval',
        );
      }
      refreshTimer = Timer(delay, refresh);
    }

    // Server-initiated AuthError → refresh now. Uses the private
    // _authRefreshRequested stream rather than _authStateController so we
    // ignore programmatic state changes (e.g. dispose → setAuth(null)).
    authErrorSub = _authRefreshRequested.stream.listen((_) {
      if (disposed) return;
      config.logger(
        ConvexLogLevel.warn,
        'web',
        'Server AuthError — refreshing token immediately',
      );
      refresh();
    });

    // Kick off the initial fetch + schedule. Use try/finally so the
    // in-flight gate is always released even if `refresh()` throws —
    // otherwise subscribes waiting on the completer would hang forever.
    try {
      await refresh();
    } finally {
      if (!completer.isCompleted) completer.complete();
      if (identical(_authInFlight, completer)) _authInFlight = null;
    }

    return _WebAuthHandle(
      isAuth: _currentAuthToken != null,
      onDispose: () async {
        disposed = true;
        refreshTimer?.cancel();
        await authErrorSub?.cancel();
        await setAuth(token: null);
      },
    );
  }

  /// Decodes a JWT's `exp` claim and returns the time remaining until
  /// expiry. Returns null if the token cannot be decoded.
  static Duration? _decodeJwtTtl(String jwt) {
    try {
      final parts = jwt.split('.');
      if (parts.length != 3) return null;
      final payload =
          jsonDecode(
                utf8.decode(base64Url.decode(base64Url.normalize(parts[1]))),
              )
              as Map<String, dynamic>;
      final exp = payload['exp'];
      if (exp is! int) return null;
      final expiresAt = DateTime.fromMillisecondsSinceEpoch(exp * 1000);
      return expiresAt.difference(DateTime.now());
    } catch (_) {
      return null;
    }
  }

  @override
  Future<void> clearAuth() async {
    await setAuth(token: null);
  }

  @override
  Stream<bool> get authState => _authStateController.stream;

  @override
  bool get isAuthenticated => _currentAuthToken != null;

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
      await query(config.healthCheckQuery!, {});
      return ConnectionStatus.connected;
    } on TimeoutException {
      return ConnectionStatus.timeout;
    } catch (e) {
      return ConnectionStatus.error;
    }
  }

  @override
  Future<bool> reconnect({String? url}) async {
    config.logger(
      ConvexLogLevel.debug,
      'web',
      'Manual reconnect requested${url != null ? ' (switching deployment)' : ''}',
    );

    // Detach the old socket's handlers BEFORE closing it. Its onclose otherwise
    // fires _scheduleReconnect(), which races this method's own _connect() and
    // would spin up a second socket (to a possibly stale URL) — a latent
    // double-connect. Nulling the handlers silences the superseded socket so
    // only our explicit _connect() below brings up the new one.
    final old = _ws;
    if (old != null) {
      old.onopen = null;
      old.onclose = null;
      old.onmessage = null;
      old.onerror = null;
      old.close();
    }
    _ws = null;

    // Cancel any pending auto-reconnect scheduled by a prior drop.
    _reconnectTimer?.cancel();

    // Switch deployment if requested — _connect() reads _activeDeploymentUrl.
    // Also drop any messages queued for the OLD deployment's session: flushing
    // them against the new deployment's fresh (version-0) session would desync
    // the query-set version counters.
    if (url != null) {
      _activeDeploymentUrl = url;
      _messageQueue.clear();
    }

    // Force a fresh server session: a new deployment (or a clean manual
    // reconnect) must not try to resume the old session's version counters.
    _sessionId = null;
    _connectionCount = 0;
    _reconnectAttempts = 0;

    try {
      await _connect();
      // Give the socket a moment to open so the returned bool is meaningful.
      // Auth (Authenticate) and re-subscription are driven by onopen regardless,
      // in the correct order (Connect → Authenticate → flush → resubscribe).
      await Future.delayed(const Duration(seconds: 2));
      return isConnected;
    } catch (e) {
      config.logger(ConvexLogLevel.error, 'web', 'Manual reconnect failed: $e');
      return false;
    }
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
    if (_isDisposed) return;

    config.logger(ConvexLogLevel.debug, 'web', 'Disposing client');
    _isDisposed = true;

    // Cancel reconnection timer
    _reconnectTimer?.cancel();

    // Close WebSocket
    _ws?.close();
    _ws = null;

    // Dispose lifecycle observer
    _lifecycleObserver.dispose();

    // Close streams
    _authStateController.close();
    _authRefreshRequested.close();
    _lifecycleController.close();
    _connectionStateController.close();

    // Clear pending requests and subscriptions
    _pendingRequests.clear();
    _subscriptions.clear();

    config.logger(ConvexLogLevel.debug, 'web', 'Client disposed');
  }
}

/// Internal subscription record for web client.
class _WebSubscription {
  final String id;
  final void Function(String) onUpdate;
  final void Function(String, String?) onError;

  /// UDF path (e.g. "users:getCurrent") — stored for re-registration after reconnect.
  final String? udfPath;

  /// Serialized args (e.g. [{}]) — stored for re-registration after reconnect.
  final List<dynamic>? args;

  /// Whether this is a long-lived subscription (not a one-shot query).
  /// One-shot queries auto-unsubscribe on first result and should NOT be
  /// re-registered after a reconnect.
  final bool isSubscription;

  _WebSubscription({
    required this.id,
    required this.onUpdate,
    required this.onError,
    this.udfPath,
    this.args,
    this.isSubscription = false,
  });
}

/// Web implementation of SubscriptionHandle.
class _WebSubscriptionHandle implements SubscriptionHandle {
  final void Function() onCancel;
  bool _isCancelled = false;

  _WebSubscriptionHandle({required this.onCancel});

  @override
  void cancel() {
    if (!_isCancelled) {
      _isCancelled = true;
      onCancel();
    }
  }

  @override
  void dispose() {
    cancel();
  }

  @override
  bool get isDisposed => _isCancelled;
}

/// Web implementation of AuthHandle.
class _WebAuthHandle implements AuthHandle {
  final bool isAuth;
  final Future<void> Function() onDispose;
  bool _isDisposed = false;

  _WebAuthHandle({required this.isAuth, required this.onDispose});

  @override
  bool isAuthenticated() => isAuth && !_isDisposed;

  @override
  void dispose() {
    if (!_isDisposed) {
      _isDisposed = true;
      onDispose();
    }
  }

  @override
  bool get isDisposed => _isDisposed;
}
