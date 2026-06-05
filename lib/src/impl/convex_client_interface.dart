import 'dart:async';

import 'package:convex_flutter/src/rust/lib.dart'
    show WebSocketConnectionState, SubscriptionHandle, AuthHandle;
import 'package:convex_flutter/src/connection_status.dart';
import 'package:convex_flutter/src/convex_config.dart';
import 'package:convex_flutter/src/app_lifecycle_event.dart';

/// Abstract interface for platform-specific Convex client implementations.
///
/// This interface defines the contract that both native (FFI) and web (pure Dart)
/// implementations must follow, ensuring API consistency across all platforms.
///
/// Implementations:
/// - [NativeConvexClient]: Uses Flutter Rust Bridge (FFI) to call Convex Rust SDK
/// - [WebConvexClient]: Uses pure Dart WebSocket for web platform
abstract class IConvexClient {
  /// Configuration for this client instance
  ConvexConfig get config;

  // ============================================================================
  // Core Operations
  // ============================================================================

  /// Executes a Convex query operation.
  ///
  /// [name] - Name of the query function to execute (e.g., "messages:list")
  /// [args] - Map of arguments to pass to the query
  ///
  /// Returns the query result as a JSON string.
  ///
  /// Throws:
  /// - [TimeoutException] if operation exceeds configured timeout
  /// - [ClientError] for Convex-specific errors
  Future<String> query(String name, Map<String, dynamic> args);

  /// Executes a Convex mutation operation.
  ///
  /// [name] - Name of the mutation function to execute
  /// [args] - Map of arguments to pass to the mutation
  ///
  /// Returns the mutation result as a JSON string.
  ///
  /// Throws:
  /// - [TimeoutException] if operation exceeds configured timeout
  /// - [ClientError] for Convex-specific errors
  Future<String> mutation({
    required String name,
    required Map<String, dynamic> args,
  });

  /// Executes a Convex action operation.
  ///
  /// [name] - Name of the action function to execute
  /// [args] - Map of arguments to pass to the action
  ///
  /// Returns the action result as a JSON string.
  ///
  /// Throws:
  /// - [TimeoutException] if operation exceeds configured timeout
  /// - [ClientError] for Convex-specific errors
  Future<String> action({
    required String name,
    required Map<String, dynamic> args,
  });

  /// Creates a real-time subscription to a Convex query.
  ///
  /// [name] - Name of the query function to subscribe to
  /// [args] - Map of arguments for the subscription
  /// [onUpdate] - Callback function called when new data arrives
  /// [onError] - Callback function called when an error occurs
  ///
  /// Returns a handle that can be used to cancel the subscription.
  Future<SubscriptionHandle> subscribe({
    required String name,
    required Map<String, dynamic> args,
    required void Function(String) onUpdate,
    required void Function(String, String?) onError,
  });

  // ============================================================================
  // Authentication
  // ============================================================================

  /// Sets the authentication token for the client.
  ///
  /// [token] - The JWT authentication token to set, or null to clear
  ///
  /// Used to authenticate requests to the Convex backend.
  Future<void> setAuth({required String? token});

  /// Sets authentication with automatic, JWT-aware token refresh.
  ///
  /// [tokenFetcher] is called whenever a token is needed:
  ///   - immediately, to obtain the initial token,
  ///   - again, ~60 seconds before the current JWT's `exp` claim,
  ///   - and again on `AuthError` from the server.
  ///
  /// The caller's [tokenFetcher] is expected to be idempotent / cache-friendly
  /// (e.g. Clerk's `sessionToken()` returns a cached JWT until it expires) —
  /// the client may call it more than once in quick succession with no
  /// special "initial vs refresh" distinction. If you need to observe each
  /// individual token rotation, do it inside [tokenFetcher].
  ///
  /// [onAuthChange] fires on auth state **transitions** only (unauthenticated
  /// → authenticated, and vice versa) — not on every refresh.
  ///
  /// Returns an [AuthHandle] that owns the refresh timer; call `dispose()`
  /// to stop refreshes and clear auth.
  Future<AuthHandle> setAuthWithRefresh({
    required Future<String?> Function() tokenFetcher,
    void Function(bool isAuthenticated)? onAuthChange,
  });

  /// Clears the authentication token and stops any active token refresh.
  Future<void> clearAuth();

  /// Stream of authentication state changes.
  ///
  /// Emits `true` when authenticated, `false` when not authenticated.
  Stream<bool> get authState;

  /// Returns whether the user is currently authenticated.
  bool get isAuthenticated;

  // ============================================================================
  // Connection Management
  // ============================================================================

  /// Stream of WebSocket connection state changes.
  ///
  /// Emits [WebSocketConnectionState.connected] when connection is established,
  /// [WebSocketConnectionState.connecting] when connecting or reconnecting.
  ///
  /// This is the recommended way to monitor connection status.
  Stream<WebSocketConnectionState> get connectionState;

  /// Returns the current WebSocket connection state (synchronous).
  WebSocketConnectionState get currentConnectionState;

  /// Returns whether the WebSocket is currently connected.
  bool get isConnected;

  /// Manually checks connection status using a health check query.
  ///
  /// **Deprecated**: Use [connectionState] stream instead for real-time monitoring.
  ///
  /// Returns [ConnectionStatus] indicating connection state.
  @Deprecated('Use connectionState stream instead')
  Future<ConnectionStatus> checkConnection();

  /// Rebuilds the transport, replays auth, and re-fires every active
  /// subscription.
  ///
  /// [url] — when non-null, switches the live client to a *different* deployment
  /// (the in-app deployment switcher); when null, reconnects to the current
  /// deployment (the resume-recovery path). Subscriptions and the consumer's
  /// stable handles survive across the reconnect; only the underlying transport
  /// is swapped.
  ///
  /// Returns a best-effort, transport-dependent success flag: on native, `true`
  /// once auth was replayed and every subscription re-fired without error; on
  /// web, `true` once the new socket reached the open state. Treat it as
  /// advisory — observe [connectionState] (and your subscriptions) for
  /// authoritative status, since "connected" and the first post-reconnect data
  /// still arrive asynchronously.
  Future<bool> reconnect({String? url});

  // ============================================================================
  // Lifecycle Management
  // ============================================================================

  /// Stream of app lifecycle events (foreground/background transitions).
  ///
  /// Useful for managing connections when app state changes.
  Stream<AppLifecycleEvent> get lifecycleEvents;

  // ============================================================================
  // Resource Management
  // ============================================================================

  /// Disposes of client resources and closes connections.
  ///
  /// Should be called when the client is no longer needed.
  void dispose();
}
