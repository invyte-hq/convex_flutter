import 'dart:async';

import 'package:convex_flutter/src/rust/lib.dart';

/// Lets every microtask and already-due event run.
Future<void> settle() => Future<void>.delayed(Duration.zero);

/// The Rust side of `set_auth_with_refresh` as the bridge exposes it: the
/// handle comes back at once and the loop's first pass runs later, on its own.
class FakeRustClient implements MobileConvexClient {
  final calls = <String>[];

  /// On, the first pass runs right after the handle is returned, the way the
  /// Rust task does. Off, the test runs it with [runFirstPass] when it decides
  /// the loop got around to it.
  bool runsFirstPassItself = false;

  /// When set, [subscribe] still records `'subscribe:$name'` and then throws
  /// it, the way the bridge throws when the socket is down.
  Object? subscribeError;

  /// The listener the client registered through [onWebsocketStateChange], so a
  /// test can drive the socket state.
  FutureOr<void> Function(WebSocketConnectionState)? onStateChange;

  /// The `onUpdate` of the most recent [subscribe] call, so a test can push a
  /// value through it.
  FutureOr<void> Function(String)? lastOnUpdate;

  FutureOr<String?> Function()? _fetchToken;
  FutureOr<void> Function(bool)? _onAuthChange;

  /// The loop's first pass, as rust/src/lib.rs runs it: fetch, then
  /// `set_auth` and `on_auth_change(true)` on a token; `set_auth(None)` and
  /// exit, with no callback, on none.
  Future<void> runFirstPass() async {
    final token = await _fetchToken!();
    calls.add(token == null ? 'set_auth:none' : 'set_auth:token');
    if (token != null) await _onAuthChange!(true);
  }

  @override
  Future<AuthHandle> setAuthWithRefresh({
    required FutureOr<String?> Function() fetchToken,
    required FutureOr<void> Function(bool) onAuthChange,
  }) async {
    calls.add('set_auth_with_refresh');
    _fetchToken = fetchToken;
    _onAuthChange = onAuthChange;
    if (runsFirstPassItself) unawaited(Future(runFirstPass));
    return FakeAuthHandle();
  }

  @override
  Future<void> setAuth({String? token}) async {
    calls.add(token == null ? 'set_auth_direct:none' : 'set_auth_direct:token');
  }

  @override
  Future<String> query({
    required String name,
    required Map<String, String> args,
  }) async {
    calls.add('query:$name');
    return 'null';
  }

  @override
  Future<String> mutation({
    required String name,
    required Map<String, String> args,
  }) async {
    calls.add('mutation:$name');
    return 'null';
  }

  @override
  Future<String> action({
    required String name,
    required Map<String, String> args,
  }) async {
    calls.add('action:$name');
    return 'null';
  }

  @override
  Future<SubscriptionHandle> subscribe({
    required String name,
    required Map<String, String> args,
    required FutureOr<void> Function(String) onUpdate,
    required FutureOr<void> Function(String, String?) onError,
  }) async {
    calls.add('subscribe:$name');
    lastOnUpdate = onUpdate;
    final error = subscribeError;
    if (error != null) throw error;
    return FakeSubscriptionHandle();
  }

  @override
  Future<void> forceReconnect() async {
    calls.add('force_reconnect');
  }

  @override
  Future<void> setDeploymentUrl({required String url}) async {
    calls.add('set_deployment_url');
  }

  @override
  Future<void> onWebsocketStateChange({
    required FutureOr<void> Function(WebSocketConnectionState) onStateChange,
  }) async {
    this.onStateChange = onStateChange;
  }

  @override
  void dispose() {}

  @override
  bool get isDisposed => false;
}

class FakeAuthHandle implements AuthHandle {
  @override
  void dispose() {}

  @override
  bool isAuthenticated() => true;

  @override
  bool get isDisposed => false;
}

class FakeSubscriptionHandle implements SubscriptionHandle {
  @override
  void cancel() {}

  @override
  void dispose() {}

  @override
  bool get isDisposed => false;
}
