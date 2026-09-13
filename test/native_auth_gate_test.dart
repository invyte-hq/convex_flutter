// The Rust side of `set_auth_with_refresh` returns its handle as soon as the
// refresh loop is spawned; the loop's first pass (fetch, set_auth,
// on_auth_change) runs later, on its own. These tests pin the Dart wrapper's
// contract on top of that timing: `setAuthWithRefresh` returns, and the calls
// made meanwhile go out, only once the first token has been pushed or the loop
// found none.
import 'dart:async';

import 'package:convex_flutter/src/convex_config.dart';
import 'package:convex_flutter/src/impl/convex_client_native.dart';
import 'package:convex_flutter/src/rust/lib.dart';
import 'package:flutter_test/flutter_test.dart';

void main() {
  TestWidgetsFlutterBinding.ensureInitialized();

  late _FakeRustClient rust;
  late List<String> logs;
  late NativeConvexClient client;

  setUp(() async {
    rust = _FakeRustClient();
    logs = [];
    client = await NativeConvexClient.wire(
      rust,
      ConvexConfig(
        deploymentUrl: 'https://unit.convex.cloud',
        logger: (level, source, message) => logs.add('${level.name} $message'),
      ),
    );
  });

  tearDown(() => client.dispose());

  test(
    'setAuthWithRefresh returns only after the loop pushed the first token',
    () async {
      var returned = false;
      final call = client
          .setAuthWithRefresh(tokenFetcher: () async => 'jwt')
          .then((_) => returned = true);
      await _settle();
      expect(rust.calls, [
        'set_auth_with_refresh',
      ], reason: 'the handle is back');
      expect(returned, isFalse, reason: 'but no token has been pushed yet');

      await rust.runFirstPass();
      await call;
      expect(rust.calls, ['set_auth_with_refresh', 'set_auth:token']);
    },
  );

  test(
    'calls made during initial auth go out after the first Authenticate',
    () async {
      final auth = client.setAuthWithRefresh(tokenFetcher: () async => 'jwt');
      final calls = <Future<Object>>[
        client.mutation(name: 'm', args: {}),
        client.query('q', {}),
        client.action(name: 'a', args: {}),
        client.subscribe(
          name: 's',
          args: {},
          onUpdate: (_) {},
          onError: (_, _) {},
        ),
      ];
      await _settle();
      expect(rust.calls, [
        'set_auth_with_refresh',
      ], reason: 'everything waits on the gate');

      await rust.runFirstPass();
      await auth;
      await Future.wait(calls);
      expect(rust.calls.sublist(0, 2), [
        'set_auth_with_refresh',
        'set_auth:token',
      ]);
      expect(
        rust.calls.sublist(2),
        unorderedEquals(['mutation:m', 'query:q', 'action:a', 'subscribe:s']),
      );
    },
  );

  test('a null first token releases the gate without a push', () async {
    final auth = client.setAuthWithRefresh(tokenFetcher: () async => null);
    final mutation = client.mutation(name: 'm', args: {});
    await rust.runFirstPass();
    await auth;
    await mutation;
    expect(rust.calls, [
      'set_auth_with_refresh',
      'set_auth:none',
      'mutation:m',
    ]);
  });

  test('a throwing fetcher is logged and counts as no token', () async {
    final auth = client.setAuthWithRefresh(
      tokenFetcher: () async => throw StateError('no session'),
    );
    await rust.runFirstPass();
    await auth;
    expect(rust.calls, ['set_auth_with_refresh', 'set_auth:none']);
    expect(
      logs.where((line) => line.startsWith('error ')).single,
      startsWith('error tokenFetcher threw: Bad state: no session'),
    );
  });

  test('reconnect pushes the token before it re-subscribes, once', () async {
    rust.runsFirstPassItself = true;
    await client.setAuthWithRefresh(tokenFetcher: () async => 'jwt');
    await client.subscribe(
      name: 's',
      args: {},
      onUpdate: (_) {},
      onError: (_, _) {},
    );
    rust.calls.clear();

    expect(await client.reconnect(), isTrue);
    expect(rust.calls, [
      'force_reconnect',
      'set_auth_with_refresh',
      'set_auth:token',
      'subscribe:s',
    ]);
  });
}

/// Lets every microtask and already-due event run.
Future<void> _settle() => Future<void>.delayed(Duration.zero);

/// The Rust side of `set_auth_with_refresh` as the bridge exposes it: the
/// handle comes back at once and the loop's first pass runs later, on its own.
class _FakeRustClient implements MobileConvexClient {
  final calls = <String>[];

  /// On, the first pass runs right after the handle is returned, the way the
  /// Rust task does. Off, the test runs it with [runFirstPass] when it decides
  /// the loop got around to it.
  bool runsFirstPassItself = false;

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
    return _FakeAuthHandle();
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
    return _FakeSubscriptionHandle();
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
  }) async {}

  @override
  void dispose() {}

  @override
  bool get isDisposed => false;
}

class _FakeAuthHandle implements AuthHandle {
  @override
  void dispose() {}

  @override
  bool isAuthenticated() => true;

  @override
  bool get isDisposed => false;
}

class _FakeSubscriptionHandle implements SubscriptionHandle {
  @override
  void cancel() {}

  @override
  void dispose() {}

  @override
  bool get isDisposed => false;
}
