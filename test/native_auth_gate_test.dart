// The Rust side of `set_auth_with_refresh` returns its handle as soon as the
// refresh loop is spawned; the loop's first pass (fetch, set_auth,
// on_auth_change) runs later, on its own. These tests pin the Dart wrapper's
// contract on top of that timing: `setAuthWithRefresh` returns, and the calls
// made meanwhile go out, only once the first token has been pushed or the loop
// found none.
import 'package:convex_flutter/src/convex_config.dart';
import 'package:convex_flutter/src/impl/convex_client_native.dart';
import 'package:flutter_test/flutter_test.dart';

import 'fake_rust_client.dart';

void main() {
  TestWidgetsFlutterBinding.ensureInitialized();

  late FakeRustClient rust;
  late List<String> logs;
  late NativeConvexClient client;

  setUp(() async {
    rust = FakeRustClient();
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
      await settle();
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
      await settle();
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
