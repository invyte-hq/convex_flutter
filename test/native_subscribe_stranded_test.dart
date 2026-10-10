// The bridge throws from `subscribe` while the socket is down or the inner
// client is being rebuilt. These tests pin that such a first subscribe is kept
// as stranded, reported through onError, and replayed once the socket
// connects, the way a reconnect's failed re-subscribe already is. A throw
// while the socket is connected is not the socket's, so it is dropped and
// rethrown rather than replayed on every reconnect.
import 'package:convex_flutter/src/convex_config.dart';
import 'package:convex_flutter/src/impl/convex_client_native.dart';
import 'package:convex_flutter/src/rust/lib.dart';
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
    'a subscribe whose Rust call throws returns a handle and reports through onError',
    () async {
      rust.subscribeError = StateError('socket down');
      final errors = <(String, String?)>[];

      final handle = await client.subscribe(
        name: 's',
        args: {},
        onUpdate: (_) {},
        onError: (message, data) => errors.add((message, data)),
      );

      expect(handle, isA<SubscriptionHandle>());
      expect(errors, hasLength(1));
      expect(errors.single.$1, contains('socket down'));
      expect(errors.single.$2, isNull);
      expect(rust.calls, ['subscribe:s']);
    },
  );

  test('a stranded subscribe is replayed when the socket connects', () async {
    rust.subscribeError = StateError('socket down');
    final updates = <String>[];
    await client.subscribe(
      name: 's',
      args: {},
      onUpdate: updates.add,
      onError: (_, _) {},
    );

    rust.subscribeError = null;
    await rust.onStateChange!(WebSocketConnectionState.connected);
    await settle();
    await settle();
    await settle();

    expect(rust.calls, ['subscribe:s', 'force_reconnect', 'subscribe:s']);
    await rust.lastOnUpdate!('{"n":1}');
    expect(updates, ['{"n":1}']);
  });

  test('a cancelled stranded subscribe is not replayed', () async {
    rust.subscribeError = StateError('socket down');
    final handle = await client.subscribe(
      name: 's',
      args: {},
      onUpdate: (_) {},
      onError: (_, _) {},
    );

    handle.cancel();
    rust.subscribeError = null;
    await rust.onStateChange!(WebSocketConnectionState.connected);
    await settle();
    await settle();
    await settle();

    expect(rust.calls, ['subscribe:s']);
  });

  test(
    'a subscribe that throws while the socket is connected is dropped and rethrown',
    () async {
      await rust.onStateChange!(WebSocketConnectionState.connected);
      await settle();
      expect(rust.calls, isEmpty);

      rust.subscribeError = StateError('bad args');
      await expectLater(
        client.subscribe(
          name: 's',
          args: {},
          onUpdate: (_) {},
          onError: (_, _) {},
        ),
        throwsA(
          isA<StateError>().having((e) => e.message, 'message', 'bad args'),
        ),
      );

      rust.subscribeError = null;
      await rust.onStateChange!(WebSocketConnectionState.connected);
      await settle();
      await settle();
      await settle();

      expect(rust.calls, ['subscribe:s']);
    },
  );
}
