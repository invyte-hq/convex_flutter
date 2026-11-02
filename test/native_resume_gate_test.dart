// A resume drops the Rust client and replays auth and the tracked
// subscriptions on a fresh one. These tests pin what the Dart wrapper
// promises around that replay: a call issued while the replayed token is
// still being minted waits for it, as it does at boot; a gate a timed-out
// attempt left open is released by the attempt that gets the token; a
// subscribe parked across the reconnect hands over to the replay; a failed
// attempt marks its subscriptions for restore and retries with backoff until
// one succeeds.
import 'dart:async';

import 'package:convex_flutter/src/convex_config.dart';
import 'package:convex_flutter/src/impl/convex_client_native.dart';
import 'package:convex_flutter/src/rust/lib.dart';
import 'package:fake_async/fake_async.dart';
import 'package:flutter_test/flutter_test.dart';

import 'fake_rust_client.dart';

void main() {
  TestWidgetsFlutterBinding.ensureInitialized();

  late FakeRustClient rust;
  late List<String> logs;
  late NativeConvexClient client;

  Future<NativeConvexClient> wire() => NativeConvexClient.wire(
    rust,
    ConvexConfig(
      deploymentUrl: 'https://unit.convex.cloud',
      logger: (level, source, message) => logs.add('${level.name} $message'),
    ),
  );

  Future<void> subscribe(String name) => client.subscribe(
    name: name,
    args: {},
    onUpdate: (_) {},
    onError: (_, _) {},
  );

  /// Signed in with one live subscription: the state a resume finds.
  Future<void> signedInWithSubscription() async {
    rust.runsFirstPassItself = true;
    await client.setAuthWithRefresh(tokenFetcher: () async => 'jwt');
    await subscribe('s');
    rust.calls.clear();
  }

  int attempts() => rust.calls.where((c) => c == 'force_reconnect').length;

  setUp(() async {
    rust = FakeRustClient();
    logs = [];
    client = await wire();
  });

  tearDown(() => client.dispose());

  test(
    'calls issued while a reconnect mints its token wait for the replayed Authenticate',
    () async {
      await signedInWithSubscription();
      // The replay's first pass is the test's to run: the mint is in flight.
      rust.runsFirstPassItself = false;

      final reconnect = client.reconnect();
      await settle();
      final mutation = client.mutation(name: 'm', args: {});
      final query = client.query('q', {});
      final late = subscribe('t');
      await settle();
      expect(rust.calls, [
        'force_reconnect',
        'set_auth_with_refresh',
      ], reason: 'nothing goes out before the token');

      await rust.runFirstPass();
      expect(await reconnect, isTrue);
      await Future.wait([mutation, query, late]);
      expect(rust.calls.sublist(0, 4), [
        'force_reconnect',
        'set_auth_with_refresh',
        'set_auth:token',
        'subscribe:s',
      ]);
      expect(
        rust.calls.sublist(4),
        unorderedEquals(['mutation:m', 'query:q', 'subscribe:t']),
      );
    },
  );

  test(
    'a gate left open by a timed-out attempt is released by the attempt that gets the token',
    () {
      fakeAsync((async) {
        client.dispose();
        wire().then((c) => client = c);
        async.flushMicrotasks();
        rust.runsFirstPassItself = true;
        client.setAuthWithRefresh(tokenFetcher: () async => 'jwt');
        async.elapse(Duration.zero);
        subscribe('s');
        async.elapse(Duration.zero);
        rust.calls.clear();
        rust.runsFirstPassItself = false;

        final first = <bool>[];
        client.reconnect().then(first.add);
        async.elapse(Duration.zero);
        var sent = false;
        client.mutation(name: 'm', args: {}).then((_) => sent = true);

        // The first attempt times out on the mint and schedules a retry.
        async.elapse(const Duration(seconds: 20));
        expect(first, [false]);
        expect(sent, isFalse);
        expect(rust.calls, ['force_reconnect', 'set_auth_with_refresh']);

        async.elapse(const Duration(seconds: 3));
        expect(rust.calls, [
          'force_reconnect',
          'set_auth_with_refresh',
          'force_reconnect',
          'set_auth_with_refresh',
        ]);
        expect(sent, isFalse, reason: 'the retry holds the same gate');

        // The mint lands for the retry, whose fetcher the fake now holds.
        rust.runFirstPass();
        async.flushMicrotasks();
        expect(sent, isTrue);
        expect(rust.calls.sublist(4), [
          'set_auth:token',
          'subscribe:s',
          'mutation:m',
        ]);
      });
    },
  );

  test(
    'a subscribe parked across a reconnect is owned by the replay',
    () async {
      await signedInWithSubscription();
      final park = rust.parkSubscribe = Completer<void>();
      final parked = subscribe('p');
      await settle();
      expect(rust.calls, ['subscribe:p']);
      rust.parkSubscribe = null;

      expect(await client.reconnect(), isTrue);
      expect(rust.calls, [
        'subscribe:p',
        'force_reconnect',
        'set_auth_with_refresh',
        'set_auth:token',
        'subscribe:s',
        'subscribe:p',
      ]);

      // The replay's handle exists already; the parked call's comes into
      // being only now, as the dropped client's answer, and is let go.
      park.complete();
      await parked;
      final handlesForP = rust.handles.where((h) => h.name == 'p').toList();
      expect(handlesForP, hasLength(2));
      expect(
        handlesForP.first.cancelled,
        isFalse,
        reason: "the replay's stays",
      );
      expect(
        handlesForP.last.cancelled,
        isTrue,
        reason: 'the handle the dropped client gave back is let go',
      );
    },
  );

  test(
    'a failed attempt marks its subscriptions for restore and a connected socket re-fires them',
    () async {
      await signedInWithSubscription();
      rust.forceReconnectError = StateError('bridge down');

      expect(await client.reconnect(), isFalse);
      expect(rust.calls, ['force_reconnect']);
      expect(
        logs.where((line) => line.startsWith('error ')).single,
        startsWith('error Reconnect failed: Bad state: bridge down'),
      );

      rust.forceReconnectError = null;
      await rust.onStateChange!(WebSocketConnectionState.connected);
      await settle();
      await settle();
      await settle();
      expect(rust.calls, [
        'force_reconnect',
        'force_reconnect',
        'set_auth_with_refresh',
        'set_auth:token',
        'subscribe:s',
      ]);
    },
  );

  test(
    'retries back off from 3 s to 30 s and keep going until one succeeds',
    () {
      fakeAsync((async) {
        client.dispose();
        wire().then((c) => client = c);
        async.flushMicrotasks();
        rust.runsFirstPassItself = true;
        client.setAuthWithRefresh(tokenFetcher: () async => 'jwt');
        async.elapse(Duration.zero);
        subscribe('s');
        async.elapse(Duration.zero);
        rust.calls.clear();

        rust.forceReconnectError = StateError('bridge down');
        client.reconnect();
        async.elapse(Duration.zero);
        expect(attempts(), 1);
        for (final (delay, expected) in [
          (3, 2),
          (6, 3),
          (12, 4),
          (24, 5),
          (30, 6),
          (30, 7),
        ]) {
          async.elapse(Duration(seconds: delay - 1));
          expect(
            attempts(),
            expected - 1,
            reason: 'not yet, after ${delay - 1}s',
          );
          async.elapse(const Duration(seconds: 1));
          expect(attempts(), expected, reason: 'the retry after ${delay}s');
        }

        rust.forceReconnectError = null;
        async.elapse(const Duration(seconds: 30));
        expect(attempts(), 8);
        expect(rust.calls.sublist(rust.calls.length - 3), [
          'set_auth_with_refresh',
          'set_auth:token',
          'subscribe:s',
        ]);

        // A success puts the delay back at the floor.
        rust.forceReconnectError = StateError('bridge down');
        client.reconnect();
        async.elapse(Duration.zero);
        expect(attempts(), 9);
        async.elapse(const Duration(seconds: 3));
        expect(attempts(), 10);
      });
    },
  );
}
