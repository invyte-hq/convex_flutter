// A dropped socket on the web used to leave every in-flight mutation and
// action waiting for the 30 s operation timeout. These tests pin what the
// client owes each kind: a sent mutation is written again on the resumed
// session, an action fails at once, and a lost session fails everything.
import 'package:convex_flutter/src/impl/web_pending_requests.dart';
import 'package:flutter_test/flutter_test.dart';

void main() {
  Map<String, dynamic> mutation(int id) => {
    'type': 'Mutation',
    'requestId': id,
    'udfPath': 'events:create',
    'args': [<String, dynamic>{}],
  };

  test('a mutation written to the socket is resent, one still queued is not',
      () {
    final pending = WebPendingRequests();
    pending.open(1, isAction: false);
    pending.open(2, isAction: false);
    pending.markSent(mutation(1));

    expect(pending.mutationsToResend(), [mutation(1)]);
    expect(pending.length, 2);
  });

  test('an answered mutation is not resent', () {
    final pending = WebPendingRequests();
    final completer = pending.open(1, isAction: false);
    pending.markSent(mutation(1));

    expect(pending.take(1), same(completer));
    expect(pending.mutationsToResend(), isEmpty);
    expect(pending.take(1), isNull);
  });

  test('a close fails actions at once and keeps mutations', () async {
    final pending = WebPendingRequests();
    final action = pending.open(1, isAction: true);
    final kept = pending.open(2, isAction: false);
    pending.markSent(mutation(2));
    final lost = StateError('connection lost');

    expect(pending.failActions(lost), 1);

    await expectLater(action.future, throwsA(same(lost)));
    expect(kept.isCompleted, isFalse);
    expect(pending.mutationsToResend(), [mutation(2)]);
    expect(pending.length, 1);
  });

  test('a lost session fails everything', () async {
    final pending = WebPendingRequests();
    final action = pending.open(1, isAction: true);
    final mutationCompleter = pending.open(2, isAction: false);
    pending.markSent(mutation(2));
    final reset = StateError('session reset');

    expect(pending.failAll(reset), 2);

    await expectLater(action.future, throwsA(same(reset)));
    await expectLater(mutationCompleter.future, throwsA(same(reset)));
    expect(pending.mutationsToResend(), isEmpty);
    expect(pending.length, 0);
  });

  test('a message that is not a Mutation is never recorded for resend', () {
    final pending = WebPendingRequests();
    pending.open(1, isAction: true);
    pending.markSent({'type': 'Action', 'requestId': 1});
    pending.markSent({'type': 'ModifyQuerySet', 'requestId': 1});

    expect(pending.mutationsToResend(), isEmpty);
  });
}
