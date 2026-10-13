import 'dart:async';

/// The one-shot requests the web client has sent and not yet answered, and
/// what a dropped socket does to each kind.
///
/// Convex answers a mutation once per `(sessionId, requestId)`, so a mutation
/// that was written to a socket which then closed is written again on the
/// next socket of the same session and the server answers it or deduplicates
/// it. An action is not idempotent and cannot be resent, so it fails at once
/// when the socket closes instead of waiting for the operation timeout. This
/// mirrors the JS client's `RequestManager.restart()`.
class WebPendingRequests {
  final Map<int, Completer<String>> _completers = {};
  final Set<int> _actions = {};

  /// Mutation messages written to a socket and unanswered, by requestId. A
  /// mutation still in the connecting queue is not here: the queue flush
  /// sends it.
  final Map<int, Map<String, dynamic>> _sentMutations = {};

  int get length => _completers.length;

  /// Registers a request and returns the completer its response resolves.
  Completer<String> open(int requestId, {required bool isAction}) {
    final completer = Completer<String>();
    _completers[requestId] = completer;
    if (isAction) _actions.add(requestId);
    return completer;
  }

  /// Records that [message] went over the wire, so a Mutation can be written
  /// again if the socket drops before its response.
  void markSent(Map<String, dynamic> message) {
    if (message['type'] != 'Mutation') return;
    final requestId = message['requestId'];
    if (requestId is int && _completers.containsKey(requestId)) {
      _sentMutations[requestId] = message;
    }
  }

  /// Removes everything held for [requestId] and returns its completer, null
  /// when the request was already answered or timed out.
  Completer<String>? take(int requestId) {
    _actions.remove(requestId);
    _sentMutations.remove(requestId);
    return _completers.remove(requestId);
  }

  /// The socket closed and the session will be resumed: every action fails
  /// with [error] now; mutations wait for [mutationsToResend]. Returns how
  /// many actions failed.
  int failActions(Object error) {
    final ids = List<int>.of(_actions);
    for (final id in ids) {
      take(id)?.completeError(error);
    }
    return ids.length;
  }

  /// The session is gone (a protocol failure, a manual reconnect, a
  /// deployment switch): nothing can be resent, so every request fails with
  /// [error]. Returns how many failed.
  int failAll(Object error) {
    final ids = List<int>.of(_completers.keys);
    for (final id in ids) {
      take(id)?.completeError(error);
    }
    return ids.length;
  }

  /// Whether a message still queued for the next socket may be written. A
  /// Mutation or Action whose caller already heard a failure or a timeout is
  /// not: writing it would run it after the app was told it did not run.
  /// Every other message type is the client's own and is flushed.
  bool shouldFlush(Map<String, dynamic> message) {
    final type = message['type'];
    if (type != 'Mutation' && type != 'Action') return true;
    return _completers.containsKey(message['requestId']);
  }

  /// The Mutation messages to write again on a new socket of the same
  /// session, in the order they were first sent.
  List<Map<String, dynamic>> mutationsToResend() =>
      List<Map<String, dynamic>>.of(_sentMutations.values);

  void clear() {
    _completers.clear();
    _actions.clear();
    _sentMutations.clear();
  }
}
