// A failed MutationResponse or ActionResponse carries its message as `result`
// and a thrown ConvexError's payload as the sibling `errorData`. These tests
// pin that the web transport turns the payload into `ClientError.convexError`,
// as the native transport does, and the bare message into `serverError`.
import 'dart:convert';

import 'package:convex_flutter/src/impl/web_failed_response.dart';
import 'package:convex_flutter/src/rust/lib.dart';
import 'package:flutter_test/flutter_test.dart';

void main() {
  const payload = {
    'code': 'SELF_ACTION',
    'message': 'You cannot send a request to yourself',
  };
  const thrown =
      'Uncaught ConvexError: {"code":"SELF_ACTION","message":"You cannot send a request to yourself"}\n'
      '    at handler (../convex/connections/requests.ts:40:11)\n';

  test('a failed mutation with errorData becomes convexError', () {
    final error = failedResponseError(<String, dynamic>{
      'type': 'MutationResponse',
      'requestId': 3,
      'success': false,
      'result': thrown,
      'logLines': <String>[],
      'errorData': payload,
    }, 'Mutation failed');

    expect(error, ClientError.convexError(data: jsonEncode(payload)));
  });

  test('a failed action with errorData becomes convexError', () {
    final error = failedResponseError(<String, dynamic>{
      'type': 'ActionResponse',
      'requestId': 3,
      'success': false,
      'result': thrown,
      'logLines': <String>[],
      'errorData': payload,
    }, 'Action failed');

    expect(error, isA<ClientError_ConvexError>());
    final data = jsonDecode((error as ClientError_ConvexError).data) as Map;
    expect(data['code'], 'SELF_ACTION');
  });

  test('a failed mutation without errorData becomes serverError', () {
    final error = failedResponseError(<String, dynamic>{
      'type': 'MutationResponse',
      'requestId': 3,
      'success': false,
      'result': 'Server Error',
      'logLines': <String>[],
    }, 'Mutation failed');

    expect(error, const ClientError.serverError(msg: 'Server Error'));
  });
}
