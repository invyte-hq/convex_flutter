import 'dart:convert';

import 'package:convex_flutter/src/rust/lib.dart' show ClientError;

/// The [ClientError] for a failed MutationResponse or ActionResponse.
///
/// The protocol carries the message as `result` (a string) and a thrown
/// ConvexError's payload as the sibling `errorData`, present only when the
/// function threw one. The payload is handed on JSON-encoded, the shape the
/// native transport produces, so the consumer decodes both the same way.
///
/// Only the payload is the same on every deployment. A development deployment
/// sends the thrown error's own message and stack trace as `result`; a
/// production deployment redacts it to the function name and "Server Error".
/// Nothing a consumer wants can be read from `result`, only from `errorData`.
ClientError failedResponseError(Map<String, dynamic> message, String fallback) {
  final errorData = message['errorData'];
  if (errorData != null) {
    return ClientError.convexError(data: jsonEncode(errorData));
  }
  return ClientError.serverError(
    msg: message['result']?.toString() ?? fallback,
  );
}
