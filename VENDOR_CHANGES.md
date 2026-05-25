# Invyte patches on top of upstream convex_flutter

This fork sits on top of [jkuldev/convex_flutter](https://github.com/jkuldev/convex_flutter)
`main` (post-PR #17 — `convex` 0.10.3 + `Map<String, dynamic>` args). Each
section below covers a discrete area of divergence from upstream; refer to
`git log` for exact commit boundaries. Upstream issue tracking the Android
WSS failure: [#18](https://github.com/jkuldev/convex_flutter/issues/18).

The fork is intentionally shaped to Invyte's needs (single-app downstream,
arm64-only mobile target, integration with a `package:logging` consumer).
Upstreaming is not a goal.

## Cargokit (`cargokit/`)

### Skip debug x86 + x86_64 force-add on Android

**cargokit/gradle/plugin.gradle** — upstream mirrors `flutter.gradle` by
force-adding `android-x86` and `android-x64` to the debug target list, so
`cargo build` runs for three architectures even when the connected device is
arm64. Invyte ships arm64-v8a only (see `app/android/app/build.gradle.kts`
`ndk.abiFilters`), and dev/test happens on physical arm64 devices via the
WSL2→Windows ADB bridge. The extra cross-compiles added ~60–90s per debug
iteration. The block is replaced with a comment; to re-enable emulator builds,
restore the upstream lines or pass `--target-platform=android-x64`.

## Rust client (`rust/`)

### TLS: disable default `native-tls-vendored` feature

**Cargo.toml** — `convex` dependency changed to `default-features = false`,
explicit `rustls = "0.23"` with the `"ring"` provider added.

The `convex` crate's default features enable `native-tls-vendored`, which
compiles OpenSSL from source. When combined with our explicit
`rustls-tls-webpki-roots` feature, both TLS backends are linked into
`tokio-tungstenite`. At runtime, `tokio-tungstenite` prefers `native-tls`
when both are present — and vendored OpenSSL cannot resolve CA certificates
on Android, causing WSS handshakes to fail silently (the connection stays
in "connecting" forever).

Setting `default-features = false` plus the explicit rustls pin ensures
only rustls (pure Rust, bundled Mozilla CA certs) is compiled. No OpenSSL.

### Logging: tracing → logcat bridge

**Cargo.toml** — added `tracing = "0.1"` with `log` feature.

**lib.rs** — replaced `println!` (goes to `/dev/null` on Android) with
`log::debug!` / `log::error!` calls. The `tracing` crate's `log` feature
causes tracing events (used by the Convex SDK for connection errors, backoff,
and reconnect diagnostics) to automatically fall through to the `log` crate
when no tracing subscriber is installed. `android_logger` then forwards
everything to logcat.

### Verbose native-logging flag

**lib.rs** — `MobileConvexClient::new()` accepts a `verbose_logging: bool`
parameter that controls `android_logger`'s max level. Off → `Warn` (default
quiet). On → `Debug` (full Convex SDK chatter).

**lib/src/convex_config.dart** — exposed as `verboseNativeLogs` on
`ConvexConfig`. Separate from the Dart-side [`ConvexLogger`](lib/src/convex_logger.dart)
callback (different layers, different audiences).

### Bump Convex SDK

**Cargo.toml** — `convex` bumped from `0.7.0` to `0.10.4`.

- `0.10.3` (upstream PR #331): newer protocol support, `WebSocketState`
  callback API, reconnect-loop fix that prevented `Base version 0 passed up
  doesn't match the current version 1` after lifecycle interruptions.
- `0.10.4`: fix for memory leak in query subscriptions
  ([convex-rs#15](https://github.com/get-convex/convex-rs/issues/15)) —
  `BaseConvexClient` retained cached `FunctionResult` entries after the final
  unsubscribe. Long-lived sessions with many distinct subscriptions (Invyte's
  exact shape: events, chat, live) accumulated ~8 KB per query result. Also
  raises Rust MSRV to 1.85; pulls in `imbl` 7.0 (major bump) and new
  transitives `safe_arch` + `wide`.

## Dart layer (`lib/src/`)

### Structured logging via `ConvexLogger`

**lib/src/convex_logger.dart** (new file) — typedef + enum:

```dart
typedef ConvexLogger = void Function(ConvexLogLevel level, String source, String message);
enum ConvexLogLevel { debug, info, warn, error }
```

Every Dart-side log call routes through `config.logger(level, source, message)`.
Two ready-made implementations are exported:

- `defaultConvexLogger` — emits `warn`+`error` via `debugPrint`, drops debug/info.
- `silentConvexLogger` — drops everything.

Replaces the old mixed-mode logging (some `debugPrint`s gated behind
`config.debugLogging`, others always-on as "diagnostic logs"). The consumer
now decides what to surface by passing a custom callback.

### Cross-transport number normalisation

**lib/src/utils.dart** — added `normalizeJsonNumbers()` helper.

Dart's `jsonDecode` preserves `1` (int) vs `1.0` (double). The web client
delivers integers for whole numbers, but the Rust FFI client serialises
all Convex `Float64` values with a decimal point. Without normalisation,
`as int` casts in consumer DTOs throw on mobile but succeed on web.

Gated by `ConvexConfig.convertWholeNumberDoublesToInts` (default `true`).
Disable when you need to preserve the int/double distinction semantically.
No effect on the web transport.

### JWT-aware auth refresh, both transports symmetric

`setAuthWithRefresh` signature is now identical on both transports:

```dart
Future<AuthHandle> setAuthWithRefresh({
  required Future<String?> Function() tokenFetcher,
  void Function(bool isAuthenticated)? onAuthChange,
});
```

- `tokenFetcher` is called immediately for the initial token, then on a
  schedule driven by the JWT's `exp` claim (~60s before expiry), and again
  on server-side `AuthError`. The caller is expected to handle their own
  token caching (e.g. Clerk's `sessionToken()` returns a cached JWT) and
  to observe individual rotations inside `tokenFetcher` itself.
- `onAuthChange` fires **only on transitions** (unauthenticated ↔ authenticated)
  — same contract as the Rust SDK. It does not fire on every scheduled
  refresh while already authenticated.

The old `initialToken` parameter is removed — it papered over the web
client's lack of JWT awareness. The web client now decodes JWT expiry and
schedules refresh like the Rust SDK does on native (decoder lives in
`_decodeJwtTtl`), and tracks `wasAuthenticated` to deduplicate
`onAuthChange` firings.

### Web client (`lib/src/impl/convex_client_web.dart`) — protocol fixes

Patches against upstream's pure-Dart web implementation. Most are
straightforward protocol-correctness fixes:

**Protocol shape**:
- `_sendAuthMessage`: correct `Authenticate` format (`tokenType`/`value`/
  `baseVersion`). Sign-out sends `null` token → `tokenType:"None"`.
- `_sendAuthMessage`: separate `_identityVersion` counter, distinct from
  `_querySetVersion`. The Convex protocol tracks these independently.
- `_handle{Mutation,Action}Response`: check `success` flag before treating
  `null` result as an error (void-returning functions return `null`
  legitimately); emit `ClientError` (`convexError` / `serverError`)
  instead of generic `Exception`, matching native FFI error types.
- `_handleTransition`: deliver `null` `QueryUpdated` values to subscribers
  (e.g. a query returning `null` for an unauthenticated user); parse and
  forward subscription error fields (`errorMessage` / `error_message` /
  `errorData` / `error`) to `subscription.onError()`.

**Connection lifecycle**:
- `_sendMessage`: queue while WebSocket is `CONNECTING`; flush in `onopen`.
- `onopen`: sync `_querySetVersion` after flushing queued `ModifyQuerySet`;
  skip queued `Authenticate` messages (already sent in `onopen`);
  re-register active subscriptions after reconnect so the new server
  session knows about them (excluding queryIds already sent via the queue
  flush to avoid duplicate-Add `InternalServerError`).
- `_WebSubscription` stores `udfPath + args` for re-registration.
- `_handleFatalError`: generate a fresh `sessionId` and reset state before
  reconnect so the server creates a clean session instead of resuming the
  broken one (which would immediately re-trigger the FatalError).
- `_sendConnectMessage`: monotonic `_connectionCount` (not
  `_reconnectAttempts`, which always sent 1).

**Auth + refresh** (covered above): JWT-aware refresh loop, private
`_authRefreshRequested` stream for server-initiated AuthErrors (decoupled
from the public `_authStateController` so programmatic state changes
don't trigger spurious refreshes).

### Buffer Subscribe messages during initial auth setup (both transports)

`setAuthWithRefresh` opens a `Completer<void>? _authInFlight` on entry and
completes it once the first `setAuth(token)` has been pushed to the WS.
`subscribe` awaits this completer before issuing its `ModifyQuerySet`
(web) / `_rustClient.subscribe` (native) call.

The race this fixes: callers commonly start subscribing the moment Clerk
(or another external auth source) reports signed-in, which kicks off
`setAuthWithRefresh` in parallel. Without the gate, Subscribe messages
reach the server before the `Authenticate` does, and the server runs
auth-required queries unauthenticated — yielding `NOT_AUTHENTICATED`
errors that are only papered over by the resubscribe-after-auth path.

The gate has zero cost outside of initial auth (`_authInFlight` is `null`,
so `await null?.future` is a no-op). It does **not** fire on subsequent
refresh-loop rotations — the old token remains valid until the new one
lands, so subscribes never see an unauthenticated WS during a refresh.
Public-query subscribes during initial auth pay one extra token-roundtrip
of latency, which is invisible to users and only happens once per
sign-in.

`finally` guarantees the completer resolves even if `tokenFetcher` throws
or returns `null` (sign-out path), so subscribes never hang.
