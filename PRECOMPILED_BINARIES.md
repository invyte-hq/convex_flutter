# Precompiled Binaries

This plugin wraps a Rust crate (`rust/`) that is normally compiled **from source on
every `flutter build`** via [cargokit](cargokit/). On a cold build that Rust compile
dominates wall-clock time. To avoid it, this fork builds, signs and publishes the
native libraries once in CI, and consuming apps download them instead of compiling.

This is cargokit's built-in precompiled-binaries mechanism — no custom tooling.

## How it works

```
 ┌─ this repo (invyte-hq/convex_flutter) ──────────────────────────────┐
 │  rust/ crate ──hash──▶ "precompiled_<hash>"                          │
 │  CI builds every target, signs each .a/.so with PRIVATE_KEY,         │
 │  uploads them as assets to GitHub Release  precompiled_<hash>        │
 └─────────────────────────────────────────────────────────────────────┘
                                   │  download + verify with public_key
                                   ▼
 ┌─ Invyte app (consumer) ─────────────────────────────────────────────┐
 │  cargokit computes the SAME <hash> from the crate source it depends  │
 │  on, downloads precompiled_<hash> assets, skips cargo entirely.      │
 └─────────────────────────────────────────────────────────────────────┘
```

- **The crate hash is the version.** It is a content hash of `rust/src/**.rs`,
  `Cargo.toml`, `Cargo.lock`, `build.rs` and `rust/cargokit.yaml`. Any change to
  those files produces a new hash → a new release is needed. There is no manual
  version number to bump.
- **Signing** is ed25519 (integrity only — unrelated to iOS/Android app signing).
  The public key lives in [`rust/cargokit.yaml`](rust/cargokit.yaml); the private
  key lives only in the `PRIVATE_KEY` GitHub Actions secret of this repo.
- **Web is unaffected** — the web platform uses a pure-Dart implementation and no
  Rust, so no binaries are needed for it.

## Maintainer workflow (this repo)

One-time setup (already done):

1. `dart run build_tool gen-key` generated an ed25519 keypair.
2. Public key committed in `rust/cargokit.yaml`.
3. Private key stored as the `PRIVATE_KEY` repo secret.
4. [`.github/workflows/precompile.yml`](.github/workflows/precompile.yml) builds
   iOS (macOS runner) and Android (Ubuntu runner) targets.

Whenever you change anything under `rust/` (or `Cargo.lock`/`cargokit.yaml`):

1. Commit and push the change to `main`.
2. Run the **Precompile Binaries** workflow — either click *Run workflow*
   (workflow_dispatch) or push a tag matching `v*` / `precompiled-*`.
3. Wait for **both** the `iOS` and `android` jobs to finish. They create/extend a
   single release `precompiled_<hash>` with all assets.
4. (Optional) Verify everything published, from `cargokit/build_tool`:
   ```
   dart pub get
   dart run build_tool verify-binaries --manifest-dir=../../rust
   ```
   The iOS + Android triples should report `OK`. The desktop triples
   (windows/linux/macos-desktop) will report `MISSING` — that's expected, this
   fork only ships mobile binaries.

> The current crate hash is shown by `verify-binaries`. The matching release tag
> is `precompiled_<hash>`.

### Targets built

| Platform | Runner        | Rust triples |
|----------|---------------|--------------|
| iOS      | macos-latest  | `aarch64-apple-ios`, `aarch64-apple-ios-sim`, `x86_64-apple-ios` |
| Android  | ubuntu-latest | `aarch64-linux-android`, `armv7-linux-androideabi`, `x86_64-linux-android`, `i686-linux-android` |

Android libraries are built with NDK `26.3.11579264` and `--android-min-sdk-version=21`.
The min-SDK must be **≤** the consuming app's `minSdkVersion`. If the Invyte app
raises its minSdk and you want to match, bump `ANDROID_MIN_SDK` in the workflow.

## Consumer workflow (the Invyte Flutter app)

1. Depend on this fork by **git ref**, pinned to a commit whose precompile
   workflow has finished:

   ```yaml
   # invyte app pubspec.yaml
   dependencies:
     convex_flutter:
       git:
         url: https://github.com/invyte-hq/convex_flutter.git
         ref: <commit-sha-or-tag-that-was-precompiled>
   ```

2. Add a `cargokit_options.yaml` at the **app root** (next to the app's
   `pubspec.yaml`):

   ```yaml
   # Force use of precompiled binaries even when Rust is installed locally.
   # Without this, cargokit's default is to build from source whenever rustup
   # is present, which defeats the purpose.
   use_precompiled_binaries: true
   ```

3. `flutter pub get`, then build. The first build downloads and verifies the
   signed binaries; cargo never runs.

### Notes & gotchas

- **Pin to a finished build.** If you point the app at a commit before its CI run
  finishes (or whose run failed), the `precompiled_<hash>` release won't exist.
  cargokit then falls back to a source build if Rust is installed, or fails if it
  isn't. Always let CI go green first, then bump the ref.
- **Rust as a safety net.** With `use_precompiled_binaries: true`, a missing or
  unreachable release silently falls back to source if rustup is present. For a CI
  machine that you want to *guarantee* never compiles Rust, simply don't install
  rustup there — then a missing binary fails loudly instead of compiling slowly.
- **Repo must stay public.** cargokit downloads release assets unauthenticated.
  `invyte-hq/convex_flutter` is currently public; if it is ever made private the
  downloads will 404 and you'd need an alternative host or an auth shim.
