# peartube-native-ffi

C ABI for [`peartube-native`](../). Kotlin (JNI) and Swift hosts link this
crate; the Mac desktop host links `peartube-native` directly and does not need
it. Everything the host can call is declared in
[`include/peartube_native.h`](include/peartube_native.h); this crate is the only
place in the workspace allowed to use `unsafe`.

## Build

```
cargo build -p peartube-native-ffi --release
```

Artifacts in `target/release/`:

| Host | Artifact | Link |
|---|---|---|
| iOS | `libpeartube_native_ffi.a` | static library, plus the system frameworks listed below |
| Android | `libpeartube_native_ffi.so` | `-lpeartube_native_ffi` |
| macOS (CLI/tests) | `libpeartube_native_ffi.dylib` or `.a` | as above |

`crate-type` also includes `rlib` so `tests/abi.rs` can link the crate as a Rust
dependency; C hosts ignore it.

Link a *static* archive with the C++ driver (`clang++`/`c++`), not a plain C
driver: the archive carries C++ dependencies and a C driver leaves the C++
runtime unresolved. It also needs the Rust runtime's system dependencies:

- macOS/iOS: `-framework Security -framework CoreFoundation`
- Linux/Android: `-lpthread -ldl -lm`

## Release artifacts

The app does not build this crate. It downloads the pinned release `ffi-<sha9>`
(the first nine hex digits of a `peartube-native` commit) and verifies every file
before use.

The sibling crates build at the commits `STACK.lock` names, not at their latest
`main`, so a release can be built again later; `ffi-manifest.json` repeats them as
`stack`. After a sibling change a release must include, push it and run
`sh ffi/stack-lock.sh`, then commit `STACK.lock`.

| Asset | Built from |
|---|---|
| `peartube_native_ffi-android-arm64.so` | `aarch64-linux-android`, NDK r29 (`29.0.14206865`), API 29, release profile, stripped, `LOAD` segments aligned to 16 KiB (`-Wl,-z,max-page-size=16384`) |
| `peartube_native_ffi-darwin-arm64.dylib` | `aarch64-apple-darwin`, release profile, install name `@rpath/libpeartube_native_ffi.dylib` |
| `peartube_native.h` | `ffi/include/peartube_native.h` at the same commit |
| `ffi-manifest.json` | `commit`, `abiVersion` (the header's `PT_ABI_VERSION`), `jsRef` (`JS_REF`), `stack`: each sibling crate → the commit both builds used, `assets`: file name → sha256 |
| `SHA256SUMS` | `sha256sum` format over the two libraries and the header |

The Android library links the C++ runtime dynamically. It needs the NDK r29
`libc++_shared.so` beside it in `jniLibs/arm64-v8a`, the same copy the app's
worklet addons load; every other dependency (`libstdc++.so`, the platform's
empty stub, `libdl`, `libm`, `libc`) is part of Android 10 (API 29) and later.
The macOS library needs only the system frameworks listed above. Both carry no
build-machine path strings (`--remap-path-prefix`).

A host links the files under their own names, not the asset names: the macOS
copy goes into the bundle as `libpeartube_native_ffi.dylib`, which is what the
install name expects, and the Android copy keeps `libpeartube_native_ffi.so`.

### Cutting a release

```
gh workflow run release-ffi.yml -f ref=<commit|branch>
```

`.github/workflows/release-ffi.yml` builds `android-arm64` on `ubuntu-latest`
and `darwin-arm64` on `macos-latest` with `ffi/build.sh`, then creates or
updates the prerelease `ffi-<sha9>` for the built commit and uploads the five
assets above. A dispatch run tags the resolved commit; pushing a matching
`ffi-<sha9>` tag runs the same workflow.

The same build locally (needs an NDK at `$ANDROID_NDK_HOME` or the newest under
`$ANDROID_HOME/ndk`):

```
sh ffi/build.sh android-arm64 /tmp/ffi-out
sh ffi/build.sh darwin-arm64 /tmp/ffi-out
```

Each run checks what it publishes before printing the asset: the ELF `LOAD`
alignment, the six `pt_*` exports, the dylib install name, and that no
build-machine home directory survived in the strings.

The app pins a release in its `native.lock` (release tag, commit, `jsRef`,
`abiVersion`, each asset's sha256) with its `sync-native.py`.

## ABI

```c
uint32_t pt_abi_version(void);
pt_status pt_init(const uint8_t *config, size_t config_len,
                  pt_line_cb on_line, void *user_data, void **out_handle);
pt_status pt_send_line(void *handle, const uint8_t *line, size_t len);
void      pt_suspend(void *handle, uint64_t linger_ms);
void      pt_resume(void *handle);
void      pt_shutdown(void *handle);
```

`config` is a UTF-8 JSON document, parsed at the boundary and handed to the
backend as a value; `{}` is the default. Lines are UTF-8 JSON, one per call, with an optional trailing newline that
`pt_send_line` strips. `pt_abi_version()` must equal `PT_ABI_VERSION` before
anything else is called.

| status | meaning |
|---|---|
| `PT_OK` (0) | accepted |
| `PT_ERR_NULL_ARG` (1) | required pointer null, or non-zero length with a null pointer |
| `PT_ERR_INVALID_UTF8` (2) | input was not UTF-8 |
| `PT_ERR_TOO_LARGE` (3) | config > 1 MiB or line > 16 MiB |
| `PT_ERR_UNKNOWN_HANDLE` (4) | unknown, null or already shut down handle |
| `PT_ERR_BACKEND` (5) | the backend could not start or rejected the line |
| `PT_ERR_PANIC` (6) | a Rust panic was caught; the handle is still usable |

A panic never crosses the boundary: every entry point runs inside
`catch_unwind`, and a poisoned lock is recovered rather than turned into a
permanently broken handle. `pt_suspend`, `pt_resume` and `pt_shutdown` return
`void`, so a contained panic there is swallowed.

### Threading and memory

- Any thread may call `pt_send_line`, `pt_suspend` and `pt_resume`.
- `pt_shutdown` must not run concurrently with another call on the same handle,
  and must not be called from inside the line callback. It is idempotent once
  the handle is gone (a second call is a no-op).
- `on_line` runs on one dedicated output thread, in order. The `line` bytes are
  borrowed from a buffer owned by the library and are valid **only during the
  call**: copy them before returning. The bytes never include the transport
  newline.
- Handles are opaque integer tokens, never Rust pointers. Using a stale or
  fabricated handle is a failed lookup, not memory corruption.
- After `pt_shutdown` returns, no callback runs and the callback pointer and
  `user_data` are no longer touched.

## Kotlin (JNI)

The Kotlin side is a thin `external` object; nothing in it is Rust-specific:

```kotlin
package app.peartube

object PearTubeNative {
    init { System.loadLibrary("peartube_native_ffi") }

    external fun ptAbiVersion(): Int                                   // ()I
    external fun ptInit(config: ByteArray, on: java.util.function.Consumer<ByteArray>): Long  // ([BLjava/util/function/Consumer;)J
    external fun ptSendLine(handle: Long, line: ByteArray): Int         // (J[B)I
    external fun ptSuspend(handle: Long, lingerMs: Long)                // (JJ)V
    external fun ptResume(handle: Long)                                 // (J)V
    external fun ptShutdown(handle: Long)                               // (J)V
}
```

`0L` from `ptInit` means failure; check `ptAbiVersion() == 1` once at startup.
The JNI glue lives next to the Android app and must:

1. Export exactly `Java_app_peartube_PearTubeNative_ptAbiVersion(JNIEnv*, jobject)`,
   `..._ptInit(JNIEnv *env, jobject self, jbyteArray config, jobject on)`,
   `..._ptSendLine(JNIEnv *env, jobject self, jlong handle, jbyteArray line)`,
   `..._ptSuspend(JNIEnv *env, jobject self, jlong handle, jlong linger_ms)`,
   `..._ptResume(JNIEnv*, jobject, jlong)` and
   `..._ptShutdown(JNIEnv*, jobject, jlong)`.
2. Copy the `jbyteArray` with `GetByteArrayRegion` into a scratch buffer, call
   `pt_init`/`pt_send_line` while the buffer is alive, and return the status.
3. Take `NewGlobalRef(on)` as `user_data` (never the local ref) and pass a
   trampoline with the ABI signature
   `void (*)(void *, const uint8_t *, size_t)`.
4. Inside the trampoline call `AttachCurrentThreadAsDaemon` on first use (the
   output thread is not a JVM thread), then `NewByteArray` +
   `SetByteArrayRegion` + `CallVoidMethod(on, "accept", "(Ljava/lang/Object;)V")` +
   `DeleteLocalRef`. Do not hold the `JNIEnv*` between calls.
5. `DeleteGlobalRef(on)` only after `pt_shutdown` returns.

Pass the handle to Kotlin as `(jlong)(intptr_t)handle` and back as
`(void *)(intptr_t)jlong`.

## Swift

With `peartube_native.h` in the bridging header, the C surface imports as:

```swift
// pt_abi_version() -> UInt32
// typealias pt_line_cb = @convention(c) (UnsafeMutableRawPointer?, UnsafePointer<UInt8>?, Int) -> Void
// pt_init(UnsafePointer<UInt8>?, Int, pt_line_cb!, UnsafeMutableRawPointer?, UnsafeMutablePointer<UnsafeMutableRawPointer?>?) -> pt_status
// pt_send_line(UnsafeMutableRawPointer?, UnsafePointer<UInt8>?, Int) -> pt_status
// pt_suspend(UnsafeMutableRawPointer?, UInt64)
// pt_resume(UnsafeMutableRawPointer?)
// pt_shutdown(UnsafeMutableRawPointer?)
// PT_OK, PT_ERR_* import as Int32
```

The wrapper keeps the closure in a retained box passed as `user_data`:

```swift
final class PearTubeBackend {
    typealias LineCallback = @convention(c) (UnsafeMutableRawPointer?, UnsafePointer<UInt8>?, Int) -> Void

    private var handle: UnsafeMutableRawPointer?
    private let box: CallbackBox          // class holding `var onLine: (Data) -> Void`
    private let trampoline: LineCallback = { user, line, len in
        guard let user, let line else { return }
        let box = Unmanaged<CallbackBox>.fromOpaque(user).takeUnretainedValue()
        box.onLine(Data(bytes: line, count: len))   // copied inside the call
    }

    func start(config: Data = Data("{}".utf8), onLine: @escaping (Data) -> Void) throws
    func send(_ line: String) throws        // PT_OK, else a status error
    func suspend(lingerMs: UInt64)
    func resume()
    func shutdown()                         // idempotent; drops the box afterwards
}
```

`start` calls `config.withUnsafeBytes { pt_init($0.bindMemory(to: UInt8.self).baseAddress, config.count, trampoline, Unmanaged.passUnretained(box).toOpaque(), &handle) }`
and must throw unless the status is `PT_OK` and `handle` is non-nil. `send`
passes `line.count` bytes of a UTF-8 buffer inside `withUnsafeBytes`. Keep the
callback box and the `Data` copy independent of the pointer: it dies with the
call.

## Tests

```
cargo test -p peartube-native-ffi
```

- `tests/abi.rs` declares the functions in an `extern "C"` block and drives them
  like a host: version, init, send, reply delivery, suspend/resume, shutdown
  with no callbacks afterwards, contained backend panic, rejected invalid UTF-8,
  oversize and null input.
- `tests/c/smoke.c` is compiled with `$CC` (default `cc`), linked against
  `libpeartube_native_ffi.a` with `$CXX` (default `c++`) and executed: it starts
  the real backend, exchanges one line, checks the reply is a JSON object that
  echoes the request id, and proves no callback fires after shutdown. `cargo
  test` only builds the rlib, so the test runs `cargo build -p
  peartube-native-ffi --lib` first to produce the archive.
- The panic path uses a private backend double selected by
  `{"__test_panic":true}` in the config. That key, the double and its module are
  not part of the C ABI and do not appear in the header.
