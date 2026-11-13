//! Drives the exported C functions the way a host does: through `extern "C"`,
//! with no Rust types, handles or errors crossing the boundary.
//!
//! The status values are the ones in `include/peartube_native.h`, restated here
//! so a drift between header and library fails the test.

use std::ffi::c_void;
use std::process::Command;
use std::sync::mpsc::{Sender, channel};
use std::time::Duration;

// Keeps the crate's archive on the link line: this test only names its C
// symbols, so without a Rust reference the linker never sees the object.
use peartube_native_ffi as _;

const PT_OK: i32 = 0;
const PT_ERR_NULL_ARG: i32 = 1;
const PT_ERR_INVALID_UTF8: i32 = 2;
const PT_ERR_TOO_LARGE: i32 = 3;
const PT_ERR_UNKNOWN_HANDLE: i32 = 4;
const PT_ERR_PANIC: i32 = 6;

const ABI_VERSION: u32 = 1;

/// The reply must echo the request id; the rest of the payload belongs to the
/// backend's protocol, which this ABI deliberately does not pin down.
fn reply_id(line: &[u8]) -> u64 {
    let reply: serde_json::Value = serde_json::from_slice(line).expect("reply is JSON");
    reply["id"].as_u64().expect("reply echoes the request id")
}
const MAX_LINE_BYTES: usize = 16 << 20;

type LineCallback = unsafe extern "C" fn(*mut c_void, *const u8, usize);

unsafe extern "C" {
    fn pt_abi_version() -> u32;
    fn pt_init(
        config: *const u8,
        config_len: usize,
        on_line: Option<LineCallback>,
        user_data: *mut c_void,
        out_handle: *mut *mut c_void,
    ) -> i32;
    fn pt_send_line(handle: *mut c_void, ptr: *const u8, len: usize) -> i32;
    fn pt_suspend(handle: *mut c_void, linger_ms: u64);
    fn pt_resume(handle: *mut c_void);
    fn pt_shutdown(handle: *mut c_void);
}

/// Copies the borrowed line out during the call; a host must do the same.
unsafe extern "C" fn collect(user: *mut c_void, line: *const u8, len: usize) {
    let sender = unsafe { &*(user as *const Sender<Vec<u8>>) };
    // SAFETY: the library guarantees `line` is readable for `len` bytes for the
    // duration of this call.
    let bytes = unsafe { std::slice::from_raw_parts(line, len) }.to_vec();
    let _ = sender.send(bytes);
}

fn init(config: &str, sender: &Sender<Vec<u8>>) -> (i32, *mut c_void) {
    let mut handle = std::ptr::null_mut();
    let user = sender as *const Sender<Vec<u8>> as *mut c_void;
    // SAFETY: `config` and `user` outlive the call, `handle` is writable, and
    // the callback stays valid until `pt_shutdown` returns.
    let status = unsafe {
        pt_init(
            config.as_ptr(),
            config.len(),
            Some(collect),
            user,
            &mut handle,
        )
    };
    (status, handle)
}

fn send(handle: *mut c_void, line: &str) -> i32 {
    // SAFETY: `handle` comes from `pt_init` and `line` outlives the call.
    unsafe { pt_send_line(handle, line.as_ptr(), line.len()) }
}

#[test]
fn abi_version_matches_header() {
    // SAFETY: no arguments.
    assert_eq!(unsafe { pt_abi_version() }, ABI_VERSION);
}

#[test]
fn line_roundtrip_and_lifecycle() {
    let (sender, lines) = channel();
    let (status, handle) = init("{}", &sender);
    assert_eq!(status, PT_OK);
    assert!(!handle.is_null());

    let request = "{\"id\":7,\"method\":\"init\"}";
    assert_eq!(send(handle, request), PT_OK);
    let reply = lines
        .recv_timeout(Duration::from_secs(5))
        .expect("reply line");
    assert_eq!(reply_id(&reply), 7);
    assert!(!reply.ends_with(b"\n"), "payload, not transport framing");

    // SAFETY: valid handle; suspend/resume keep it usable.
    unsafe {
        pt_suspend(handle, 250);
        pt_resume(handle);
    }
    assert_eq!(send(handle, "{\"id\":8,\"method\":\"init\"}"), PT_OK);
    let reply = lines
        .recv_timeout(Duration::from_secs(5))
        .expect("second reply");
    assert_eq!(reply_id(&reply), 8);

    // SAFETY: valid handle, shut down once.
    unsafe { pt_shutdown(handle) };
    assert!(
        lines.recv_timeout(Duration::from_millis(150)).is_err(),
        "callback after shutdown"
    );
    assert_eq!(send(handle, request), PT_ERR_UNKNOWN_HANDLE);
    // SAFETY: a second shutdown of the same token is a no-op, not a crash.
    unsafe { pt_shutdown(handle) };
    assert!(lines.recv_timeout(Duration::from_millis(50)).is_err());
}

#[test]
fn backend_panic_is_contained() {
    let (sender, lines) = channel();
    let (status, handle) = init("{\"__test_panic\":true}", &sender);
    assert_eq!(status, PT_OK);

    assert_eq!(send(handle, "{}"), PT_ERR_PANIC);
    // The handle survived the contained panic and still answers.
    assert_eq!(send(handle, "{}"), PT_ERR_PANIC);
    assert!(
        lines.try_recv().is_err(),
        "no line from a panicking backend"
    );

    // SAFETY: valid handle.
    unsafe { pt_shutdown(handle) };
}

#[test]
fn invalid_input_is_rejected() {
    let (sender, lines) = channel();
    let (status, handle) = init("{}", &sender);
    assert_eq!(status, PT_OK);

    let bad = [0xff_u8, 0xfe];
    // SAFETY: `bad` outlives the call.
    assert_eq!(
        unsafe { pt_send_line(handle, bad.as_ptr(), bad.len()) },
        PT_ERR_INVALID_UTF8
    );

    // SAFETY: the length is checked before the pointer is touched.
    assert_eq!(
        unsafe { pt_send_line(handle, bad.as_ptr(), MAX_LINE_BYTES + 1) },
        PT_ERR_TOO_LARGE
    );
    // SAFETY: null pointer with a non-zero length.
    assert_eq!(
        unsafe { pt_send_line(handle, std::ptr::null(), 4) },
        PT_ERR_NULL_ARG
    );
    // SAFETY: null handle.
    assert_eq!(
        unsafe { pt_send_line(std::ptr::null_mut(), bad.as_ptr(), 1) },
        PT_ERR_UNKNOWN_HANDLE
    );

    let mut unused = std::ptr::null_mut();
    // SAFETY: `unused` is writable; the config bytes outlive the call.
    let status = unsafe {
        pt_init(
            bad.as_ptr(),
            bad.len(),
            Some(collect),
            std::ptr::null_mut(),
            &mut unused,
        )
    };
    assert_eq!(status, PT_ERR_INVALID_UTF8);
    assert!(unused.is_null(), "no handle on a failed init");
    // SAFETY: null callback is the only required-argument failure here.
    let status = unsafe { pt_init("{}".as_ptr(), 2, None, std::ptr::null_mut(), &mut unused) };
    assert_eq!(status, PT_ERR_NULL_ARG);

    assert!(lines.try_recv().is_err());
    // SAFETY: valid handle.
    unsafe { pt_shutdown(handle) };
}

/// Compiles `tests/c/smoke.c` with `cc`, links the static library and runs it:
/// the check that a plain C host can use this crate at all.
#[test]
fn c_host_links_staticlib() {
    let manifest = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
    // The profile directory is the test binary's ancestor right under the
    // target root (`CARGO_TARGET_TMPDIR` is `<target>/tmp`). Newer Cargo puts
    // test binaries under `<profile>/build/<pkg>/<hash>/`, not `<profile>/deps/`.
    let target_dir = std::path::Path::new(env!("CARGO_TARGET_TMPDIR"))
        .parent()
        .expect("target root");
    let exe = std::env::current_exe().expect("test binary path");
    let profile = exe
        .ancestors()
        .find(|dir| dir.parent() == Some(target_dir))
        .expect("profile dir under the target root")
        .to_path_buf();
    let static_lib = profile.join("libpeartube_native_ffi.a");
    // `cargo test` builds the rlib, not the C artifacts, so ask for the archive
    // (a no-op when it is fresh). The target dir is pinned to the profile's
    // parent: an inherited relative CARGO_TARGET_DIR would resolve against this
    // crate's directory and drop the archive somewhere else.
    let target_root = profile.parent().expect("target root");
    let cargo = std::env::var("CARGO").unwrap_or_else(|_| "cargo".into());
    let built = Command::new(cargo)
        .args(["build", "-p", "peartube-native-ffi", "--lib"])
        .current_dir(manifest)
        .env("CARGO_TARGET_DIR", target_root)
        .output()
        .expect("run cargo build");
    assert!(
        built.status.success(),
        "{}",
        String::from_utf8_lossy(&built.stderr)
    );
    assert!(static_lib.exists(), "missing {}", static_lib.display());

    // A host normally drives this by hand; the test does the same, honouring
    // CC/CXX. The value may carry a wrapper ("sccache cc"), so split it the way
    // a shell would, and treat an exported-but-empty value as unset.
    let tool = |name: &str, default: &str| {
        let value = std::env::var(name)
            .ok()
            .filter(|value| !value.is_empty())
            .unwrap_or_else(|| default.into());
        let mut words = value.split_whitespace();
        let program = words.next().unwrap_or(default).to_string();
        (program, words.map(str::to_owned).collect::<Vec<_>>())
    };
    let (cc, cc_args) = tool("CC", "cc");
    let (cxx, cxx_args) = tool("CXX", "c++");

    let object = profile.join("peartube_ffi_smoke.o");
    let compiled = Command::new(&cc)
        .args(&cc_args)
        .args(["tests/c/smoke.c", "-Iinclude", "-c", "-o"])
        .arg(&object)
        .current_dir(manifest)
        .output()
        .expect("run the C compiler");
    assert!(
        compiled.status.success(),
        "cc failed:\n{}",
        String::from_utf8_lossy(&compiled.stderr)
    );

    let smoke = profile.join("peartube_ffi_smoke");
    // The archive carries C++ dependencies, so link with the C++ driver; it
    // also pulls in the C++ runtime that a plain C driver would leave out.
    let mut link = Command::new(&cxx);
    link.args(&cxx_args)
        .arg(&object)
        .arg(&static_lib)
        .arg("-o")
        .arg(&smoke);
    if cfg!(target_os = "macos") {
        // Static Rust archives still call into the system security RNG.
        link.args(["-framework", "Security", "-framework", "CoreFoundation"]);
    } else if cfg!(target_os = "linux") {
        link.args(["-lpthread", "-ldl", "-lm"]);
    }
    let linked = link.current_dir(manifest).output().expect("run c++");
    assert!(
        linked.status.success(),
        "link failed:\n{}",
        String::from_utf8_lossy(&linked.stderr)
    );

    let ran = Command::new(&smoke).output().expect("run smoke test");
    assert!(
        ran.status.success(),
        "smoke failed:\n{}{}",
        String::from_utf8_lossy(&ran.stdout),
        String::from_utf8_lossy(&ran.stderr)
    );
}
