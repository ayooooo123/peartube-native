//! C ABI for the PearTube native backend.
//!
//! Hosts link `libpeartube_native_ffi.a` (or the `.so`/`.dylib`) and drive one
//! backend instance per handle. The byte-level line protocol is owned by
//! [`peartube_native::backend`]; this crate only moves UTF-8 lines in and out.
//!
//! This crate is the only one in the workspace allowed to use `unsafe`, and
//! every block says why it is sound. The exported surface is
//! `include/peartube_native.h`; three properties hold for all of it:
//!
//! - No Rust type, trait or panic crosses the boundary. Every entry point is
//!   `extern "C"` and runs inside [`catch_unwind`], returning `PT_ERR_PANIC`.
//! - Handles are opaque integer tokens, never dereferenced Rust pointers, so a
//!   stale or hostile handle cannot become a use-after-free.
//! - Backend lines are delivered by one dedicated output thread; the host's
//!   callback receives borrowed bytes that are valid only during the call.

use std::collections::{HashMap, VecDeque};
use std::ffi::c_void;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Condvar, LazyLock, Mutex, MutexGuard};
use std::thread::{self, JoinHandle};

mod engine;
mod test_support;

/// Bumped whenever a signature, a status code or a callback contract changes;
/// mirrored by `PT_ABI_VERSION` in `include/peartube_native.h`.
pub const ABI_VERSION: u32 = 1;

pub const PT_OK: i32 = 0;
/// A required pointer was null, or a length was non-zero with a null pointer.
pub const PT_ERR_NULL_ARG: i32 = 1;
pub const PT_ERR_INVALID_UTF8: i32 = 2;
pub const PT_ERR_TOO_LARGE: i32 = 3;
pub const PT_ERR_UNKNOWN_HANDLE: i32 = 4;
pub const PT_ERR_BACKEND: i32 = 5;
/// A Rust panic was caught and contained; the handle remains usable.
pub const PT_ERR_PANIC: i32 = 6;

/// Mirrors `PT_MAX_CONFIG_BYTES`.
const MAX_CONFIG_BYTES: usize = 1 << 20;
/// Mirrors `PT_MAX_LINE_BYTES`.
const MAX_LINE_BYTES: usize = 16 << 20;

/// The host callback; called on the output thread with borrowed bytes.
type LineCallback = unsafe extern "C" fn(*mut c_void, *const u8, usize);

/// Runs an entry point body, containing panics instead of unwinding into C.
fn guard(body: impl FnOnce() -> i32) -> i32 {
    catch_unwind(AssertUnwindSafe(body)).unwrap_or(PT_ERR_PANIC)
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    // A panic while holding the lock happens inside `guard`; keep going instead
    // of turning one contained panic into a permanently unusable handle.
    mutex
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

/// Borrows a caller-owned byte range, enforcing the size and null rules first.
///
/// # Safety
///
/// When `len > 0`, `ptr` must point to `len` readable bytes that stay valid for
/// the duration of the call. Both callers document that this is the C contract.
unsafe fn input<'a>(ptr: *const u8, len: usize, max: usize) -> Result<&'a [u8], i32> {
    if len > max {
        return Err(PT_ERR_TOO_LARGE);
    }
    if len == 0 {
        return Ok(&[]);
    }
    if ptr.is_null() {
        return Err(PT_ERR_NULL_ARG);
    }
    // SAFETY: `len > 0`, `ptr` is non-null and, per the caller's contract,
    // readable for `len` bytes for the whole call.
    Ok(unsafe { std::slice::from_raw_parts(ptr, len) })
}

/// Lines queued by the backend, plus the closed flag that ends the thread.
#[derive(Default)]
struct Queue {
    lines: VecDeque<String>,
    closed: bool,
}

/// The dedicated output thread and everything it needs. Lines are queued
/// without bound: a slow host must never drop a reply or block a backend.
struct Bridge {
    queue: Mutex<Queue>,
    wake: Condvar,
    on_line: LineCallback,
    user_data: usize,
}

impl Bridge {
    fn push(&self, line: String) {
        let mut queue = lock(&self.queue);
        if queue.closed {
            return;
        }
        queue.lines.push_back(line);
        drop(queue);
        self.wake.notify_one();
    }

    /// Stops the thread after the lines already queued have been delivered.
    fn close(&self) {
        lock(&self.queue).closed = true;
        self.wake.notify_all();
    }

    fn run(&self) {
        loop {
            let line = {
                let mut queue = lock(&self.queue);
                loop {
                    if let Some(line) = queue.lines.pop_front() {
                        break line;
                    }
                    if queue.closed {
                        return;
                    }
                    queue = self
                        .wake
                        .wait(queue)
                        .unwrap_or_else(|poisoned| poisoned.into_inner());
                }
            };
            // Framing is the transport's; the host sees payload only.
            let line = line.trim_end_matches(['\n', '\r']);
            // SAFETY: the host owns the callback and its `user_data` and keeps
            // them valid until `pt_shutdown` returns. `line` is alive across the
            // call and is the borrowed memory the header promises.
            unsafe { (self.on_line)(self.user_data as *mut c_void, line.as_ptr(), line.len()) };
        }
    }
}

struct Handle {
    engine: Box<dyn engine::Engine>,
    bridge: Arc<Bridge>,
    output: Mutex<Option<JoinHandle<()>>>,
}

static HANDLES: LazyLock<Mutex<HashMap<u64, Arc<Handle>>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

/// Ids start at 1 and never repeat, so `0` means "no handle" in C.
static NEXT_ID: AtomicU64 = AtomicU64::new(1);

/// Handles cross the boundary as integer tokens: nothing points into Rust, so a
/// stale handle is a failed lookup rather than undefined behaviour.
fn to_token(id: u64) -> *mut c_void {
    id as usize as *mut c_void
}

fn from_token(handle: *mut c_void) -> u64 {
    handle as usize as u64
}

/// A token lookup; any pointer value is a valid (possibly missing) key, so this
/// is safe to call with whatever a host passes in.
fn resolve(handle: *mut c_void) -> Option<Arc<Handle>> {
    lock(&HANDLES).get(&from_token(handle)).cloned()
}

#[unsafe(no_mangle)]
pub extern "C" fn pt_abi_version() -> u32 {
    ABI_VERSION
}

/// # Safety
///
/// `config_ptr` must be null or readable for `config_len` bytes, `out_handle`
/// must be null or writable for one pointer, and `on_line`/`user_data` must stay
/// valid until [`pt_shutdown`] returns.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn pt_init(
    config_ptr: *const u8,
    config_len: usize,
    on_line: Option<LineCallback>,
    user_data: *mut c_void,
    out_handle: *mut *mut c_void,
) -> i32 {
    guard(|| {
        let Some(on_line) = on_line else {
            return PT_ERR_NULL_ARG;
        };
        if out_handle.is_null() {
            return PT_ERR_NULL_ARG;
        }
        // SAFETY: the header makes the caller guarantee the range is readable
        // for the duration of this call.
        let bytes = match unsafe { input(config_ptr, config_len, MAX_CONFIG_BYTES) } {
            Ok(bytes) => bytes,
            Err(status) => return status,
        };
        let Ok(config) = std::str::from_utf8(bytes) else {
            return PT_ERR_INVALID_UTF8;
        };
        let Ok(config) = serde_json::from_str(config) else {
            return PT_ERR_BACKEND;
        };

        let bridge = Arc::new(Bridge {
            queue: Mutex::new(Queue::default()),
            wake: Condvar::new(),
            on_line,
            user_data: user_data as usize,
        });
        let sink_bridge = Arc::clone(&bridge);
        let sink = Box::new(move |line: String| sink_bridge.push(line));
        let engine = match engine::spawn(config, sink) {
            Ok(engine) => engine,
            Err(_) => return PT_ERR_BACKEND,
        };

        let thread_bridge = Arc::clone(&bridge);
        let output = match thread::Builder::new()
            .name("peartube-ffi-output".into())
            .spawn(move || thread_bridge.run())
        {
            Ok(output) => output,
            Err(_) => {
                let _ = catch_unwind(AssertUnwindSafe(|| engine.shutdown()));
                return PT_ERR_BACKEND;
            }
        };

        let id = NEXT_ID.fetch_add(1, Ordering::Relaxed);
        lock(&HANDLES).insert(
            id,
            Arc::new(Handle {
                engine,
                bridge,
                output: Mutex::new(Some(output)),
            }),
        );
        // SAFETY: `out_handle` was checked non-null; the header makes the caller
        // guarantee it is writable for one pointer.
        unsafe { out_handle.write(to_token(id)) };
        PT_OK
    })
}

/// # Safety
///
/// `handle` must come from [`pt_init`]; `ptr` must be null or readable for
/// `len` bytes for the duration of the call.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn pt_send_line(handle: *mut c_void, ptr: *const u8, len: usize) -> i32 {
    guard(|| {
        let Some(handle) = resolve(handle) else {
            return PT_ERR_UNKNOWN_HANDLE;
        };
        // SAFETY: the header makes the caller guarantee the range is readable
        // for the duration of this call.
        let bytes = match unsafe { input(ptr, len, MAX_LINE_BYTES) } {
            Ok(bytes) => bytes,
            Err(status) => return status,
        };
        let Ok(line) = std::str::from_utf8(bytes) else {
            return PT_ERR_INVALID_UTF8;
        };
        // The line terminator is transport framing, not payload.
        let line = line.trim_end_matches(['\n', '\r']);
        match handle.engine.send(line) {
            Ok(()) => PT_OK,
            Err(_) => PT_ERR_BACKEND,
        }
    })
}

/// # Safety
///
/// `handle` must come from [`pt_init`] and must not be shut down concurrently.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn pt_suspend(handle: *mut c_void, linger_ms: u64) {
    let _ = catch_unwind(AssertUnwindSafe(|| {
        if let Some(handle) = resolve(handle) {
            handle.engine.suspend(linger_ms);
        }
    }));
}

/// # Safety
///
/// `handle` must come from [`pt_init`] and must not be shut down concurrently.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn pt_resume(handle: *mut c_void) {
    let _ = catch_unwind(AssertUnwindSafe(|| {
        if let Some(handle) = resolve(handle) {
            handle.engine.resume();
        }
    }));
}

/// # Safety
///
/// `handle` must come from [`pt_init`] and no other call may run on it
/// concurrently. The callback and `user_data` are not touched afterwards.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn pt_shutdown(handle: *mut c_void) {
    let _ = catch_unwind(AssertUnwindSafe(|| {
        let id = from_token(handle);
        let handle = lock(&HANDLES).remove(&id);
        let Some(handle) = handle else {
            // Already shut down, or never a handle: nothing to do.
            return;
        };
        let _ = catch_unwind(AssertUnwindSafe(|| handle.engine.shutdown()));
        // Drain what the backend already emitted, then stop the thread. After
        // `join` returns no callback can run, which is the header's promise.
        handle.bridge.close();
        let output = lock(&handle.output).take();
        if let Some(output) = output {
            // `shortcut: shutdown from inside the line callback skips the join
            // instead of deadlocking; the header forbids that call.`
            if output.thread().id() != thread::current().id() {
                let _ = output.join();
            }
        }
    }));
}
