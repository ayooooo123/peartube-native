/*
 * peartube-native C ABI.
 *
 * One backend instance per handle. Calls are thread-safe, except that
 * pt_shutdown must not run concurrently with another call on the same handle.
 * The line callback runs on the library's output thread, never on the caller's.
 *
 * Threading and memory:
 *   - pt_send_line may be called from any thread, including from inside the
 *     line callback, but a caller must not call pt_shutdown from inside it.
 *   - The bytes passed to the line callback are borrowed from the library and
 *     are valid only for the duration of that call. Copy what you keep.
 *   - Config and line buffers are read only during the call.
 *   - Every pointer parameter is borrowed; the library frees nothing.
 *   - Lines wait in an unbounded queue, so a host that stops draining the
 *     callback grows memory instead of losing a reply.
 */

#ifndef PEARTUBE_NATIVE_H
#define PEARTUBE_NATIVE_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/* Bumped on any change to a signature, a status code or a callback contract. */
#define PT_ABI_VERSION 1u

/* Largest config document accepted by pt_init. */
#define PT_MAX_CONFIG_BYTES (1u << 20)
/* Largest single line accepted by pt_send_line. */
#define PT_MAX_LINE_BYTES (16u << 20)

typedef int32_t pt_status;

#define PT_OK 0
/* A required pointer was NULL, or a length was non-zero with a NULL pointer. */
#define PT_ERR_NULL_ARG 1
/* Input was not UTF-8. */
#define PT_ERR_INVALID_UTF8 2
/* Input exceeded PT_MAX_CONFIG_BYTES / PT_MAX_LINE_BYTES. */
#define PT_ERR_TOO_LARGE 3
/* The handle is unknown, already shut down, or was never returned by pt_init. */
#define PT_ERR_UNKNOWN_HANDLE 4
/* The backend could not be created or the call failed. */
#define PT_ERR_BACKEND 5
/* A Rust panic was caught and contained; the handle remains usable. */
#define PT_ERR_PANIC 6

/*
 * Called once per complete line emitted by the backend, in order, without a
 * trailing newline. `line` is not NUL-terminated. See the lifetime rules above.
 */
typedef void (*pt_line_cb)(void *user_data, const uint8_t *line, size_t len);

/* ABI version of the loaded library. Compare with PT_ABI_VERSION at startup. */
uint32_t pt_abi_version(void);

/*
 * Starts the backend from a UTF-8 JSON config document and writes its opaque
 * handle to *out_handle. The line callback fires until pt_shutdown returns.
 * Returns PT_OK or an error; *out_handle is untouched on error.
 */
pt_status pt_init(const uint8_t *config_ptr, size_t config_len,
                  pt_line_cb on_line, void *user_data, void **out_handle);

/* Sends one UTF-8 JSON line (a trailing newline is optional and stripped). */
pt_status pt_send_line(void *handle, const uint8_t *ptr, size_t len);

/* Marks the backend idle, lingering on the network for linger_ms. */
void pt_suspend(void *handle, uint64_t linger_ms);

/* Ends the idle state entered by pt_suspend. */
void pt_resume(void *handle);

/*
 * Stops the backend, delivers every line already emitted, then returns. No
 * callback runs after it returns. Idempotent for a valid handle.
 */
void pt_shutdown(void *handle);

#ifdef __cplusplus
}
#endif

#endif /* PEARTUBE_NATIVE_H */
