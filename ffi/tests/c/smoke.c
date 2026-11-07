/*
 * C smoke test for the PearTube native C ABI.
 *
 * Compiled and linked against libpeartube_native_ffi.a by `tests/abi.rs`, then
 * run. A host that can do this can host the backend; it starts the real
 * backend, exchanges exactly one line and stops it.
 */
#include <peartube_native.h>
#include <stdio.h>
#include <string.h>
#include <time.h>

static int lines = 0;
static char last[512];
static size_t last_len = 0;

static void on_line(void *user, const uint8_t *line, size_t len) {
    (void)user;
    lines++;
    last_len = len < sizeof last - 1 ? len : sizeof last - 1;
    memcpy(last, line, last_len);
    last[last_len] = '\0';
}

/* Waits up to timeout_ms for the output thread to deliver `count` lines. */
static int wait_for_lines(int count, int timeout_ms) {
    struct timespec tick = {0, 1000000};
    for (int ms = 0; ms < timeout_ms; ms++) {
        if (lines >= count) return 0;
        nanosleep(&tick, NULL);
    }
    return -1;
}

int main(void) {
    if (pt_abi_version() != PT_ABI_VERSION) {
        fprintf(stderr, "abi version %u, header %u\n", pt_abi_version(), PT_ABI_VERSION);
        return 1;
    }

    /* The backend reads its configuration as JSON; `{}` is the default host. */
    static const char config[] = "{}";
    void *handle = NULL;
    pt_status status = pt_init((const uint8_t *)config, strlen(config), on_line, NULL, &handle);
    if (status != PT_OK || handle == NULL) {
        fprintf(stderr, "pt_init: %d\n", status);
        return 1;
    }

    /* A null handle is rejected, not dereferenced. */
    if (pt_send_line(NULL, (const uint8_t *)"{}", 2) != PT_ERR_UNKNOWN_HANDLE) {
        fprintf(stderr, "null handle accepted\n");
        return 1;
    }

    static const char request[] = "{\"id\":1,\"method\":\"init\"}";
    status = pt_send_line(handle, (const uint8_t *)request, strlen(request));
    if (status != PT_OK) {
        fprintf(stderr, "pt_send_line: %d\n", status);
        return 1;
    }

    pt_suspend(handle, 0);
    pt_resume(handle);

    if (wait_for_lines(1, 5000) != 0) {
        fprintf(stderr, "no reply line\n");
        return 1;
    }
    /* Only the envelope is the ABI's business; the payload belongs to the
     * backend protocol. A reply must be a JSON object that echoes the id. */
    if (last_len == 0 || last[0] != '{' || strstr(last, "\"id\"") == NULL) {
        fprintf(stderr, "unexpected reply: %s\n", last);
        return 1;
    }

    pt_shutdown(handle);
    int after = lines;
    struct timespec pause = {0, 50000000};
    nanosleep(&pause, NULL);
    if (lines != after) {
        fprintf(stderr, "callback after shutdown\n");
        return 1;
    }

    printf("smoke ok: %zu-byte reply, %d line(s)\n", last_len, lines);
    return 0;
}
