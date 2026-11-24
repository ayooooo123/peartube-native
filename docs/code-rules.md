# Code rules

This file gives the one way to do the work that this crate does again and again. Each section says what to do, why, and which fault the rule prevents. CI checks the rules that a machine can check. A reviewer judges the rest.

## How CI checks the rules

The workflow `.github/workflows/code-rules.yml` runs on every pull request.

- `ast-grep test` runs the rule tests in `ast-grep/tests/`. Each test has the faulty code as an invalid case.
- `scripts/ci/ast_grep_changed.py` runs `ast-grep scan` with the rules in `ast-grep/rules/`. It fails when a match touches a line that the pull request adds or changes. Matches on lines that the pull request does not touch do not fail. When you change such a line, fix the match in the same pull request.
- `scripts/ci/test_guard.py` fails when a pull request weakens a test (see [Test changes](#test-changes)).
- `ci.yml` runs Clippy with `-D warnings`, so every Clippy warning is an error.

Every ast-grep rule is an error. To run the rules on your machine, use `ast-grep test` and `ast-grep scan` in the repository root.

If a match is correct code, put a comment on the line before it: `// ast-grep-ignore: <rule-id> -- <reason>`. A reviewer judges the reason.

## Machine-checked rules

### duration-from-float

- **The one way:** convert a computed float to a `Duration` with `Duration::try_from_secs_f64` (or `try_from_secs_f32`), and handle the error on purpose. Better, where the code reads the setting, reject a value that is not finite, is negative, or makes a wait too long. Add a computed `Duration` to an `Instant` with `checked_add`: `Instant + Duration` panics on overflow, so `Duration::MAX` is not a safe fallback.
- **Why:** `Duration::from_secs_f64`, `from_secs_f32`, `mul_f64`, `mul_f32`, `div_f64` and `div_f32` panic on a negative value, on NaN and on a value that is too large. A rate of 0 gives an infinite value.
- **Fault:** a benchmark reader paced its reads with `Duration::from_secs_f64(count as f64 / rate)`. With a rate of 0, the reader panicked and exited with status 101. That work had no pull request. The same call is in `pace()` in `src/replicate.rs`: by the code, a very small positive `replicateMbps` (for example 1e-20) makes the wait too large, and the call panics.
- **Check:** ast-grep rule `duration-from-float`.

### text-byte-slice

- **The one way:** split text with `strip_prefix`, `strip_suffix`, `split_once`, `char_indices` or `get(range)`. These never cut a character: they return `None` or a position on a character boundary. Do not index text with a byte range (`&text[..2]`). Decode hex with one shared function that checks the input before it reads it.
- **Why:** a byte range that ends inside a multi-byte character panics. Text from links, peers, settings and HTTP requests can contain any character.
- **Fault:** [pull request 31](https://github.com/ayooooo123/peartube-native/pull/31). `js_number` in `src/protocol.rs` ran `match &t[..2]` after a byte-length check. A `peartube://` link with a non-ASCII character in a number field caused a panic in the task that reads the answer of a stranger's relay (14 of 85 test links).
- **Check:** ast-grep rule `text-byte-slice`. ast-grep has no types, so the rule decides that a value is text from its declaration (`&str`, `String`, `.trim()`, `.as_str()` and similar) or from its use (compared to a string literal, given to `from_str_radix`, `.parse()`). It can miss a slice of text that has none of these signs.

### node-change-outside-finish

- **The one way:** an HTTP request that closes or reopens the node (settings apply, restart, identity recovery) runs that change inside `http::finish_node_change(...)`. The function spawns the change and waits for it, so the change finishes when the client disconnects.
- **Why:** hyper drops the request future when the client disconnects. A node change that runs in that future stops between close and open.
- **Fault:** [pull request 22](https://github.com/ayooooo123/peartube-native/pull/22). On main, a client disconnect after `POST /v1/restart` or a settings change left the node closed, and the next restart failed with `rocksdb: IO error: lock hold by current process`. The first revision of the pull request protected restart and settings, but not `POST /v1/identity/recover`.
- **Check:** ast-grep rule `node-change-outside-finish`. It checks the calls of the `apply` and `restart` callbacks in `src/http.rs`, and `reopen()` and `apply_settings()` in relay functions that return `HttpError`.

### test-wait-needs-limit

- **The one way:** every wait in a test on a socket, a channel, a notification or another task runs inside `tokio::time::timeout(limit, ...)`, and the test fails with a message when the limit passes. Async test code does not block on a thread `join()`. A `timeout` around a blocking `join()` does not help, because the join blocks the poll before the timer can fire. Use `spawn_blocking` under a timeout, or a channel with `recv_timeout`.
- **Why:** a wait with no limit turns a stuck peer or server into a test run that never ends, and the run shows no reason.
- **Fault:** [pull request 22](https://github.com/ayooooo123/peartube-native/pull/22). An earlier version of the identity disconnect test read the HTTP answer with no time limit. The test hung for 58 minutes until a person stopped it. `reader.join()` in `src/relay/reopen_tests.rs` still blocks outside every limit.
- **Check:** ast-grep rule `test-wait-needs-limit`, in `tests/` and in `src/**/*_tests.rs`. It does not see waits inside macro arguments such as `assert!(rx.recv().await.is_some())`, and it does not check `#[cfg(test)]` modules inside other source files. It does not check a trait impl in a test, for example a test provider, because the code that calls the trait sets the limit. It cannot follow a variable, so write the async block inside the `timeout` call, not in a `let` before it.

## The one way for recurring work

### Open, close and shutdown

- **The one way:** `Relay` in `src/relay.rs` owns the node. Code opens, closes or reopens the node only while it holds the open lock (`Inner.open`). `open_locked`, `close_locked` and `reopen_locked` take that guard. `close_locked` is the only teardown: it closes the parts in a fixed order (mirror, cast stack, replication, acquirer, change feed, API handles, node) and logs a failed node close. `shutdown` runs once: the `shutting_down` flag makes a second call return at once and makes `open_locked` refuse to open. It closes the API servers, then the node, then the private guard.
- **Why:** a second open of the same store fails with a RocksDB lock error. A close that stops half way leaves the store locked.
- **Fault:** [pull request 22](https://github.com/ayooooo123/peartube-native/pull/22) (see `node-change-outside-finish`).
- **Check:** `node-change-outside-finish` checks the HTTP paths. A reviewer judges the teardown order, because a pattern cannot tell a correct order from a wrong one.

### Restart

- **The one way:** a restart is `reopen_locked`: close, then open, under one guard. When the open fails and the relay is not shutting down, the relay starts one `retry_open` (the `retry_pending` flag keeps it to one). A failed open after a settings change never ends the process: the API stays up so that the owner can repair the setting.
- **Why:** a saved setting that cannot open the node (for example a peer port that another program holds) is still in `settings.json` at the next start. If the process exits, every start fails in the same way.
- **Fault:** [pull request 22](https://github.com/ayooooo123/peartube-native/pull/22). The first revision ended the relay about 1 second after a bad `dhtPort`, and each new start failed again.
- **Check:** a reviewer judges this. `std::process::exit` is only in `src/bin/peartube-relay.rs` now; keep it there.

### Timeouts and waits

- **The one way:** every wait on a peer, a socket, a child process or another task has a limit (`tokio::time::timeout(limit, future)`), and the code says what happens when the limit passes. Async code sleeps with `tokio::time::sleep`, never `std::thread::sleep`. Convert a computed float to a `Duration` with `Duration::try_from_secs_f64`.
- **Why:** a wait with no limit hangs the caller forever when a peer does not answer.
- **Fault:** see `duration-from-float` and `test-wait-needs-limit`.
- **Check:** `duration-from-float`, and `test-wait-needs-limit` for tests. For waits in production code, a reviewer judges, because ast-grep cannot tell a wait on a peer from a wait on local work.

### Spawning and stopping tasks

- **The one way:** a task that lives as long as an open node belongs to the part that closes it. Keep its `JoinHandle` (for example `Open.changes` and the replication `kicker`) and abort it in `close_locked`. Use a detached `tokio::spawn` only for work that must finish when its caller goes away (`finish_node_change`, `retry_open`) and for one-shot jobs that end by themselves (`mirror.run(...)`).
- **Why:** a task that nobody stops keeps a closed store in use. A change that runs in a future that a caller can drop stops half way.
- **Fault:** [pull request 22](https://github.com/ayooooo123/peartube-native/pull/22) (the reopen ran in a future that a disconnect dropped).
- **Check:** a reviewer judges this, because ast-grep cannot see if a handle is stored and aborted later.

### Locks

- **The one way:** use `std::sync::Mutex` for short sections with no `.await` while the guard lives. Use `tokio::sync::Mutex` only where a guard must live across an `.await` (the open lock and the settings), and never hold it across a wait on a peer or the network.
- **Why:** a guard held across a wait on a peer blocks every other user of the lock for as long as the peer does not answer.
- **Fault:** [pull request 22](https://github.com/ayooooo123/peartube-native/pull/22). The identity lock is held across `self.channel.append(...).await`, which can wait for a peer. That is the probable cause of the 58-minute test hang; nobody proved it.
- **Check:** Clippy's `await_holding_lock` (a warning by default; CI makes it an error) checks `std` guards across `.await`. A reviewer judges `tokio` guards, because a pattern cannot tell a wait on a peer from a short wait.

### Byte budgets

- **The one way:** the budget of the cast window (`reserve_window` in `src/cast/media.rs`) plans in whole blocks, reserves exactly the bytes of the blocks that it plans, and never downloads more bytes than it holds. A test of a budget asserts the permits that each window holds, so that it fails when one window takes too much.
- **Why:** the budget bounds memory on small devices. A plan that rounds, estimates or reads the budget twice downloads more than it reserved.
- **Fault:** [pull request 10](https://github.com/ayooooo123/peartube-native/pull/10) (open). Four revisions failed their checks: a reservation of the unclipped byte count; 79 blocks for a budget of 75 after a backward seek; a plan from the mean block size that downloaded 20,447,232 bytes against a pool of 20,132,658 bytes; and a window without the reader's own block. A test summed the permits of all windows, so it could not fail when one window took too much. On main, the steering reserves 75 blocks and then 1 block, because it reserves the new window before it drops the old one.
- **Check:** a reviewer judges this. These faults are arithmetic, and a pattern cannot tell a correct plan from a wrong one.

### Errors

- **The one way:** a function returns `Result<T, String>` with a message that a person can act on, or a typed error where a caller decides by the kind of error. At the HTTP edge, return `HttpError::new(status, message)` with the error of the change that the request started; never answer 200 when the change failed. Work that has no caller to receive its error logs it with `log(event, json!({ "error": ... }))`.
- **Why:** an error that nobody receives turns a failed change into a silent success.
- **Fault:** [pull request 22](https://github.com/ayooooo123/peartube-native/pull/22). On main, `POST /v1/restart` answered 200 when the reopen failed.
- **Check:** a reviewer judges this, with the tests in `tests/relay.rs`.

### Sorting

- **The one way:** a sort comparison is a total order. Compare floats with `f64::total_cmp`. Do not sort with `partial_cmp(...).unwrap()`. The only `partial_cmp` now (`natural_order` in `src/providers/soulseek.rs`) compares parsed integers and is total.
- **Why:** `partial_cmp` returns `None` for NaN, so `unwrap()` panics, and since Rust 1.81 a sort can panic when the comparison is not a total order.
- **Fault:** none in this repository yet.
- **Check:** none yet. Add an ast-grep rule when a fault shows that one is needed.

### Waits in tests

- **The one way:** see `test-wait-needs-limit`. Wait for a condition with a polling helper that has a deadline (`wait_for` in `src/relay/reopen_tests.rs`), not with a fixed sleep. Run fake servers in a test inside `tokio::spawn`.
- **Why:** a fixed sleep is too short on a slow runner and too long on a fast one.
- **Fault:** [pull request 22](https://github.com/ayooooo123/peartube-native/pull/22).
- **Check:** `test-wait-needs-limit`.

### Test changes

- **The one way:** do not weaken a test to make it pass. Do not remove a test or an assertion, loosen an assertion, add `#[ignore]` or a skip, make a time limit in a test longer, or change how tests run (CI test commands, `[profile.*]`, `[dev-dependencies]`). If a test is wrong, change it and give the reason in the pull request text. The guard still fails, and a person decides at merge time.
- **Why:** a weaker test passes on broken code, and nobody sees the change unless a person reads every test diff.
- **Fault:** [pull request 11](https://github.com/ayooooo123/peartube-native/pull/11) removed a unit test of `src/node/storage.rs` that also checked that disk mode survives. [Pull request 31](https://github.com/ayooooo123/peartube-native/pull/31) added its new test with `#[ignore]`, so a plain `cargo test` skips it (CI runs it with `--include-ignored`).
- **Check:** `scripts/ci/test_guard.py` fails the pull request. It cannot tell a correct change from a weakening, so it fails on every removed or changed assertion. A change of spaces, line breaks or a last comma in an assertion does not count, a test or an assertion that moves to another file does not count as removed, and a new or shorter time limit does not fail. The guard does not see a time limit that a change removes from test code; `test-wait-needs-limit` flags a wait that is left without a limit.

### Test build and shipped build

- **The one way:** tests build the crate with the same dependency features as the shipped engine. A feature that changes behaviour (for example `serde_json`'s `float_roundtrip`) goes in `[dependencies]`, not only in `[dev-dependencies]`.
- **Why:** with different features, the tests check a different program from the one that ships.
- **Fault:** [pull request 31](https://github.com/ayooooo123/peartube-native/pull/31). `float_roundtrip` is only in `[dev-dependencies]`, so the tests parse numbers in a different way from the shipped engine.
- **Check:** `scripts/ci/test_guard.py` flags every change to `[dev-dependencies]`. Main still has this split; move the feature after the release.

### Module boundaries

- **The one way:** `relay`, `app` and `backend` put the parts together and may use any module. `traffic` and `control_channel` use no other module of the crate, and `ssrf`, `tags` and `mnemonic` use only `protocol`. Keep them so. Do not add a use that closes a cycle, for example from `node` to a module that uses `node`.
- **Why:** a cycle between modules makes a change in one module break the other, and an agent must read both to change either.
- **Fault:** none yet. Two cycles exist now. `node` uses types from `acquire`, `search`, `mirror`, `backend` and `cast`, and those modules use `node`. `protocol` uses `ipc::well_formed`, `ipc` uses `app_settings::js_truthy`, and `app_settings` uses `protocol`.
- **Check:** none. A rule would write the current cycles into CI. First move the shared types into `node`, and move `js_truthy` out of `app_settings` into a module that uses no other module of the crate. Then a rule can check the boundary.

### Engine safety and complexity rules

- `no-prod-unwrap` (`src/**/*.rs`): avoid `.unwrap()` and `panic!()` in production engine code; return `Err` to match JS error recovery.
- `no-std-sleep-in-async` (`src/**/*.rs`): use `tokio::time::sleep(...).await` inside `async fn` instead of blocking `std::thread::sleep`.
- `unsafe-outside-ffi` (`src/**/*.rs`): keep `unsafe` blocks and `unsafe fn` confined to the `ffi/` crate.
- `clippy.toml` & `[workspace.lints.clippy]`: set complexity thresholds (`cognitive-complexity-threshold = 20`, `too-many-lines-threshold = 120`, `too-many-arguments-threshold = 7`) and deny `todo`, `dbg_macro`, and `await_holding_lock`.
