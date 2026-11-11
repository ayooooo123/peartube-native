# JS ↔ Rust parity

The PearTube app (`src/`, `worklet/`, `bin/`) is the reference for this repository. `JS_REF` holds the full hash
of the one app commit the Rust port is matched against. Nothing in this repository is a copy of app code:

- `fixtures/gen/*.mjs` import the app's own modules from the `PEARTUBE_APP` checkout (default: the sibling
  `../peartube`) and write `fixtures/*-reference.json`: what that app commit produces.
- `fixtures/gen/check.mjs` (`npm run check` in `fixtures/gen`) regenerates every reference and exits 1 when one
  differs from the committed file.
- The ignored interop tests (`#[ignore]`) run the app's own JS beside the Rust port. CI checks out the app at
  `JS_REF` and runs `cargo test --workspace -- --include-ignored --test-threads=1`, so either kind of
  disagreement fails the build.

Status meanings:

- `match`: the Rust module implements the JS module at `JS_REF`, and the fixtures and interop tests that compare
  them pass.
- `debt: <reason>`: ported, but a named behavior is missing or differs; the reason says which and where the
  note lives.
- `JS-only: <reason>`: there is deliberately no Rust code for it.
- `pending: align-<slice>`: the named alignment worktree owns the port of this JS change; the row moves to
  `match`, or to `debt:` with a reason, when that slice lands.

The last column is the app commit the Rust tree was last matched against, short form: `2b88b0281` is the tree
this port was written against before the alignment, and `73d2dfa47` is the relay change absorbed as Rust
`d9a520e`. Every `pending` row is expected to reach `3f3c9f840` (the commit in `JS_REF`) when its slice lands.
`—` means there is no Rust code to compare.

| JS module | Rust module | Status | JS commit last absorbed |
| --- | --- | --- | --- |
| `bin/private-relay.js` | `private_overlay`, `relay` (in-process guard) | `pending: align-relay` (standalone guard mode `PEARTUBE_PRIVATE_RELAY`) | `2b88b0281` |
| `bin/rebuild-view.js` | — | `JS-only:` one-off repair tool, run with the relay stopped; no Rust runtime path | — |
| `bin/relay.js` | `relay`, `src/bin/peartube-relay.rs` | `pending: align-relay` (standalone guard, `status.lan` shape) | `73d2dfa47` |
| `src/acquire.js` | `acquire` | `pending: align-acquire` | `2b88b0281` |
| `src/app-settings.js` | `app_settings` | `match` | `2b88b0281` |
| `src/apply.js` | `node/apply` | `pending: align-node` (the `makeApply` writer hook was removed in `fe2897172`) | `2b88b0281` |
| `src/art.js` | `art` | `debt:` artwork HTTP timeout covers the whole exchange, unlike the app's socket inactivity timeout — see README "Known gaps" | `2b88b0281` |
| `src/blob-server.js` | `node/blobs`, `http` (Range streaming) | `match` | `2b88b0281` |
| `src/cast-control.js` | `cast/rpc` | `pending: align-cast` | `2b88b0281` |
| `src/cast.js` | `cast/` (`controller`, `receiver`, `pairing`, `state`, `media`, `mdns`, `http`, `disk`, `traits`) | `pending: align-cast` | `2b88b0281` |
| `src/hiverelay.js` | `hiverelay` | `pending: align-relay` | `2b88b0281` |
| `src/http.js` | `http` | `pending: align-cast` (removed cast routes) | `73d2dfa47` |
| `src/identify.js` | `identify` | `pending: align-acquire` | `2b88b0281` |
| `src/index.js` | `node/core`, `node/factory` | `pending: align-node, align-cast` | `2b88b0281` |
| `src/lan.js` | `lan/` (`discovery`, `mdns`, `host`) | `debt:` the real-interface interop matrix is blocked on this host's network — see README "Known gaps" | `2b88b0281` |
| `src/mirror.js` | `mirror`, `replicate` | `pending: align-acquire` | `2b88b0281` |
| `src/mnemonic.js` | `mnemonic` | `match` | `2b88b0281` |
| `src/node/blobs.js` | `node/blobs` | `pending: align-acquire, align-node` | `2b88b0281` |
| `src/node/catalog.js` | `node/catalog` | `pending: align-node` | `2b88b0281` |
| `src/node/channel.js` | `node/channel` | `pending: align-node` | `2b88b0281` |
| `src/node/identity.js` | `identity` | `match` | `2b88b0281` |
| `src/node/network.js` | `node/network` | `pending: align-node` | `2b88b0281` |
| `src/node/sources.js` | `node/sources` | `pending: align-node` | `2b88b0281` |
| `src/node/storage.js` | `node/storage` | `pending: align-node` | `2b88b0281` |
| `src/private-overlay.js` | `private_overlay` | `pending: align-node` (RPC channel paired before it opens) | `2b88b0281` |
| `src/protocol.js` | `protocol` | `pending: align-node` (`parseReceiver` `p2p` default) | `2b88b0281` |
| `src/qr.js` | `cast/qr` | `pending: align-cast` (the cast fixture generator needs the `JS_REF` receiver) | `2b88b0281` |
| `src/rebuild-view.js` | — | `JS-only:` the library behind `bin/rebuild-view.js`; an operator repair tool, not a runtime path | — |
| `src/resolution.js` | `resolution` | `match` | `2b88b0281` |
| `src/settings.js` | `settings` | `match` | `73d2dfa47` |
| `src/traffic.js` | `traffic` | `pending: align-node` (bulk-busy only from source peers) | `2b88b0281` |
| `src/ui.js` | — | `JS-only:` shared asset; `relay.Dockerfile` copies the app's own file to `web/index.html` at image build and the relay serves it | `3f3c9f840` |
| `worklet/worker.js` | `app`, `lifecycle`, `ipc`, `backend` | `pending: align-cast` (cast method and event shapes) | `2b88b0281` |
| `<storage>/plugins/*.mjs` (user plugins) | — | `JS-only:` Rust cannot execute JS plugins; the built-in plugins are ports of the app's own (`http-acquire`, `channel-backfill`, `blob-rechunk`, `watch-dir`, `feed-mirror`, `p2p-replicate`) | — |

`src/cast-ui.js`, `bin/receiver.js` and the cast web routes exist before `fe2897172` but not at `JS_REF`; the
matching Rust files are deleted by `align-cast`. `src/ui.js` is the one file taken as-is from the app commit
rather than ported, so its row follows `JS_REF`.

## Generator dependencies on the app

The generators read only what the app exports at `JS_REF`. At `3f3c9f840` these names are module-private, so
`check.mjs` reports an `ERROR` for the matching references until the app exports them:

- `src/mnemonic.js`: `entropyToMnemonic` (`gen.mjs`, `apply.mjs`).
- `src/apply.js`: `makeApply(getChannelHex, onWriterChange)` — the optional callback was removed in
  `fe2897172`; `apply-reference.json` records the calls it makes.
- `src/art.js`: `metaFor`. `src/identify.js`: `fileName`, `probe`, `ubitInfo`, `norm`, `strong`.
- `src/hiverelay.js`: `SEED_HANDSHAKE`, `seedRequestEncoding`, `seedAcceptEncoding`, `unseedRequestEncoding`,
  `seedDenyEncoding`, `signSeedRequest`, `computeBoundBytes`, `blake2bTopic`, `encodeHyperdriveHeader`,
  `encodeHyperdriveSingleRootNode`.

One generator has no way to run against the current app at all: `tests/js/cast-fixtures.mjs` lifts
`receiverAddress` out of `src/cast.js`, and `fe2897172` deleted that function together with the browser
receiver and the HTTP control routes: a cast now runs over the peer connection both nodes already share
(`src/cast-control.js`), so the cast fixtures are rebuilt against that protocol by `align-cast`. Until then
`check.mjs` reports `ERROR fixtures/cast-reference.json`.

The check compares against whatever `PEARTUBE_APP` points at and says so when that checkout has uncommitted
changes; CI's checkout is the `JS_REF` commit itself, so a local run should use a clean checkout at `JS_REF`
(or a fresh `git archive` of it) to mean the same thing.

## How to bump JS_REF

`JS_REF` moves only together with the comparisons that prove the port still matches it. In one change:

1. Write the new app commit's full hash into `JS_REF` (one line, no other text).
2. With the app checkout at that commit, run `PEARTUBE_APP=<checkout> node fixtures/gen/check.mjs` (or
   `npm run check` in `fixtures/gen`). Each `DIFF` line names the reference, the first differing path and the
   top-level sections that moved. An `ERROR` line means a generator could not import what it needs from the
   app; add that export in the app or record the gap here.
3. Run the interop suite against the same checkout:
   `PEARTUBE_APP=<checkout> cargo test --workspace -- --include-ignored --test-threads=1`.
4. Port every difference, or record it as `debt: <reason>` in the table above (and the matching
   `docs/port-notes-*.md`). A reference is regenerated with `node check.mjs --write` only after the port caught
   up: never to silence a difference.
5. Commit `JS_REF`, the regenerated `fixtures/*-reference.json` and the Rust port together. CI checks out the
   app at `JS_REF` and repeats steps 2 and 3 on Linux and macOS, so a split change fails.
