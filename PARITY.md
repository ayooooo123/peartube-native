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

The last column is the app commit the Rust module was last matched against (short form of `JS_REF`). `—` means
there is no Rust code to compare.

| JS module | Rust module | Status | JS commit last absorbed |
| --- | --- | --- | --- |
| `bin/private-relay.js` | `relay` (`PEARTUBE_PRIVATE_RELAY` standalone guard), `private_overlay::guard` | `match` | `8286e22f6` |
| `bin/rebuild-view.js` | — | `JS-only:` one-off repair tool, run with the relay stopped; no Rust runtime path | — |
| `bin/relay.js` | `relay`, `src/bin/peartube-relay.rs` | `match` (cast routes, `hiverelay-seed`, standalone guard, `status.lan` as `{host, port}`) | `8286e22f6` |
| `src/acquire.js` | `acquire` | `match` | `8286e22f6` |
| `src/app-settings.js` | `app_settings` | `match` | `8286e22f6` |
| `src/apply.js` | `node/apply` | `match` | `8286e22f6` |
| `src/art.js` | `art` | `debt:` artwork HTTP timeout covers the whole exchange, unlike the app's socket inactivity timeout — see README "Known gaps" | `8286e22f6` |
| `src/blob-server.js` | `node/blobs`, `http` (Range streaming) | `match` | `8286e22f6` |
| `src/cast-control.js` | `cast/control` | `match` | `8286e22f6` |
| `src/cast.js` | `cast/` (`controller`, `receiver`, `pairing`, `state`, `media`, `mdns`, `http`, `disk`) | `match` (the live LAN cast test needs working multicast; see `src/lan.js`) | `8286e22f6` |
| `src/hiverelay.js` | `hiverelay`, `hiverelay::seeder` | `match` (on stop the relay leaves its topics; it has no `leavePeer` for recruited relays, and the process closes right after) | `8286e22f6` |
| `src/http.js` | `http` | `match` | `8286e22f6` |
| `src/identify.js` | `identify` | `match` | `8286e22f6` |
| `src/index.js` | `node/core`, `node/factory` | `match` | `8286e22f6` |
| `src/lan.js` | `lan/` (`discovery`, `mdns`, `host`) | `debt:` `tests/lan_mdns.rs::mdns_interop_with_bonjour_service` fails (bonjour-service never reports the Rust record; seen on Linux CI and a Mac, and at `d9a520e` before this alignment) while JS↔Rust LAN discovery in `tests/lan_interop.rs` passes on Linux; macOS runners have no LAN multicast, so CI skips the mDNS discovery tests there — see README "Known gaps" | `8286e22f6` |
| `src/mirror.js` | `mirror`, `replicate` | `match` | `8286e22f6` |
| `src/mnemonic.js` | `mnemonic` | `match` | `8286e22f6` |
| `src/node/blobs.js` | `node/blobs`, `node/hash_state` | `match` | `8286e22f6` |
| `src/node/catalog.js` | `node/catalog` | `match` | `8286e22f6` |
| `src/node/channel.js` | `node/channel` | `match` | `8286e22f6` |
| `src/node/identity.js` | `identity` | `match` | `8286e22f6` |
| `src/node/network.js` | `node/network` | `match` | `8286e22f6` |
| `src/node/sources.js` | `node/sources` | `debt:` with no `findingPeers` guard held, hypercore-rs `update()` still waits for a first peer where Hypercore returns at once; every app path holds the guard, so the node behaves the same | `8286e22f6` |
| `src/node/storage.js` | `node/storage` | `match` | `8286e22f6` |
| `src/private-overlay.js` | `private_overlay` | `match` | `8286e22f6` |
| `src/protocol.js` | `protocol` | `match` | `8286e22f6` |
| `src/qr.js` | `cast/qr` | `match` | `8286e22f6` |
| `src/rebuild-view.js` | — | `JS-only:` the library behind `bin/rebuild-view.js`; an operator repair tool, not a runtime path | — |
| `src/resolution.js` | `resolution` | `match` | `8286e22f6` |
| `src/settings.js` | `settings` | `match` | `8286e22f6` |
| `src/traffic.js` | `traffic` | `match` | `8286e22f6` |
| `src/ui.js` | — | `JS-only:` shared asset; `relay.Dockerfile` copies the app's own file to `web/index.html` at image build and the relay serves it | `8286e22f6` |
| `worklet/worker.js` | `app`, `lifecycle`, `ipc`, `backend` | `match` | `8286e22f6` |
| `<storage>/plugins/*.mjs (user plugins)` | — | `JS-only:` Rust cannot execute JS plugins; the built-in plugins are ports of the app's own (`http-acquire`, `channel-backfill`, `blob-rechunk`, `watch-dir`, `feed-mirror`, `p2p-replicate`, `hiverelay-seed`) | — |

`src/cast-ui.js`, `bin/receiver.js` and the browser-receiver cast routes were removed from the app in `fe2897172`;
their Rust counterparts are deleted too.

## Generator dependencies on the app

The generators read only what the app exports at `JS_REF`: `entropyToMnemonic` (`src/mnemonic.js`), `metaFor`
(`src/art.js`), `fileName`, `probe`, `ubitInfo`, `norm`, `strong` (`src/identify.js`) and the HiveRelay wire
helpers (`src/hiverelay.js`) are exported by the app for exactly this. A generator must never copy app code;
if it needs something the app keeps private, export it from the app in the same change.

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
