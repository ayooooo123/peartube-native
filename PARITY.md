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
| `bin/private-relay.js` | `relay` (`PEARTUBE_PRIVATE_RELAY` standalone guard), `private_overlay::guard` | `match` on v2 branch: the standalone guard serves `peartube/private-guard/v2` and announces on the `peartube/private-guard/v2/<0..3>` topics | `15298dd16` (last pinned) |
| `bin/rebuild-view.js` | — | `JS-only:` one-off repair tool, run with the relay stopped; no Rust runtime path | — |
| `bin/relay.js` | `relay`, `src/bin/peartube-relay.rs` | `match` (cast routes, `hiverelay-seed`, standalone guard, `status.lan` as `{host, port}`, `globalSearch`, `PEARTUBE_WATCH_DIR` env-only, `0`/`false` env flags, `SIGTERM` exit 0) | `15298dd16` |
| `spec/build.mjs`, `spec/hyperschema/` | `build.rs`, `spec/hyperschema/schema.json`, `wire::peartube` | `match` on v2 branch: the port compiles a byte-equal copy of the app's `schema.json` with `hyperschema-rs` at build time; `fixtures/gen/check.mjs` fails when the copy differs, `tests/control_wire.rs` checks every struct's bytes both ways | `15298dd16` (last pinned) |
| `spec/protocols.js` | `control_protocol` (`SEARCH`, `CAST`, `GUARD`) | `match` on v2 branch: same channel names, op order, kinds, structs and `maxItems`, held equal by `fixtures/control-wire-reference.json` | `15298dd16` (last pinned) |
| `src/acquire.js` | `acquire` | `match` (`existing` peer-copy + SHA-256 verification, `keepCopy`, `Retry-After` 300 s cap, rotated-source response close, SSRF guard) | `15298dd16` |
| `src/app-settings.js` | `app_settings` | `match` | `8326afa2c` |
| `src/apply.js` | `node/apply` | `match` | `8326afa2c` |
| `src/art.js` | `art` | `debt:` artwork HTTP timeout covers the whole exchange, unlike the app's socket inactivity timeout — see README "Known gaps" (SSRF guard absorbed). App PR #574 (the YouTube source) added a `binary` option to `send` (the reply body as bytes); not ported | `15298dd16` |
| `src/blob-server.js` | `node/blobs`, `http` (Range streaming) | `debt:` `byte-stream-rs`'s `Resolved` carries only `session` (not an overridden `blob` span), which matches whole-core `sameBytes` failover (`blockOffset == 0` and equal `length`/`byteLength`) — see `src/node/blobs.js` | `15298dd16` |
| `src/cast-control.js` | `cast/control` | `match` on v2 branch: bounded Hyperschema state, command, pairing and host mappings | `2c97f5b7d` (last pinned) |
| `src/control-channel.js` | `control_channel`, `control_protocol` | `match` on v2 branch: one typed frame per message, requests both ways, handlers started in wire order (polled once inline), trailing bytes ignored, per-channel bad-frame budget (the eighth bad request is answered first), safe-integer and finite-float checks both ways, foreign error codes delivered as `HANDLER_ERROR`, three event-send outcomes, one send per frame | `15298dd16` (last pinned) |
| `src/guarded-frame.js` | — | `JS-only:` the port's registered decoders (`traffic`, `hiverelay`, `control_channel`) return errors and check size and safe range before they change state, so nothing needs wrapping | — |
| `src/cast.js` | `cast/` (`controller`, `receiver`, `pairing`, `state`, `media`, `mdns`, `http`, `disk`) | `match` on v2 branch: rotated LAN topic, TXT `v=3`, version refusal before credentials, a not-current receiver is not dialed also under a saved id that differs (matched by key), a subscription ends with its channel, an empty `magnetUrl` is `null`. The real LAN discovery test needs multicast; both DHT interop directions use an isolated testnet. | `15298dd16` (last pinned) |
| `src/hiverelay.js` | `hiverelay`, `hiverelay::seeder` | `match` (on stop the relay leaves its topics; it has no `leavePeer` for recruited relays, and the process closes right after; on the v2 branch the four seed messages are size- and safe-range-checked before dispatch, an unseed request at most 137 bytes) | `15298dd16` (last pinned) |
| `src/http.js` | `http` | `debt:` app PR #573 added `POST /v1/entries/remove` (album/announce delete with on-disk fate for mirrored files) and the `soulseek` provider routes (`/v1/providers/soulseek/search`, `browse`, `mirror`); not ported. App PR #574 (the YouTube source) added `POST /v1/search/sources` (one search over every source, rows marked with `source` and `lossy`, optional server-sent events with one `source` event per source then `done`), `GET /v1/providers/:name/thumb/:id` (owner only, 204 when none) and the `youtube` provider routes (`/v1/providers/youtube/search`, `mirror`); not ported. Otherwise `match` (`requireOwner` + DNS-rebinding `localHost` guard, read-only `GET /v1/resolve`, `/v1/entries/info` `mediaInfo` + owner `health`, SHA-256 web ETags, dead-SSE pruning, `GET`/`HEAD`-only CORS) | `15298dd16` |
| `src/identify.js` | `identify` | `debt:` App PR #574 (the YouTube source) added an `audio` option to `downloadPage` (`ubit get -a`, audio only) and calls the exported `ubitInfo` with `ytsearch:<query>` for search; not ported. Otherwise `match` | `15298dd16` |
| `src/index.js` | `node/core`, `node/factory`, `node/storage` (`StoreSuspension`), `suspend` | `match` (`entryHealth`, `keepCopy`, `copyExisting`, `search.close()` order; `suspend()` parks the swarm, then the store; `resume()` reopens the store, then the swarm, and a store that cannot reopen is reported as `fatal` and stays suspended until the next resume; `close()` resumes a suspended store first, and closes one that cannot reopen, so its waiters fail instead of waiting. A store access during suspension waits, as with `rocksdb-native`, but it blocks its thread, so the host's resume and shutdown reopen the store on the backend's suspension thread before the loop hears them; while the loop waits in the store, later host requests queue behind it until the resume) | `15298dd16` |
| `src/lan.js` | `lan/` (`discovery`, `mdns`, `host`) | `debt:` `tests/lan_mdns.rs::mdns_interop_with_bonjour_service` fails (bonjour-service never reports the Rust record; seen on Linux CI and a Mac, and at `d9a520e` before this alignment) while JS↔Rust LAN discovery in `tests/lan_interop.rs` passes on Linux; macOS runners have no LAN multicast, so CI skips the mDNS discovery tests there — see README "Known gaps" | `8326afa2c` |
| `src/mirror.js` | `mirror`, `replicate`, `media_check` | `debt:` app PR #573 made `watch-dir` scan one level of subfolders as albums (identity from tags, `<release>:sNNeMM` ids, `music` on each announce, `.peartube-album.json` pins, `mirror-watch.json` keyed by relative path, unchanged albums not re-read) and added the `soulseek` provider plugin; not ported. App PR #574 (the YouTube source) added `searchSources`, `searchable` on each `/v1/plugins` row, the `youtube` provider plugin, and an `artist` field in the `.peartube-album.json` pin (used as `music.artist` for files without readable tags) with `yt:<20hex>` accepted as a pinned release; not ported. Otherwise `match` (`feed-mirror` `existing` handoff, env-only `watch-dir`) | `15298dd16` |
| `src/mnemonic.js` | `mnemonic` | `match` | `8326afa2c` |
| `src/node/blobs.js` | `node/blobs`, `node/hash_state` | `debt:` `entryHealth` (`wholePeers`/`lanPeers`) approximates per-peer replicator counts from `hypercore-rs`'s public session API (`remote_contiguous_length >= length` for 0/1 `wholePeers` and node-level `is_lan()` connections for `lanPeers`); Merkle `sameBytes` failover and `kept-copies.json` match. `put` does not yet write the `music` field app PR #573 added (`cleanMusic`) | `15298dd16` |
| `src/node/catalog.js` | `node/catalog` | `debt:` entries do not yet carry `music` (`{ title, artist }`, clamped by `cleanMusic` on read), added in app PR #573. Otherwise `match` (`publicEntries`, `isKept` `local: true`, opened watch links capped at 32) | `15298dd16` |
| `src/node/channel.js` | `node/channel` | `match` (startup `saveIdentity` best-effort) | `15298dd16` |
| `src/node/identity.js` | `identity` | `match` (`dirty` startup guard, rollback + throw on explicit mutations) | `15298dd16` |
| `src/node/network.js` | `node/network` | `match` (`destroySwarm` `clear()` before force-destroy) | `15298dd16` |
| `src/node/sources.js` | `node/sources` | `match` | `8326afa2c` |
| `src/node/storage.js` | `node/storage` | `match` | `15298dd16` |
| `src/private-overlay.js` | `private_overlay` | `debt:` one deliberate difference, a fix of a JS defect that the app does not have yet: when both endpoints dial each other at once, `admitInnerStream` keeps a duplicate only if `!enc.isInitiator && remote < local`, so the higher key always keeps the lower key's dial while the lower key keeps whichever stream opened first; when that is the higher key's dial, each side closes the stream the other kept, both re-dial and the race can repeat. The Rust swarm keeps the lower key's dial on both keys in every arrival order. On the higher key the rule is the JS one, and on the lower key it ends with the stream a JS higher key keeps, so mixed Rust/JS pairs converge at least as often as JS pairs. Becomes `match` when the app takes the same rule. Otherwise `match` on v2 branch: `peartube/private-guard/v2` on the typed envelope with the `GUARD` op table (requests both ways: `incOpen` guard → endpoint, `rvFetch`/`hopOpen` guard → guard); role, routes, rendezvous slots and circuits belong to the channel session, the DHT rate budget to the connection; a client closes its dedicated connection to a guard whose channel is refused, refuses it, or closes later; route card, mutable signable and seals unchanged; canonical IPv6 `/48` buckets, 256/10 s DHT op rate limit | `15298dd16` (last pinned) |
| `src/private-overlay.js` (`attemptDirectHopBypass`) | — | `JS-only:` role-separation regression test helper; no port caller needs it. `tests/private_overlay.rs::interop::js_interop_over_peeroxide` runs the app's own helper against a route on the Rust guard | — |
| `src/protocol.js` | `protocol` | `debt:` `cleanMusic` (the announce's `music` object, clamped to `MAX_TITLE`) from app PR #573 is not ported. App PR #574 (the YouTube source) added `m4a` to `MEDIA_FILE` and uses the id prefix `yt:` (already valid under `ID`) for albums saved from YouTube; not ported. Otherwise `match` (`safe_equal`, `ipv6_groups`, `media_format`) | `15298dd16` |
| `src/providers/soulseek.js` | — | `debt:` Soulseek provider plugin from app PR #573 (search, browse, mirror into the import folder, jobs, opt-in sharing via `slskShare`/`slskShareMaxFiles`). The client is the Rust crate `peartube-soulseek`, so a port can link it directly; not ported | — |
| `src/providers/staging.js` | — | `debt:` what the source providers share about the import folder, from app PR #574: `ALBUM_PIN` (`{ release, title?, artist? }`), `RELEASE_ID` (now also `yt:<20hex>`), `MIRRORED_LIST`, folder and file naming; not ported | — |
| `src/providers/youtube.js` | — | `debt:` YouTube provider plugin from app PR #574 (search through `ubit`, audio-only saves into `<Artist> - <Album> (<Year>) [YouTube]/NN - Title.m4a` pinned to `yt:<20hex>`, jobs with `skipped`, `canSave`, relay-served result pictures; since app PR #576 a finished save waits for a running import with nothing placed); relay-only, not ported | — |
| `src/qr.js` | `cast/qr` | `match` | `8326afa2c` |
| `src/rebuild-view.js` | — | `JS-only:` the library behind `bin/rebuild-view.js`; an operator repair tool, not a runtime path | — |
| `src/resolution.js` | `resolution` | `debt:` app PR #573 added FLAC STREAMINFO and MPEG audio (`MP1`/`MP2`/`MP3`, Xing duration) readers to `mediaInfo`; not ported. Otherwise `match` (`media_info` container/duration/tracks + string/track caps) | `15298dd16` |
| `src/search.js` | `search` | `debt:` peeroxide's `join` has no per-lookup `limit`, so a Rust searcher dials every relay the search-topic lookup returns where hyperswarm stops at 4; it still asks at most 4 (2 per `/24`) and closes the search-only connections a minute after the last search — see README "Known gaps". Keyed per peer, `serve(null)` before `closed`, `FRESH` 10 s. On the v2 branch: `peartube/search/v2` and topic `peartube/search/v2/0`, `limit` 0 means 20, hit URIs at most 2,048 UTF-8 bytes | `15298dd16` |
| `src/settings.js` | `settings` | `debt:` the `slskUser`/`slskPass`/`slskServer`/`slskListenPort`/`slskShare`/`slskShareMaxFiles` settings from app PR #573 are not in the Rust SPEC. Otherwise `match` (`watchDir` migrated out of saved settings) | `15298dd16` |
| `src/ssrf.js` | `ssrf` | `match` (pre-connect IP/DNS + NAT64 `64:ff9b::/96` guard, no pooled-socket bypass, `PEARTUBE_ALLOW_PRIVATE_URLS`) | `15298dd16` |
| `src/tags.js` | — | `debt:` music tag reader (FLAC and Ogg Vorbis/Opus comments, ID3v2.3/2.4) and `albumKey`, added in app PR #573; not ported | — |
| `src/traffic.js` | `traffic` | `match` (on the v2 branch the busy flag is checked as kind 0, at most 9 bytes, safe uint, before the last claim changes) | `15298dd16` (last pinned) |
| `src/ui.js` | — | `JS-only:` shared asset; `relay.Dockerfile` copies the app's own file to `web/index.html` at image build and the relay serves it | `15298dd16` |
| `worklet/worker.js` | `app`, `lifecycle`, `ipc`, `backend` | `match` (`searchGlobal`) | `d6b9fc3ce` |
| `<storage>/plugins/*.mjs (user plugins)` | — | `JS-only:` Rust cannot execute JS plugins; the built-in plugins are ports of the app's own (`http-acquire`, `channel-backfill`, `blob-rechunk`, `watch-dir`, `feed-mirror`, `p2p-replicate`, `hiverelay-seed`, `media-check`) | — |

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
