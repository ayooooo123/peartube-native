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
| `bin/private-relay.js` | `relay` (`PEARTUBE_PRIVATE_RELAY` standalone guard), `private_overlay::guard` | `match` | `15298dd16` |
| `bin/rebuild-view.js` | — | `JS-only:` one-off repair tool, run with the relay stopped; no Rust runtime path | — |
| `bin/relay.js` | `relay`, `src/bin/peartube-relay.rs` | `debt:` Rust retries a failed reopen once and answers 503 on `/v1/entries` when the relay has no node. JS waits for another request and lists no entries. Both keep the settings API available: a failed apply returns 200 with a warning and a failed POST restart returns 400 with the error. | `2a3f14eeb` |
| `src/acquire.js` | `acquire` | `match` (`existing` peer-copy + SHA-256 verification, `keepCopy`, `Retry-After` 300 s cap, rotated-source response close, SSRF guard) | `15298dd16` |
| `src/app-settings.js` | `app_settings` | `match` | `8326afa2c` |
| `src/apply.js` | `node/apply` | `match` | `8326afa2c` |
| `src/art.js` | `art` | `debt:` artwork HTTP timeout covers the whole exchange, unlike the app's socket inactivity timeout — see README "Known gaps" (SSRF guard absorbed). App PR #574 (the YouTube source) added a `binary` option to `send` (the reply body as bytes); not ported | `15298dd16` |
| `src/blob-server.js` | `node/blobs`, `http` (Range streaming) | `debt:` `byte-stream-rs`'s `Resolved` carries only `session` (not an overridden `blob` span), which matches whole-core `sameBytes` failover (`blockOffset == 0` and equal `length`/`byteLength`) — see `src/node/blobs.js` | `15298dd16` |
| `src/cast-control.js` | `cast/control` | `match` | `2c97f5b7d` |
| `src/control-channel.js` | `control_channel` | `match` (symmetric request-before-reply dispatch) | `15298dd16` |
| `src/cast.js` | `cast/` (`controller`, `receiver`, `pairing`, `state`, `media`, `mdns`, `http`, `disk`) | `match` (per-peer pairing lockout, `leavePeer`; the live LAN cast test needs working multicast; see `src/lan.js`) | `15298dd16` |
| `src/hiverelay.js` | `hiverelay`, `hiverelay::seeder` | `match` (on stop the relay leaves its topics; it has no `leavePeer` for recruited relays, and the process closes right after) | `8326afa2c` |
| `src/http.js` | `http` | `debt:` App PR #574 (the YouTube source) added `POST /v1/search/sources` (one search over every source, rows marked with `source` and `lossy`, optional server-sent events with one `source` event per source then `done`), `GET /v1/providers/:name/thumb/:id` (owner only, 204 when none) and the `youtube` provider routes (`/v1/providers/youtube/search`, `mirror`); not ported. Otherwise `match` (`requireOwner` + DNS-rebinding `localHost` guard, read-only `GET /v1/resolve`, `/v1/entries/info` `mediaInfo` + owner `health`, SHA-256 web ETags, dead-SSE pruning, `GET`/`HEAD`-only CORS, `POST /v1/entries/remove` by `key`, `seriesKey`, `ids` or `keys` with the fate of each mirrored file, `/v1/providers/<name>/search`, `browse` and `mirror`, provider mirrors under `/v1/jobs`). `removeEntries` runs one pass where the app allows four: each file is settled by its first try, so the app's later passes find nothing | `2a3f14eeb` |
| `src/identify.js` | `identify` | `debt:` App PR #574 (the YouTube source) added an `audio` option to `downloadPage` (`ubit get -a`, audio only) and calls the exported `ubitInfo` with `ytsearch:<query>` for search; not ported. Otherwise `match` | `15298dd16` |
| `src/index.js` | `node/core`, `node/factory`, `node/storage` (`StoreSuspension`), `suspend` | `match` (`entryHealth`, `keepCopy`, `copyExisting`, `search.close()` order; `suspend()` parks the swarm, then the store; `resume()` reopens the store, then the swarm, and a store that cannot reopen is reported as `fatal` and stays suspended until the next resume; `close()` resumes a suspended store first, and closes one that cannot reopen, so its waiters fail instead of waiting. A store access during suspension waits, as with `rocksdb-native`, but it blocks its thread, so the host's resume and shutdown reopen the store on the backend's suspension thread before the loop hears them; while the loop waits in the store, later host requests queue behind it until the resume) | `15298dd16` |
| `src/lan.js` | `lan/` (`discovery`, `mdns`, `host`) | `debt:` `tests/lan_mdns.rs::mdns_interop_with_bonjour_service` fails (bonjour-service never reports the Rust record; seen on Linux CI and a Mac, and at `d9a520e` before this alignment) while JS↔Rust LAN discovery in `tests/lan_interop.rs` passes on Linux; macOS runners have no LAN multicast, so CI skips the mDNS discovery tests there — see README "Known gaps" | `8326afa2c` |
| `src/mirror.js` | `mirror`, `mirror::watch`, `replicate`, `media_check` | `debt:` App PR #574 (the YouTube source) added `searchSources`, `searchable` on each `/v1/plugins` row, the `youtube` provider plugin, and an `artist` field in the `.peartube-album.json` pin (used as `music.artist` for files without readable tags) with `yt:<20hex>` accepted as a pinned release; not ported. Otherwise `match` (`feed-mirror` `existing` handoff, env-only `watch-dir`; `watch-dir` sweeps the import folder and `<storage>/staging`, one level of subfolders as albums: identity from tags, `<release>:sNNeMM` ids, `music` on each announce, `.peartube-album.json` pins, `mirror-watch.json` keyed by relative path and written as the app writes it, unchanged albums not re-read, one bad file reported and not fatal; `forgetImported`; the `soulseek` provider registered with its staging root and label, `searchProvider`/`browseProvider`/`mirrorProvider` and the provider job calls) | `2a3f14eeb` |
| `src/mnemonic.js` | `mnemonic` | `match` | `8326afa2c` |
| `src/node/blobs.js` | `node/blobs`, `node/hash_state` | `debt:` `entryHealth` (`wholePeers`/`lanPeers`) approximates per-peer replicator counts from `hypercore-rs`'s public session API (`remote_contiguous_length >= length` for 0/1 `wholePeers` and node-level `is_lan()` connections for `lanPeers`); Merkle `sameBytes` failover and `kept-copies.json` match, and `put` writes the announce's `music` (`cleanMusic`) | `2a3f14eeb` |
| `src/node/catalog.js` | `node/catalog` | `match` (`publicEntries`, `isKept` `local: true`, opened watch links capped at 32, `music` on entries, clamped by `cleanMusic` on read) | `2a3f14eeb` |
| `src/node/channel.js` | `node/channel` | `match` (startup `saveIdentity` best-effort) | `15298dd16` |
| `src/node/identity.js` | `identity` | `match` (`dirty` startup guard, rollback + throw on explicit mutations) | `15298dd16` |
| `src/node/network.js` | `node/network` | `match` (`destroySwarm` `clear()` before force-destroy) | `15298dd16` |
| `src/node/sources.js` | `node/sources` | `match` | `8326afa2c` |
| `src/node/storage.js` | `node/storage` | `match` | `15298dd16` |
| `src/private-overlay.js` | `private_overlay` | `debt:` one deliberate difference, a fix of a JS defect that the app does not have yet: when both endpoints dial each other at once, `admitInnerStream` keeps a duplicate only if `!enc.isInitiator && remote < local`, so the higher key always keeps the lower key's dial while the lower key keeps whichever stream opened first; when that is the higher key's dial, each side closes the stream the other kept, both re-dial and the race can repeat. The Rust swarm keeps the lower key's dial on both keys in every arrival order. On the higher key the rule is the JS one, and on the lower key it ends with the stream a JS higher key keeps, so mixed Rust/JS pairs converge at least as often as JS pairs. Becomes `match` when the app takes the same rule. Otherwise `match` (session-owned routes & RV slots, canonical IPv6 `/48` buckets, 64 KiB frame cap, 256/10 s DHT op rate limit) | `15298dd16` |
| `src/protocol.js` | `protocol` | `debt:` App PR #574 (the YouTube source) added `m4a` to `MEDIA_FILE` and uses the id prefix `yt:` (already valid under `ID`) for albums saved from YouTube; not ported. Otherwise `match` (`safe_equal`, `ipv6_groups`, `media_format`, `cleanMusic`) | `2a3f14eeb` |
| `src/providers/soulseek.js` | `providers::soulseek` (`place`, `rank`, `client`) | `debt:` search hits that tie on every other key are listed by name in the CLDR root collation, the order the app's `localeCompare` gives on a host with an English or no locale. The app takes the host's locale, so where that reorders letters (with `LC_ALL=sv_SE.UTF-8`, Node and Bare put `ä` after `z`) such ties come in another order — see README "Known gaps". Otherwise `match` (search, browse, mirror into the folder `watch-dir` sweeps, pins and mirrored lists, jobs, live sign-in, opt-in sharing via `slskShare`/`slskShareMaxFiles`). The app loads the `peartube-soulseek-bare` addon; the port links the crate it wraps, `peartube-soulseek`, behind `client::Backend` (the `soulseek` feature: on for the relay, off for the apps' engine). `slskListenPort` is a `u16` here, so the app's fallback to 50300 for an unusable value is applied where the relay reads the setting | `2a3f14eeb` |
| `src/providers/staging.js` | — | `debt:` what the source providers share about the import folder, from app PR #574: `ALBUM_PIN` (`{ release, title?, artist? }`), `RELEASE_ID` (now also `yt:<20hex>`), `MIRRORED_LIST`, folder and file naming; not ported | — |
| `src/providers/youtube.js` | — | `debt:` YouTube provider plugin from app PR #574 (search through `ubit`, audio-only saves into `<Artist> - <Album> (<Year>) [YouTube]/NN - Title.m4a` pinned to `yt:<20hex>`, jobs with `skipped`, `canSave`, relay-served result pictures; since app PR #576 a finished save waits for a running import with nothing placed); relay-only, not ported | — |
| `src/qr.js` | `cast/qr` | `match` | `8326afa2c` |
| `src/rebuild-view.js` | — | `JS-only:` the library behind `bin/rebuild-view.js`; an operator repair tool, not a runtime path | — |
| `src/resolution.js` | `resolution` | `match` (`media_info` container/duration/tracks + string/track caps, FLAC STREAMINFO and MPEG audio `MP1`/`MP2`/`MP3` with Xing duration) | `2a3f14eeb` |
| `src/search.js` | `search` | `debt:` peeroxide's `join` has no per-lookup `limit`, so a Rust searcher dials every relay the search-topic lookup returns where hyperswarm stops at 4; it still asks at most 4 (2 per `/24`) and closes the search-only connections a minute after the last search — see README "Known gaps". Keyed per peer, `serve(null)` before `closed`, `FRESH` 10 s | `15298dd16` |
| `src/settings.js` | `settings` | `match` (`watchDir` migrated out of saved settings; `slskUser`, `slskPass`, `slskServer`, `slskListenPort`, `slskShare`, `slskShareMaxFiles`) | `2a3f14eeb` |
| `src/ssrf.js` | `ssrf` | `match` (pre-connect IP/DNS + NAT64 `64:ff9b::/96` guard, no pooled-socket bypass, `PEARTUBE_ALLOW_PRIVATE_URLS`) | `15298dd16` |
| `src/tags.js` | `tags` | `match` (FLAC and Ogg Vorbis/Opus comments, ID3v2.3/2.4, `albumKey`) | `2a3f14eeb` |
| `src/traffic.js` | `traffic` | `match` | `8326afa2c` |
| `src/ui.js` | — | `JS-only:` shared asset; `relay.Dockerfile` copies the app's own file to `web/index.html` at image build and the relay serves it | `2a3f14eeb` |
| `worklet/worker.js` | `app`, `lifecycle`, `ipc`, `backend` | `match` (`searchGlobal`: the worker and HTTP API return up to 20 rows when the request omits `limit` or gives zero or a nonnumeric value) | `d6b9fc3ce` |
| `<storage>/plugins/*.mjs (user plugins)` | — | `JS-only:` Rust cannot execute JS plugins; the built-in plugins are ports of the app's own (`http-acquire`, `channel-backfill`, `blob-rechunk`, `watch-dir`, `feed-mirror`, `soulseek`, `p2p-replicate`, `hiverelay-seed`, `media-check`) | — |

`src/cast-ui.js`, `bin/receiver.js` and the browser-receiver cast routes were removed from the app in `fe2897172`;
their Rust counterparts are deleted too.

## Generator dependencies on the app

The generators read only what the app exports at `JS_REF`: `entropyToMnemonic` (`src/mnemonic.js`), `metaFor`
(`src/art.js`), `fileName`, `probe`, `ubitInfo`, `norm`, `strong` (`src/identify.js`) and the HiveRelay wire
helpers (`src/hiverelay.js`) are exported by the app for exactly this. A generator must never copy app code;
if it needs something the app keeps private, export it from the app in the same change. What the app keeps
private and a generator cannot reach (the import folder's album rules in `src/mirror.js`, the Soulseek plugin's
job handling) is compared through the app's own entry points instead: `tests/js/acquire-mirror-library.mjs` runs
`createMirrorHost` and `tests/js/soulseek-provider.mjs` runs `createSoulseekPlugin` beside the port.

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
