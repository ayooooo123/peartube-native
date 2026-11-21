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
| `bin/relay.js` | `relay`, `src/bin/peartube-relay.rs` | `debt:` cast, HiveRelay, guard, LAN status, global search, the env-only import folder, live Soulseek sign-in, YouTube search, audio downloads and guarded pictures. The relay uses the embedded `ubit` unless `UBIT_BIN` is set. Lifecycle steps run one at a time and continue if the client disconnects. Plugins start after the node answers. A restart closes the host before the node, ends an import between files, reopens and applies the guard. A failed reopen or guard start answers `400` with the reason. Startup logs a failed guard step, including a new request refused during shutdown. Work already queued may still run. On a signal Rust stops the visible host before it waits for the lifecycle lock. A host published later is stopped only on shutdown’s turn; a long import there can reach the 3 s process deadline (exit 1). The app allows the lifecycle up to 2 s before stopping the current host. Rust serializes the reopen of a recovery, not the claim. App #586 keeps a node close failure: later restarts answer `500`, a settings reopen reports a warning and shutdown exits 1. Rust discards the Autobee error, finishes closing the store and returns `Ok(())`; the relay has no such error to report. Rust retries a failed reopen once and answers 503 on `/v1/entries` when the relay has no node. JS waits for another request and lists no entries. Both keep the settings API available: a failed apply returns 200 with a warning and a failed POST restart returns 400 with the error. | `d1a297c54` |
| `spec/build.mjs`, `spec/hyperschema/` | `build.rs`, `spec/hyperschema/schema.json`, `wire::peartube` | `match`: the port compiles a byte-equal copy of the app's `schema.json` with `hyperschema-rs` at build time; `fixtures/gen/check.mjs` fails when the copy differs, `tests/control_wire.rs` checks every struct's bytes both ways | `96655e223` |
| `spec/protocols.js` | `control_protocol` (`SEARCH`, `CAST`, `GUARD`) | `match`: same channel names, op order, kinds, structs and `maxItems`, held equal by `fixtures/control-wire-reference.json` | `96655e223` |
| `src/acquire.js` | `acquire` | `match` (`existing` peer-copy + SHA-256 verification, `keepCopy`, `Retry-After` 300 s cap, rotated-source response close, SSRF guard) | `15298dd16` |
| `src/app-settings.js` | `app_settings` | `match` | `8326afa2c` |
| `src/apply.js` | `node/apply` | `match` | `8326afa2c` |
| `src/art.js` | `art` | `debt:` the HTTP timeout covers the whole exchange instead of socket inactivity. The guarded fetch supports both text and raw bytes, including the size limit on provider pictures. See README “Known gaps”. | `26a40b718` |
| `src/blob-server.js` | `node/blobs`, `http` (Range streaming) | `debt:` `byte-stream-rs`'s `Resolved` carries only `session` (not an overridden `blob` span), which matches whole-core `sameBytes` failover (`blockOffset == 0` and equal `length`/`byteLength`) — see `src/node/blobs.js` | `15298dd16` |
| `src/cast-control.js` | `cast/control` | `match`: bounded Hyperschema state, command, pairing and host mappings | `96655e223` |
| `src/control-channel.js` | `control_channel`, `control_protocol` | `match`: one typed frame per message, requests both ways, handlers started in wire order (polled once inline), trailing bytes ignored, per-channel bad-frame budget (the eighth bad request is answered first), safe-integer and finite-float checks both ways, foreign error codes delivered as `HANDLER_ERROR`, three event-send outcomes, one send per frame | `96655e223` |
| `src/guarded-frame.js` | — | `JS-only:` the port's registered decoders (`traffic`, `hiverelay`, `control_channel`) return errors and check size and safe range before they change state, so nothing needs wrapping | — |
| `src/cast.js` | `cast/` (`controller`, `receiver`, `pairing`, `state`, `media`, `mdns`, `http`, `disk`) | `match`: rotated LAN topic, TXT `v=3`, version refusal before credentials, a not-current receiver is not dialed also under a saved id that differs (matched by key), a subscription ends with its channel, an empty `magnetUrl` is `null`. The real LAN discovery test needs multicast; both DHT interop directions use an isolated testnet. | `96655e223` |
| `src/hiverelay.js` | `hiverelay`, `hiverelay::seeder` | `match` (on stop the relay leaves its topics; it has no `leavePeer` for recruited relays, and the process closes right after; the four seed messages are size- and safe-range-checked before dispatch, an unseed request at most 137 bytes) | `96655e223` |
| `src/http.js` | `http` | `match`: owner guard, read-only resolve, private health, web ETags, read-only CORS, removal by key, series or id with each file's fate, provider jobs and calls. The owner can search sources with JSON or server-sent events and fetch result pictures (204 if absent). A fast source reaches the client before a slow one finishes. Removal takes one pass where the app allows four: each file is settled on the first pass. | `26a40b718` |
| `src/identify.js` | `identify` | `match`: `ytsearch:` queries and audio-only downloads through `ubit`. For the SDK, a save uses private temporary audio settings and retains the configured cookie path; `tests/art_identify.rs` checks that the save leaves the relay's settings folder unchanged. For an external binary, the relay supplies `-a`. | `26a40b718` |
| `src/index.js` | `node/core`, `node/factory`, `node/storage` (`StoreSuspension`), `suspend` | `match` (`entryHealth`, `keepCopy`, `copyExisting`, `search.close()` order; `suspend()` parks the swarm, then the store; `resume()` reopens the store, then the swarm, and a store that cannot reopen is reported as `fatal` and stays suspended until the next resume; `close()` resumes a suspended store first, and closes one that cannot reopen, so its waiters fail instead of waiting. A store access during suspension waits, as with `rocksdb-native`, but it blocks its thread, so the host's resume and shutdown reopen the store on the backend's suspension thread before the loop hears them; while the loop waits in the store, later host requests queue behind it until the resume). A caller can look up a core and release that lookup through `CoreNode::net()`. | `0a40ae022` |
| `src/lan.js` | `lan/` (`discovery`, `mdns`, `host`) | `debt:` `tests/lan_mdns.rs::mdns_interop_with_bonjour_service` fails (bonjour-service never reports the Rust record; seen on Linux CI and a Mac, and at `d9a520e` before this alignment) while JS↔Rust LAN discovery in `tests/lan_interop.rs` passes on Linux; macOS runners have no LAN multicast, so CI skips the mDNS discovery tests there — see README "Known gaps" | `8326afa2c` |
| `src/mirror.js` | `mirror`, `mirror::watch`, `mirror::sources`, `replicate`, `media_check` | `debt:` the host lists searchable providers, starts source searches together and reports each source independently. Both providers wait for the import folder before placing files. `watch-dir` scans the import folder and `<storage>/staging`, saves the ledger by relative path, accepts `yt:` album pins and uses the pinned artist when tags are absent. The host keeps the provider and job calls, `forgetImported`, and the existing-copy handoff for feeds. The startup backfill runs even if the host already closed; later startup work checks the closed flag. Each `close()` starts every plugin stop anew. A stop that calls `close()` only once, after a suspension, can await it and finish its cleanup. In the app, `Promise.all` rejects on the first rejected stop and skips `traffic.stop()`; the relay discards the rejection. Rust cannot load user JS plugins. Its built-in stops return `()`, so it waits for them all and releases the traffic gate once. A pass counts slices one core at a time and opens the current core again for the download and after a cache prune. After five seconds without a block it looks up the core until the slice ends. When ordering replica slices, Rust uses the signed tree length. The app also uses each connected peer’s reported length, which the pinned Rust Hypercore does not expose. | `0a40ae022` |
| `src/mnemonic.js` | `mnemonic` | `match` | `8326afa2c` |
| `src/node/blobs.js` | `node/blobs`, `node/hash_state` | `debt:` `entryHealth` (`wholePeers`/`lanPeers`) approximates per-peer replicator counts from `hypercore-rs`'s public session API (`remote_contiguous_length >= length` for 0/1 `wholePeers` and node-level `is_lan()` connections for `lanPeers`); Merkle `sameBytes` failover and `kept-copies.json` match, and `put` writes the announce's `music` (`cleanMusic`) | `2a3f14eeb` |
| `src/node/catalog.js` | `node/catalog` | `match` (`publicEntries`, `isKept` `local: true`, opened watch links capped at 32, `music` on entries, clamped by `cleanMusic` on read) | `2a3f14eeb` |
| `src/node/channel.js` | `node/channel` | `match` (startup `saveIdentity` best-effort) | `15298dd16` |
| `src/node/identity.js` | `identity` | `match` (`dirty` startup guard, rollback + throw on explicit mutations) | `15298dd16` |
| `src/node/network.js` | `node/network` | `debt:` replication differs from the JS stack when a peer closes a core's channel before this side has answered the open: protomux-rs rejects that open, so the peer can open the core again on the same connection, where npm protomux keeps the open pending until its pairer answers, and ends the connection if the peer then uses the closed channel's id again while that channel is still open. See README "Known gaps and differences". The rest matches: `destroySwarm` `clear()` before force-destroy. A client looks up the discovery key without announcing. Releasing the lookup preserves an existing announcement, including one promoted from that lookup. | `0a40ae022` |
| `src/node/sources.js` | `node/sources` | `debt:` a saved source that is not a channel link or a 64-hex key stops the app's node from opening (`Invalid PearTube URI or 64-hex key`): the relay does not start, and a restart answers that reason until the owner corrects `sources.json`. The Rust node opens without that source and reports nothing. The rest matches. | `8326afa2c` |
| `src/node/storage.js` | `node/storage` | `match` | `15298dd16` |
| `src/private-overlay.js` | `private_overlay` | `debt:` App main `20e7db53b` (#589) also changed guard startup; addressed in the next commit. Wire v2: `peartube/private-guard/v2` on the typed envelope with the `GUARD` op table (requests both ways: `incOpen` guard → endpoint, `rvFetch`/`hopOpen` guard → guard); role, routes, rendezvous slots and circuits belong to the channel session, the DHT rate budget to the connection; a client closes its dedicated connection to a guard whose channel is refused, refuses it, or closes later; route card, mutable signable and seals unchanged; canonical IPv6 `/48` buckets, 256/10 s DHT op rate limit | `20e7db53b` |
| `src/private-overlay.js` (`attemptDirectHopBypass`) | — | `JS-only:` role-separation regression test helper; no port caller needs it. `tests/private_overlay.rs::interop::js_interop_over_peeroxide` runs the app's own helper against a route on the Rust guard | — |
| `src/protocol.js` | `protocol` | `match`: `m4a` media, `yt:` ids, `safe_equal`, `ipv6_groups`, `media_format`, `cleanMusic`. | `26a40b718` |
| `src/providers/soulseek.js` | `providers::soulseek` (`place`, `rank`, `client`) | `debt:` the last tie in search uses CLDR root collation; the app uses its host's locale. See README “Known gaps”. Search, browse, saving, cancellation, jobs, sign-in and opt-in sharing otherwise match. A save stages files until it holds the import folder, then identifies its own files by relative path. The relay links the same client that the app's addon wraps; the FFI build omits it. The relay applies the fallback to port 50300 when reading the setting. | `26a40b718` |
| `src/providers/staging.js` | `providers::staging` | `match`: shared album pins, release ids (including `yt:<20hex>`), lists of mirrored files, and folder and file names. | `26a40b718` |
| `src/providers/youtube.js` | `providers::youtube` | `match`: search, save availability, result pictures, staged audio saves, album pins and skipped tracks. Cancellation or expiry while waiting for the import folder places nothing. `tests/youtube_provider.rs` compares the app and port on the same extractor replies, files and clock. | `26a40b718` |
| `src/qr.js` | `cast/qr` | `match` | `8326afa2c` |
| `src/rebuild-view.js` | — | `JS-only:` the library behind `bin/rebuild-view.js`; an operator repair tool, not a runtime path | — |
| `src/resolution.js` | `resolution` | `match` (`media_info` container/duration/tracks + string/track caps, FLAC STREAMINFO and MPEG audio `MP1`/`MP2`/`MP3` with Xing duration) | `2a3f14eeb` |
| `src/search.js` | `search` | `debt:` peeroxide's `join` has no per-lookup `limit`, so a Rust searcher dials every relay the search-topic lookup returns where hyperswarm stops at 4; it still asks at most 4 (2 per `/24`) and closes the search-only connections a minute after the last search — see README "Known gaps". Keyed per peer, `serve(null)` before `closed`, `FRESH` 10 s. On the v2 branch: `peartube/search/v2` and topic `peartube/search/v2/0`, `limit` 0 means 20, hit URIs at most 2,048 UTF-8 bytes | `15298dd16` |
| `src/settings.js` | `settings` | `match` (`watchDir` migrated out of saved settings; `slskUser`, `slskPass`, `slskServer`, `slskListenPort`, `slskShare`, `slskShareMaxFiles`) | `2a3f14eeb` |
| `src/ssrf.js` | `ssrf` | `match` (pre-connect IP/DNS + NAT64 `64:ff9b::/96` guard, no pooled-socket bypass, `PEARTUBE_ALLOW_PRIVATE_URLS`) | `15298dd16` |
| `src/tags.js` | `tags` | `match`: FLAC and Ogg Vorbis/Opus comments, ID3v2.3/2.4, and Unicode album keys. Distinct names in non-Latin scripts remain distinct. | `26a40b718` |
| `src/traffic.js` | `traffic` | `match` (the busy flag is checked as kind 0, at most 9 bytes, safe uint, before the last claim changes) | `96655e223` |
| `src/ui.js` | — | `JS-only:` shared asset; `relay.Dockerfile` copies the app's own file to `web/index.html` at image build and the relay serves it | `26a40b718` |
| `worklet/worker.js` | `app`, `lifecycle`, `ipc`, `backend` | `match` (`searchGlobal`: the worker and HTTP API return up to 20 rows when the request omits `limit` or gives zero or a nonnumeric value) | `d6b9fc3ce` |
| `<storage>/plugins/*.mjs (user plugins)` | — | `JS-only:` Rust cannot execute JS plugins. The relay provides native implementations of `http-acquire`, `channel-backfill`, `blob-rechunk`, `watch-dir`, `feed-mirror`, `soulseek`, `youtube`, `p2p-replicate`, `hiverelay-seed` and `media-check`. | — |

`src/cast-ui.js`, `bin/receiver.js` and the browser-receiver cast routes were removed from the app in `fe2897172`;
their Rust counterparts are deleted too.

## Generator dependencies on the app

The generators read only what the app exports at `JS_REF`: `entropyToMnemonic` (`src/mnemonic.js`), `metaFor`
(`src/art.js`), `fileName`, `probe`, `ubitInfo`, `norm`, `strong` (`src/identify.js`) and the HiveRelay wire
helpers (`src/hiverelay.js`) are exported by the app for exactly this. A generator must never copy app code;
if it needs something the app keeps private, export it from the app in the same change. What the app keeps
private is compared through the app's own entry points instead: the library oracle runs `createMirrorHost`,
and the Soulseek and YouTube oracles run `createSoulseekPlugin` and `createYoutubePlugin` beside their ports.

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
