# peartube-native

Rust backend for PearTube, replacing the app's Bare worklet. It ports the worker,
node, discovery, media and protocol logic onto the sibling Rust Hypercore stack.
The JavaScript app is the reference; parity is covered by fixtures and mixed-node
tests, not a claim that every runtime behavior is identical.

## Hosts

In-process Rust hosts call the typed async `app::App` service through `AppHost` /
`NativeApp` and subscribe to typed events. `lifecycle` adapts JSON lines to the
same service; `backend::Backend` owns its runtime thread and line callback.
[`ffi/`](ffi/README.md) exposes that JSON-line backend through a C ABI for JNI
and Swift hosts, including suspend, resume and shutdown. See its header and
README for ownership and threading requirements.

`relay` and the `peartube-relay` binary are the headless relay (`bin/relay.js`): the
same `PEARTUBE_*` environment, `settings.json`, `sources.json`, `directory.json` and
`jobs.json` in `PEARTUBE_STORAGE`, and one port, `PEARTUBE_API_HOST:PEARTUBE_API_PORT`, for the
relay UI, the API and video (`/v1/blob`; the node's blob server stays on loopback). `PEARTUBE_CHANNEL`
(the `channel` setting) is the channel it serves; a pre-channel `settings.json` has its `tracker`
moved to `channel` and its `streamHost`/`streamPort` dropped at start. HTTPS runs on
`PEARTUBE_HTTPS_PORT` when `tls.crt`/`tls.key` exist. The acquirer comes with the
`http-acquire`, `channel-backfill`, `blob-rechunk`, `watch-dir`, `feed-mirror`, `soulseek`, `youtube`,
`p2p-replicate` and `hiverelay-seed` plugins. `watch-dir` sweeps the import folder
(`PEARTUBE_WATCH_DIR`) and `<storage>/staging`: a media file is announced once, and a folder
of music files becomes one album, each track announced as `<release>:sNNeMM` with its own
name in `music`. `soulseek` is the app's Soulseek provider (the `slsk*` settings):
`/v1/providers/soulseek/search`, `browse` and `mirror` find albums on Soulseek peers and
download them into the folder `watch-dir` sweeps, and `/v1/jobs` lists those mirrors with
the acquirer's jobs. Where the app loads the `peartube-soulseek-bare` addon, the relay links
the crate that addon wraps, `peartube-soulseek`.

The owner can search Soulseek and YouTube together with `POST /v1/search/sources`.
The JSON reply groups rows in the requested source order and keeps each source's ranking.
With `stream: true`, each source sends an event as it finishes. The last event gives counts
and errors without sending the rows again. A source that times out does not hide other results.
The relay serves result pictures at `/v1/providers/youtube/thumb/<id>`; the browser does
not contact the picture host. The `youtube` plugin uses `ubit` to search and to download
audio. Both providers wait for a running import before placing any files. Cancelling a
waiting save leaves the folder unchanged. The owner can see the outcome in `/v1/jobs`.

An open, a restart, a settings change and the reopen of a recovery run one at a time. The
plugins start after the node answers, on their own task, so a folder import does not hold the
lifecycle lock. A restart or a stop closes the plugin host before the node. As app PR #581
requires, the host marks itself closed and starts every built-in plugin's stop before it
awaits any, so a save that waits for the import folder ends as a cancel instead of waiting
out its limit. A startup import ends between files, as the app's does: a restart waits for the
file in flight, and the next startup imports the rest. The startup backfill runs even when
the host closed before it, as the app's does; the work after it checks the closed host. A
guard step asked for after a shutdown signal refuses at once, and startup logs that refusal
like any other; a step admitted before the signal can still run when its turn comes.

`p2p-replicate` and `hiverelay-seed` share one `traffic` gate
(`peartube/bulk-busy`, `hiverelayUploadMbps`). `hiverelay-seed`
recruits existing HiveRelays for this relay's own cores: `POST /seed-core` on the
`hiverelays` list (`=TOKEN` per relay, `hiverelayApiKey` as its default), and the
zero-key `hiverelay-discovery-v1` DHT (`hiverelayDiscover`, `PEARTUBE_HIVERELAY=1`)
with a wire-exact 2-core Hyperdrive v11/v13 beacon and signed
`hiverelay.seed-request.v3` requests over the `hiverelay-seed` Protomux channel
(`hiverelayReplicas`, 30-day TTL, `replicate` also offers cached copies).
The `privateGuard` setting runs the guard forwarder in-process on its own HyperDHT and port
(JS spawns `bin/private-relay.js`); `PEARTUBE_PRIVATE_RELAY` (or
`PEARTUBE_PRIVATE_RELAY_PORT`) makes the same binary the standalone Corestore-free guard
of `bin/private-relay.js`: no storage folder, no Corestore, and channel or content flags
are refused. A guard whose fixed port another socket holds does not start at startup: the
relay logs `private guard failed to start` and runs without it.
The cast stack (the app's `createCastController`) opens with the node and
answers `/v1/cast/*` from it, closing with a stop or reopen. `status.lan` reports
`{host, port}`, as JS does. Not yet ported: JS plugins from `<storage>/plugins`.
The UI and web player come from `PEARTUBE_WEB_DIR` (default: `web/` beside the binary).
`relay.Dockerfile` builds the image from this stack and the app's `src/ui.js` and
`web/assets`.

## Module map

Paths on the right are in the PearTube app unless otherwise stated.

| Rust | JavaScript reference / role |
| --- | --- |
| `app`, `lifecycle`, `backend`, `ipc` | `worklet/worker.js`: typed worker methods and events, JSON framing, serialized lifecycle and backend thread |
| `http` | `src/http.js`: loopback API, media and event endpoints |
| `acquire`, `mirror`, `mirror::watch` | `src/acquire.js`, `src/mirror.js`: acquisition jobs, mirror plugins, and the import folder (`watch-dir`: one announce for each media file, one album for each folder of music files) |
| `providers::soulseek` | `src/providers/soulseek.js`: the Soulseek provider (search ranking, browse, album and lone-track placement, mirror jobs). `providers::soulseek::client` is the `peartube-soulseek` client it drives |
| `providers::youtube`, `providers::staging`, `mirror::sources` | `src/providers/youtube.js`, `src/providers/staging.js`, `src/mirror.js`: audio saves, shared album names and pins, and search across providers |
| `tags` | `src/tags.js`: music tags (FLAC and Ogg comments, ID3v2.3/2.4) and the album key two rips of one album share |
| `cast` | `src/cast.js`, `src/cast-control.js`: controller, receiver, the cast channel on the peer connection, pairing and media serving |
| `lan` | `src/lan.js`: mDNS and isolated LAN DHT; worker host Bonjour events and interface selection |
| `hiverelay`, `hiverelay::seeder`, `traffic`, `private_overlay` | `src/hiverelay.js`, `src/traffic.js`, `src/private-overlay.js`: relay protocol and the `hiverelay-seed` seeder, traffic gate and private routing (the standalone guard is `bin/private-relay.js`) |
| `node/storage`, `network`, `channel`, `apply`, `sources`, `catalog`, `blobs` | Corresponding `src/node/*.js`: storage, replication, channel operations, source and catalog management, blob storage and streaming. `node/hash_state` is the libsodium-layout SHA-256 state `blobs` checkpoints a resumed ingest with |
| `node/core`, `node/factory` | `src/index.js`: compose and open the native node; default worker factory |
| `protocol`, `mnemonic`, `identity` | `src/protocol.js`, `src/mnemonic.js`, `src/identity.js`: wire formats, recovery and keys, identity storage |
| `settings`, `resolution` | App settings and media resolution selection |
| `art`, `identify` | `src/art.js`, `src/identify.js`: artwork lookup, media identification and baked-in `ubit` SDK (`UBIT_BIN` overrides the binary) |

The node contract, JSON shapes and adapter details are in [docs/node-api.md](docs/node-api.md).

Custom bootstrap names are resolved with peeroxide's IPv4-preferring DNS resolver before
selecting the DHT bind address. A loopback name therefore binds like its numeric address;
otherwise the Noise handshake advertises non-loopback interfaces and misses a local testnet.
The public DHT starts firewalled and probes reachability like JS; only isolated
LAN DHTs assume an open socket. Neither worklet mode nor LAN discovery disables
public topic joins.
Watch links carry their channel, and opening one joins that channel's swarm
(`source-swarm:<channel>`, at most four held at a time) without following or
hydrating it: the peers that cached the film are there, since only its publisher
and relays announce the video itself. A private-routed node instead follows the
channel, because private discovery reaches only the channels it follows.
A node announces a video's DHT topic (its Hypercore discovery key) only when it
published that video, plus, as a relay (`announceCache`, which `peartube-relay`
sets), complete cached copies under the same 48-topic cap and 20-second rotation.
A complete legacy slot is announced once at boot, never rotated back in: the
rotation walks the per-video cores this node published.
The title being read is looked up as a client (at most four at once) and never
announced, so watching a film does not tell the DHT this address holds it.
`peersBusy` in the traffic gate counts a `peartube/bulk-busy` claim only from a
peer that served this node blocks (`isSourcePeer`, fed by each download event's
`from`); a peer's record is dropped with its last connection. The gate's
`pause`/`resume` stop and restart the mux reading that connection's inbound
stream, so a held bulk HiveRelay connection is capped on the wire and still
times out, resumes and closes normally.
Loopback `/v1/status` includes `connectedPeers`:
16-hex public-key prefixes, the discovery route (`public`, `lan`, `private`),
and the live transport address (null if unavailable). A public-DHT connection
can use a private address; its discovery route remains public. Cast counts
each connection once, and per-route rates retain independent sampling state.

## Evidence

The fixture tests replay outputs generated from app code; interop tests run the
real JS and Rust sides together. Both are tied to one app commit, the hash in
[`JS_REF`](JS_REF): `fixtures/gen/*.mjs` import the app's own modules from that
checkout (no local copies) and write the committed references, and CI regenerates
them with `node fixtures/gen/check.mjs`, which fails when a committed reference is
not what `JS_REF` produces. `PARITY.md` maps every app module to its Rust
counterpart, its status and the app commit last absorbed. Tests using fakes are
identified below rather than presented as live network parity.

- `tests/js_fixtures.rs`: protocol and mnemonic reference vectors, including JS opening Rust-wrapped keys.
- `tests/identity_fixtures.rs`, `tests/settings_fixtures.rs`: JS identity and settings results, file bytes and modes; JS reads Rust-written files.
- `tests/resolution_fixtures.rs`: JS media resolution fixture results, FLAC and MPEG audio included.
- `tests/tags_fixtures.rs`: JS tag-reader results for FLAC, Ogg and ID3 files, each also cut short at every length, and album keys. `tests/soulseek_fixtures.rs`: JS Soulseek placement, ranking and browse results.
- `tests/art_identify.rs`: JS artwork and identification results against scripted HTTP and fake `ubit`. An audio save through the embedded SDK must leave the relay's settings folder as it was.
- `tests/http.rs`: responses from the app and Rust API, including owner-only pictures, search errors and exact event frames. A client must receive the fast source's event before it releases the slow source.
- `tests/acquire_mirror.rs`: calls and results from both acquirers and mirror hosts. The library sessions compare album imports on real files, pinned names and artists, saved file hashes by relative path, cancellation during an import and each source's results before and after its deadline.
- `tests/soulseek_provider.rs`: the app's own `createSoulseekPlugin` beside the Rust provider on one scripted Soulseek client and clock, with real files: every call, job view and folder listing compared as JSON text.
- `tests/youtube_provider.rs`: the app and Rust search, save, cancel and fetch pictures against the same scripted extractor and clock. The test compares jobs, skipped tracks and files on disk without depending on live YouTube availability.
- `tests/apply.rs`: JS apply operations, host calls and view bytes on a real Hyperbee.
- `tests/blobs.rs`: JS blob operations, core keys, announce fields, encrypted and plain block bytes, cache decisions, and ingested SHA-256 checkpoint/resume in both directions with `sodium-native` (`tests/js/sha256-state.mjs`).
- `tests/cast.rs`: JS cast fixtures, the `peartube-cast/v1` control channel over in-memory Protomux connections (pairing rules, state pushes, session control, a non-paired peer refused), and live JS↔Rust interop on a local testnet in both directions. `tests/cast_default.rs` casts between two default backends over the LAN, from pairing through exact bytes on the receiver's player port.
- `tests/hiverelay.rs`: JS relay wire vectors, secret-stream interop, and the app's own `createHiverelaySeeder` beside this crate's seeder against one HiveRelay stand-in: identical `/seed-core` bodies and identical `hiverelay-seed` request bytes.
- `tests/traffic.rs`: JS traffic-gate decisions, npm Protomux busy-channel interop, and a real hypercore transfer through a paused connection: no block arrives until it is resumed, and a held bulk connection serves nothing under its cap. `tests/blobs.rs` proves a claim counts only from a peer whose real download event named it.
- `tests/replicate.rs`: `p2p-replicate` pulls a followed channel's title on a local testnet and skips it under an unmet disk reserve. `tests/relay.rs`: the relay founds its channel and keeps it across restarts; `PEARTUBE_PRIVATE_RELAY` starts a guard with no storage folder and refuses channel flags.
- `tests/private_overlay.rs`: JS overlay vectors and behavior with a fake DHT, plus JS interop. `tests/network_private.rs` checks private routing fails closed and public routing joins topics; it is not a JS comparison.
- `tests/lan_mdns.rs`: JS TXT records and parsing plus live Bonjour interop. `tests/lan_interop.rs`: JS/Rust and Rust/Rust isolated LAN discovery, streaming and connection rules.
- `tests/node_api_shapes.rs`: node JSON shape round trips. `tests/node_mixed.rs`: real JS/Rust publication, following, streaming and restart on a local HyperDHT testnet.
  Its opt-in `catalog_search_and_update_latency` case measures metadata-only local
  append/apply/list latency for 552 entries; this is not a replication latency test.
- `tests/network_mixed_a.rs`, `network_mixed_b.rs`, `network_mixed_c.rs`: mixed JS/Rust slices of the app's `network.test.js`. A covers acquisition, interrupted transfers, restart and HTTP resume; B covers ACL, malformed announces and convergence; C covers private-routing fail-closed, cache clearing and eviction. Roles are swapped where applicable.
- `tests/ipc.rs`: framing and a scripted `worker.js` scenario compared line for line, including JSON bytes, with identical fakes and clock. `tests/backend_default.rs` checks the real default backend's loopback API, LAN and shutdown rather than comparing JS.

Catalog snapshots keep one JSON copy of each followed entry: restored snapshots
are consumed during hydration, and persistence does not retain another in-memory
copy. RocksDB's runtime cache and memtable budgets are shared across Corestores
in a process (see `../hypercore-storage-rs/README.md`); the JS storage format is
unchanged.
Search reuses the fresh awake-source scan returned by snapshot refresh instead
of walking each view twice; sleeping sources still use their merged snapshot.
Snapshot refresh notifies only when the merged records actually change, avoiding
an update/refresh loop when the offline snapshot contains more rows than the live
view; unchanged rows keep their existing JSON allocation.
The primary channel caches a successful complete scan by applied view head (key
and length), shared by status, full-list and id lookups. A changed head or failed
walk forces another scan; a polling host does not repeatedly decode unchanged
metadata. This does not disable the eager system/view history mirror on landing.
Blob wake observers do not retain sessions: Corestore's peer/session-aware GC
can reclaim an owned or cached core after its last reader and peer leave.
Cache reclamation offloads both the blocking RocksDB flush and compaction from
the backend's runtime workers, so HTTP and replication keep polling.

The standalone cast receiver keeps its store on disk in `peer_dir/corestore`
without a write-ahead log (`storageWal: false`), as the app's receiver does; it
persists across restarts. A RAM store (RocksDB's memory env, since removed) kept
everything a cast received on a Pi 4 (4.1–4.5 GB on a 4K film), while the disk
store held 323–325 MB and played the same. `receiver.json` and `identity.json` stay beside
the store. The receiver's memory budget is `min(1.5 GB, MemTotal/4,
MemAvailable/2)` from `/proc/meminfo`, with a conservative 256 MiB fallback when
unavailable and a 64 MiB floor so zero available RAM cannot disable eviction.
The cache ceiling (`set_cache_max_bytes`) is 40% of this budget; read-ahead
reservations across all concurrent Range/hybrid responses share at most 75% of
that ceiling. A response without
remaining read-ahead capacity fetches on demand. Trailing data gets at most
64 MiB (or one eighth of the ceiling), with the existing small block guards.
Each response may reserve at most half the pool, leaving room for an overlapping
seek. Demand-only readers retry reservations as they advance. After 15 seconds
of client backpressure the lease parks, releasing its reader, requests and
reservation without closing the HTTP body. Reading again re-registers the
reader and reopens the stream at its current position.
Watched blocks are trimmed every three seconds even while
buffering. Only the active load trims windows; title changes clear prior blobs.
Trailing eviction protects the slowest reader, and stale future data beyond the
furthest reader plus 7/8 of the live ceiling is dropped after backwards seeks.
Closing a response cancels its requests and releases its read-ahead reservation.
Seeks fetch discarded blocks again. The TV app currently has no
`onTrimMemory` hook; there is no claim of an OS-enforced allocator hard limit.

## Running tests

Rust 1.90 or newer, Node.js and npm are required. Clone the sibling path
dependencies listed in `Cargo.toml` and their transitive dependencies alongside
this repository; CI lists the required repositories. The app is the reference for
every JS-backed test: check out the PearTube app at the commit in [`JS_REF`](JS_REF)
and install its dependencies (`npm ci` in that checkout). `PEARTUBE_APP` names
that checkout; without it the tests use the sibling `../peartube`. Fixture
generators read the app's own modules, so `fixtures/gen` has no copies and no
dependencies of its own; the interop harnesses need `tests/js` dependencies:

```sh
(cd tests/js && npm install)
export PEARTUBE_APP=$PWD/../peartube   # the checkout at JS_REF
(cd fixtures/gen && node check.mjs)    # committed references vs that checkout
cargo test --workspace -- --include-ignored --test-threads=1
```

`node fixtures/gen/check.mjs` exits 1 when a committed `fixtures/*-reference.json`
is not what the app at `JS_REF` produces. It reports every reference that differs;
a reference is regenerated only together with the Rust port that catches up (see
[PARITY.md](PARITY.md), "How to bump JS_REF"). Tests may report a skip when a
prerequisite is absent. LAN tests require working multicast and a usable
interface. The mixed network slices are run individually with
`-- --include-ignored --test-threads=1`; slice A documents a `PEARTUBE_ACQUIRE`
copy needed for the app's announcing-job restart bug. See
[docs/node-api.md](docs/node-api.md) and the test sources for the harnesses.

`tests/blobs.rs::http_stream_survives_cache_ceiling` uses a real loopback JS
Corestore seeder, incompressible blocks, HTTP Range playback and disk reclamation;
only channel/swarm bookkeeping is scripted. It runs on two runtime workers, the
engine's minimum, paces the reader below 8 MiB/s, starts with an older cached
film, and reports the maximum chunk/read gap:

```sh
PEARTUBE_APP=/path/to/peartube CACHE_FREEZE_BUDGET=2147483648 \
  CACHE_FREEZE_MOVIE_BYTES=3221225472 RUSTC_WRAPPER= CARGO_BUILD_JOBS=4 \
  CARGO_TARGET_DIR=target/CacheFreeze nice -n 15 cargo test --release --test blobs \
  http_stream_survives_cache_ceiling -- --ignored --nocapture
```
Use `CACHE_FREEZE_BLOCK_BYTES=65536` with a 1 GiB ceiling to exercise more than
32768 cached tree nodes without needing a ten-gigabyte fixture.

## Throughput A/B

`examples/download_ab.rs` and `tests/js/download-ab.mjs` consume the same explicit
UDX peers and blob range with a fresh private `/tmp` store. They assert HTTP 206,
byte count and the configured SHA-256, and report decimal MB/s and downloader
CPU-seconds/GiB, including the HTTP sink and validation hash but excluding setup,
shutdown and seeder CPU. `worklet` defaults to `true`, as in the actual desktop
app. Discovery is disabled so ambient LAN peers cannot change the comparison.
The optional `trace: true` JSON field samples status every 500 ms; leave it off
for timing.

Use a private copy of the JS app's `src` and `package.json`, with its installed
dependencies available read-only. In one terminal, keep a loopback JS seeder open:

```sh
PEARTUBE_APP=/tmp/peartube-reference nice -n 10 node tests/js/download-ab.mjs \
  seed 1073741824 /tmp/download-ab.json
```

In another, run the consumers back to back (never during compilation):

```sh
RUSTC_WRAPPER= CARGO_BUILD_JOBS=4 CARGO_TARGET_DIR=target/ThroughputAB \
  nice -n 10 cargo build --release --example download_ab
PEARTUBE_APP=/tmp/peartube-reference nice -n 10 node tests/js/download-ab.mjs \
  consume /tmp/download-ab.json
nice -n 10 target/ThroughputAB/release/examples/download_ab /tmp/download-ab.json
```

`--release` is the relay's build. The apps ship the `engine` profile (`Cargo.toml`): to
time that build, pass `--profile engine` and run `target/ThroughputAB/engine/examples/download_ab`.

For a real relay, provide its LAN `{host,port,publicKey}` in `peers`, the consumer's
interface address in `host`, an announce in `entry`, and byte `start`/`bytes`.
Confirm that the seeder actually holds that range before timing: an HTTP proxy
can fetch absent blocks, but a sparse P2P peer cannot serve blocks it lacks.
Capture `netstat -s -p udp` before/after each run; its full-socket-buffer delta
is machine-wide, not a per-process loss counter.

For the seeder side, `examples/seed_ab.rs <file> [workers]` and
`tests/js/seed-ab.js <file>` (run with `bare` from a JS app copy's `bin/`)
publish a file through their node's own `put`, as a relay's acquirer does, print
one JSON line with the loopback LAN peer and announce, and serve until stdin
closes. `workers` 0 selects Tokio's current-thread scheduler; the default is 4.
Build that line into a `download_ab` config (`host` `127.0.0.1`, `peers`
`[peer]`, `entry`, `start` 0, `bytes` the size, `sha256`) and run one or more
consumers against it; measure seeder CPU from outside (`ps`, `/proc/<pid>/stat`).

Linux x86-64 builds use `.cargo/config.toml` to enable SSE4.2 and CLMUL, so
RocksDB checks blob CRC32C in hardware; its portable build spent about 9 % of
a seeder's CPU in software CRC32C.

### 2026-11-10 seeder measurements

Linux: NUC (i7-10710U), loopback, stores on tmpfs, seeder pinned to three cores
and N `download_ab` consumers (this build) on the other three; 1 GiB random file;
two interleaved rounds. JS is the relay image's `relay.js` stack under Bare.
Aggregate MB/s is all bytes over first-ready to last-result; CPU is the seeder's.

| Peers | JS MB/s | Rust MB/s | JS CPU-s/GiB | Rust CPU-s/GiB | JS / Rust peak RSS MB |
|---:|---:|---:|---:|---:|---|
| 1 | 80–82 | 115 | 12.6–13.2 | 4.7–4.9 | 313–350 / 148–153 |
| 2 | 99–102 | 172–181 | 12.2–12.6 | 6.2–6.3 | 342–343 / 179 |
| 4 | 109–110 | 183–184 | 12.0–12.2 | 7.2–7.4 | 376–383 / 174–194 |
| 6 | 109–114 | 176–179 | 11.8–12.5 | 7.9–8.1 | 374–407 / 203–207 |

Before this round, on the same setup with the old consumers, Rust seeded 63–66 MB/s
at 18–19 CPU-s/GiB to one peer and 103–108 MB/s at 29–31 to six. From four peers
up the consumers use about all six of their hardware threads while the Rust
seeder uses under one core, so 180 MB/s is this setup's ceiling, not the seeder's.

macOS loopback: one 1 GiB pull from the Rust seeder 56.6 MB/s at 20.2 CPU-s/GiB
(was 33.1 at 31.0); four peers 67.3 MB/s at 21.3 (38.8 at 74.5 with only the
bookkeeping fix: worker threads contended for the socket in `sendto`). The JS
seeder to the same consumers: 53.3 at 18.8 and 55.9 at 19.6.

### 2026-11-08 desktop-mode measurements

One back-to-back 1 GiB read per backend/path, fresh consumer stores, four Rust
workers, identical SHA-256 and one explicitly pinned JS seeder throughout.
LAN reads use Interstellar at byte offset 8 GiB. Decimal MB/s:

| Seeder | JS | Rust before | Rust with UDX windows | CPU-s/GiB: JS / before / after |
|---|---:|---:|---:|---|
| Loopback | 37.35 | 37.24 | 38.36 | 30.28 / 45.89 / 44.61 |
| NUC, same subnet | 15.17 | 15.27 | 14.02 | 25.06 / 26.16 / 26.64 |
| Unraid, routed LAN | 7.50 | 7.98 | 13.18 | 29.90 / 30.41 / 27.22 |

Rust incorrectly used JS's non-UDX three-times-minimum request window on UDX.
The mux now supplies live RTT, enabling JS's 50 ms/peer-count scaling. A separate
cold 64 MiB NUC trace showed peak in-flight requests **16 / 48 / 16** and
**13.97 / 10.30 / 16.44 MB/s** for JS/before/after. The full-range Unraid result
improved 65%, but the full-range NUC after-run was 7.6% below JS: this is **not**
a universal throughput-win certification, and LAN variation remains material.
Loopback CPU remains higher than JS; sampled running stacks are dominated by
UDP ACK sends/receives. Socket-buffer enlargement and read coalescing were
tested and rejected for lack of repeatable wins. No ACK, pacing or wire changes.

## Known gaps and differences

- JS plugins from `<storage>/plugins` (`PEARTUBE_PLUGINS_DIR`, `*.mjs`) are a permanent difference: this crate cannot load JavaScript. Its built-in plugins register through `MirrorHost::register`; `p2p-replicate` (`replicate`) and `hiverelay-seed` are built on `CoreNode` and registered by the relay.
- `hiverelay-seed` marks HiveRelay connections bulk on the shared traffic gate, which buckets them and reports `peersBusy`; the gate's `pause`/`resume` hold and release that connection's inbound reads (`ReadGate` in `src/node/network.rs`), so `hiverelayUploadMbps` caps what the peer can draw from it.
- Global search (`search`): peeroxide's `join` has no per-lookup `limit`, so a Rust searcher dials every relay the search-topic lookup returns, where the app's hyperswarm join stops at 4. It still asks at most 4 relays (2 per `/24`) and closes the search-only connections a minute after the last search. `media-check` (`media_check`) is built on `CoreNode` and registered by the relay, like `hiverelay-seed`.
- The default cast adapter has no `PeerFactory`: a load targeting a different network reuses the current node. Retry window timing differs. See [cast notes](docs/port-notes-cast.md).
- Cast: the receiver's read-ahead is one retargeted `core.download` window per stream, sized like the app's (`holdIfThin` and watched-block trimming included), with the TV build's memory-cache permits capping it. A v1 receiver is listed but not castable. See [cast notes](docs/port-notes-cast.md).
- LAN mDNS and the isolated DHT are implemented, but the real-interface interoperability matrix remains environment-blocked on this Mac. A standalone raw UDP probe also receives no LAN multicast; an ALF “permitted” query alone does not establish delivery. See [LAN notes](docs/port-notes-lan.md).
- Corestore storage tuning uses engine defaults; free disk space is unknown. Status reports live blob-peer in-flight requests and UDX RTT/retransmitted packets; see [node notes](docs/node-api.md).
- Rust cannot represent already-lone JS surrogates; exponent-form receiver ports can format differently. Artwork HTTP timeout covers the whole exchange, unlike the JS socket inactivity timeout. See [identify/art notes](docs/port-notes-identify-art.md).
- Soulseek: the provider links `peartube-soulseek` where the app loads its addon. That is the `soulseek` cargo feature, on by default and off in the apps' engine (`ffi/`): the client crate exports its own C ABI for the addon, and a library that links it exports those symbols too. A build without the feature, or a host that gives the provider no client (`SoulseekSettings::backend` is `None`), reports `unavailable`, as the app does on a platform with no prebuild. The folder shared with peers stays the client's own choice from the environment (`PEARTUBE_SLSK_SHARE_DIR`, else `PEARTUBE_WATCH_DIR`, else `<PEARTUBE_STORAGE>/staging`).
- Soulseek search hits that tie on every other key are listed by name in the CLDR root collation (`providers/soulseek/rank.rs`): the order the app's `localeCompare` gives on a host with an English or no locale. The app takes the host's locale, so where that reorders letters (with `LC_ALL=sv_SE.UTF-8`, Node and Bare put `ä` after `z`) such ties come in another order, and `fixtures/soulseek-reference.json` does not regenerate equal on such a host.
- `POST /v1/entries/remove` makes one pass over the entries it picked. The app's loop allows four, but each file is settled by its first try, so its later passes find nothing to do.
- A saved source (`sources.json`) that is not a channel link or a 64-hex key stops the app's node from opening, with `Invalid PearTube URI or 64-hex key`: the app's relay does not start. The Rust node opens without that source and reports nothing.
- Relay restart and shutdown: a restart reopens the node and applies the guard; an ordinary failure answers `400` with the reason. At shutdown the Rust relay stops the host visible at the signal before it waits for the lifecycle lock. A host that a queued open or reopen publishes later is stopped only on shutdown’s turn. A long import on that host can reach the 3 s deadline; the relay logs `relay shutdown timed out` and exits 1. The app gives lifecycle work up to 2 s before it stops the current host. A shutdown alone does not stop the same host twice, but a concurrent restart can also stop that host. A step that was already queued can still run; a new request is refused. Rust serializes the reopen of a recovery, while the app also serializes the claim.
- App #586 keeps a node close failure. Later restarts answer `500`, a settings change that reopens the node reports the reason as a warning, and shutdown exits 1. The Rust channel close discards the Autobee error. `CoreNode::close()` finishes closing the store and returns `Ok(())`, so the relay has no corresponding error to report.
- Plugin close: each call starts all stops again. The app’s `Promise.all` rejects on the first rejected stop and skips `traffic.stop()`; the relay discards that rejection. Rust does not run user JS plugins. Its built-in stops return `()`, so the host waits for all of them and releases the traffic gate once.

## License

MIT OR Apache-2.0.
