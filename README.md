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
`http-acquire`, `channel-backfill`, `blob-rechunk`, `watch-dir`, `feed-mirror`,
`p2p-replicate` and `hiverelay-seed` plugins. `p2p-replicate` and `hiverelay-seed`
share one `traffic` gate (`peartube/bulk-busy`, `hiverelayUploadMbps`). `hiverelay-seed`
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
are refused. `status.lan` reports `{host, port}`, as JS does. The UI and web player
come from `PEARTUBE_WEB_DIR` (default: `web/` beside the binary). `relay.Dockerfile`
builds the image from this stack and the app's `src/ui.js` and `web/assets`.

## Module map

Paths on the right are in the PearTube app unless otherwise stated.

| Rust | JavaScript reference / role |
| --- | --- |
| `app`, `lifecycle`, `backend`, `ipc` | `worklet/worker.js`: typed worker methods and events, JSON framing, serialized lifecycle and backend thread |
| `http` | `src/http.js`: loopback API, media and event endpoints |
| `acquire`, `mirror` | `src/acquire.js`, `src/mirror.js`: acquisition jobs and mirror plugins |
| `cast` | `src/cast.js`, `src/cast-ui.js`: controller, receiver, pairing and media serving |
| `lan` | `src/lan.js`: mDNS and isolated LAN DHT; worker host Bonjour events and interface selection |
| `hiverelay`, `hiverelay::seeder`, `traffic`, `private_overlay` | `src/hiverelay.js`, `src/traffic.js`, `src/private-overlay.js`: relay protocol and the `hiverelay-seed` seeder, traffic gate and private routing (the standalone guard is `bin/private-relay.js`) |
| `node/storage`, `network`, `channel`, `apply`, `sources`, `catalog`, `blobs` | Corresponding `src/node/*.js`: storage, replication, channel operations, source and catalog management, blob storage and streaming |
| `node/core`, `node/factory` | `src/index.js`: compose and open the native node; default worker factory |
| `protocol`, `mnemonic`, `identity` | `src/protocol.js`, `src/mnemonic.js`, `src/identity.js`: wire formats, recovery and keys, identity storage |
| `settings`, `resolution` | App settings and media resolution selection |
| `art`, `identify` | `src/art.js`, `src/identify.js`: artwork lookup, media identification and `ubit` integration |

The node contract, JSON shapes and adapter details are in [docs/node-api.md](docs/node-api.md).

Custom bootstrap names are resolved with peeroxide's IPv4-preferring DNS resolver before
selecting the DHT bind address. A loopback name therefore binds like its numeric address;
otherwise the Noise handshake advertises non-loopback interfaces and misses a local testnet.
The public DHT starts firewalled and probes reachability like JS; only isolated
LAN DHTs assume an open socket. Neither worklet mode nor LAN discovery disables
public topic joins.
Public watch links with `joinSource: false` still join `source-swarm:<channel>`,
as JS does, without following or hydrating the source catalog. This lets a
receiver find the publisher even when its rotating content-topic cap omits the
film. Private-routed watches do not use this public rendezvous.
Loopback `/v1/status` and receiver `/cast/v1/info` include `connectedPeers`:
16-hex public-key prefixes, the discovery route (`public`, `lan`, `private`),
and the live transport address (null if unavailable). A public-DHT connection
can use a private address; its discovery route remains public. Cast counts
each connection once, and per-route rates retain independent sampling state.

## Evidence

The fixture tests replay outputs generated from app code; interop tests run the
real JS and Rust sides together. Tests using fakes are identified below rather
than presented as live network parity.

- `tests/js_fixtures.rs`: protocol and mnemonic reference vectors, including JS opening Rust-wrapped keys.
- `tests/identity_fixtures.rs`, `tests/settings_fixtures.rs`: JS identity and settings results, file bytes and modes; JS reads Rust-written files.
- `tests/resolution_fixtures.rs`: JS media resolution fixture results.
- `tests/art_identify.rs`: JS artwork and identification results against scripted HTTP and fake `ubit`.
- `tests/http.rs`: JS API responses, status and headers against a scripted node.
- `tests/acquire_mirror.rs`: JS acquisition and mirror call and JSON traces against scripted dependencies.
- `tests/apply.rs`: JS apply operations, host calls and view bytes on a real Hyperbee.
- `tests/blobs.rs`: JS blob operations, core keys, announce fields, encrypted and plain block bytes, and cache decisions.
- `tests/cast.rs`: JS cast fixtures and receiver/controller interop over HTTP and a TCP secret-stream stand-in. `tests/cast_default.rs` exercises the real default adapters and a JS receiver on a local testnet.
- `tests/hiverelay.rs`: JS relay wire vectors, secret-stream interop, and the app's own `createHiverelaySeeder` beside this crate's seeder against one HiveRelay stand-in: identical `/seed-core` bodies and identical `hiverelay-seed` request bytes.
- `tests/traffic.rs`: JS traffic-gate decisions and npm Protomux busy-channel interop.
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
the backend's single-thread Tokio executor, so HTTP and replication keep polling.

The standalone cast receiver uses RocksDB MemEnv (including media blobs), not
flash. Its obsolete `peer_dir/corestore` cache and stale cache/catalog indexes
are removed on startup;
`receiver.json` and `identity.json` remain on disk. Phone/desktop owned nodes keep
their disk stores. The receiver's store budget is `min(1.5 GB, MemTotal/4,
MemAvailable/2)` from `/proc/meminfo`, with a conservative 256 MiB fallback when
unavailable and a 64 MiB floor so zero available RAM cannot disable eviction.
Live media gets 40% of this budget; read-ahead reservations across all concurrent
Range/hybrid responses share at most 75% of that live ceiling. A response without
remaining read-ahead capacity fetches on demand. Trailing data gets at most
64 MiB (or one eighth of the ceiling), with the existing small block guards.
Each response may reserve at most half the pool, leaving room for an overlapping
seek. Demand-only readers retry reservations as they advance. After 15 seconds
of client backpressure the lease parks, releasing its reader, requests and
reservation without closing the HTTP body. Reading again re-registers the
reader and reopens the stream at its current position.
The remaining room covers memtables, metadata, in-flight writes and old/new blob
files during GC; watched blocks are trimmed every three seconds even while
buffering. Only the active load trims windows; title changes clear prior blobs.
Trailing eviction protects the slowest reader, and stale future data beyond the
furthest reader plus 7/8 of the live ceiling is dropped after backwards seeks.
Closing a response cancels its requests and releases its read-ahead reservation.
Seeks fetch discarded blocks again. The TV app currently has no
`onTrimMemory` hook; there is no claim of an OS-enforced allocator hard limit.

## Running tests

Rust 1.90 or newer, Node.js and npm are required. Clone the sibling path
dependencies listed in `Cargo.toml` and their transitive dependencies alongside
this repository; CI lists the required repositories. Install both JS dependency
sets before running JS-backed tests:

```sh
(cd fixtures/gen && npm install)
(cd tests/js && npm install)
cargo test --workspace -- --include-ignored
```

Some ignored interop tests also need a PearTube app checkout with its npm
dependencies installed. Set `PEARTUBE_APP` to that checkout; tests may report a
skip when prerequisites are absent. LAN tests require working multicast and a
usable interface. The mixed network slices are run individually with
`-- --include-ignored --test-threads=1`; slice A documents a `PEARTUBE_ACQUIRE`
copy needed for the app's announcing-job restart bug. See
[docs/node-api.md](docs/node-api.md) and the test sources for the harnesses.
Fixtures are committed; regenerate the protocol/mnemonic reference with
`(cd fixtures/gen && npm run gen)` when changing its reference inputs.

`tests/blobs.rs::http_stream_survives_cache_ceiling` uses a real loopback JS
Corestore seeder, incompressible blocks, HTTP Range playback and disk reclamation;
only channel/swarm bookkeeping is scripted. It paces the reader below 8 MiB/s,
starts with an older cached film, and reports the maximum chunk/read gap:

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
- `hiverelay-seed` marks HiveRelay connections bulk on the shared traffic gate, which buckets them and reports `peersBusy`; the gate's `pause`/`resume` are still no-ops at the transport (`src/node/network.rs`), so `hiverelayUploadMbps` does not yet throttle the wire itself.
- The default cast adapter has no `PeerFactory`: a load targeting a different network reuses the current node. Continuous read-ahead retargeting and `holdIfThin` are not wired; retry window timing differs. See [cast notes](docs/port-notes-cast.md).
- LAN mDNS and the isolated DHT are implemented, but the real-interface interoperability matrix remains environment-blocked on this Mac. A standalone raw UDP probe also receives no LAN multicast; an ALF “permitted” query alone does not establish delivery. See [LAN notes](docs/port-notes-lan.md).
- Corestore storage tuning uses engine defaults; free disk space is unknown. Status reports live blob-peer in-flight requests and UDX RTT/retransmitted packets; see [node notes](docs/node-api.md).
- Rust cannot represent already-lone JS surrogates; exponent-form receiver ports can format differently. Artwork HTTP timeout covers the whole exchange, unlike the JS socket inactivity timeout. See [identify/art notes](docs/port-notes-identify-art.md).

## License

MIT OR Apache-2.0.
