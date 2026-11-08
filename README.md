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

## Module map

Paths on the right are in the PearTube app unless otherwise stated.

| Rust | JavaScript reference / role |
| --- | --- |
| `app`, `lifecycle`, `backend`, `ipc` | `worklet/worker.js`: typed worker methods and events, JSON framing, serialized lifecycle and backend thread |
| `http` | `src/http.js`: loopback API, media and event endpoints |
| `acquire`, `mirror` | `src/acquire.js`, `src/mirror.js`: acquisition jobs and mirror plugins |
| `cast` | `src/cast.js`, `src/cast-ui.js`: controller, receiver, pairing and media serving |
| `lan` | `src/lan.js`: mDNS and isolated LAN DHT; worker host Bonjour events and interface selection |
| `hiverelay`, `traffic`, `private_overlay` | `src/hiverelay.js`, `src/traffic.js`, `src/private-overlay.js`: relay protocol, traffic gate and private routing |
| `node/storage`, `network`, `channel`, `apply`, `sources`, `catalog`, `blobs` | Corresponding `src/node/*.js`: storage, replication, channel operations, source and catalog management, blob storage and streaming |
| `node/core`, `node/factory` | `src/index.js`: compose and open the native node; default worker factory |
| `protocol`, `mnemonic`, `identity` | `src/protocol.js`, `src/mnemonic.js`, `src/identity.js`: wire formats, recovery and keys, identity storage |
| `settings`, `resolution` | App settings and media resolution selection |
| `art`, `identify` | `src/art.js`, `src/identify.js`: artwork lookup, media identification and `ubit` integration |

The node contract, JSON shapes and adapter details are in [docs/node-api.md](docs/node-api.md).

Custom bootstrap names are resolved with peeroxide's IPv4-preferring DNS resolver before
selecting the DHT bind address. A loopback name therefore binds like its numeric address;
otherwise the Noise handshake advertises non-loopback interfaces and misses a local testnet.

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
- `tests/hiverelay.rs`: JS relay wire vectors and secret-stream interop.
- `tests/traffic.rs`: JS traffic-gate decisions and npm Protomux busy-channel interop.
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

## Known gaps and differences

- `p2p-replicate` and `hiverelay-seed` mirror plugins are not built in; hosts can register plugins with `MirrorHost::register`.
- The default cast adapter has no `PeerFactory`: a load targeting a different network reuses the current node. Continuous read-ahead retargeting and `holdIfThin` are not wired; retry window timing differs. See [cast notes](docs/port-notes-cast.md).
- LAN mDNS and the isolated DHT are implemented, but the real-interface interoperability matrix remains environment-blocked on this Mac. A standalone raw UDP probe also receives no LAN multicast; an ALF “permitted” query alone does not establish delivery. See [LAN notes](docs/port-notes-lan.md).
- Corestore storage tuning uses engine defaults; free disk space is unknown, and traffic RTT/retransmit counters remain zero. See [node notes](docs/node-api.md).
- Rust cannot represent already-lone JS surrogates; exponent-form receiver ports can format differently. Artwork HTTP timeout covers the whole exchange, unlike the JS socket inactivity timeout. See [identify/art notes](docs/port-notes-identify-art.md).

## License

MIT OR Apache-2.0.
