# Cast: port notes for `src/cast.js`

`cast.js` has two roles the worker drives, and both are ported:

| JS | Rust | Notes |
| --- | --- | --- |
| `createReceiver` | `src/cast/receiver.rs` (`Receiver::start`) | identity file, LAN HTTP server, Noise RPC server, mDNS, media |
| `createCastController` | `src/cast/controller.rs` (`CastController::start`) | discovery, pairing, LAN blob server, hosted web receivers, cast session |
| `cast-ui.js` `receiverPage` | `src/cast/ui.rs` + `src/cast/ui/receiver.html` | the page is embedded byte-for-byte; only the boot literal is substituted |
| `qr.js` `qrSvg` | `src/cast/qr.rs` | versions 1–14, byte mode, ECC M, same mask choice, same SVG |

Shared modules: `state.rs` (the cast state document and its transitions), `pairing.rs`
(`receiver.json`, pair codes, rate limit, `safeEqual`), `records.rs` (`cast-receivers.json`,
`receiverAddress`, `listReceivers`), `http.rs` (Range parsing, MIME, CORS, the server loop and the
JSON client), `media.rs` (blob windowing, cached-byte accounting, the HTTP→P2P splice),
`rpc.rs` (the newline JSON protocol on the Noise pipe), `disk.rs` (free space, the low-disk
reclaim, the peer-storage probe), `mdns.rs` (`peartube-cast` advertise/browse).

## Wire compatibility

Every route, method, status, header and JSON body the receiver answers with is the app's:

- `GET /`, `/tv`, `/cast/receiver` → the Smart TV page (`mode: "standalone"`).
- `GET|HEAD /cast/v1/p2p/<32 hex>` → windowed Range reads from the video core (no auth; the token
  is unguessable).
- `GET|HEAD /cast/v1/hybrid/<32 hex>` → the byte-exact HTTP→P2P splice for the active load.
- `GET /cast/v1/events` (bearer or `?token=`) and `GET /cast/v1/web-events?token=<webToken>` → SSE.
- `GET /cast/v1/info`, `GET /cast/v1/web-info`, `POST /cast/v1/web-state`, `POST /cast/v1/pair`,
  `POST /cast/v1/command` → JSON with the app's field order.
- Controller hub: `GET /tv`, `GET /v1/cast/web-events`, `GET /cast/v1/events`,
  `POST /v1/cast/web-register`, `POST /v1/cast/web-state`, `/cast/v1/{info,pair,command}`.

The Noise pipe is `{id, type, ...payload}` requests with `{id, ok, result}` / `{id, ok:false,
error}` replies and `{event:"state", state}` pushes; `info` is called as the handshake, `pair`,
`subscribe` and `cmd` are the receiver's verbs. Retryable dial codes are `HOLEPUNCH_ABORTED`,
`PEER_CONNECTION_FAILED`, `TRY_LATER`, `ERR_PRIVACY_UNAVAILABLE` plus an attempted `ETIMEDOUT`,
over a 90 s deadline, exactly like `connectNoiseRpc`.

Integration seams (`src/cast/traits.rs`): `CastPeer` (the P2P node), `PeerFactory`, `DhtConnect` /
`DhtServer` (peeroxide secret streams), `BlobServerFactory` / `LanBlobServer`. Nothing in the cast
port opens P2P sockets itself; `src/cast/adapters.rs` fills every seam for the real node and
`Backend::spawn` wires it as the `CastFactory`, while `tests/cast.rs` keeps the TCP stand-ins.

`src/cast/adapters.rs` is the app's wiring, one seam per JS call:

- `NodePeer` is `CastPeer` over `CoreNode`: the catalog's `find_entry`/`search`/`open_uri`, the
  blob core from `Node::blob_core`, the swarm's `join_peer`/`dial_lan_peer`, `evict_behind`,
  `clear_cache` and `warm`. `warm` takes the raw announcement, the shape `Node::warm` parses. Its
  `close` is a no-op: the backend owns that node and closes it with the worker.
- `NodeDht` dials with the node's own HyperDHT handle and key pair; `NodeDhtServer` is
  `dht.createServer` + `listen(keyPair)` — a peeroxide swarm bound to the receiver identity's Noise
  key that accepts the controller's dials. Both wrap a `SecretStream` in a pump onto a tokio
  duplex so the newline JSON RPC sees a byte pipe (`shortcut: one protocol frame per 64 KiB read
  chunk, no coalescing`).
- `CastBlobServers` binds one `byte_stream::BlobServer` on `0.0.0.0:0` with the default random
  token and 96 MiB window. Its resolver admits only `allow`ed blob keys, calls `Node::blob_core`
  for the swarm/ledger/ceiling side effects, then opens the request's own Corestore session, the
  way the app's `store.session().get()` does. `get_link` re-hosts the server's link on the LAN
  address the controller picked.
- `Factory::start_local_receiver` loads the receiver identity first so the DHT server has its
  seed, and binds the receiver on `RECEIVER_PORT` (`any_port` fallback) with mDNS on, exactly like
  `startLocalReceiver`. `PeerFactory` stays `None`: a load whose network differs from the current
  node reuses it instead of opening a second one.

## Deliberate differences

| JS | Rust | Why |
| --- | --- | --- |
| `state` is a plain object; `reportState` may leave `videoDecoder: undefined` | the state map keeps the key with `null` and `state::to_json` drops a null `videoDecoder` | JSON has no `undefined`; this keeps the key's position (before `droppedFrames`) and the app's bytes |
| `freeDiskBytes` via `statfsSync` | `df -kP <dir>` | `statvfs` needs `unsafe`, which the crate forbids; `df -kP` reports the same `bavail * 1024` |
| `resolvePeerStorage` probes with a RocksDB Corestore | probes with a hypercore-rs store and one appended block | the Rust backend's storage is hypercore-rs; the probe still proves lock+append works there |
| `steerReadAhead` keeps one download window per stream and retargets it every second | each range response opens one `byte-stream-rs` window sized by the same disk-aware arithmetic (`media::read_ahead_bytes`); eviction still runs every 15 s (3 s on a tight disk) through `CastPeer::evict_behind` | the window is per-response in the Rust port, so a started range is never retargeted mid-stream; see deferred pieces below |
| `serveWindowedStream` counts `core.download` blocks via `core.count` | `Media::cached_bytes` walks `core.has(i)` over the blob's block range, cached 3 s | hypercore-rs has no range count; the 3 s cache bounds the cost |
| `bonjour-service` browse removes a service by TXT `id` | `mdns-sd` `ServiceRemoved` carries only the full name, so the entry stays until the next resolve | `mdns-sd` cannot resolve a removed record; the stale row is only cosmetic in the picker |
| `interfaceAdapter`/`selfAddressAdapter` per host | one `mdns-sd` daemon per role, advertising the LAN IPv4 it was given | same simplification as the LAN port (see `docs/port-notes-lan.md`) |
| `os.hostname()` default receiver name | callers pass `name`; `ReceiverOptions::default()` uses `"PearTube TV"` | `std` has no `gethostname` without `unsafe`; the worker always passes the TV name |
| `worklet: true` shrinks the node's request windows | not modelled | that is the node's own option, passed through `PeerFactory` |
| hosted web receiver `pairCode` in `/cast/v1/web-register` is `OPEN` | same; `/cast/v1/pair` therefore falls through to the active browser | the app never exposes the random code, so the code match never fires there either |

## Deferred pieces

1. **Continuous read-ahead steering.** `steerReadAhead` retargets a stream's download window every
   second as the playhead moves. The port sizes a window when a response starts and leaves it; a
   later response opens a new window. Eviction and free-space trimming are ported. Wiring a
   background retarget needs a per-stream window handle from `byte-stream-rs`; when that lands,
   replace the per-response window.
2. **`holdIfThin`.** The player cushion (wait for 10 s buffered after 2 MiB, 2 s once cushioned,
   at most 20 s) is specified by `media::hold_deadline` and the `HOLD_*` constants but not wired
   into `windowed_response`; the bounded window already removes the worst stall pattern.
3. **LAN isolated HyperDHT for cast.** `network.lan`/`privateRouting` are forwarded to
   `PeerFactory`, which owns the node; the isolated LAN DHT itself is the LAN port's deferred half.
4. **`serveSpliceStream`'s 1 MiB cold HTTP window after the first splice error.** The port keeps the
   app's window, stall timeouts (3.5 s with an HTTP fallback, 8 s upstream) and mid-response
   fallback, but a retried HTTP slice always uses the extended window; the app shrinks it only
   while P2P is available. Same bytes, marginally different retry timing.
5. **`receiverPage` boot literal.** The page is copied verbatim from `cast-ui.js`; if that file
   changes, re-extract `src/cast/ui/receiver.html` (single `__BOOT__` placeholder).

## Tests

- `fixtures/cast-reference.json` is generated by `tests/js/cast-fixtures.mjs` from the app's own
  code: `parseHttpRange`, `guessMediaMime` and `receiverAddress` are lifted from `src/cast.js`;
  command and player-report transitions come from a real `createReceiver` over HTTP; pairing
  records come from a real `createCastController`. Regenerate with
  `node tests/js/cast-fixtures.mjs > fixtures/cast-reference.json`.
- `cargo test --test cast` runs the pure-function parity and the full receiver flow, including the
  `info`/`web-info` bodies, the pairing record, the saved file and the merged listing.
- `cargo test --test cast -- --include-ignored` additionally runs five tests (needs `node` and the
  app checkout via `PEARTUBE_APP`): the real `cast.js` controller drives this receiver over HTTP and
  over a TCP stand-in for the secret stream, this controller drives the real `cast.js` receiver both
  ways, and the committed fixture is regenerated and compared, so a stale reference fails the suite.
- `cargo test --test cast_default` runs the real stack: two `Backend::spawn` instances on one
  local HyperDHT testnet, a fixture published into the controller's storage, pairing over LAN with
  the receiver's code, `castPlay` (asserting `mode: "p2p"`), the receiver's player URL serving the
  source's exact bytes full and ranged, and the `castControl`/`castStop` transitions.
  `js_cast_receiver_is_driven_by_the_default_backend` (ignored) drives the app's own `cast.js`
  receiver through the same backend.
- 29 module unit tests sit next to the code (`cargo test --lib cast`); clippy is clean for
  `src/cast/**`, `tests/cast.rs` and `tests/cast_default.rs`.
