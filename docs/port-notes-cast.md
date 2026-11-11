# Cast: port notes for `src/cast.js` and `src/cast-control.js`

A cast runs over the peer connection the two nodes already share. The control channel is a
Protomux protocol paired on that connection (`src/cast-control.js`); there is no separate Noise
stream, no info handshake, no browser receiver, no thin mode and no HTTP video path. Receivers
advertise mDNS TXT `v=2` with the LAN DHT port (`lp`); a v1 receiver is listed but not current.
The receiver's HTTP is `/cast/v1/p2p/<token>` and `/cast/v1/info` on `127.0.0.1` only.

| JS | Rust | Notes |
| --- | --- | --- |
| `cast-control.js` `serveControl` / `openControl` | `src/cast/control.rs` | the channel, its `c.json` frames, request ids and state pushes |
| `createReceiver` | `src/cast/receiver.rs` (`Receiver::start`) | identity, the channel on every connection, the loopback player port, mDNS, media |
| `createCastController` | `src/cast/controller.rs` (`CastController::start`) | mDNS discovery, pairing, the cast session over the peer connection |
| `receiverKeyPair` | `src/cast/pairing.rs` `receiver_key_pair` | the seed in `receiver.json` becomes the receiver node's key pair |
| `qr.js` `qrSvg` | `src/cast/qr.rs` | versions 1–14, byte mode, ECC M, same mask choice, same SVG |

Shared modules: `state.rs` (the cast state document and its transitions), `pairing.rs`
(`receiver.json`, pair codes, the rate limit, `safeEqual`), `records.rs` (`cast-receivers.json`,
`pairedWith`, `receiverTarget`, `listReceivers`), `http.rs` (Range parsing, MIME, CORS, the
loopback server), `media.rs` (blob windowing, cached-byte accounting, the windowed P2P response),
`mdns.rs` (`peartube-cast` advertise/browse), `disk.rs` (free space, the low-disk reclaim, the
peer-storage probe), `traits.rs` (the seams the host fills).

## The channel

One Protomux protocol, `peartube-cast/v1`, one channel per connection (a second open is refused),
one message type whose payload is `c.json` — a `compact uint` length followed by UTF-8 JSON, byte
for byte what the app writes.

- Sender → receiver: `{id, type: 'pair' | 'subscribe' | 'cmd', ...payload}`.
- Receiver → sender: `{id, ok, result | error}`, and `{event: 'state', state}` once subscribed.
- A message that is too long (> 64 KiB), not JSON, not an object, or broken is dropped; a decoder
  never throws, because Protomux closes the whole connection on a throwing handler.
- Requests are handled in order of arrival, each on its own task; a reply is sent as
  `{id, ok: true, result}` or `{id, ok: false, error}` with the handler's message.
- Backpressure: the app's `message.send` always writes and reports `drained`; `protomux-rs`'s
  `send_raw` refuses while the transport is above its high-water mark. The port waits on
  `Mux::drained` and retries, so a request or reply is never dropped; state pushes keep the
  app's rule (skip while backpressured, push the newest state once the connection drains).

Pairing rules are the receiver's, exactly as in the app:

- `pair` with the 6-character code answers `{deviceId, name, publicKey, token}`. Wrong codes count
  per link kind (LAN and everything else apart), six inside 15 s lock that kind out.
- An open receiver (`openAuth`) on a **LAN** link is trusted: `pair` needs no code and answers an
  **empty** token, never its permanent pair token; commands from that link need no token.
- Every other request needs the pair token (`Unauthorized; pair with this TV first`).
- `subscribe` registers the channel for state pushes and answers `{state}`; `cmd` applies a
  command and answers `{ok: true, state}`.

## The receiver

`Receiver::start` loads `receiver.json` (creating it on first run), joins the LAN topic
`hash('peartube-cast/v1')`, serves the channel on every connection the node has and will have,
binds the loopback player port (any-port fallback) and advertises mDNS:

```
v=2, proto=peartube-cast, id, name, h, pk, lp=<LAN DHT port>, auth=open|code
```

There is no `p2p` key and no receiver HTTP port in the record: a sender dials the node's LAN DHT
port, or joins it through the DHT. Like `refreshMdns`, a receiver with no LAN host advertises
nothing (the app's `h: '127.0.0.1'` fallback belongs to an adapter a host injects); one whose
LAN DHT did not start but that has a host advertises `lp=0` and the service port `RECEIVER_PORT`.
`info()` carries `protocol`, `deviceId`, `name`, `publicKey`,
`host`, `port` (the loopback player port), `authRequired`, `routes`, `storageFreeBytes`,
`externalStorage`, `state`, `pairCode` (`OPEN` when open) and `pairingUri`/`qrSvg`.

The loopback server answers the player and nothing else:

- `GET|HEAD /cast/v1/p2p/<32 hex>` — Range reads of a remembered load through that stream's one
  sliding download window (no prefetch per response).
- `GET /cast/v1/info` — the telemetry document (`deviceId`, `name`, `routes`, `state`).
- anything else — `404`.

A load keeps the last four streams addressable by token; the player's URL is
`http://127.0.0.1:<port>/cast/v1/p2p/<token>`. A reader past its first 2 MiB is held while the
film's cushion ahead is thin (2 s of it, 10 s before that reader's first cushion, for at most
20 s), which is the app's `holdIfThin`.

## The controller

`CastController::start` reads `cast-receivers.json`, browses mDNS for `_peartube-cast._tcp`,
merges the discovered receivers with the saved pairings (`listReceivers`, with `current` and
`transport`), and drives one session at a time. A session opens the controller's node to the
receiver (`connectPeer`: the open connection, else a LAN dial and a DHT join), opens the channel on
it, subscribes, and sends `load` (20 s) or `status` (10 s). The session records its transport
(`lan` or `p2p`), and the receiver's state pushes update it until it closes.

`startSession` retries once, on the new connection, when the connection closes during setup
(`Receiver connection closed`); the retry keeps the attempt its first try started with, so a newer
cast, attach or stop issued meanwhile wins and the retry refuses to install its connection.
`connectReceiver` itself opens one connection and one channel on it: a connection that is already
gone is reported, not redialed. A `stop` resets the receiver's state as `updateState(idleCastState())`
does, spreading the idle fields over the old state (the player's `videoDecoder` and
`droppedFrames` survive it). When the channel closes under the active session, the controller
re-attaches to the TV on the link that is left.

Pairing: by a `peartube://receiver/<64-hex key>?token=…` link (the token is trusted as given), or
by the 64-hex key plus the code. A discovered receiver contributes its LAN address, display name
and `current`; a v1 receiver is refused with `This receiver runs an older PearTube: update it`.
The saved key wins over mDNS, which is unsigned.

`castPlay` resolves the entry, warms the film when this device holds it (the TV then fetches from
this node over their connection), and sends the load with the watch link plus its channel. A
receiver without a token, and not open, is refused before connecting:
`Pair with <name> using its 6-character TV code before casting`.

The relay (`bin/relay.js`) opens a controller with its node, gets `getNode` from the API's live
node and `getLanHost` from the relay's LAN address (the `lanHost` setting, else the detected
Wi-Fi/Ethernet IPv4), passes `onchange` through as the `cast` event, and calls `setLanHost` when
it opens and when `lan`/`lanHost` change. The controller is mounted as the API's `state.cast`, so
`/v1/cast/{status,pair,receivers/:id,play,control,stop}` answer while it is open and 404 between
opens, and a stop or reopen closes it before the node it drives.

## Integration seams

`src/cast/traits.rs` narrows the host to what the app's node API offers:

- `CastPeer` — `peerKey`, `privateRouting`, `hasLan`, `lanEndpoint`, `status`, `joinLanTopic`,
  `peerConnections`, `onConnection`, `connectPeer`, `openUri`, `blobCore`, `clearCache`,
  `evictBehind`, `findEntry`, `search`, `warm`, `close`.
- `PeerConnection` — `id`, `remotePublicKey`, `isLan` (`node.isLanConnection`), `destroyed`, `mux`
  (`Protomux.from(conn)`), `closed` and `close`.

`src/cast/adapters.rs` fills them for `CoreNode`: `NodePeer` wraps the node's catalog, blob cores,
connection set, LAN topic and cache trimming; `Conn` wraps a `ConnectionHandle`'s mux;
`Service`/`Factory` are the worker's `Cast`/`CastFactory`.

Node hooks added for this port:

- `createNode({ keyPair })` — `{ publicKey, secretKey }` hex (or the 64-hex seed), so a receiver's
  node runs under `receiverKeyPair(castStorage)` and senders find it after a restart.
  `Network::open_with_key` carries it; `Network::open` stays as the no-key form.
- `Network::join_lan_topic(id, topic)` — the LAN-only topic, re-joined when the LAN binding is
  replaced (`setLan`), never announced on the DHT.
- `isLanConnection`, `peerConnections` and `onConnection` already existed on the node; the cast
  adapter reads them directly (`node.net()`), so `Node` needed no new trait method.

## Deliberate differences

| JS | Rust | Why |
| --- | --- | --- |
| `steerReadAhead` keeps one `core.download` window per stream, retargeted every second | the same: `Media::steer_window` holds one unpolled `Core::download` range per stream, retargeted by the receiver's one-second `steerStreams` tick with the same step rule; eviction still runs every 15 s (3 s on a tight disk) through `CastPeer::evict_behind` | hypercore-rs cancels a range when its future drops, so the range is held unpolled and replaced only when the playhead or the window end left its step |
| `serveWindowedStream` counts `core.download` blocks via `core.count` | `Media::cached_bytes` walks `core.has(i)` over the blob's block range, cached 3 s | hypercore-rs has no range count; the 3 s cache bounds the cost |
| `holdIfThin` pauses the response for a cushion | the same (`media::hold_for`): a reader past its first 2 MiB waits while 2 s (10 s before its first cushion) of film are not downloaded ahead, up to 20 s, then resumes | the response loop awaits the cushion instead of pausing a stream; a disconnected player ends the wait |
| `steerReadAhead`'s cache trim counts `cachedAhead` in bytes | `read_ahead_bytes` reserves 4/3 of new cached bytes | the storage engine gates blob relocation at 25% garbage, avoiding live-video rewrites while bounding retained garbage |
| `state` is a plain object; `reportState` may leave `videoDecoder: undefined` | the state map keeps the key with `null` and `state::to_json` drops a null `videoDecoder` | JSON has no `undefined`; the app's bytes and key order are preserved |
| `freeDiskBytes` via `statfsSync` | `df -kP <dir>` | `statvfs` needs `unsafe`, which the crate forbids; `df -kP` reports the same `bavail * 1024` |
| `resolvePeerStorage` probes with a RocksDB Corestore | probes with a hypercore-rs store and one appended block | the Rust backend's storage is hypercore-rs; the probe still proves lock+append works there |
| `bonjour-service` browse removes a service by TXT `id` | `mdns-sd` `ServiceRemoved` carries only the full name; the browse keeps a fullname→id index to remove the row | the index reproduces the app's removal |
| `interfaceAdapter`/`selfAddressAdapter` per host | one `mdns-sd` daemon per role, advertising the LAN IPv4 it was given | same simplification as the LAN port (see `docs/port-notes-lan.md`) |
| `os.hostname()` default receiver name | callers pass `name`; `ReceiverOptions::default()` uses `"PearTube TV"` | `std` has no `gethostname` without `unsafe`; the worker always passes the TV name |
| Physical disk reserve | read-ahead reserves 4/3 of new cached bytes and eviction budgets four rather than three logical windows | the storage engine gates blob relocation at 25% garbage, avoiding live-video rewrites while bounding retained garbage |
| Receiver-only cache writes | only `App::open_receiver_node` passes `storageWal: false`; ordinary app and writer nodes retain the WAL | re-fetchable verified blocks need one flash write rather than WAL plus blob writes |

## Not ported

- The app's `#1`/`#2` cast e2e scenarios around a 3rd peer seeding and private-routing casts are
  covered by the JS suite; the Rust tests cover pairing, the control channel, the data path and
  both interop directions.
