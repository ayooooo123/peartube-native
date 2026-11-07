# LAN discovery: port notes for `src/lan.js`

`lan.js` has two halves that must be ported together:

1. **mDNS** — advertise the little information peers need to dial each other (public key, DHT port,
   topic tokens) as a `hyperdht-mdns._udp` service, and browse for the same. Ported in
   `src/lan/mdns.rs` + `src/lan/mod.rs`.
2. **Isolated LAN HyperDHT** — a private, bootstrap-free DHT whose only addresses are the ones mDNS
   handed out. Not ported yet; the behaviour below is what the port must reproduce.

## mDNS half (ported)

Facts a reader of the Rust code needs but cannot see there:

- Service: type `hyperdht-mdns`, protocol `udp`, so `_hyperdht-mdns._udp.local.`.
- TXT keys: `v="1"`, `peerKey` (64 lowercase hex), `port`, `tc` (topic count **of this shard**), then
  `t0`, `t1`, … with up to `TOPICS_PER_TXT=5` tokens joined by `,`.
- A token is `base64url(SHA-256("hyperdht-mdns:" || topic))`, 43 characters, no padding.
- Sharding: tokens sorted, packed into shards while a shard stays within `MAX_SHARD_TOPICS=32`
  topics and `MAX_TXT_BYTES=900` TXT bytes (key + value + 2 per entry). In practice the byte budget
  binds first: 18 tokens fit (898 bytes), the 19th starts a new shard. With 5 tokens per key, the
  32-topic cap is unreachable, but it is still parsed and enforced.
- Instance name: `hyperdht-mdns-<peerKey[0..12]>-<generation>-<index>-<total>`, where `generation`
  is `base64url(SHA-256(token || "\0" for every sorted token))[0..8]` plus a 3-character random
  nonce, truncated to 11. The nonce exists because `bonjour-service` never reports a lost name: it
  logs and silently stops advertising, so no advertisement may reuse a name (not even across runs,
  since caches and reflectors can answer for it).
- `h` (only from `selfAddressAdapter`) carries the advertiser's LAN IPv4, so a peer can dial the
  address instead of a resolved/reflected one. The browse wrapper prefers `h` over resolved A
  records when it parses as IPv4.
- Rebrowse: the app restarts its browse every 10 s (`DEFAULT_REBROWSE`) on the same socket, to
  recover from dropped packets. The Rust port restarts `mdns-sd`'s query for the type instead, which
  re-asks and re-emits cached records.
- Refcounting: `BonjourAdapter` keeps a daemon open while any advertisement or browse is open, and
  shuts it down when the last one stops. The app has one socket per interface for the whole process
  (`interfaceAdapter(host)`); one `mdns-sd` daemon covers every interface, so `process_adapter` is a
  single process-wide adapter instead of a per-host map.
- `lanAddress`: first private IPv4 whose interface starts with `wl`, else `en`/`eth`, skipping
  internal and non-RFC1918 addresses, so VPN/cellular tunnels never win.

Deviations from the JavaScript, all deliberate:

| JS | Rust | Why |
| --- | --- | --- |
| `bonjour-service` adds an A record for every non-internal interface | the caller passes one LAN IPv4, used as the A record and as the host name (`<ip>.local.`) | `mdns-sd` needs explicit addresses; one address is all a LAN node needs |
| first IPv4 in `service.addresses` | addresses sorted, then the first | `mdns-sd` resolves into a `HashSet` |
| `referer.address` fallback in `serviceIPv4` | dropped | `mdns-sd` has no referer; TXT `h` covers the case the fallback was for |
| `Number()` coercion for `port`/`tc` | decimal parse (`""` → 0) | our own records are plain decimals; exotic numeric strings were never valid |
| TXT keys looked up by exact case | case-insensitive | RFC 6763 §6.4, and `mdns-sd` resolves/filters them case-insensitively |
| `browser.on('up')` may fire repeatedly per service | same (resolved events repeat on re-announcement); `down` carries the last resolved service | `ServiceRemoved` only carries the name; the app's `down` also carries the cached service |
| name probing is not done at all | `set_requires_probe(false)` | probing would rename the instance and lose the generation/shard suffix; the nonce already prevents collisions |

## Isolated LAN DHT half (not ported yet)

`LanDiscovery` in `lan.js` never touches the public network. In constructor order:

```
new HyperDHT({ host, port: port ? [port, port + 1] : [0, 0], bootstrap: [],
               ephemeral: false, firewalled: false, anyPort: !port, keyPair })
```

- **`bootstrap: []`** — nothing to bootstrap against, so `fullyBootstrapped()` resolves immediately.
  This is the whole "isolated LAN" trick: no rendezvous, no public nodes.
- **`port: [port, port + 1]`** — server socket on `port`, client socket on `port + 1`; both ephemeral
  with `anyPort` when no port was requested.
- **`fullyBootstrapped()`** — then `tuneUdxSocket` raises the server and client UDP buffers to
  8 MiB / 4 MiB / 2 MiB (first value the platform accepts), because Android's default 208 KiB
  `rmem_default` gags on 512 KiB LAN bursts.
- **`createServer(conn => …)` + `listen(keyPair)`** — accepts incoming encrypted streams; the port is
  read back from `dht.address()` (falling back to `localAddress()`) and becomes the advertised port.
- **`createServer`/`connect` are the only transport.** No `announce` is ever issued, even though
  `join(topic)` records tokens: topics are advertised **only** in mDNS TXT. A peer is dialled when it
  shares at least one token.
- **Dial ordering** — `connect(publicKey, { keyPair, nodes: [{host, port}], relayAddresses: [{host, port}] })`.
  The mDNS `host:port` is passed both as the DHT node list and as relay addresses.
  The peer with the **smaller** public key dials immediately; the larger one waits `dialAfter`
  (default 3000 ms) and dials only if still unconnected. That asymmetry is the one-way-firewall
  escape hatch: the blocked side never gets a packet in, but it can still dial out after the timeout.
- **Accept gating** — an incoming socket is dropped when no topic has been joined (unless the peer was
  added by `dialDirect`), and when the peer's shard set is complete and shares no token.
  `dialDirect(publicKey, host, port)` mounts a known peer by IP for networks where multicast is
  blocked.
- **Reconnect** — on close, the smaller key retries after 500 ms, the larger after `dialAfter`.
- **`stats()`** — `{ peers, connections, topics, direct }`.
- **`destroy()`** — clears timers, pending dials and connections, stops the browse and advertisements,
  closes the server, then `dht.destroy({ force: true })`.
- mDNS state is folded in here: `_peerShards` keyed by peer key with `generation`, `totalShards` and a
  shard→tokens map; a peer only exists once `shards.size >= totalShards` bookkeeping allows it, and a
  `down` for one shard only removes that shard (dialing is cleared only when the last shard goes).

### peeroxide API this half needs

| `lan.js` | peeroxide today | gap |
| --- | --- | --- |
| `HyperDHT({ bootstrap: [], host, port: [p, p+1], ephemeral: false, firewalled: false, anyPort })` | `HyperDhtConfig { dht: DhtConfig { bootstrap: vec![], host, port, ephemeral, firewalled, .. } }` — empty bootstrap is already the default | no `port + 1` pairing and no `anyPort`; needs either two handles or one config knob |
| `keyPair` / `HyperDHT.keyPair()` | `KeyPair::generate()` / `KeyPair::from_seed()` | — |
| `fullyBootstrapped()` | `HyperDhtHandle::bootstrapped()` | — |
| `dht.address().port` | `local_port()`, `remote_address()`, `noise_addresses4(port)` | — |
| `dht.io.serverSocket` / `clientSocket` | `server_socket()` / `listen_socket()` return the `UdxSocket` | buffer setters exist on `UdxSocket`; port must call them |
| `createServer(conn => …)` + `listen(keyPair)` | `register_server(&target)` + `server_sender()` emits `ServerEvent::PeerHandshake` / `PeerHolepunch` with reply channels; the accept/connect handshake is consumed by `peeroxide::swarm` | **missing**: a `listen(key_pair)` style API that yields an accepted `PeerConnection` (with `remote_public_key`) to a LAN caller that is not the full swarm |
| `connect(pk, { keyPair, nodes, relayAddresses })` | `connect_with_nodes(&key_pair, pk, &[Ipv4Peer] /* relay_addresses */, &UdxRuntime)`; `ConnectOpts` for the LAN shortcut | JS `nodes` (peers to query directly) has no counterpart; `relay_addresses` is the working equivalent here |
| `destroy({ force: true })` | `destroy()` | — |

So the LAN half is buildable on `peeroxide-dht` today except for the server accept loop: `lan.js`
needs "listen as this key pair and give me every accepted encrypted stream", while `peeroxide`
wraps that inside `swarm`. The port should either expose a minimal accept helper in `peeroxide-dht`
or reuse `peeroxide::spawn` with an empty bootstrap and the LAN addresses injected as peers — both
keep `announce` out of the picture, which is required: announcing a topic to an isolated DHT with an
empty routing table would be a no-op, and the mDNS record is the only rendezvous.
