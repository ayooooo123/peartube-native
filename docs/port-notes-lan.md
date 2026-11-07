# LAN discovery: port notes for `src/lan.js`

`lan.js` has two halves that must be ported together:

1. **mDNS** — advertise the little information peers need to dial each other (public key, DHT port,
   topic tokens) as a `hyperdht-mdns._udp` service, and browse for the same. Ported in
   `src/lan/mdns.rs` + `src/lan/mod.rs`.
2. **Isolated LAN HyperDHT** — a private, bootstrap-free DHT whose only addresses are the ones mDNS
   handed out. Ported in `src/lan/discovery.rs`.

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
  shuts it down when the last one stops. The adapter itself is **per `LanDiscovery`**
  (`BonjourAdapter::new()` in `LanDiscovery::open`), not process-wide: `mdns-sd` never reports a
  daemon's own registrations to its own browse, so one shared daemon would hide two nodes in one
  process from each other. The app never hits this because each of its nodes is its own process.
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

## Isolated LAN DHT half (ported)

Ported in `src/lan/discovery.rs`, with the `LanFactory` `worker.js` uses in `src/lan/mod.rs`. The
`lan.js` behaviour below is the reference; where the Rust port deviates it says so.

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

The Rust port reuses `peeroxide::spawn` with an empty bootstrap and the node's own keypair
(`Network::key_pair()`), exactly as `lan.js` shares the swarm's key pair: the DHT server is registered
by joining one server-only topic that can never be announced (the routing table is empty), inbound
streams arrive on the swarm's connection channel, and outbound dials go through
`swarm.dht().connect_with_nodes(..., &[Ipv4Peer { host, port }], ..)` with `establish`d secret
streams wrapped in a `protomux::Mux` and handed to `LanBinding::on_connection`.

`Network::join_overlay_topic` and `join_content` register their LAN tokens synchronously,
independently of the asynchronous public swarm join. An unreachable public bootstrap must not
delay same-LAN discovery; `network_private::public_routing_still_joins_its_topics` checks this
without yielding to the public join task, and `lan_interop` checks the publisher/follower topic.

The isolated DHT binds the configured host, as JS does; a loopback host must not silently become
a wildcard listener. The JS LAN interop fixture uses the worker's `interfaceAdapter(host)` so
loopback multicast stays on loopback too. A macOS firewall denial for unsigned test executables
on a real interface is an environment block, not a reason to change firewall policy.

Advertisement updates serialize the token snapshot, publish and replacement as one operation,
matching JS's `_adQueue`; otherwise an old empty-token publication can overtake a topic update.
`advertisement_updates_cannot_overtake_each_other` blocks the first host publication and verifies
the later generation cannot pass it.

Deviations from `lan.js`:

- no `port`/`port + 1` socket pairing: peeroxide binds one ephemeral socket pair per DHT; the LAN
  case never asks for a fixed port, so no caller needs the pairing.
- no `tuneUdxSocket` buffer tuning: libudx's socket buffers stay at the platform defaults.
- a handshake-accepted connection carries no topic list; the accept gate reads the tokens the mDNS
  bookkeeping recorded, which is what `_onIncoming` consults as well.
- both ends may dial each other at once (`dialDirect` on the receiver, the smaller key on mDNS);
  the connection the smaller key initiated is canonical on both sides and the other is destroyed,
  which the other end sees as a close. This leans on three stack facts the JS stack gets for free:
  protomux-rs holds a remote open that lands before `pair()`/`open_channel` instead of rejecting it
  (JS attaches its handlers before the first frame is parsed); peeroxide's server- and client-side
  UDX stream ids come from one sequence (libudx keys a socket's streams by local id alone); and a
  `SwarmConnection`'s `slot` is kept with its stream, so a closed connection frees the peer's entry
  and its redial gets a stream instead of a reply nobody answers.
- a Rust publisher answers a wakeup lookup with its drained, attached writers (JS
  `_getWakeupWriters`); a Rust follower starts downloading the bootstrap core from that announce.

## Multicast receive regression and environment evidence (2026-11-07)

`cargo test --lib independent_daemons_receive_each_others_loopback_advertisements -- --ignored`
checks actual PTR/SRV/TXT/A resolution between independent adapters, including topic parsing
and the self-address wrapper. It requires working multicast even on loopback.

On this Mac's `en0` (`10.0.10.210`), a standalone `mdns-sd 0.21.7` daemon joined the IPv4
group and advertised successfully but cached no received records. A separate minimal
`socket2`/`std::net::UdpSocket` probe, with no PearTube or mDNS parser, also received no
multicast, including its own sends. On port 48240 it received a Node sender's loopback
unicast but neither LAN-address unicast nor multicast; on 5353 it received none. Probe
sources are in the stack's `_scratch/lan-sol-probe/`.

ALF was enabled. `socketfilterfw --getappblocked` reported “permitted” for the exact
`peartube-native/target/LanSol/debug/raw` and `debug/lan-sol-probe` paths, but neither path
appeared in `--listapps`. This proves a receive block outside PearTube's adapter; it does
not identify ALF, local-network privacy, or another OS policy as the specific cause.
No firewall settings were changed. The new focused multicast test also timed out here;
loopback is not a substitute for the required real-interface a/b/c three-run matrix.

With the final code, real-interface cases a, b and c each timed out (c reported zero
discovered peers and connections). The b three-run command stopped after its first
failure; none is reported as a three-run pass. Plain `cargo test` passed 168 tests
(44 ignored), and warnings-denied all-target Clippy passed on Rust 1.96.1.
The session's `target/LanSol` build directory was removed after recording evidence.
