# Node contract

`node::Node` is a trait, not an implementation. Its associated native handle and stream types let the actual backend expose its real Corestore, Autobee, swarm, DHT, core, connections, LAN and blob server without serializing them or inventing placeholder handles. Async methods let implementations coordinate storage and network work; synchronous JS members become async where acquiring a Rust state lock may be necessary. The implementation must preserve JS defaults and errors, including resumable `put` sources: the factory receives the already-written byte offset, progress is reported, and the source must be able to update expected size and hash checkpoints while running (`src/acquire.js:219-259`). A checkpoint is `{ state, bytes }` where `state` is base64 of libsodium's 104-byte `crypto_hash_sha256_state` (eight little-endian words, a bit count, a 64-byte partial block) and `bytes` is the plaintext prefix it covers; `put` offers one only after the core has committed that many bytes, so a resumed ingest imports it when `hashBytes <= core byteLength` and re-reads only the gap. Native handles and callbacks are not JSON. `onchange` is a coalescing watch signal, corresponding to the node's `onchange` callback (`src/index.js:87-90`); it is not the worker output queue. Worker replies and fatal events must not be dropped.

All `Node` and `PutSource::open` futures are `Send`, so consumers may await them in spawned tasks.

Line references below are relative to the JavaScript PearTube repository. The definition of every member is `src/index.js:247-318`. `—` means no call in the six requested consumers, not that the member is unused elsewhere. `void` is JS `undefined`, not JSON `null`. JSON field names are case-sensitive. Values passed through as `Value` are deliberately open JS objects; their nested types are defined by their originating APIs, not invented here. Private keys, recovery phrases, epoch keys and access URIs must never be logged.

The worker's integration contracts are `NodeFactory`, `Swarm`, `LanFactory<N>`, and `ApiServer<N>` in `node::api`. `NodeFactory::create` receives the createNode options and returns an `Arc` of its concrete node type; `Node` is not object-safe. `Swarm` suspends and resumes its native handle. `LanFactory` supplies interface addresses and LAN bindings and receives host Bonjour events. `ApiServer` starts the loopback API, forwards notifications, and closes it; its source callback receives the current source list.

| Member | Consumer call sites | Arguments → return |
|---|---|---|
| `store` | `src/cast.js:1488`, `src/hiverelay.js:467,473` | Corestore native handle; supports `session()` and `get()` |
| `findEntry` | `worklet/worker.js:286`, `src/cast.js:1961,1965` | key string → decoded operation object or null |
| `blobCore` | `src/mirror.js:354,531` | hex key → native Hypercore handle |
| `seedableCores` | `src/hiverelay.js:561` | `{includeCached?: boolean}` → `[{key:string,kind:"channel"\|"blobs",length:number,byteLength:number}]` |
| `privateRouting` | `worklet/worker.js:324`, `src/cast.js:920,2007`, `src/hiverelay.js:317` | boolean |
| `bootstrap`, `relayThrough` | `src/cast.js:921-922,2011-2012` | configuration value or null |
| `hasLan` | `worklet/worker.js:123`, `src/cast.js:923,2013` | → boolean |
| `lanEndpoint` | `src/cast.js:1067,2008` | → null or `{host:string,port:number,publicKey:string}` |
| `dialLanPeer` | `src/cast.js:998,2058` | LAN endpoint → void |
| `peerKey` | `src/cast.js:1246,2006` | hex string |
| `dht` | `src/cast.js:1270-1271,1851-1852` | native DHT supporting `createServer` and `connect` |
| `joinPeer` | `src/cast.js:999,2055,2059,2085` | hex key, `{lanFirst?:boolean}` → void |
| `evictBehind` | `src/cast.js:761` | blob key, blob, `{playheadByte?:number,keepBehindBytes?:number,maxBytes?:number}` → reclaimed bytes number |
| `refreshBlockCount` | `src/mirror.js:378` | native core → void |
| `channel` | `src/hiverelay.js:322` | native Autobee; local writer key pair is needed for the relay publisher derivation |
| `swarm` | `worklet/worker.js:145-146,400,407`, `src/cast.js:1852`, `src/hiverelay.js:364,458,469,526,535` | native swarm, including updates, suspend/resume, relay settings, topic and peer joins |
| `peerConnections` | `src/hiverelay.js:455,528,540` | → native live connection handles |
| `onConnection` | `src/hiverelay.js:454` | connection callback → unsubscribe handle |
| `search` | `src/http.js:236,243,355,368,502`, `src/mirror.js:93,491,493`, `src/cast.js:1967` | nullable id, `{source?:string|null,query?:string|null,limit?:number,offset?:number}` → array of Entry below |
| `put` | `src/acquire.js:213,237`, `src/mirror.js:103` | metadata `{id,title,private?,epoch?,store?,expectedSize?,hashState?,hashBytes?,storeKey?}`, stream or offset-based stream factory, progress callback → Announce below; `expectedSize` can be null |
| `dropStore` | `src/acquire.js:279` | name string → void |
| `append` | `src/acquire.js:267`, `src/mirror.js:104,110` | decoded operation object → void |
| `publish` | — | metadata, stream → Announce plus `{key,channel,streamUrl,magnetUrl}` strings |
| `publishChannel` | `src/http.js:278` | metadata or announce, optional stream → same shape as `publish` |
| `remove` | `src/http.js:249` | entry key → void |
| `publishSource` | `src/http.js:276`, `src/mirror.js:116` | `{sourceKey,title}` → `{type:"source",sourceKey,kind:"channel",title}` |
| `syncLocalChannel` | `src/mirror.js:174` | → `{backfilled:number,total:number}` |
| `rechunkLegacyBlobs` | `src/mirror.js:187,742` | `{id?,foreground?,limit?,onMigrate?,onProgress?}` → `{total,migrated,deduped,remaining,migratedBytes}` numbers; progress adds nullable `currentId,currentTitle` |
| `unpublishSource` | `src/http.js:283` | source key → `{unpublished:true,key:string}` |
| `publishedSources` | `src/http.js:256` | → `[{key:string,kind:"channel",title:string}]` |
| `addSource` | `worklet/worker.js:292`, `src/http.js:261` | URI string or `{uri?\|key?,kind?,alias?}`, optional `{wake?:boolean}` → Source |
| `removeSource` | `worklet/worker.js:297`, `src/http.js:268` | key → `{removed:boolean}` |
| `listSources` | `worklet/worker.js:270,293,298`, `src/http.js:256,262-263,269-270`, `src/mirror.js:492` | → array of Source |
| `focusSource` | — | key → boolean |
| `setPlayerActive` | — | boolean → `{ok:true}` |
| `activeSourceCount`, `overlayTopicCount` | — | → number |
| `discoveredSources` | `worklet/worker.js:264,276,293,298`, `src/http.js:256` | → `[{key:string,kind:"channel",title:string,via:string}]` |
| `openUri` | `src/http.js:290`, `src/cast.js:1959` | URI string, `{joinSource?:boolean}` → `{action:"source",source:Source}` or `{action:"access",channel:string,epoch:number,source:Source}` or `{action:"watch",entry:Entry}`. The reference dropped `joinSource`: a watch link joins its channel's swarm, and follows the channel only when the node private-routes |
| `warm` | `worklet/worker.js:287`, `src/mirror.js:111`, `src/cast.js:1988` | announce → void |
| `identity` | `src/http.js:198,204,210,215,222` | `{revealMnemonic?:boolean}` → Identity below |
| `grantWriter` | `src/http.js:202` | writer, `{role?:string,alias?:string}` → `{granted:true,writer,role,alias}` |
| `revokeWriter` | `src/http.js:208` | writer, `{purgeEntries?:boolean}` → `{revoked:true,writer,purgeEntries:boolean}` |
| `claimWithMnemonic` | `src/http.js:220` | phrase, `{alias?:string}` → `{claimed:boolean,reopenNeeded:boolean,channel:string,writer?:string}` |
| `rotateEpoch` | `src/http.js:213` | → `{epoch:number,epochKey:string,accessUri:string}` |
| `setAllowedPeers` | — | array of hex strings or null → void |
| `clearCache` | `src/http.js:297` | key or null, `{keepBlobsHex?:string|null}` → `{clearedBytes:number,reclaimedBytes:number}` |
| `setCacheMaxBytes` | `worklet/worker.js:157,250` | bytes number → normalized bytes number |
| `enforceCacheCeiling` | — | `{keepHex?:string|null}` → `{evicted:number,cachedBytes:number}` |
| `gcStorage` | `src/mirror.js:381` | `{full?:boolean,streaming?:boolean}` → `{beforeBytes:number,afterBytes:number,reclaimedBytes:number}` |
| `status` | `worklet/worker.js:197,416`, `src/http.js:165,255`, `src/mirror.js:637`, `src/cast.js:1080,1355`, `src/hiverelay.js:492` | → Status below |
| `streamUrl` | — | announce → string |
| `httpViewing` | — | → boolean |
| `markBulkPeer` | — | hex key, boolean → void |
| `blobServer` | `src/http.js:116,120,373,429,438,449` | native server with `host` and `port` |
| `setLan` | `worklet/worker.js:128` | native LAN factory or null → void |
| `lanStats` | `worklet/worker.js:259` | → LAN stats object or null |
| `close` | `worklet/worker.js:68-81` | → void; idempotent |
| `closing` | `src/acquire.js:283,293`, `src/cast.js:919` | boolean |

`has` and `ingestStream` are **not** members returned by `createNode`: they are mirror plugin-context methods (`src/mirror.js:85-106`). `has` also considers active acquire jobs, then searches the node; `ingestStream` calls `put`, then `append`, then signals change. Their owner must implement those semantics in the mirror adapter rather than introducing misleading node members.

## Shared JSON shapes

* `Source`: `{key:string,kind:"channel",alias:string,primary:boolean,writable:boolean,entries?:number,active?:boolean,hydrated?:boolean}`. The last three occur on followed sources in `listSources`, not on primary or `addSource` results (`src/node/sources.js:302-307,324-349`).
* `Blob`: `{blockOffset:number,blockLength:number,byteOffset:number,byteLength:number,...}`; extensions such as `blockMap` are retained. `Announce`: `{type:"announce",id:string,title:string,size:number,sha256:string,blobs:string,blob:Blob,epoch?:number,wrappedKey?:string,...}` (`src/node/blobs.js:855-876`, `src/protocol.js:170-176`).
* `Entry`: `{key,id,title,sha256,source,sourceKind,seriesKey,baseTitle,streamUrl,magnetUrl:string,size,cachedBytes,epoch:number,local,private,locked:boolean,season,episode:number|null,year:string|null}` (`src/node/catalog.js:130-164`, `src/protocol.js:24-34`). All fields are present; year is a string even though it looks numeric.
* `Status`: `{tracker,channel,writer,blobs:string,writable:boolean,blobBytes,cachedBytes,cacheMaxBytes,peers,lanPeers,downBps,upBps,rxBytes,txBytes,inflightBlocks,rtt,retransmits:number,routes:{private:Route,public:Route,lan:Route},sources:Source[],reciprocity:{tier:string,seededBytes,contributingPeers,ratio,inflightMax:number}}`. Each `Route` is `{peers,rxBytes,txBytes,downBps,upBps:number}` (`src/index.js:12-20,131-171`). `Number` preserves integer versus fractional JSON values.
* `Identity`: `{channel:string,writer:string,role:string|null,writable:boolean,mnemonicSet:boolean,mnemonic?:string,writers:[{key:string,role:string,alias:string,self:boolean}],epoch:number,epochKey?:string,accessUri?:string}`. Mnemonic is only included on explicit reveal; epochKey and accessUri only when manageable (`src/node/identity.js:83-98`).

## Worker events

The worker emits `{event:"update"}` (`worklet/worker.js:51`), `{event:"sources",sources:[{key,kind,alias}]}` (line 260), `{event:"cast",cast:<cast.status()>}` (line 56), `{event:"receiverCommand",command:<command>}` (line 349), and `{event:"fatal",error:string}` (lines 357,385). Cast status has `{webReceiverUrl:string|null,receivers:array,session:object|null,receiverMode:object|null}` (`src/cast.js:2198-2210`); command is the command object plus `seq`, `action`, and sometimes `positionMs` (`src/cast.js:1137`). These are worker/cast events, not all node-originated; do not conflate their transport with `onchange`.

## Worker adapters

`app::App` is `worklet/worker.js`'s port as a typed service: one public async method per worker method, with serde-derived request and reply types, and an `AppEvent` stream (`node::api::WorkerEvent`) carrying the same coalescing, timer and suspend behaviour. `lifecycle` is the thin JSON adapter over it — it decodes a request line into the method's request and serializes the reply — and `backend::Backend` runs that pair on its own thread. `Backend::spawn` keeps the FFI surface (`send`, `suspend`, `resume`, `shutdown`, all synchronous; `Backend` is `Send + Sync`) and reports through the per-line callback; `Backend::spawn_with` takes the same plus `backend::Adapters`, so nothing that opens a socket, binds a port or reads a clock is compiled into the app.

A host that links the backend in-process (`app::AppHost`, over `app::NativeApp`) calls the same methods directly: no line is serialized, replies are the typed structs, and `subscribe` maps the events into the host's own types.

| Adapter | Role |
|---|---|
| `NodeFactory` | `createNode`: the options object below, minus callbacks; the node exposes `onchange()` instead. |
| `Clock` | `Date.now()` and the timer deadlines: the two polling intervals, the `update` coalescing delay and the suspend linger all schedule on it. A host test advances a fake clock instead of sleeping. |
| `Swarm::on_update` | `swarm.on('update', …)`; the returned closure removes the listener. Also used for the `update` coalescing tests. |
| `LanFactory` | `os.networkInterfaces()` (name, address, internal), `createLan`, and the host's Bonjour events (`Bare.emit('appEvent', …)`). |
| `ApiServer` | `createApi` plus `listenLoopback`: `start` binds the loopback port (preferring `protocol::API_PORT`, then an ephemeral one) and answers with the URL, or `null`; `notify` forwards the `update` and `cast` notifications. |
| `CastFactory` / `Cast` | `createCastController` and the receiver it drives, plus `resolvePeerStorage` and `reclaimLowDiskReceiverStorage`. `CastOptions` carries the `getNode` and `getLanHost` closures and the `onchange` sink. |

Every asynchronous member of `Node`, `PutSource`, `NodeFactory`, `Swarm`, `LanFactory`, `ApiServer` and `CastFactory` is declared as `fn name(...) -> impl Future<Output = T> + Send`, not `async fn`, so a caller can `tokio::spawn` a node call on the backend's runtime. Implementations may still write `async fn`; rustc then requires that future to be `Send`. `Node` is `Send + Sync + 'static` and `Node::Swarm: api::Swarm`. `Cast` is the one exception — it is used behind `Arc<dyn Cast>`, so its asynchronous members return `BoxFuture` instead. `ApiServer::notify` is synchronous: it only queues an event for the API's stream.

The app owns the `castStorage` sent by `init`, the last settings a `start` was given, the LAN note and the update coalescing timer. Inputs are handled one at a time on the backend thread, in arrival order, so `start`, `stop` and the LAN moves can never overlap — the guarantee the JavaScript worker gets from `serial()`. Request lines and host events may arrive in one chunk or split across chunks; the framer keeps the partial line.

`CastOptions::get_lan_host` answers with the device address the worker last sampled, because the interface list is asynchronous; the cast controller reads it between LAN polls.

## Parity harness

`tests/ipc-parity.mjs` runs the real `worklet/worker.js` under node with scripted `createNode`, `createApi`, `createLan`, `createCastController` and `bare-os` modules (redirected by `tests/js/ipc/hooks.mjs`, with a fake clock), drives `tests/js/ipc-scenario.json`, and prints the lines the worker wrote. `tests/ipc.rs` drives the same scenario through `Backend::spawn_with` with fakes that answer identically and compares the two streams line for line. `cargo test --test ipc -- --include-ignored` runs it; it needs node and a PEARTUBE_APP checkout (defaults to the sibling `../peartube`).

## The native node

`src/node/{storage,network,channel,sources,catalog,core,factory}.rs` port `src/node/{storage,network,channel,sources,catalog}.js` and `src/index.js`. `core::create_node` opens them in the reference's order (storage, identity, channel, blob store, swarm, sources, blob server, catalog) and `core::CoreNode` implements `Node`; `factory::Factory` is the default `NodeFactory`, so `Backend::spawn` now runs a real node (previously it returned "Native node factory is not configured"). `Backend::spawn_with` is unchanged.

Deliberate deviations from the JavaScript:

* **Storage tuning.** `createStorage`'s `hypercore-storage` options (write buffer, caches, pinning) are not configurable through the Rust Corestore, so the engine's own defaults apply; the worklet flag still selects the mobile request window.
  `storageWal` defaults to true; only the receiver-owned cache node passes false.
  Ordinary app nodes and writer-owned stores retain WAL durability. A WAL-free
  receiver cache may lose its unflushed tail on a crash and re-fetch it from peers.
* **Swarm.** Topics, joins, content announcements, replication fan-out (`Corestore::replicate` on every accepted connection) and the channel/corestore wakeup sessions match the JS.
  A video's topic is announced only for videos this node published (and, with `announceCache`, its complete cached copies, 48 at a time rotating every 20 s); a title being read is looked up as a client, four at a time, and never announced.
  `privateRouting` runs `create_private_swarm` like `src/node/network.js`: channel discovery becomes `joinSource` on the overlay, `joinPeer` dials through the guards, content topics stay unannounced, and LAN bindings are refused. If the overlay cannot start the node fails closed (`Network::can_announce`), joining no public or LAN topic and attributing nothing to the private route.
  Inner-stream duplicate selection and active-peer registration are atomic, matching one JS
  connection callback; replacement close callbacks run after that state is published.
* **`isSourcePeer`.** `peersBusy` accepts a `peartube/bulk-busy` claim only from a peer that served this node blocks. hypercore-rs reports a download without its peer (`CoreEvent::Download { index, byte_length }`), so the node has no evidence to answer with: the gate is built with the predicate optional and runs without one, counting every claim as before, until the engine attributes a block to a peer.
* **Fast-forward landings.** `channel::Landings` holds a landing per channel (`mirrorLanding`/`releaseLanding`/`closeWhenWhole`): a re-landing closes the superseded cores, a hibernating channel keeps them only until they are whole and reopens them on the next update, an unfollowed channel purges them, and `close` drops them all. The seedable list includes them past 16 flushes, plus the owner's own system and view cores with a `length * 4096` size estimate (never a store-internals read).
* **Traffic sample.** Mux frames count route bytes. On status reads, `inflightBlocks` sums the outstanding wire requests across open blob cores' peers (not queued download ranges). `rtt` is the rounded mean of positive live UDX smoothed RTTs in milliseconds; `retransmits` sums their cumulative retransmitted packet counts. Public, LAN and private-overlay inner streams use the same counters. Closed connections no longer contribute, as in JS; zero RTT means no positive millisecond sample is currently available, including sub-millisecond paths.
* **`joinPeer`.** Hyperswarm has no `joinPeer`, so the node dials the peer's public key through HyperDHT and wires the resulting stream into the same replication set. `lanFirst` defers the dial when a LAN binding exists.
* **LAN.** A host binding is the new `network::LanBinding` trait (join/leave/destroy/host/port/stats/dialDirect plus an `on_connection` callback that yields a `LanConnection` carrying an already-framed Protomux). `Node::Lan` is `Arc<dyn LanBinding>`.
* **Authority.** `channel::AppendAuthority` is the small read-only face of the vault that channel writes need (channel key, owner key, `signClaim`); `Shared` keeps a clone of the vault and the saved access keys so blob-host reads stay synchronous.
* **Wakeup lookups.** The published Autobee asks peers for writer heads from its fast-forward search; autobee-rs's search omits that request, so the node asks (`wakeup.request_lookup()`) for every attached bee as a connection pairs and on a forced catalog refresh. Without it a fresh follower stalls at the head it woke with.
  `store.findingPeers()` is per Corestore instance in the app and corestore-rs has no equivalent, so the refresh instead waits for `swarm.flush()` with a 3 s cap (throttled to one every 10 s, public routing only) before a channel's `update()`: the topic lookups answer first, so a partial peer cannot conclude the update.
* **Blob host maintenance.** `flush`/`compact` run on blocking workers, not the backend's single-thread executor. Streaming/ceiling reclamation retains live blobs; only a full viewer wipe forces relocation. `freeBytes` is `None`, which the blob store treats as unknown; the configured cache ceiling still applies.
* **`Node::BlobServer`** is the `SocketAddr` the Range server bound (`Blobs::start_server`'s local address, kept in `Shared`), which the `/v1/blob` proxy needs.
* **Catalog ordering** keeps the JS `Map`'s insertion order (primary channel first, then followed sources, then direct watches).

## Mixed network acceptance

The JS/Rust scenarios live in `network_mixed_a` (acquire/restart, eight tests),
`network_mixed_b` (ACL/malformed announces/convergence, eight tests), and `network_mixed_c`
(private routing/cache/eviction, seven tests). Run each with `-- --include-ignored --test-threads=1`;
the old combined target was removed when these slices replaced it.
The documented `PEARTUBE_ACQUIRE` copy in `tests/network_mixed_a.rs` is required for the app's
known announcing-job restart bug; it does not change the app checkout.
