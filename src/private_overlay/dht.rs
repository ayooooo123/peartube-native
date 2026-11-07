//! The narrow DHT seam this port is written against.
//!
//! Every method mirrors one HyperDHT call made by `src/private-overlay.js`; the trait exists so
//! the guard/swarm logic can run on an in-process fake in tests and on peeroxide in production.

use std::any::Any;
use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;

use futures_util::future::BoxFuture;
use protomux::Mux;
use tokio::io::{AsyncRead, AsyncWrite};

use super::{Error, KeyPair};

/// A bidirectional byte pipe (a Noise stream once the handshake is done).
pub trait Duplex: AsyncRead + AsyncWrite + Send + Unpin {}
impl<T: AsyncRead + AsyncWrite + Send + Unpin> Duplex for T {}

/// What `enc.opened` resolves to: the finished handshake and its duplex.
pub struct NoiseOpened {
    pub remote_public_key: [u8; 32],
    pub is_initiator: bool,
    pub stream: Box<dyn Duplex>,
}

/// A Noise handshake in flight (`new NoiseSecretStream(...)` before `opened`).
///
/// Splitting creation from the handshake is required for parity: the accepting side must reply
/// `OP_INC_READY` before the far side can start its half of the handshake.
pub struct PendingNoise {
    /// `enc.opened`.
    pub opened: BoxFuture<'static, Result<NoiseOpened, Error>>,
    /// `enc.destroy()`.
    pub close: Arc<dyn Fn() + Send + Sync>,
    /// Flips to `true` when the stream closes (`enc.once('close')`).
    pub closed: tokio::sync::watch::Receiver<bool>,
}

/// The end-to-end stream handed to the app after `admitInnerStream` accepts it.
pub struct NoiseConnection {
    pub remote_public_key: [u8; 32],
    pub is_initiator: bool,
    pub stream: Box<dyn Duplex>,
    /// `enc.destroy()` for the stream the app now owns.
    pub close: Arc<dyn Fn() + Send + Sync>,
    /// Flips to `true` when the stream closes (`enc.once('close')`).
    pub closed: tokio::sync::watch::Receiver<bool>,
}

impl std::fmt::Debug for NoiseConnection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NoiseConnection")
            .field("remote_public_key", &super::to_hex(&self.remote_public_key))
            .field("is_initiator", &self.is_initiator)
            .finish_non_exhaustive()
    }
}

/// Raw-stream firewall: `true` accepts a source, mirroring the negation of HyperDHT's
/// `firewall(stream, port, host)` callback.
pub type AcceptFn = Arc<dyn Fn(u16, IpAddr) -> bool + Send + Sync>;

/// Resolves a peer's advertised address before bucketing (`opts.resolveAddress`).
pub type ResolveAddress = Arc<dyn Fn(Option<&str>, Option<u16>, &[u8; 32]) -> String + Send + Sync>;

/// Called once per accepted control connection (`server.on('connection')`).
pub type ConnectionHandler = Arc<dyn Fn(Arc<dyn Connection>) + Send + Sync>;

/// An established control connection (Noise, initiator or responder).
pub trait Connection: Send + Sync + Any {
    /// Downcast seam for the in-process fake.
    fn as_any(&self) -> &dyn Any;
    fn remote_public_key(&self) -> [u8; 32];
    /// `conn.rawStream.remoteHost`, if the transport knows it.
    fn remote_host(&self) -> Option<IpAddr>;
    /// `conn.rawStream.remotePort`.
    fn remote_port(&self) -> Option<u16>;
    /// `conn.destroyed`.
    fn is_closed(&self) -> bool;
    /// Resolves when the connection closes (`conn.once('close')`).
    fn closed(&self) -> BoxFuture<'static, ()>;
    /// `conn.destroy()`.
    fn destroy(&self);
}

/// One UDX stream allocated for the overlay (`dht.createRawStream`).
pub trait RawStream: Send + Sync + Any {
    /// `rawStream.id`.
    fn id(&self) -> u32;
    /// `rawStream.connected`.
    fn is_connected(&self) -> bool;
    /// Resolves on `rawStream.on('close')`.
    fn closed(&self) -> BoxFuture<'static, ()>;
    /// `rawStream.destroy()`.
    fn destroy(&self);
    /// `rawStream.relayTo(other)`: splice packets at the UDX layer.
    fn relay_to(&self, other: &dyn RawStream) -> Result<(), Error>;
}

impl dyn RawStream {
    pub fn downcast_ref<T: RawStream>(&self) -> Option<&T> {
        (self as &dyn Any).downcast_ref::<T>()
    }
}

/// `dht.createServer(...)` -> `server.listen(keyPair)`.
pub trait Server: Send + Sync {
    fn listen(&self, key_pair: &KeyPair) -> BoxFuture<'_, Result<(), Error>>;
    /// `server.address()`.
    fn address(&self) -> Option<SocketAddr>;
    fn suspend(&self) -> BoxFuture<'_, Result<(), Error>>;
    fn resume(&self) -> BoxFuture<'_, Result<(), Error>>;
    fn close(&self) -> BoxFuture<'_, Result<(), Error>>;
}

/// `dht.mutableGet(...)` result.
#[derive(Debug, Clone)]
pub struct MutableRecord {
    pub value: Vec<u8>,
    pub seq: u64,
    pub signature: [u8; 64],
}

/// How the DHT is brought up (`new HyperDHT({ host, bootstrap, port, ... })`).
#[derive(Debug, Clone, Default)]
pub struct DhtOptions {
    /// Bind host; `None` leaves the wildcard default.
    pub host: Option<String>,
    pub port: u16,
    /// Bootstrap nodes as `host:port` or `ip@host:port`.
    pub bootstrap: Vec<String>,
}

pub trait Dht: Send + Sync {
    /// `dht.lookup(topic)` flattened to the replying peers' public keys, in reply order.
    fn lookup(&self, topic: [u8; 32]) -> BoxFuture<'_, Result<Vec<[u8; 32]>, Error>>;
    /// `dht.announce(topic, keyPair).finished()`.
    fn announce(&self, topic: [u8; 32], key_pair: &KeyPair) -> BoxFuture<'_, Result<(), Error>>;
    /// `dht.unannounce(topic, keyPair)`.
    fn unannounce(&self, topic: [u8; 32], key_pair: &KeyPair) -> BoxFuture<'_, Result<(), Error>>;
    /// `dht.connect(publicKey, { keyPair })`, once the handshake has opened.
    fn connect(
        &self,
        remote_public_key: [u8; 32],
        key_pair: &KeyPair,
    ) -> BoxFuture<'_, Result<Arc<dyn Connection>, Error>>;
    /// `dht.createServer({ firewall: () => false }, onConnection)`.
    fn create_server(
        &self,
        handler: ConnectionHandler,
    ) -> BoxFuture<'_, Result<Arc<dyn Server>, Error>>;
    /// `Protomux.from(conn)`.
    fn protomux(&self, conn: &Arc<dyn Connection>) -> BoxFuture<'_, Result<Mux, Error>>;
    /// `dht.createRawStream({ framed, firewall })`.
    fn create_raw_stream(
        &self,
        framed: bool,
        accept: AcceptFn,
    ) -> BoxFuture<'_, Result<Arc<dyn RawStream>, Error>>;
    /// `HyperDHT.connectRawStream(conn, rawStream, remoteId)`.
    fn connect_raw_stream(
        &self,
        conn: &Arc<dyn Connection>,
        stream: &Arc<dyn RawStream>,
        remote_id: u32,
    ) -> BoxFuture<'_, Result<(), Error>>;
    /// `new NoiseSecretStream(initiator, rawStream, { keyPair, remotePublicKey, keepAlive })`.
    /// The handshake runs in the background; await [`PendingNoise::opened`] for the result.
    fn open_noise(
        &self,
        initiator: bool,
        raw: Arc<dyn RawStream>,
        key_pair: &KeyPair,
        remote_public_key: Option<[u8; 32]>,
        timeout_ms: u64,
    ) -> BoxFuture<'_, Result<PendingNoise, Error>>;
    /// `dht.mutablePut({ publicKey }, value, { seq, signMutable })`.
    fn mutable_put_signed(
        &self,
        public_key: [u8; 32],
        seq: u64,
        signature: [u8; 64],
        value: &[u8],
    ) -> BoxFuture<'_, Result<(), Error>>;
    /// `dht.mutableGet(publicKey, { latest: true })`.
    fn mutable_get(
        &self,
        public_key: [u8; 32],
    ) -> BoxFuture<'_, Result<Option<MutableRecord>, Error>>;
    /// `dht.address()`.
    fn address(&self) -> Option<SocketAddr>;
    /// `dht.localAddress()`.
    fn local_address(&self) -> Option<SocketAddr>;
    /// `dht.remoteAddress()`.
    fn remote_address(&self) -> Option<SocketAddr>;
    /// `dht.suspend()`.
    fn suspend(&self) -> BoxFuture<'_, Result<(), Error>>;
    /// `dht.resume()`.
    fn resume(&self) -> BoxFuture<'_, Result<(), Error>>;
    /// `dht.destroy({ force: true })`.
    fn destroy(&self) -> BoxFuture<'_, Result<(), Error>>;
}

/// `bucketOfConn(conn)`: the diversity bucket of a peer, honouring `opts.resolveAddress`.
pub fn bucket_for_conn(
    resolve: Option<&ResolveAddress>,
    allow_loopback: bool,
    conn: &Arc<dyn Connection>,
) -> Option<String> {
    let host = match resolve {
        Some(resolve) => {
            let raw = conn.remote_host().map(|h| h.to_string());
            Some(resolve(
                raw.as_deref(),
                conn.remote_port(),
                &conn.remote_public_key(),
            ))
        }
        None => conn.remote_host().map(|h| h.to_string()),
    };
    host.and_then(|host| {
        super::diversity_bucket_for_address(
            &host,
            conn.remote_port().map(u32::from),
            allow_loopback,
        )
    })
}

/// `allocRawStream(dht, ctrlConn, framed)`: a raw stream that only accepts the control peer.
pub async fn alloc_raw_stream(
    dht: &Arc<dyn Dht>,
    conn: &Arc<dyn Connection>,
    framed: bool,
) -> Result<Arc<dyn RawStream>, Error> {
    let (Some(host), Some(port)) = (conn.remote_host(), conn.remote_port()) else {
        return Err(Error::Closed("Control stream disconnected"));
    };
    let accept: AcceptFn = Arc::new(move |candidate_port, candidate_host| {
        candidate_port == port && candidate_host == host
    });
    loop {
        let stream = dht.create_raw_stream(framed, Arc::clone(&accept)).await?;
        if stream.id() != 0 {
            return Ok(stream);
        }
        stream.destroy();
    }
}
