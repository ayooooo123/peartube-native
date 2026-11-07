//! The `peeroxide-dht` implementation of [`Dht`], behind the `peeroxide-dht` feature.
//!
//! Everything here maps one-to-one onto a HyperDHT call; capabilities the current peeroxide-dht
//! fork does not expose return [`Error::Unsupported`] naming the missing call, so the guard and
//! swarm fail with a precise message instead of hanging. Missing on the stable fork:
//!
//! - `HyperDhtHandle::create_server` / `HyperDhtServer::listen` (guards cannot accept)
//! - `HyperDhtHandle::create_raw_stream` / `PeerConnection::connect_raw_stream` (no splicing)
//! - `HyperDhtHandle::suspend` / `resume`
//! - `mutablePut` with a caller-supplied signature (a guard cannot forward an endpoint's card)

use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use futures_util::future::BoxFuture;
use libudx::{UdxAsyncStream, UdxRuntime};
use peeroxide_dht::hyperdht::{self, HyperDhtConfig, HyperDhtHandle, PeerConnection};
use peeroxide_dht::rpc::DhtConfig;
use peeroxide_dht::secret_stream::SecretStream;
use protomux::Mux;

use super::dht::{
    AcceptFn, Connection, ConnectionHandler, Dht, DhtOptions, MutableRecord, PendingNoise,
    RawStream, Server,
};
use super::{Error, KeyPair, Result};

/// Opens a peeroxide-backed DHT (`new HyperDHT({ host, bootstrap, port })`).
pub async fn open_dht(opts: DhtOptions) -> Result<Arc<dyn Dht>> {
    let runtime = UdxRuntime::new().map_err(|error| Error::dht(error.to_string()))?;
    let mut config = HyperDhtConfig::default();
    let mut dht_config = DhtConfig::default();
    dht_config.bootstrap = opts.bootstrap;
    dht_config.port = opts.port;
    dht_config.host = opts.host.unwrap_or_else(|| "0.0.0.0".to_owned());
    // A guard is reachable by design (`firewalled: false`); the swarm lets the probe decide.
    dht_config.firewalled = false;
    dht_config.quick_firewall = true;
    config.dht = dht_config;
    let runtime_handle = runtime.handle();
    let (join, handle, events) = hyperdht::spawn(&runtime, config)
        .await
        .map_err(|error| Error::dht(error.to_string()))?;
    let local = match handle.server_socket().await {
        Ok(Some(socket)) => match socket.local_addr().await {
            Ok(addr) => Some(addr),
            Err(_) => None,
        },
        _ => None,
    };
    let remote = handle
        .remote_address()
        .await
        .ok()
        .flatten()
        .and_then(|peer| format!("{}:{}", peer.host, peer.port).parse().ok());
    Ok(Arc::new(PeeroxideDht {
        runtime: runtime_handle,
        handle,
        join: Mutex::new(Some(join)),
        events: Mutex::new(Some(events)),
        local,
        remote: Mutex::new(remote),
        closed: AtomicBool::new(false),
    }))
}

struct PeeroxideDht {
    runtime: Arc<libudx::RuntimeHandle>,
    handle: HyperDhtHandle,
    join: Mutex<Option<tokio::task::JoinHandle<Result<(), hyperdht::HyperDhtError>>>>,
    /// Kept alive so the dispatch task does not see a closed channel.
    #[allow(dead_code)]
    events: Mutex<Option<tokio::sync::mpsc::UnboundedReceiver<hyperdht::ServerEvent>>>,
    local: Option<SocketAddr>,
    remote: Mutex<Option<SocketAddr>>,
    closed: AtomicBool,
}

fn unsupported<T>(capability: &'static str) -> BoxFuture<'static, Result<T>> {
    Box::pin(async move { Err(Error::Unsupported(capability)) })
}

impl Dht for PeeroxideDht {
    fn lookup(&self, topic: [u8; 32]) -> BoxFuture<'_, Result<Vec<[u8; 32]>>> {
        let handle = self.handle.clone();
        Box::pin(async move {
            let replies = handle
                .lookup(topic)
                .await
                .map_err(|error| Error::dht(error.to_string()))?;
            let mut peers = Vec::new();
            for reply in replies {
                for peer in reply.peers {
                    if !peers.contains(&peer.public_key) {
                        peers.push(peer.public_key);
                    }
                }
            }
            Ok(peers)
        })
    }

    fn announce(&self, topic: [u8; 32], key_pair: &KeyPair) -> BoxFuture<'_, Result<()>> {
        let handle = self.handle.clone();
        let key_pair = key_pair.clone();
        Box::pin(async move {
            handle
                .announce(topic, &key_pair, &[])
                .await
                .map(|_| ())
                .map_err(|error| Error::dht(error.to_string()))
        })
    }

    fn unannounce(&self, topic: [u8; 32], key_pair: &KeyPair) -> BoxFuture<'_, Result<()>> {
        let handle = self.handle.clone();
        let key_pair = key_pair.clone();
        Box::pin(async move {
            handle
                .unannounce(topic, &key_pair)
                .await
                .map_err(|error| Error::dht(error.to_string()))
        })
    }

    fn connect(
        &self,
        remote_public_key: [u8; 32],
        key_pair: &KeyPair,
    ) -> BoxFuture<'_, Result<Arc<dyn Connection>>> {
        let handle = self.handle.clone();
        let runtime = UdxRuntime::shared(Arc::clone(&self.runtime));
        let key_pair = key_pair.clone();
        Box::pin(async move {
            let connection = handle
                .connect(&key_pair, remote_public_key, &runtime)
                .await
                .map_err(|error| Error::dht(error.to_string()))?;
            Ok(Arc::new(PeeroxideConnection::new(connection)) as Arc<dyn Connection>)
        })
    }

    fn create_server(&self, _handler: ConnectionHandler) -> BoxFuture<'_, Result<Arc<dyn Server>>> {
        unsupported("createServer/listen")
    }

    fn protomux(&self, conn: &Arc<dyn Connection>) -> BoxFuture<'_, Result<Mux>> {
        let Some(conn) = conn.as_any().downcast_ref::<PeeroxideConnection>() else {
            return Box::pin(async { Err(Error::dht("foreign connection")) });
        };
        let stream = conn.take_stream();
        match stream {
            Some(stream) => Box::pin(async move { Ok(Mux::from_secret_stream(stream)) }),
            None => Box::pin(async { Err(Error::Closed("Control channel closed")) }),
        }
    }

    fn create_raw_stream(
        &self,
        _framed: bool,
        _accept: AcceptFn,
    ) -> BoxFuture<'_, Result<Arc<dyn RawStream>>> {
        unsupported("createRawStream")
    }

    fn connect_raw_stream(
        &self,
        _conn: &Arc<dyn Connection>,
        _stream: &Arc<dyn RawStream>,
        _remote_id: u32,
    ) -> BoxFuture<'_, Result<()>> {
        unsupported("connectRawStream")
    }

    fn open_noise(
        &self,
        _initiator: bool,
        _raw: Arc<dyn RawStream>,
        _key_pair: &KeyPair,
        _remote_public_key: Option<[u8; 32]>,
        _timeout_ms: u64,
    ) -> BoxFuture<'_, Result<PendingNoise>> {
        unsupported("raw streams to carry a Noise stream")
    }

    fn mutable_put_signed(
        &self,
        _public_key: [u8; 32],
        _seq: u64,
        _signature: [u8; 64],
        _value: &[u8],
    ) -> BoxFuture<'_, Result<()>> {
        unsupported("mutablePut with a caller-supplied signature")
    }

    fn mutable_get(&self, public_key: [u8; 32]) -> BoxFuture<'_, Result<Option<MutableRecord>>> {
        let handle = self.handle.clone();
        Box::pin(async move {
            let record = handle
                .mutable_get(&public_key, 0)
                .await
                .map_err(|error| Error::dht(error.to_string()))?;
            Ok(record.map(|record| MutableRecord {
                value: record.value,
                seq: record.seq,
                signature: record.signature,
            }))
        })
    }

    fn address(&self) -> Option<SocketAddr> {
        self.local
    }

    fn local_address(&self) -> Option<SocketAddr> {
        self.local
    }

    fn remote_address(&self) -> Option<SocketAddr> {
        *self.remote.lock().unwrap_or_else(|e| e.into_inner())
    }

    fn suspend(&self) -> BoxFuture<'_, Result<()>> {
        unsupported("suspend/resume on the DHT handle")
    }

    fn resume(&self) -> BoxFuture<'_, Result<()>> {
        unsupported("suspend/resume on the DHT handle")
    }

    fn destroy(&self) -> BoxFuture<'_, Result<()>> {
        if self.closed.swap(true, Ordering::Relaxed) {
            return Box::pin(async { Ok(()) });
        }
        let handle = self.handle.clone();
        let join = self.join.lock().unwrap_or_else(|e| e.into_inner()).take();
        Box::pin(async move {
            if let Err(error) = handle.destroy().await {
                return Err(Error::dht(error.to_string()));
            }
            if let Some(join) = join {
                join.abort();
            }
            Ok(())
        })
    }
}

struct PeeroxideConnection {
    remote_public_key: [u8; 32],
    remote_addr: Option<SocketAddr>,
    stream: Mutex<Option<SecretStream<UdxAsyncStream>>>,
    closed: Arc<AtomicBool>,
    closed_tx: tokio::sync::watch::Sender<bool>,
}

impl PeeroxideConnection {
    fn new(connection: PeerConnection) -> Self {
        let (closed_tx, _) = tokio::sync::watch::channel(false);
        Self {
            remote_public_key: connection.remote_public_key,
            remote_addr: connection.remote_addr,
            stream: Mutex::new(Some(connection.stream)),
            closed: Arc::new(AtomicBool::new(false)),
            closed_tx,
        }
    }

    fn take_stream(&self) -> Option<SecretStream<UdxAsyncStream>> {
        self.stream.lock().unwrap_or_else(|e| e.into_inner()).take()
    }
}

impl Connection for PeeroxideConnection {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn remote_public_key(&self) -> [u8; 32] {
        self.remote_public_key
    }

    fn remote_host(&self) -> Option<std::net::IpAddr> {
        self.remote_addr.map(|addr| addr.ip())
    }

    fn remote_port(&self) -> Option<u16> {
        self.remote_addr.map(|addr| addr.port())
    }

    fn is_closed(&self) -> bool {
        self.closed.load(Ordering::Relaxed)
    }

    fn closed(&self) -> BoxFuture<'static, ()> {
        let mut rx = self.closed_tx.subscribe();
        let closed = Arc::clone(&self.closed);
        Box::pin(async move {
            let _ = rx.wait_for(|closed| *closed).await;
            closed.store(true, Ordering::Relaxed);
        })
    }

    fn destroy(&self) {
        self.closed.store(true, Ordering::Relaxed);
        self.closed_tx.send_replace(true);
        self.take_stream();
    }
}
