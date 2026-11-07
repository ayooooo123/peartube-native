//! The `peeroxide-dht` implementation of [`Dht`], behind the `peeroxide-dht` feature.
//!
//! Everything here maps one-to-one onto a HyperDHT call: the overlay's `createServer`, raw
//! streams, Noise-over-raw-stream and caller-signed mutable puts are all peeroxide APIs.

use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use futures_util::future::BoxFuture;
use libudx::{UdxAsyncStream, UdxRuntime, UdxSocket, UdxStreamAbort};
use peeroxide_dht::hyperdht::{
    self, HyperDhtConfig, HyperDhtHandle, HyperDhtServer, KeyPair as DhtKeyPair, PeerConnection,
    RawStream as DhtRawStream, RawStreamOptions, ServerConfig,
};
use peeroxide_dht::noise::Keypair;
use peeroxide_dht::rpc::DhtConfig;
use peeroxide_dht::secret_stream::{SecretStream, SecretStreamReadHalf, SecretStreamWriteHalf};
use protomux::Mux;
use tokio::io::{ReadHalf, WriteHalf};
use tokio::sync::Mutex as AsyncMutex;

use super::dht::{
    AcceptFn, Connection, ConnectionHandler, Dht, DhtOptions, MessageDuplex, MessageRead,
    MessageWrite, MutableRecord, NoiseOpened, PendingNoise, RawStream, Server,
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
        Ok(Some(socket)) => socket.local_addr().await.ok(),
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
    /// The single server-event stream `spawn` returned; the first `createServer` claims it.
    events: Mutex<Option<tokio::sync::mpsc::UnboundedReceiver<hyperdht::ServerEvent>>>,
    local: Option<SocketAddr>,
    remote: Mutex<Option<SocketAddr>>,
    closed: AtomicBool,
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

    fn create_server(&self, handler: ConnectionHandler) -> BoxFuture<'_, Result<Arc<dyn Server>>> {
        let events = self.events.lock().unwrap_or_else(|e| e.into_inner()).take();
        let handle = self.handle.clone();
        let runtime = UdxRuntime::shared(Arc::clone(&self.runtime));
        Box::pin(async move {
            let events = events.ok_or_else(|| Error::dht("server event stream already claimed"))?;
            // The identity is supplied by `listen`; `ServerConfig` only needs a placeholder.
            let config = ServerConfig::new(DhtKeyPair::generate(), 0);
            let server = handle.create_server(&runtime, config, events);
            Ok(Arc::new(PeeroxideServer {
                server: AsyncMutex::new(server),
                handler,
                accepting: AtomicBool::new(false),
                address: Mutex::new(None),
            }) as Arc<dyn Server>)
        })
    }

    fn protomux(&self, conn: &Arc<dyn Connection>) -> BoxFuture<'_, Result<Mux>> {
        let stream = conn
            .as_any()
            .downcast_ref::<PeeroxideConnection>()
            .and_then(PeeroxideConnection::take_stream);
        match stream {
            Some(stream) => Box::pin(async move { Ok(Mux::from_secret_stream(stream)) }),
            None => Box::pin(async { Err(Error::Closed("Control channel closed")) }),
        }
    }

    fn create_raw_stream(
        &self,
        _framed: bool,
        accept: AcceptFn,
    ) -> BoxFuture<'_, Result<Arc<dyn RawStream>>> {
        // `framed: true` in Node adds a length-prefix layer to the raw stream; the port's
        // Noise stream frames its own messages, so the raw stream stays unframed here.
        let handle = self.handle.clone();
        let runtime = UdxRuntime::shared(Arc::clone(&self.runtime));
        Box::pin(async move {
            let options = RawStreamOptions::new()
                .with_firewall(move |_socket, port, host| !accept(port, host));
            let stream = handle
                .create_raw_stream(&runtime, options)
                .await
                .map_err(|error| Error::dht(error.to_string()))?;
            Ok(Arc::new(PeeroxideRawStream::new(stream)) as Arc<dyn RawStream>)
        })
    }

    fn connect_raw_stream(
        &self,
        conn: &Arc<dyn Connection>,
        stream: &Arc<dyn RawStream>,
        remote_id: u32,
    ) -> BoxFuture<'_, Result<()>> {
        let conn = Arc::clone(conn);
        let stream = Arc::clone(stream);
        Box::pin(async move {
            let Some(connection) = conn.as_any().downcast_ref::<PeeroxideConnection>() else {
                return Err(Error::dht("foreign connection"));
            };
            let (Some(socket), Some(addr)) = (connection.socket.clone(), connection.remote_addr)
            else {
                return Err(Error::dht("control connection has no address"));
            };
            let Some(raw) = stream.downcast_ref::<PeeroxideRawStream>() else {
                return Err(Error::dht("foreign raw stream"));
            };
            raw.connect(&socket, remote_id, addr).await
        })
    }

    fn open_noise(
        &self,
        initiator: bool,
        raw: Arc<dyn RawStream>,
        key_pair: &KeyPair,
        remote_public_key: Option<[u8; 32]>,
        timeout_ms: u64,
    ) -> BoxFuture<'_, Result<PendingNoise>> {
        let Some(raw) = raw.downcast_ref::<PeeroxideRawStream>() else {
            return Box::pin(async { Err(Error::dht("foreign raw stream")) });
        };
        let Some(stream) = raw.take() else {
            return Box::pin(async { Err(Error::Closed("Raw stream already used")) });
        };
        let keypair = Keypair {
            public_key: key_pair.public_key,
            secret_key: key_pair.secret_key,
        };
        let (_, transport) = stream.into_stream();
        let abort = transport.abort_handle();
        let (closed_tx, closed_rx) = tokio::sync::watch::channel(false);
        {
            let abort = abort.clone();
            tokio::spawn(async move {
                abort.closed().await;
                closed_tx.send_replace(true);
            });
        }
        let close_abort = abort.clone();
        let (tx, rx) = tokio::sync::oneshot::channel();
        tokio::spawn(async move {
            let transport = transport.into_async_stream();
            let handshake = tokio::time::timeout(
                Duration::from_millis(timeout_ms.max(1)),
                SecretStream::new(initiator, transport, keypair),
            )
            .await;
            let result = match handshake {
                Ok(Ok(stream)) => {
                    let opened = NoiseOpened {
                        remote_public_key: *stream.remote_public_key(),
                        is_initiator: stream.is_initiator(),
                        handshake_hash: Some(*stream.handshake_hash()),
                        stream: Box::new(SecretMessages(Some(stream))),
                    };
                    match remote_public_key {
                        Some(expected) if expected != opened.remote_public_key => {
                            Err(Error::Rejected("Inner identity mismatch"))
                        }
                        _ => Ok(opened),
                    }
                }
                Ok(Err(_error)) => Err(Error::Closed("Noise handshake failed")),
                Err(_) => Err(Error::Closed("Noise handshake timed out")),
            };
            let _ = tx.send(result);
        });
        Box::pin(async move {
            Ok(PendingNoise {
                opened: Box::pin(async move {
                    rx.await
                        .map_err(|_| Error::Closed("Noise handshake cancelled"))?
                }),
                close: Arc::new(move || close_abort.abort()),
                closed: closed_rx,
            })
        })
    }

    fn mutable_put_signed(
        &self,
        public_key: [u8; 32],
        seq: u64,
        signature: [u8; 64],
        value: &[u8],
    ) -> BoxFuture<'_, Result<()>> {
        let handle = self.handle.clone();
        let value = value.to_vec();
        Box::pin(async move {
            handle
                .mutable_put_signed(&public_key, &value, seq, signature)
                .await
                .map(|_| ())
                .map_err(|error| Error::dht(error.to_string()))
        })
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
        let handle = self.handle.clone();
        Box::pin(async move {
            handle
                .suspend()
                .await
                .map_err(|error| Error::dht(error.to_string()))
        })
    }

    fn resume(&self) -> BoxFuture<'_, Result<()>> {
        let handle = self.handle.clone();
        Box::pin(async move {
            handle
                .resume()
                .await
                .map_err(|error| Error::dht(error.to_string()))
        })
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

/// `server.listen(keyPair)`, with the accepted connections handed to the overlay handler.
struct PeeroxideServer {
    server: AsyncMutex<HyperDhtServer>,
    handler: ConnectionHandler,
    accepting: AtomicBool,
    address: Mutex<Option<SocketAddr>>,
}

impl Server for PeeroxideServer {
    fn listen(&self, key_pair: &KeyPair) -> BoxFuture<'_, Result<()>> {
        let key_pair = key_pair.clone();
        Box::pin(async move {
            let mut server = self.server.lock().await;
            server
                .listen(&key_pair)
                .await
                .map_err(|error| Error::dht(error.to_string()))?;
            *self.address.lock().unwrap_or_else(|e| e.into_inner()) = server
                .address()
                .and_then(|address| format!("{}:{}", address.host, address.port).parse().ok());
            if !self.accepting.swap(true, Ordering::AcqRel) {
                let mut connections = server.take_connections();
                let handler = Arc::clone(&self.handler);
                tokio::spawn(async move {
                    while let Some(connection) = connections.recv().await {
                        handler(
                            Arc::new(PeeroxideConnection::new(connection)) as Arc<dyn Connection>
                        );
                    }
                });
            }
            Ok(())
        })
    }

    fn address(&self) -> Option<SocketAddr> {
        *self.address.lock().unwrap_or_else(|e| e.into_inner())
    }

    fn suspend(&self) -> BoxFuture<'_, Result<()>> {
        Box::pin(async move {
            self.server
                .lock()
                .await
                .suspend()
                .await
                .map_err(|error| Error::dht(error.to_string()))
        })
    }

    fn resume(&self) -> BoxFuture<'_, Result<()>> {
        Box::pin(async move {
            self.server
                .lock()
                .await
                .resume()
                .await
                .map_err(|error| Error::dht(error.to_string()))
        })
    }

    fn close(&self) -> BoxFuture<'_, Result<()>> {
        Box::pin(async move {
            self.server.lock().await.close().await;
            Ok(())
        })
    }
}

/// One overlay raw stream: owned exclusively until `open_noise` consumes it.
struct PeeroxideRawStream {
    stream: Mutex<Option<DhtRawStream>>,
    abort: UdxStreamAbort,
    id: u32,
    connected: AtomicBool,
}

impl PeeroxideRawStream {
    fn new(stream: DhtRawStream) -> Self {
        let abort = stream.stream().abort_handle();
        let id = stream.id();
        Self {
            stream: Mutex::new(Some(stream)),
            abort,
            id,
            connected: AtomicBool::new(false),
        }
    }

    async fn connect(&self, socket: &UdxSocket, remote_id: u32, addr: SocketAddr) -> Result<()> {
        // Take the stream so no lock is held across the await; `open_noise` takes it too, and
        // the ordering the overlay uses always connects before it opens the handshake.
        let Some(stream) = self.take() else {
            return Err(Error::Closed("Raw stream already used"));
        };
        let result = stream
            .connect(socket, remote_id, addr)
            .await
            .map_err(|error| Error::dht(error.to_string()));
        if result.is_ok() {
            self.connected.store(true, Ordering::Release);
        }
        *self.stream.lock().unwrap_or_else(|e| e.into_inner()) = Some(stream);
        result
    }

    fn take(&self) -> Option<DhtRawStream> {
        self.stream.lock().unwrap_or_else(|e| e.into_inner()).take()
    }
}

impl RawStream for PeeroxideRawStream {
    fn id(&self) -> u32 {
        self.id
    }

    fn is_connected(&self) -> bool {
        self.connected.load(Ordering::Acquire) && self.abort.is_alive()
    }

    fn closed(&self) -> BoxFuture<'static, ()> {
        let abort = self.abort.clone();
        Box::pin(async move { abort.closed().await })
    }

    fn destroy(&self) {
        self.abort.abort();
        // Dropping the wrapper also deregisters it from its sockets.
        self.take();
    }

    fn relay_to(&self, other: &dyn RawStream) -> Result<()> {
        let Some(other) = other.downcast_ref::<PeeroxideRawStream>() else {
            return Err(Error::dht("foreign raw stream"));
        };
        let source = self.stream.lock().unwrap_or_else(|e| e.into_inner());
        let destination = other.stream.lock().unwrap_or_else(|e| e.into_inner());
        let (Some(source), Some(destination)) = (source.as_ref(), destination.as_ref()) else {
            return Err(Error::Closed("Raw stream already used"));
        };
        source
            .relay_to(destination)
            .map_err(|error| Error::dht(error.to_string()))
    }
}

/// An established control connection (Noise, initiator or responder).
struct PeeroxideConnection {
    remote_public_key: [u8; 32],
    remote_addr: Option<SocketAddr>,
    socket: Option<UdxSocket>,
    stream: Mutex<Option<SecretStream<UdxAsyncStream>>>,
    closed: UdxStreamAbort,
}

impl PeeroxideConnection {
    fn new(connection: PeerConnection) -> Self {
        let closed = connection.stream.abort_handle();
        Self {
            remote_public_key: connection.remote_public_key,
            remote_addr: connection.remote_addr,
            socket: Some(connection.socket.clone()),
            stream: Mutex::new(Some(connection.stream)),
            closed,
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
        !self.closed.is_alive()
    }

    fn closed(&self) -> BoxFuture<'static, ()> {
        let abort = self.closed.clone();
        Box::pin(async move { abort.closed().await })
    }

    fn destroy(&self) {
        self.closed.abort();
        self.take_stream();
    }
}

/// The end-to-end Noise stream as the JS `NoiseSecretStream` hands it to the app: each write is
/// one message, each read returns exactly one.
struct SecretMessages(Option<SecretStream<UdxAsyncStream>>);

impl MessageDuplex for SecretMessages {
    fn split(mut self: Box<Self>) -> (Box<dyn MessageRead>, Box<dyn MessageWrite>) {
        let stream = self.0.take().expect("split once");
        let (read, write) = stream.into_split();
        (Box::new(SecretRead(read)), Box::new(SecretWrite(write)))
    }
}

type SecretReadHalf = SecretStreamReadHalf<ReadHalf<UdxAsyncStream>>;
type SecretWriteHalf = SecretStreamWriteHalf<WriteHalf<UdxAsyncStream>>;

struct SecretRead(SecretReadHalf);

impl MessageRead for SecretRead {
    fn read_message<'a>(&'a mut self) -> BoxFuture<'a, Result<Option<Bytes>, Error>> {
        Box::pin(async move {
            self.0
                .read()
                .await
                .map_err(|error| Error::dht(error.to_string()))
                .map(|message| message.map(Bytes::from))
        })
    }
}

struct SecretWrite(SecretWriteHalf);

impl MessageWrite for SecretWrite {
    fn write_message<'a>(&'a mut self, message: &'a [u8]) -> BoxFuture<'a, Result<(), Error>> {
        Box::pin(async move {
            self.0
                .write(message)
                .await
                .map_err(|error| Error::dht(error.to_string()))
        })
    }

    fn shutdown(&mut self) -> BoxFuture<'_, ()> {
        Box::pin(async move {
            let _ = self.0.shutdown().await;
        })
    }
}
