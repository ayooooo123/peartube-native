//! Thread-owned runtime and host-facing transport.

use std::sync::Arc;
use std::{
    fmt,
    sync::{Mutex, mpsc},
    thread::{self, JoinHandle},
};

use serde_json::Value;
use tokio::sync::mpsc as tokio_mpsc;

use crate::app::{App, SystemClock};
use crate::ipc::Framer;
use crate::lifecycle;

#[derive(Debug)]
pub struct BackendError(String);

impl BackendError {
    /// An error the host shows as-is.
    pub fn new(message: impl Into<String>) -> Self {
        Self(message.into())
    }
}
impl fmt::Display for BackendError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}
impl std::error::Error for BackendError {}

/// The engine's runtime. The app loop stays on the calling thread (`block_on`); spawned work —
/// replication, verification, HTTP serving, storage — runs on a small worker pool, so one busy or
/// blocking task no longer stalls playback. Capped at four workers for big.LITTLE phones.
pub(crate) fn runtime() -> std::io::Result<tokio::runtime::Runtime> {
    raise_fd_limit();
    let workers = thread::available_parallelism().map_or(2, |n| n.get().clamp(2, 4));
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(workers)
        .thread_name("peartube-worker")
        .enable_all()
        .build()
}

/// macOS and iOS start GUI apps with a 256 soft descriptor limit. Node/libuv raised it at
/// startup, and the engine's peers, loopback HTTP pairs and RocksDB files can briefly pass
/// 256 when playback starts (`EMFILE` on the player's first request). Raise the soft limit
/// to the hard one, capped at macOS's `OPEN_MAX`.
fn raise_fd_limit() {
    #[cfg(unix)]
    {
        use rustix::process::{Resource, Rlimit, getrlimit, setrlimit};
        const OPEN_MAX: u64 = 10_240;
        let limit = getrlimit(Resource::Nofile);
        let target = limit.maximum.map_or(OPEN_MAX, |max| max.min(OPEN_MAX));
        if limit.current.is_none_or(|current| current >= target) {
            return;
        }
        let _ = setrlimit(
            Resource::Nofile,
            Rlimit {
                current: Some(target),
                maximum: limit.maximum,
            },
        );
    }
}

enum Command {
    Input(String),
    Suspend(u64),
    Resume,
    Shutdown,
}

/// Everything the worker drives: the node factory, the clock it schedules on, LAN discovery, the
/// loopback API and the cast controller. `spawn_with` takes them; the FFI's `pt_init` passes the
/// platform's own.
pub struct Adapters<F, C, L, A, K> {
    pub factory: Arc<F>,
    pub clock: Arc<C>,
    pub lan: Arc<L>,
    pub api: Option<Arc<A>>,
    pub cast: Option<Arc<K>>,
}

pub struct Backend {
    commands: tokio_mpsc::UnboundedSender<Command>,
    thread: Mutex<Option<JoinHandle<()>>>,
}

impl Backend {
    /// A backend wired like `worker.js`: the native node factory, the loopback API, LAN discovery
    /// and the cast controller. Hosts that need their own transports use [`Self::spawn_with`].
    ///
    /// `config.apiPort` picks the loopback API's preferred port (`0` for an ephemeral one); the
    /// default is the app's [`crate::protocol::API_PORT`].
    ///
    /// `config.bonjour: "host"` asks the platform's Bonjour daemon over the line protocol instead
    /// of opening a multicast socket (iOS, see [`crate::lan::host`]): the requests go out on the
    /// same channel the events come back on.
    pub fn spawn(
        config: Value,
        on_line: impl Fn(String) + Send + 'static,
    ) -> Result<Self, BackendError> {
        let api_port = config
            .get("apiPort")
            .and_then(Value::as_u64)
            .and_then(|port| u16::try_from(port).ok())
            .unwrap_or(crate::protocol::API_PORT);
        if config.get("bonjour").and_then(Value::as_str) == Some("host") {
            let sink = Arc::new(Mutex::new(on_line));
            let lan = Arc::new(crate::lan::HostFactory::new({
                let sink = Arc::clone(&sink);
                move |value: Value| write(&sink, value.to_string())
            }));
            let adapters = adapters(api_port, lan);
            return Self::spawn_with(config, adapters, move |line| write(&sink, line));
        }
        Self::spawn_with(config, native_adapters(api_port), on_line)
    }

    pub fn spawn_with<F, C, L, A, K>(
        config: Value,
        adapters: Adapters<F, C, L, A, K>,
        on_line: impl Fn(String) + Send + 'static,
    ) -> Result<Self, BackendError>
    where
        F: crate::node::api::NodeFactory + Send + Sync + 'static,
        C: crate::node::api::Clock + Send + Sync + 'static,
        L: crate::node::api::LanFactory<F::Node> + Send + Sync + 'static,
        A: crate::node::api::ApiServer<F::Node> + Send + Sync + 'static,
        K: crate::node::api::CastFactory<F::Node> + Send + Sync + 'static,
        F::Node: 'static,
    {
        let _ = config;
        let (commands, mut incoming) = tokio_mpsc::unbounded_channel();
        let (ready_tx, ready_rx) = mpsc::sync_channel(1);
        let thread = thread::Builder::new()
            .name("peartube-backend".into())
            .spawn(move || {
                let runtime = match runtime() {
                    Ok(runtime) => {
                        let _ = ready_tx.send(Ok(()));
                        runtime
                    }
                    Err(err) => {
                        let _ = ready_tx.send(Err(err.to_string()));
                        return;
                    }
                };
                let Adapters { factory, clock, lan, api, cast } = adapters;
                runtime.block_on(async move {
                    let (changes, mut change_rx) = tokio_mpsc::unbounded_channel();
                    let (events, mut event_rx) = tokio_mpsc::unbounded_channel();
                    let sink = Mutex::new(on_line);
                    let write = |line: String| {
                        let sink = sink.lock().unwrap_or_else(|error| error.into_inner());
                        sink(line);
                    };
                    let mut app = App::new(factory, clock.clone(), lan, api, cast, events, changes);
                    let mut framing = Framer::new();
                    let mut lines: Vec<String> = Vec::new();
                    loop {
                        while let Some(timer) = app.take_due() {
                            app.timer(timer).await;
                        }
                        write_events(&mut event_rx, &write);
                        let deadline = app.next_deadline();
                        tokio::select! {
                            command = incoming.recv() => match command {
                                None => break,
                                Some(Command::Input(text)) => {
                                    lines.clear();
                                    framing.push(text.as_bytes(), |line| lines.push(line.to_owned()));
                                    for line in lines.drain(..) {
                                        let response = lifecycle::input(&mut app, &line).await;
                                        write_events(&mut event_rx, &write);
                                        if let Some(response) = response {
                                            write(response);
                                        }
                                    }
                                }
                                Some(Command::Suspend(linger)) => app.suspend(linger),
                                Some(Command::Resume) => app.resume().await,
                                Some(Command::Shutdown) => {
                                    app.shutdown().await;
                                    break;
                                }
                            },
                            () = clock.wait_until(deadline.unwrap_or(u64::MAX)), if deadline.is_some() => {},
                            Some(()) = change_rx.recv() => app.changed(),
                            Some(event) = event_rx.recv() => {
                                if let Some(line) = lifecycle::event_line(&event) {
                                    write(line);
                                }
                            }
                        }
                    }
                });
            })
            .map_err(|e| BackendError(e.to_string()))?;
        ready_rx
            .recv()
            .map_err(|e| BackendError(e.to_string()))?
            .map_err(BackendError)?;
        Ok(Self {
            commands,
            thread: Mutex::new(Some(thread)),
        })
    }

    pub fn send(&self, line: &str) -> Result<(), BackendError> {
        let mut line = line.to_owned();
        if !line.ends_with('\n') {
            line.push('\n');
        }
        self.commands
            .send(Command::Input(line))
            .map_err(|_| BackendError("backend is shut down".into()))
    }
    pub fn suspend(&self, linger_ms: u64) {
        let _ = self.commands.send(Command::Suspend(linger_ms));
    }
    pub fn resume(&self) {
        let _ = self.commands.send(Command::Resume);
    }
    pub fn shutdown(&self) {
        let _ = self.commands.send(Command::Shutdown);
        if let Some(handle) = self.thread.lock().unwrap_or_else(|e| e.into_inner()).take()
            && handle.thread().id() != thread::current().id()
        {
            let _ = handle.join();
        }
    }
}

/// One line to the host; the sink is shared by the event loop and the Bonjour bridge.
fn write(sink: &Mutex<impl Fn(String) + Send>, line: String) {
    let sink = sink.lock().unwrap_or_else(|error| error.into_inner());
    sink(line);
}

/// The adapters a native host has: the node factory, the process clock, LAN discovery, the
/// loopback API on `api_port` and the cast stack.
pub fn native_adapters(
    api_port: u16,
) -> Adapters<
    crate::node::factory::Factory,
    SystemClock,
    crate::lan::Factory,
    crate::adapters::LoopbackApi,
    crate::cast::adapters::Factory,
> {
    adapters(api_port, Arc::new(crate::lan::Factory))
}

/// [`native_adapters`] with the LAN factory the platform needs.
fn adapters<L>(
    api_port: u16,
    lan: Arc<L>,
) -> Adapters<
    crate::node::factory::Factory,
    SystemClock,
    L,
    crate::adapters::LoopbackApi,
    crate::cast::adapters::Factory,
> {
    Adapters {
        factory: Arc::new(crate::node::factory::Factory),
        clock: Arc::new(SystemClock),
        lan,
        api: Some(Arc::new(crate::adapters::LoopbackApi::new(api_port))),
        cast: Some(Arc::new(crate::cast::adapters::Factory)),
    }
}

/// Writes every event the app has queued, in the order it emitted them.
fn write_events(
    events: &mut tokio_mpsc::UnboundedReceiver<crate::app::AppEvent>,
    write: &impl Fn(String),
) {
    while let Ok(event) = events.try_recv() {
        if let Some(line) = lifecycle::event_line(&event) {
            write(line);
        }
    }
}

impl Drop for Backend {
    fn drop(&mut self) {
        self.shutdown();
    }
}

/// The adapters a bare `spawn` has: no node, no LAN, no API.
pub struct Unconfigured;

/// The node type of an unconfigured backend; it is never constructed.
pub enum MissingNode {}

impl crate::node::Node for MissingNode {
    type Error = std::convert::Infallible;
    type Store = ();
    type Core = ();
    type Channel = ();
    type Swarm = MissingSwarm;
    type Dht = ();
    type Connection = ();
    type ConnectionSubscription = ();
    type Lan = ();
    type BlobServer = ();
    type PutSource = MissingPutSource;
    type RechunkOptions = ();

    fn store(&self) -> &Self::Store {
        match *self {}
    }
    fn channel(&self) -> &Self::Channel {
        match *self {}
    }
    fn swarm(&self) -> &Self::Swarm {
        match *self {}
    }
    fn dht(&self) -> &Self::Dht {
        match *self {}
    }
    fn blob_server(&self) -> &Self::BlobServer {
        match *self {}
    }
    fn private_routing(&self) -> bool {
        match *self {}
    }
    fn bootstrap(&self) -> Option<Value> {
        match *self {}
    }
    fn relay_through(&self) -> Option<Value> {
        match *self {}
    }
    fn peer_key(&self) -> String {
        match *self {}
    }
    fn closing(&self) -> bool {
        match *self {}
    }
    fn peer_connections(&self) -> Vec<Self::Connection> {
        match *self {}
    }
    fn on_connection(
        &self,
        _callback: Box<dyn Fn(Self::Connection) + Send + Sync>,
    ) -> Self::ConnectionSubscription {
        match *self {}
    }
    fn onchange(&self) -> tokio::sync::watch::Receiver<u64> {
        match *self {}
    }
    async fn find_entry(&self, _key: &str) -> Result<Option<Value>, Self::Error> {
        match *self {}
    }
    async fn blob_core(&self, _key: &str) -> Result<Self::Core, Self::Error> {
        match *self {}
    }
    async fn seedable_cores(
        &self,
        _include_cached: bool,
    ) -> Result<Vec<crate::node::api::SeedableCore>, Self::Error> {
        match *self {}
    }
    async fn has_lan(&self) -> bool {
        match *self {}
    }
    async fn lan_endpoint(&self) -> Option<crate::node::api::LanEndpoint> {
        match *self {}
    }
    async fn dial_lan_peer(&self, _endpoint: crate::node::api::LanEndpoint) {
        match *self {}
    }
    async fn join_peer(&self, _key: &str, _lan_first: bool) {
        match *self {}
    }
    async fn evict_behind(
        &self,
        _blobs: &str,
        _blob: crate::node::api::Blob,
        _options: Value,
    ) -> Result<u64, Self::Error> {
        match *self {}
    }
    async fn refresh_block_count(&self, _core: &Self::Core) -> Result<(), Self::Error> {
        match *self {}
    }
    async fn search(
        &self,
        _id: Option<&str>,
        _options: crate::node::api::SearchOptions,
    ) -> Result<Vec<crate::node::api::Entry>, Self::Error> {
        match *self {}
    }
    async fn put(
        &self,
        _meta: crate::node::api::PutMeta,
        _source: Self::PutSource,
    ) -> Result<crate::node::api::Announce, Self::Error> {
        match *self {}
    }
    async fn drop_store(&self, _name: &str) -> Result<(), Self::Error> {
        match *self {}
    }
    async fn append(&self, _op: Value) -> Result<(), Self::Error> {
        match *self {}
    }
    async fn publish(
        &self,
        _meta: crate::node::api::PutMeta,
        _source: Self::PutSource,
    ) -> Result<crate::node::api::Published, Self::Error> {
        match *self {}
    }
    async fn publish_channel(
        &self,
        _meta_or_op: Value,
        _source: Option<Self::PutSource>,
    ) -> Result<crate::node::api::Published, Self::Error> {
        match *self {}
    }
    async fn remove(&self, _key: &str) -> Result<(), Self::Error> {
        match *self {}
    }
    async fn publish_source(&self, _source_key: &str, _title: &str) -> Result<Value, Self::Error> {
        match *self {}
    }
    async fn sync_local_channel(&self) -> Result<crate::node::api::SyncResult, Self::Error> {
        match *self {}
    }
    async fn rechunk_legacy_blobs(
        &self,
        _options: Self::RechunkOptions,
    ) -> Result<crate::node::api::RechunkResult, Self::Error> {
        match *self {}
    }
    async fn unpublish_source(&self, _key: &str) -> Result<Value, Self::Error> {
        match *self {}
    }
    async fn published_sources(
        &self,
    ) -> Result<Vec<crate::node::api::PublishedSource>, Self::Error> {
        match *self {}
    }
    async fn add_source(
        &self,
        _spec: crate::node::api::SourceSpec,
        _wake: bool,
    ) -> Result<crate::node::api::Source, Self::Error> {
        match *self {}
    }
    async fn remove_source(&self, _key: &str) -> Result<crate::node::api::Removed, Self::Error> {
        match *self {}
    }
    async fn list_sources(&self) -> Vec<crate::node::api::Source> {
        match *self {}
    }
    async fn focus_source(&self, _key: &str) -> Result<bool, Self::Error> {
        match *self {}
    }
    async fn set_player_active(&self, _active: bool) -> crate::node::api::OkResult {
        match *self {}
    }
    async fn active_source_count(&self) -> usize {
        match *self {}
    }
    async fn overlay_topic_count(&self) -> usize {
        match *self {}
    }
    async fn discovered_sources(
        &self,
    ) -> Result<Vec<crate::node::api::DiscoveredSource>, Self::Error> {
        match *self {}
    }
    async fn open_uri(
        &self,
        _uri: &str,
        _join_source: bool,
    ) -> Result<crate::node::api::OpenUri, Self::Error> {
        match *self {}
    }
    async fn warm(&self, _entry: &Value) -> Result<(), Self::Error> {
        match *self {}
    }
    async fn identity(
        &self,
        _reveal_mnemonic: bool,
    ) -> Result<crate::node::api::Identity, Self::Error> {
        match *self {}
    }
    async fn grant_writer(
        &self,
        _writer: &str,
        _role: &str,
        _alias: &str,
    ) -> Result<crate::node::api::Grant, Self::Error> {
        match *self {}
    }
    async fn revoke_writer(
        &self,
        _writer: &str,
        _purge_entries: bool,
    ) -> Result<crate::node::api::Revoke, Self::Error> {
        match *self {}
    }
    async fn claim_with_mnemonic(
        &self,
        _mnemonic: &str,
        _alias: &str,
    ) -> Result<crate::node::api::Claim, Self::Error> {
        match *self {}
    }
    async fn rotate_epoch(&self) -> Result<crate::node::api::Epoch, Self::Error> {
        match *self {}
    }
    async fn set_allowed_peers(&self, _peers: Option<Vec<String>>) -> Result<(), Self::Error> {
        match *self {}
    }
    async fn clear_cache(
        &self,
        _key: Option<&str>,
        _keep_blobs_hex: Option<&str>,
    ) -> Result<crate::node::api::CacheCleared, Self::Error> {
        match *self {}
    }
    async fn set_cache_max_bytes(&self, _bytes: f64) -> u64 {
        match *self {}
    }
    async fn enforce_cache_ceiling(
        &self,
        _keep_hex: Option<&str>,
    ) -> Result<crate::node::api::CacheCeiling, Self::Error> {
        match *self {}
    }
    async fn gc_storage(
        &self,
        _full: bool,
        _streaming: bool,
    ) -> Result<crate::node::api::StorageGc, Self::Error> {
        match *self {}
    }
    async fn status(&self) -> crate::node::api::Status {
        match *self {}
    }
    async fn stream_url(&self, _entry: &crate::node::api::Announce) -> String {
        match *self {}
    }
    async fn http_viewing(&self) -> bool {
        match *self {}
    }
    async fn mark_bulk_peer(&self, _key: &str, _on: bool) {
        match *self {}
    }
    async fn set_lan(&self, _lan: Option<Self::Lan>) -> Result<(), Self::Error> {
        match *self {}
    }
    async fn lan_stats(&self) -> Option<Value> {
        match *self {}
    }
    async fn close(&self) -> Result<(), Self::Error> {
        match *self {}
    }
}

pub enum MissingPutSource {}

impl crate::node::PutSource for MissingPutSource {
    type Error = std::convert::Infallible;
    async fn open(
        &mut self,
        _offset: u64,
        _meta: &mut crate::node::api::PutMeta,
    ) -> Result<std::pin::Pin<Box<dyn tokio::io::AsyncRead + Send>>, Self::Error> {
        match *self {}
    }
    fn on_data(&mut self, _bytes: u64) {
        match *self {}
    }
    fn on_hash_state(&mut self, _state: &[u8], _bytes: u64) {
        match *self {}
    }
}

/// The swarm of a backend with no node factory; it is never constructed.
pub enum MissingSwarm {}

impl crate::node::api::Swarm for MissingSwarm {
    type Error = std::convert::Infallible;
    async fn suspend(&self) -> Result<(), Self::Error> {
        match *self {}
    }
    async fn resume(&self) -> Result<(), Self::Error> {
        match *self {}
    }
    fn on_update(&self, _callback: Arc<dyn Fn() + Send + Sync>) -> Box<dyn Fn() + Send + Sync> {
        match *self {}
    }
}

impl crate::node::api::NodeFactory for Unconfigured {
    type Node = MissingNode;
    type Error = BackendError;
    async fn create(&self, _options: Value) -> Result<Arc<Self::Node>, Self::Error> {
        Err(BackendError("Native node factory is not configured".into()))
    }
}

impl<N: crate::node::Node> crate::node::api::LanFactory<N> for Unconfigured {
    type Error = BackendError;
    async fn interfaces(&self) -> Result<Vec<(String, std::net::IpAddr, bool)>, Self::Error> {
        Err(BackendError("LAN adapters are not configured".into()))
    }
    async fn create(&self, _host: std::net::Ipv4Addr, _node: &N) -> Result<N::Lan, Self::Error> {
        Err(BackendError("LAN adapters are not configured".into()))
    }
    async fn host_event(&self, _event: Value) {}
}

impl<N: crate::node::Node> crate::node::api::ApiServer<N> for Unconfigured {
    type Error = BackendError;
    async fn start(
        &self,
        _node: Arc<N>,
        _on_sources: Arc<dyn Fn(Vec<crate::node::api::Source>) + Send + Sync>,
    ) -> Result<Option<String>, Self::Error> {
        Err(BackendError("The loopback API is not configured".into()))
    }
    fn notify(&self, _event: &str) {}
    async fn close(&self) {}
}

impl<N: crate::node::Node> crate::node::api::CastFactory<N> for Unconfigured {
    async fn create(
        &self,
        _options: crate::node::api::CastOptions<N>,
    ) -> Result<Arc<dyn crate::node::api::Cast>, String> {
        Err("Cast adapters are not configured".into())
    }
    async fn peer_storage(&self, candidate: Option<&str>, fallback: &str) -> String {
        candidate.unwrap_or(fallback).to_owned()
    }
    fn reclaim(&self, _storage: &str) {}
}

#[cfg(all(test, unix))]
mod tests {
    use rustix::process::{Resource, Rlimit, getrlimit, setrlimit};

    #[test]
    fn engine_runtime_lifts_a_256_descriptor_limit() {
        let original = getrlimit(Resource::Nofile);
        // The limit a macOS GUI app starts with.
        setrlimit(
            Resource::Nofile,
            Rlimit {
                current: Some(256),
                maximum: original.maximum,
            },
        )
        .unwrap();
        drop(super::runtime().unwrap());
        let raised = getrlimit(Resource::Nofile).current.unwrap_or(u64::MAX);
        setrlimit(Resource::Nofile, original).unwrap();
        assert!(raised > 256, "soft limit stayed at {raised}");
    }
}
