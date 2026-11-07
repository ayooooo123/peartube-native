//! The app: `worklet/worker.js`, as a typed service.
//!
//! One [`App`] owns the node, the loopback API, the cast controller and the LAN binding, and runs
//! every input one at a time, so `start`, `stop` and the LAN moves can never overlap — the
//! guarantee `serial()` gives the JavaScript worker. Its methods take and return typed requests
//! and replies; the events it emits are [`AppEvent`]s.
//!
//! JSON hosts speak lines through [`crate::lifecycle`]; a host that links the backend in-process
//! drives [`AppHost`], which runs the app on its own thread and hands out typed calls and events.

use std::collections::BTreeMap;
use std::future::Future;
use std::net::Ipv4Addr;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc as std_mpsc;
use std::sync::{Arc, Mutex, MutexGuard};
use std::thread::{self, JoinHandle};

use serde::{Deserialize, Serialize};
use serde_json::{Number, Value, json};
use tokio::sync::mpsc::{self, UnboundedSender};

use crate::app_settings::{self, AppSettings};
use crate::node::Node;
use crate::node::api::{
    ApiServer, CacheCleared, Cast, CastFactory, CastOptions, CastStatus, Clock, DiscoveredSource,
    Epoch, FollowedSource, Identity, LanFactory, NodeFactory, OkResult, OpenUri, ReceiverStart,
    Reciprocity, Revoke, Route, Routes, Source, SourceSpec, Status, Swarm,
};
use crate::protocol::{self, ParsedUri};

/// The events the app emits, each the body of one `worker.js` event line.
pub use crate::node::api::WorkerEvent as AppEvent;

/// Longest a `suspend` waits before the peers are dropped.
const MAX_LINGER_MS: u64 = 5_000;
/// Re-check the device's address on this cadence.
const LAN_POLL_MS: u64 = 10_000;
/// Sample the transfer rates on this cadence, for the `update` event.
const TRAFFIC_POLL_MS: u64 = 2_000;
/// An `update` after recent activity waits this long, to coalesce a burst of changes.
const UPDATE_SLOW_MS: u64 = 1_500;
/// An `update` after a quiet spell is sent this soon.
const UPDATE_FAST_MS: u64 = 200;
/// A change this soon after the last event was part of the same burst.
const UPDATE_QUIET_MS: u64 = 3_000;
/// A TV box keeps only a rolling window of whatever it plays.
const RECEIVER_CACHE_MAX_BYTES: f64 = 2e9;

fn lock<T>(cell: &Mutex<T>) -> MutexGuard<'_, T> {
    cell.lock().unwrap_or_else(|error| error.into_inner())
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum Owner {
    App,
    Receiver,
}

#[derive(Clone, Copy, Debug)]
pub(crate) enum Timer {
    Update,
    LanPoll,
    TrafficPoll,
    Suspend,
}

// --------------------------------------------------------------------------------------------
// Requests

/// `init`: the app's storage for the cast stack, sent once before any other call.
#[derive(Clone, Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct InitRequest {
    pub cast_storage: Option<String>,
}

/// `start`: where trackers live and the settings form the node opens with.
#[derive(Clone, Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct StartRequest {
    pub trackers_dir: Option<String>,
    pub default_folder: Option<String>,
    /// The app's settings form, validated by [`crate::app_settings`].
    pub settings: Value,
}

/// `normalizeSettings`: the settings form, and whether it must name a channel.
#[derive(Clone, Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct NormalizeSettingsRequest {
    pub settings: Value,
    pub require_channel: Option<Value>,
}

/// `search`: `id` picks one entry, otherwise the query filters the catalog.
#[derive(Clone, Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct SearchRequest {
    pub id: Option<String>,
    pub source: Option<String>,
    pub query: Option<String>,
    pub limit: Option<Number>,
    pub offset: Option<Number>,
}

/// A call that names one key: `focusSource`, `warm`, `removeSource` and `remove`.
#[derive(Clone, Debug, Default, Deserialize)]
#[serde(default)]
pub struct KeyRequest {
    pub key: String,
}

/// `setPlayerActive`: a film is playing on this device.
#[derive(Clone, Debug, Default, Deserialize)]
#[serde(default)]
pub struct ActiveRequest {
    pub active: Option<Value>,
}

/// `identity`: reveal the recovery phrase only for the owner's own eyes.
#[derive(Clone, Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct IdentityRequest {
    pub reveal_mnemonic: Option<Value>,
}

/// `revokeWriter`.
#[derive(Clone, Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct RevokeWriterRequest {
    pub writer: String,
    pub purge_entries: Option<Value>,
}

/// `clearCache`: no key clears every entry's cache.
#[derive(Clone, Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct ClearCacheRequest {
    pub key: Option<String>,
}

/// `setCacheMax`.
#[derive(Clone, Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct SetCacheMaxRequest {
    pub max_bytes: Option<f64>,
}

/// `castRemove`.
#[derive(Clone, Debug, Default, Deserialize)]
#[serde(default)]
pub struct CastRemoveRequest {
    pub id: String,
}

/// `receiverStart`: the TV's name, whether pairing needs a code, and its own peer storage.
#[derive(Clone, Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct ReceiverStartRequest {
    pub peer_storage: Option<String>,
    pub name: Option<String>,
    pub open_auth: Option<Value>,
}

/// `openUri`: every link an app can paste.
#[derive(Clone, Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct OpenUriRequest {
    pub uri: String,
}

// --------------------------------------------------------------------------------------------
// Replies

/// `status` and `start`: the node's status plus the worker's own view of the device.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Report {
    #[serde(flatten)]
    pub status: Status,
    pub lan: Option<String>,
    pub api_url: Option<String>,
    pub discovered_sources: Vec<DiscoveredSource>,
}

/// One catalog entry, with the loopback URL that saves it under its own name.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SearchEntry {
    #[serde(flatten)]
    pub entry: crate::node::api::Entry,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub download_url: Option<String>,
}

/// `openUri`: which kind of link the app opened, and what it opened.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "action", rename_all = "lowercase")]
pub enum OpenUriReply {
    Receiver {
        receiver: Value,
    },
    Source {
        source: OpenedSource,
    },
    Access {
        channel: String,
        epoch: u64,
        source: Source,
    },
    Watch {
        entry: Box<SearchEntry>,
    },
}

/// A channel link: the whole source once a node is open, only its identity before.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum OpenedSource {
    Full(Source),
    Brief(FollowedSource),
}

/// `addSource`: the source added, and the catalog it joined.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AddSourceReply {
    pub source: Source,
    pub sources: Vec<Source>,
    pub discovered: Vec<DiscoveredSource>,
}

/// `removeSource`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RemoveSourceReply {
    pub removed: bool,
    pub sources: Vec<Source>,
    pub discovered: Vec<DiscoveredSource>,
}

/// `remove`: the entry is gone.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct RemoveReply {
    pub removed: bool,
    pub key: Option<String>,
}

/// `setCacheMax`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CacheMax {
    pub cache_max_bytes: u64,
}

/// `castPair`: the cast status with the receiver just paired.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CastPairReply {
    #[serde(flatten)]
    pub status: CastStatus,
    pub paired_id: String,
}

// --------------------------------------------------------------------------------------------
// State

/// State the adapters reach back into while the app is inside an adapter call.
struct Shared<N: Node, A> {
    node: Mutex<Option<Arc<N>>>,
    api: Mutex<Option<Arc<A>>>,
    cast: Mutex<Option<Arc<dyn Cast>>>,
    suspended: AtomicBool,
    live_ip: Mutex<Option<String>>,
}

pub struct App<N, F, C, L, A, K>
where
    N: Node,
    F: NodeFactory<Node = N>,
    C: Clock,
    L: LanFactory<N>,
    A: ApiServer<N>,
    K: CastFactory<N>,
{
    factory: Arc<F>,
    clock: Arc<C>,
    lan: Arc<L>,
    api_factory: Option<Arc<A>>,
    cast_factory: Option<Arc<K>>,
    events: UnboundedSender<AppEvent>,
    changes: UnboundedSender<()>,
    shared: Arc<Shared<N, A>>,
    last_settings: Option<AppSettings>,
    cast_storage: Option<String>,
    cast_peer_storage: Option<String>,
    node_owner: Option<Owner>,
    api_url: Option<String>,
    lan_wanted: bool,
    lan_host: Option<String>,
    lan_error: Option<String>,
    lan_note: Option<String>,
    swarm_unlisten: Option<Box<dyn Fn() + Send + Sync>>,
    timers: BTreeMap<(u64, u64), Timer>,
    seq: u64,
    update_timer: Option<(u64, u64)>,
    suspending: Option<(u64, u64)>,
    last_update_at: u64,
    was_transferring: bool,
}

impl<N, F, C, L, A, K> App<N, F, C, L, A, K>
where
    N: Node + 'static,
    F: NodeFactory<Node = N> + 'static,
    C: Clock + 'static,
    L: LanFactory<N> + 'static,
    A: ApiServer<N> + 'static,
    K: CastFactory<N> + 'static,
{
    pub fn new(
        factory: Arc<F>,
        clock: Arc<C>,
        lan: Arc<L>,
        api: Option<Arc<A>>,
        cast: Option<Arc<K>>,
        events: UnboundedSender<AppEvent>,
        changes: UnboundedSender<()>,
    ) -> Self {
        let shared = Arc::new(Shared {
            node: Mutex::new(None),
            api: Mutex::new(None),
            cast: Mutex::new(None),
            suspended: AtomicBool::new(false),
            live_ip: Mutex::new(None),
        });
        let mut app = Self {
            factory,
            clock,
            lan,
            api_factory: api,
            cast_factory: cast,
            events,
            changes,
            shared,
            last_settings: None,
            cast_storage: None,
            cast_peer_storage: None,
            node_owner: None,
            api_url: None,
            lan_wanted: false,
            lan_host: None,
            lan_error: None,
            lan_note: None,
            swarm_unlisten: None,
            timers: BTreeMap::new(),
            seq: 0,
            update_timer: None,
            suspending: None,
            last_update_at: 0,
            was_transferring: false,
        };
        app.schedule(LAN_POLL_MS, Timer::LanPoll);
        app.schedule(TRAFFIC_POLL_MS, Timer::TrafficPoll);
        app
    }

    fn now(&self) -> u64 {
        self.clock.now_ms()
    }

    pub(crate) fn next_deadline(&self) -> Option<u64> {
        self.timers.keys().next().map(|(deadline, _)| *deadline)
    }

    pub(crate) fn take_due(&mut self) -> Option<Timer> {
        let (deadline, seq) = *self.timers.keys().next()?;
        if deadline > self.now() {
            return None;
        }
        let timer = self.timers.remove(&(deadline, seq))?;
        if matches!(timer, Timer::Update) {
            self.update_timer = None;
        }
        if matches!(timer, Timer::Suspend) {
            self.suspending = None;
        }
        Some(timer)
    }

    pub(crate) async fn timer(&mut self, timer: Timer) {
        match timer {
            Timer::Update => {
                self.last_update_at = self.now();
                self.send(AppEvent::Update);
            }
            Timer::LanPoll => {
                self.move_lan().await;
                self.schedule(LAN_POLL_MS, Timer::LanPoll);
            }
            Timer::TrafficPoll => {
                if let Some(node) = self.node()
                    && !self.is_suspended()
                {
                    let status = node.status().await;
                    let active = status.down_bps.as_f64().is_some_and(|rate| rate > 0.0)
                        || status.up_bps.as_f64().is_some_and(|rate| rate > 0.0)
                        || status.inflight_blocks > 0;
                    if active || self.was_transferring {
                        self.was_transferring = active;
                        self.update();
                    }
                }
                self.schedule(TRAFFIC_POLL_MS, Timer::TrafficPoll);
            }
            Timer::Suspend => {
                self.set_suspended(true);
                let error = match self.node() {
                    Some(node) => node.swarm().suspend().await.err(),
                    None => None,
                };
                if let Some(error) = error {
                    self.fatal(error);
                }
                self.move_lan().await;
            }
        }
    }

    /// The app moved to the background; peers are dropped only if it lasts.
    pub(crate) fn suspend(&mut self, linger_ms: u64) {
        let pending = self.suspending.take();
        self.cancel(pending);
        self.suspending = Some(self.schedule(linger_ms.min(MAX_LINGER_MS), Timer::Suspend));
    }

    /// Comes back from [`Self::suspend`].
    pub(crate) async fn resume(&mut self) {
        let pending = self.suspending.take();
        self.cancel(pending);
        self.set_suspended(false);
        if let Some(node) = self.node()
            && let Err(error) = node.swarm().resume().await
        {
            self.fatal(error);
        }
        self.move_lan().await;
    }

    /// A tracker/peer change reported through the swarm listener or the node's change stream.
    pub(crate) fn changed(&mut self) {
        self.update();
    }

    /// A host's own LAN event (`Incoming::Event`), forwarded to the LAN factory.
    pub(crate) async fn host_event(&mut self, event: Value) {
        self.lan.host_event(event).await;
    }

    /// Releases the node, the loopback API and the cast controller before the host exits.
    pub(crate) async fn shutdown(&mut self) {
        self.timers.clear();
        self.close_worker().await;
    }

    /// The host could not read its input; the app reports it and carries on.
    pub(crate) fn fatal(&self, error: impl std::fmt::Display) {
        self.send(AppEvent::Fatal {
            error: error.to_string(),
        });
    }

    fn schedule(&mut self, delay_ms: u64, timer: Timer) -> (u64, u64) {
        let key = (self.now() + delay_ms, self.seq);
        self.seq += 1;
        self.timers.insert(key, timer);
        key
    }

    fn cancel(&mut self, key: Option<(u64, u64)>) {
        if let Some(key) = key {
            self.timers.remove(&key);
        }
    }

    fn is_suspended(&self) -> bool {
        self.shared.suspended.load(Ordering::Relaxed)
    }

    fn set_suspended(&self, suspended: bool) {
        self.shared.suspended.store(suspended, Ordering::Relaxed);
    }

    fn node(&self) -> Option<Arc<N>> {
        lock(&self.shared.node).clone()
    }

    fn set_node(&self, node: Option<Arc<N>>) {
        *lock(&self.shared.node) = node;
    }

    fn cast(&self) -> Option<Arc<dyn Cast>> {
        lock(&self.shared.cast).clone()
    }

    fn set_cast(&self, cast: Option<Arc<dyn Cast>>) {
        *lock(&self.shared.cast) = cast;
    }

    fn api(&self) -> Option<Arc<A>> {
        lock(&self.shared.api).clone()
    }

    fn set_api(&self, api: Option<Arc<A>>) {
        *lock(&self.shared.api) = api;
    }

    fn send(&self, event: AppEvent) {
        let _ = self.events.send(event);
    }

    fn update(&mut self) {
        self.notify("update");
        if self.update_timer.is_some() {
            return;
        }
        let delay = if self.now().saturating_sub(self.last_update_at) < UPDATE_QUIET_MS {
            UPDATE_SLOW_MS
        } else {
            UPDATE_FAST_MS
        };
        self.update_timer = Some(self.schedule(delay, Timer::Update));
    }

    /// `api.notify(event)`: the loopback API tells its own SSE clients.
    fn notify(&self, event: &str) {
        if let Some(api) = self.api() {
            api.notify(event);
        }
    }

    async fn report(&self, discovered: Vec<DiscoveredSource>) -> Report {
        Report {
            status: match self.node() {
                Some(node) => node.status().await,
                None => idle_status(),
            },
            lan: self.lan_note.clone(),
            api_url: self.api_url.clone(),
            discovered_sources: discovered,
        }
    }

    fn current(&self) -> Result<Arc<N>, String> {
        self.node().ok_or_else(|| "Not started".to_owned())
    }

    async fn current_device_ip(&self) -> Option<String> {
        if self.is_suspended() {
            return None;
        }
        let address = match self.lan.interfaces().await {
            Ok(interfaces) => crate::lan::lan_address(
                &interfaces
                    .iter()
                    .map(|(name, address, internal)| crate::lan::IfaceAddress {
                        name: name.as_str(),
                        address: *address,
                        internal: *internal,
                    })
                    .collect::<Vec<_>>(),
            )
            .map(|address| address.to_string()),
            Err(_) => None,
        };
        *lock(&self.shared.live_ip) = address.clone();
        address
    }

    async fn move_lan(&mut self) {
        let mut host: Option<String> = None;
        let mut seen = String::new();
        let live_ip = self.current_device_ip().await;
        if self.node().is_some() && self.lan_wanted && !self.is_suspended() {
            match self.lan.interfaces().await {
                Ok(interfaces) => {
                    let addresses: Vec<crate::lan::IfaceAddress<'_>> = interfaces
                        .iter()
                        .map(|(name, address, internal)| crate::lan::IfaceAddress {
                            name: name.as_str(),
                            address: *address,
                            internal: *internal,
                        })
                        .collect();
                    host = crate::lan::lan_address(&addresses).map(|address| address.to_string());
                    seen = ipv4s(&interfaces);
                }
                Err(error) => seen = format!("cannot list interfaces ({error})"),
            }
        }
        let bound = match self.node() {
            Some(node) if node.has_lan().await => self.lan_host.clone(),
            _ => None,
        };
        if host != bound {
            self.lan_host = host.clone();
            self.lan_error = None;
            if let Some(node) = self.node() {
                let lan = match host.as_deref() {
                    Some(host) => match host.parse::<Ipv4Addr>() {
                        Ok(address) => self
                            .lan
                            .create(address, &node)
                            .await
                            .map(Some)
                            .map_err(|error| error.to_string()),
                        Err(_) => Err(format!("Invalid LAN address {host}")),
                    },
                    None => Ok(None),
                };
                match lan {
                    Ok(lan) => {
                        if let Err(error) = node.set_lan(lan).await {
                            self.lan_error = Some(error.to_string());
                        }
                    }
                    Err(error) => self.lan_error = Some(error),
                }
            }
        }
        if let Some(cast) = self.cast() {
            cast.set_lan_host(live_ip).await;
        }
        let note = self.note_for(&seen);
        if note != self.lan_note {
            self.lan_note = note;
            self.update();
        }
    }

    fn note_for(&self, seen: &str) -> Option<String> {
        if self.node().is_none() || !self.lan_wanted {
            return None;
        }
        if self.is_suspended() {
            return Some("paused".to_owned());
        }
        match &self.lan_host {
            None => Some(format!(
                "found no Wi-Fi address; this device has {}",
                if seen.is_empty() {
                    "no IPv4 address"
                } else {
                    seen
                }
            )),
            Some(host) => Some(match &self.lan_error {
                Some(error) => format!("failed on {host}: {error}"),
                None => format!("on {host}"),
            }),
        }
    }

    async fn attach_node(&mut self, node: Arc<N>, owner: Owner) {
        self.set_node(Some(node.clone()));
        self.node_owner = Some(owner);
        let changes = self.changes.clone();
        let listener = node.swarm().on_update(Arc::new(move || {
            let _ = changes.send(());
        }));
        self.swarm_unlisten = Some(listener);
        let changes = self.changes.clone();
        let mut onchange = node.onchange();
        tokio::spawn(async move {
            while onchange.changed().await.is_ok() {
                if changes.send(()).is_err() {
                    return;
                }
            }
        });
        if self.is_suspended()
            && let Err(error) = node.swarm().suspend().await
        {
            self.fatal(error);
        }
    }

    /// A receiver's own node, when no start opened one.
    async fn open_receiver_node(&mut self) -> Result<Arc<N>, String> {
        if let Some(node) = self.node() {
            return Ok(node);
        }
        let factory = self
            .cast_factory
            .clone()
            .ok_or_else(|| "Cast adapters are not configured".to_owned())?;
        let storage = self
            .cast_storage
            .clone()
            .ok_or_else(|| "The app must send init with its cast storage first".to_owned())?;
        let peer_dir = factory
            .peer_storage(self.cast_peer_storage.as_deref(), &storage)
            .await;
        factory.reclaim(&peer_dir);
        let node = self
            .factory
            .create(json!({"storage": peer_dir, "worklet": true}))
            .await
            .map_err(|error| error.to_string())?;
        self.attach_node(node.clone(), Owner::Receiver).await;
        node.set_cache_max_bytes(RECEIVER_CACHE_MAX_BYTES).await;
        self.lan_wanted = true;
        self.move_lan().await;
        Ok(node)
    }

    async fn ensure_cast(&mut self) -> Result<Arc<dyn Cast>, String> {
        let storage = self
            .cast_storage
            .clone()
            .ok_or_else(|| "The app must send init with its cast storage first".to_owned())?;
        if let Some(cast) = self.cast() {
            return Ok(cast);
        }
        let factory = self
            .cast_factory
            .clone()
            .ok_or_else(|| "Cast adapters are not configured".to_owned())?;
        let shared_node = self.shared.clone();
        let live_ip = self.shared.clone();
        let shared_cast = self.shared.clone();
        let events = self.events.clone();
        let api_cell = self.shared.clone();
        let options = CastOptions {
            storage,
            node: Arc::new(move || lock(&shared_node.node).clone()),
            get_lan_host: Arc::new(move || lock(&live_ip.live_ip).clone()),
            on_change: Arc::new(move || {
                if let Some(api) = lock(&api_cell.api).clone() {
                    api.notify("cast");
                }
                send_cast_event(&events, &lock(&shared_cast.cast).clone());
            }),
            worklet: true,
        };
        loop {
            let created = factory.create(options_clone(&options)).await?;
            self.set_cast(Some(created.clone()));
            created.init_hub().await;
            created.set_lan_host(self.current_device_ip().await).await;
            if self
                .cast()
                .is_some_and(|current| Arc::ptr_eq(&current, &created))
            {
                return Ok(created);
            }
        }
    }

    /// The old swarm's listener goes first: closing it fires an update per connection.
    async fn close_worker(&mut self) {
        let update_timer = self.update_timer.take();
        self.cancel(update_timer);
        let node = self.node();
        let api = self.api();
        let cast = self.cast();
        self.set_node(None);
        self.set_api(None);
        self.set_cast(None);
        self.api_url = None;
        self.node_owner = None;
        self.lan_host = None;
        self.lan_error = None;
        self.lan_note = None;
        if let Some(cast) = cast {
            cast.close().await;
        }
        if let Some(api) = api {
            api.close().await;
        }
        let Some(node) = node else { return };
        if let Some(unlisten) = self.swarm_unlisten.take() {
            unlisten();
        }
        let _ = node.close().await;
    }

    // ----------------------------------------------------------------------------------------
    // The worker's methods

    /// `init`: the cast stack's storage, sent once before any other call.
    pub async fn init(&mut self, request: InitRequest) -> Result<OkResult, String> {
        self.cast_storage = request.cast_storage.filter(|dir| !dir.is_empty());
        Ok(OkResult { ok: true })
    }

    /// `start`: opens the node, the loopback API and the LAN binding; closes the previous ones.
    pub async fn start(&mut self, request: StartRequest) -> Result<Report, String> {
        let trackers_dir = request.trackers_dir.as_deref().unwrap_or("");
        let default_folder = request.default_folder.as_deref().unwrap_or("");
        self.close_worker().await;
        let settings = app_settings::normalize_app_settings(&request.settings, false, None)
            .map_err(|error| error.to_string())?;
        let options = app_settings::node_options(&settings, trackers_dir, default_folder);
        self.last_settings = Some(settings);
        let node = self
            .factory
            .create(json!({
                "storage": options.storage,
                "tracker": options.tracker,
                "mnemonic": options.mnemonic,
                "join": options.join,
                "sources": options.sources,
                "relayThrough": options.relay_through,
                "bootstrap": options.bootstrap,
                "privateRouting": options.private_routing,
                "worklet": true,
            }))
            .await
            .map_err(|error| error.to_string())?;
        self.attach_node(node.clone(), Owner::App).await;
        node.set_cache_max_bytes(options.cache_max_gb * 1e9).await;
        self.lan_wanted = options.lan;
        self.move_lan().await;
        let Some(api_factory) = self.api_factory.clone() else {
            return Err("The loopback API is not configured".to_owned());
        };
        let events = self.events.clone();
        let on_sources = Arc::new(move |list: Vec<Source>| {
            let _ = events.send(AppEvent::Sources {
                sources: followed_sources(&list),
            });
        });
        let url = api_factory
            .start(node.clone(), on_sources)
            .await
            .map_err(|error| error.to_string())?;
        self.api_url = url.clone();
        self.set_api(url.is_some().then_some(api_factory));
        let discovered = node
            .discovered_sources()
            .await
            .map_err(|error| error.to_string())?;
        Ok(self.report(discovered).await)
    }

    /// `normalizeSettings`: the settings form as `settings.json` stores it.
    pub async fn normalize_settings(
        &mut self,
        request: NormalizeSettingsRequest,
    ) -> Result<AppSettings, String> {
        let previous = match self.node() {
            Some(node) => Some(json!({"sources": node.list_sources().await})),
            None => self
                .last_settings
                .as_ref()
                .map(|settings| serde_json::to_value(settings).unwrap_or(Value::Null)),
        };
        app_settings::normalize_app_settings(
            &request.settings,
            request
                .require_channel
                .as_ref()
                .is_some_and(app_settings::js_truthy),
            previous.as_ref(),
        )
        .map_err(|error| error.to_string())
    }

    /// `status`: the node's status, the LAN note and the loopback API's URL.
    pub async fn status(&mut self) -> Result<Report, String> {
        let Some(node) = self.node() else {
            return Ok(self.report(Vec::new()).await);
        };
        let discovered = node
            .discovered_sources()
            .await
            .map_err(|error| error.to_string())?;
        Ok(self.report(discovered).await)
    }

    /// `search`: every entry for `id`, or the catalog the query filters.
    pub async fn search(&mut self, request: SearchRequest) -> Result<Vec<SearchEntry>, String> {
        let node = self.current()?;
        let options = crate::node::api::SearchOptions {
            source: Some(request.source),
            query: Some(request.query),
            limit: Some(request.limit.unwrap_or_else(|| Number::from(0))),
            offset: Some(request.offset.unwrap_or_else(|| Number::from(0))),
        };
        let entries = node
            .search(request.id.as_deref(), options)
            .await
            .map_err(|error| error.to_string())?;
        let api_url = self.api_url.clone();
        entries
            .into_iter()
            .map(|entry| with_download(entry, api_url.as_deref()))
            .collect()
    }

    /// `focusSource`.
    pub async fn focus_source(&mut self, request: KeyRequest) -> Result<OkResult, String> {
        let node = self.current()?;
        let ok = node
            .focus_source(&request.key)
            .await
            .map_err(|error| error.to_string())?;
        Ok(OkResult { ok })
    }

    /// `setPlayerActive`: stop fetching blocks ahead while a film plays here.
    pub async fn set_player_active(&mut self, request: ActiveRequest) -> Result<OkResult, String> {
        let node = self.current()?;
        Ok(node
            .set_player_active(request.active.as_ref().is_some_and(app_settings::js_truthy))
            .await)
    }

    /// `warm`.
    pub async fn warm(&mut self, request: KeyRequest) -> Result<OkResult, String> {
        let node = self.current()?;
        let entry = node
            .find_entry(&request.key)
            .await
            .map_err(|error| error.to_string())?;
        if let Some(entry) = &entry {
            node.warm(entry).await.map_err(|error| error.to_string())?;
        }
        Ok(OkResult {
            ok: entry.is_some(),
        })
    }

    /// `addSource`.
    pub async fn add_source(&mut self, spec: SourceSpec) -> Result<AddSourceReply, String> {
        let node = self.current()?;
        let source = node
            .add_source(spec, true)
            .await
            .map_err(|error| error.to_string())?;
        Ok(AddSourceReply {
            source,
            sources: node.list_sources().await,
            discovered: node
                .discovered_sources()
                .await
                .map_err(|error| error.to_string())?,
        })
    }

    /// `removeSource`.
    pub async fn remove_source(
        &mut self,
        request: KeyRequest,
    ) -> Result<RemoveSourceReply, String> {
        let node = self.current()?;
        let removed = node
            .remove_source(&request.key)
            .await
            .map_err(|error| error.to_string())?;
        Ok(RemoveSourceReply {
            removed: removed.removed,
            sources: node.list_sources().await,
            discovered: node
                .discovered_sources()
                .await
                .map_err(|error| error.to_string())?,
        })
    }

    /// `remove`: deletes an entry from the catalog this device writes.
    pub async fn remove_entry(&mut self, request: KeyRequest) -> Result<RemoveReply, String> {
        let node = self.current()?;
        node.remove(&request.key)
            .await
            .map_err(|error| error.to_string())?;
        self.update();
        Ok(RemoveReply {
            removed: true,
            key: Some(request.key).filter(|key| !key.is_empty()),
        })
    }

    /// `identity`.
    pub async fn identity(&mut self, request: IdentityRequest) -> Result<Identity, String> {
        let node = self.current()?;
        node.identity(
            request
                .reveal_mnemonic
                .as_ref()
                .is_some_and(app_settings::js_truthy),
        )
        .await
        .map_err(|error| error.to_string())
    }

    /// `revokeWriter`.
    pub async fn revoke_writer(&mut self, request: RevokeWriterRequest) -> Result<Revoke, String> {
        let node = self.current()?;
        let revoke = node
            .revoke_writer(
                &request.writer,
                request
                    .purge_entries
                    .as_ref()
                    .is_some_and(app_settings::js_truthy),
            )
            .await
            .map_err(|error| error.to_string())?;
        self.update();
        Ok(revoke)
    }

    /// `rotateEpoch`.
    pub async fn rotate_epoch(&mut self) -> Result<Epoch, String> {
        let node = self.current()?;
        let epoch = node
            .rotate_epoch()
            .await
            .map_err(|error| error.to_string())?;
        self.update();
        Ok(epoch)
    }

    /// `clearCache`: one entry's cache, or every entry's when no key is given.
    pub async fn clear_cache(
        &mut self,
        request: ClearCacheRequest,
    ) -> Result<CacheCleared, String> {
        let node = self.current()?;
        let cleared = node
            .clear_cache(request.key.as_deref(), None)
            .await
            .map_err(|error| error.to_string())?;
        self.update();
        Ok(cleared)
    }

    /// `setCacheMax`: the playback cache ceiling, applied without reopening the node.
    pub async fn set_cache_max(&mut self, request: SetCacheMaxRequest) -> Result<CacheMax, String> {
        let node = self.current()?;
        let cache_max_bytes = node
            .set_cache_max_bytes(request.max_bytes.unwrap_or(0.0))
            .await;
        self.update();
        Ok(CacheMax { cache_max_bytes })
    }

    /// `castStatus`.
    pub async fn cast_status(&mut self) -> Result<CastStatus, String> {
        let status = self.ensure_cast().await?.status();
        serde_json::from_value(status).map_err(|error| error.to_string())
    }

    /// `castPair`.
    pub async fn cast_pair(&mut self, params: Value) -> Result<CastPairReply, String> {
        let cast = self.ensure_cast().await?;
        let receiver = cast.pair_receiver(&params).await?;
        let paired_id = receiver
            .get("id")
            .and_then(Value::as_str)
            .filter(|id| !id.is_empty())
            .unwrap_or("")
            .to_owned();
        let status = serde_json::from_value(cast.status()).map_err(|error| error.to_string())?;
        Ok(CastPairReply { status, paired_id })
    }

    /// `castRemove`.
    pub async fn cast_remove(&mut self, request: CastRemoveRequest) -> Result<CastStatus, String> {
        let cast = self.ensure_cast().await?;
        cast.remove_receiver(&request.id);
        serde_json::from_value(cast.status()).map_err(|error| error.to_string())
    }

    /// `castPlay`.
    pub async fn cast_play(&mut self, params: Value) -> Result<Value, String> {
        let cast = self.ensure_cast().await?;
        let needs_lan = match self.node() {
            Some(node) => !node.private_routing() && !self.lan_wanted,
            None => false,
        };
        if needs_lan {
            self.lan_wanted = true;
            self.move_lan().await;
        } else {
            let host = self.current_device_ip().await;
            cast.set_lan_host(host).await;
        }
        cast.cast_play(&params).await
    }

    /// `castAttach`.
    pub async fn cast_attach(&mut self, params: Value) -> Result<Value, String> {
        self.ensure_cast().await?.cast_attach(&params).await
    }

    /// `castControl`.
    pub async fn cast_control(&mut self, params: Value) -> Result<Value, String> {
        self.ensure_cast().await?.cast_control(&params).await
    }

    /// `castStop`.
    pub async fn cast_stop(&mut self, params: Value) -> Result<Value, String> {
        self.ensure_cast().await?.cast_stop(&params).await
    }

    /// `receiverStart`: this device becomes the TV a sender pairs with.
    pub async fn receiver_start(&mut self, request: ReceiverStartRequest) -> Result<Value, String> {
        let next_peer = request
            .peer_storage
            .clone()
            .or_else(|| self.cast_peer_storage.clone());
        let moved_peer = next_peer.is_some() && next_peer != self.cast_peer_storage;
        let retire = match self.cast() {
            Some(cast) => {
                moved_peer
                    && self.node_owner == Some(Owner::Receiver)
                    && cast
                        .status()
                        .get("receiverMode")
                        .and_then(|mode| mode.get("state"))
                        .and_then(|state| state.get("status"))
                        .and_then(Value::as_str)
                        == Some("idle")
            }
            None => false,
        };
        if retire {
            self.close_worker().await;
        }
        if let Some(next) = next_peer {
            self.cast_peer_storage = Some(next);
        }
        let cast = self.ensure_cast().await?;
        self.open_receiver_node().await?;
        let events = self.events.clone();
        let options = ReceiverStart {
            name: request.name.unwrap_or_else(|| "PearTube TV".to_owned()),
            open_auth: request
                .open_auth
                .as_ref()
                .is_some_and(app_settings::js_truthy),
            peer_storage: self.cast_peer_storage.clone(),
        };
        cast.start_local_receiver(
            options,
            Arc::new(move |command| {
                if let Ok(command) = serde_json::from_value(command) {
                    let _ = events.send(AppEvent::ReceiverCommand { command });
                }
            }),
        )
        .await
    }

    /// `receiverReportState`: the TV's playback state, or the whole request when it has none.
    pub async fn receiver_report_state(&mut self, params: Value) -> Result<Value, String> {
        let cast = self.ensure_cast().await?;
        let patch = match params
            .get("state")
            .filter(|state| app_settings::js_truthy(state))
            .cloned()
        {
            Some(state) => state,
            None => params,
        };
        Ok(cast
            .report_local_receiver_state(&patch)
            .await
            .unwrap_or(Value::Null))
    }

    /// `openUri`: a channel, access, watch or receiver link.
    pub async fn open_uri(&mut self, request: OpenUriRequest) -> Result<OpenUriReply, String> {
        let parsed = protocol::parse_uri(&request.uri).map_err(|error| error.to_string())?;
        if matches!(&parsed, ParsedUri::Receiver { .. }) {
            let cast = self.ensure_cast().await?;
            let receiver = cast
                .pair_receiver(&json!({"uri": protocol::js_trim(&request.uri)}))
                .await?;
            return Ok(OpenUriReply::Receiver { receiver });
        }
        if self.node().is_none()
            && let ParsedUri::Source { key, kind, alias } = parsed
        {
            return Ok(OpenUriReply::Source {
                source: OpenedSource::Brief(FollowedSource {
                    key,
                    kind: kind.to_owned(),
                    alias,
                }),
            });
        }
        let node = self.current()?;
        let opened = node
            .open_uri(&request.uri, false)
            .await
            .map_err(|error| error.to_string())?;
        let api_url = self.api_url.clone();
        Ok(match opened {
            OpenUri::Source { source } => OpenUriReply::Source {
                source: OpenedSource::Full(source),
            },
            OpenUri::Access {
                channel,
                epoch,
                source,
            } => OpenUriReply::Access {
                channel,
                epoch,
                source,
            },
            OpenUri::Watch { entry } => OpenUriReply::Watch {
                entry: Box::new(with_download(entry, api_url.as_deref())?),
            },
        })
    }
}

fn options_clone<N: Node>(options: &CastOptions<N>) -> CastOptions<N> {
    CastOptions {
        storage: options.storage.clone(),
        node: options.node.clone(),
        get_lan_host: options.get_lan_host.clone(),
        on_change: options.on_change.clone(),
        worklet: options.worklet,
    }
}

fn send_cast_event(events: &UnboundedSender<AppEvent>, cast: &Option<Arc<dyn Cast>>) {
    if let Some(cast) = cast
        && let Ok(cast) = serde_json::from_value(cast.status())
    {
        let _ = events.send(AppEvent::Cast { cast });
    }
}

/// `idleStatus()`: the empty node the app shows before (or without) a start.
fn idle_status() -> Status {
    let zero = || Number::from(0);
    let route = || Route {
        peers: 0,
        rx_bytes: 0,
        tx_bytes: 0,
        down_bps: zero(),
        up_bps: zero(),
    };
    Status {
        tracker: String::new(),
        channel: String::new(),
        writer: String::new(),
        writable: false,
        blobs: String::new(),
        blob_bytes: 0,
        cached_bytes: 0,
        cache_max_bytes: 0,
        peers: 0,
        lan_peers: 0,
        down_bps: zero(),
        up_bps: zero(),
        rx_bytes: 0,
        tx_bytes: 0,
        routes: Routes {
            private: route(),
            public: route(),
            lan: route(),
        },
        inflight_blocks: 0,
        rtt: zero(),
        retransmits: 0,
        sources: Vec::new(),
        reciprocity: Reciprocity {
            tier: "leecher".to_owned(),
            seeded_bytes: 0,
            contributing_peers: 0,
            ratio: zero(),
            inflight_max: 0,
        },
    }
}

/// `followedSources()`: what settings.json saves, the node's own channel excluded.
pub(crate) fn followed_sources(list: &[Source]) -> Vec<FollowedSource> {
    list.iter()
        .filter(|source| !source.writable && (!source.primary || !source.alias.is_empty()))
        .map(|source| FollowedSource {
            key: source.key.clone(),
            kind: source.kind.clone(),
            alias: source.alias.clone(),
        })
        .collect()
}

/// `ipv4s()`: the interfaces line of the LAN note.
fn ipv4s(interfaces: &[(String, std::net::IpAddr, bool)]) -> String {
    interfaces
        .iter()
        .filter(|(_, address, internal)| address.is_ipv4() && !internal)
        .map(|(name, address, _)| format!("{name} {address}"))
        .collect::<Vec<_>>()
        .join(", ")
}

/// `withDownload()`: the loopback URL that saves a film with the right extension.
fn with_download(
    entry: crate::node::api::Entry,
    api_url: Option<&str>,
) -> Result<SearchEntry, String> {
    let download_url = match api_url {
        Some(api_url) if !entry.stream_url.is_empty() => {
            let url = url::Url::parse(&entry.stream_url).map_err(|_| "Invalid URL".to_owned())?;
            let query = match url.query() {
                Some(query) => format!("?{query}"),
                None => String::new(),
            };
            Some(format!(
                "{api_url}/v1/blob{query}&download={}",
                encode_uri_component(&entry.title)
            ))
        }
        _ => None,
    };
    Ok(SearchEntry {
        entry,
        download_url,
    })
}

/// `encodeURIComponent()`: the unreserved set plus the characters it leaves alone.
fn encode_uri_component(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    for byte in text.bytes() {
        match byte {
            b'A'..=b'Z'
            | b'a'..=b'z'
            | b'0'..=b'9'
            | b'-'
            | b'_'
            | b'.'
            | b'!'
            | b'~'
            | b'*'
            | b'\''
            | b'('
            | b')' => out.push(byte as char),
            _ => out.push_str(&format!("%{byte:02X}")),
        }
    }
    out
}

/// Clocks the app itself: the process clock and a tokio sleep.
pub struct SystemClock;

impl Clock for SystemClock {
    fn now_ms(&self) -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|elapsed| elapsed.as_millis() as u64)
            .unwrap_or(0)
    }

    fn wait_until(&self, deadline_ms: u64) -> Pin<Box<dyn Future<Output = ()> + Send + '_>> {
        Box::pin(async move {
            let wait = deadline_ms.saturating_sub(self.now_ms());
            tokio::time::sleep(std::time::Duration::from_millis(wait)).await;
        })
    }
}

// --------------------------------------------------------------------------------------------
// In-process host

/// The app over the native adapters: the same stack `worker.js` runs on this machine.
pub type NativeApp = App<
    crate::node::core::CoreNode,
    crate::node::factory::Factory,
    SystemClock,
    crate::lan::Factory,
    crate::adapters::LoopbackApi,
    crate::cast::adapters::Factory,
>;

type Call = Box<
    dyn for<'a> FnOnce(&'a mut NativeApp) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>> + Send,
>;
type Subscriber = Box<dyn Fn(&AppEvent) -> bool + Send + Sync>;

enum Command {
    Call(Call),
    Suspend(u64),
    Resume,
    Shutdown,
}

/// A [`NativeApp`] on its own thread, called and subscribed to by typed messages.
pub struct AppHost {
    commands: UnboundedSender<Command>,
    subscribers: Arc<Mutex<Vec<Subscriber>>>,
    thread: Mutex<Option<JoinHandle<()>>>,
}

impl AppHost {
    /// Starts the app with `cast_storage` already initialized, as the Mac app does at launch.
    pub fn spawn(cast_storage: Option<String>) -> Result<Self, String> {
        let (commands, mut incoming) = mpsc::unbounded_channel();
        let subscribers: Arc<Mutex<Vec<Subscriber>>> = Arc::new(Mutex::new(Vec::new()));
        let (ready, started) = std_mpsc::sync_channel(1);
        let forwarded = subscribers.clone();
        let thread = thread::Builder::new()
            .name("peartube-app".into())
            .spawn(move || {
                let runtime = match tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                {
                    Ok(runtime) => {
                        let _ = ready.send(Ok(()));
                        runtime
                    }
                    Err(error) => {
                        let _ = ready.send(Err(error.to_string()));
                        return;
                    }
                };
                runtime.block_on(async move {
                    let clock = Arc::new(SystemClock);
                    let (events, mut event_rx) = mpsc::unbounded_channel();
                    let (changes, mut change_rx) = mpsc::unbounded_channel();
                    let adapters = crate::backend::native_adapters(protocol::API_PORT);
                    let mut app = NativeApp::new(
                        adapters.factory,
                        clock.clone(),
                        adapters.lan,
                        adapters.api,
                        adapters.cast,
                        events,
                        changes,
                    );
                    let _ = app.init(InitRequest { cast_storage }).await;
                    loop {
                        while let Some(timer) = app.take_due() {
                            app.timer(timer).await;
                        }
                        drain(&forwarded, &mut event_rx);
                        let deadline = app.next_deadline();
                        tokio::select! {
                            command = incoming.recv() => match command {
                                None => break,
                                Some(Command::Call(call)) => call(&mut app).await,
                                Some(Command::Suspend(linger)) => app.suspend(linger),
                                Some(Command::Resume) => app.resume().await,
                                Some(Command::Shutdown) => {
                                    app.shutdown().await;
                                    break;
                                }
                            },
                            () = clock.wait_until(deadline.unwrap_or(u64::MAX)), if deadline.is_some() => {},
                            Some(()) = change_rx.recv() => app.changed(),
                            Some(event) = event_rx.recv() => broadcast(&forwarded, &event),
                        }
                        drain(&forwarded, &mut event_rx);
                    }
                });
            })
            .map_err(|error| error.to_string())?;
        started
            .recv()
            .map_err(|error| error.to_string())?
            .map_err(|error: String| error)?;
        Ok(Self {
            commands,
            subscribers,
            thread: Mutex::new(Some(thread)),
        })
    }

    /// Runs one method on the app's thread and answers with its typed reply.
    pub async fn call<T: Send + 'static>(
        &self,
        task: impl for<'a> FnOnce(
            &'a mut NativeApp,
        )
            -> Pin<Box<dyn Future<Output = Result<T, String>> + Send + 'a>>
        + Send
        + 'static,
    ) -> Result<T, String> {
        let (reply, answered) = tokio::sync::oneshot::channel();
        let call: Call = Box::new(move |app| {
            Box::pin(async move {
                let _ = reply.send(task(app).await);
            })
        });
        self.commands
            .send(Command::Call(call))
            .map_err(|_| "the app is shut down".to_owned())?;
        answered.await.map_err(|_| "the app stopped".to_owned())?
    }

    /// Every event, as the host's own type: `map` may answer with several values (or none).
    pub fn subscribe<T: Send + 'static>(
        &self,
        map: impl Fn(&AppEvent) -> Vec<T> + Send + Sync + 'static,
    ) -> futures_channel::mpsc::UnboundedReceiver<T> {
        let (events, received) = futures_channel::mpsc::unbounded();
        lock(&self.subscribers).push(Box::new(move |event| {
            map(event)
                .into_iter()
                .all(|value| events.unbounded_send(value).is_ok())
        }));
        received
    }

    pub fn suspend(&self, linger_ms: u64) {
        let _ = self.commands.send(Command::Suspend(linger_ms));
    }

    pub fn resume(&self) {
        let _ = self.commands.send(Command::Resume);
    }

    pub fn shutdown(&self) {
        let _ = self.commands.send(Command::Shutdown);
        if let Some(handle) = lock(&self.thread).take()
            && handle.thread().id() != thread::current().id()
        {
            let _ = handle.join();
        }
    }
}

impl Drop for AppHost {
    fn drop(&mut self) {
        self.shutdown();
    }
}

fn broadcast(subscribers: &Mutex<Vec<Subscriber>>, event: &AppEvent) {
    lock(subscribers).retain(|sink| sink(event));
}

fn drain(subscribers: &Mutex<Vec<Subscriber>>, events: &mut mpsc::UnboundedReceiver<AppEvent>) {
    while let Ok(event) = events.try_recv() {
        broadcast(subscribers, &event);
    }
}
