//! The app worker: `worklet/worker.js`, ported.
//!
//! One worker owns the node, the loopback API, the cast controller and the LAN binding, and turns
//! newline-delimited JSON request lines into response lines and events. Everything the worker does
//! runs on the backend's own thread, one input at a time, so `start`, `stop` and the LAN moves can
//! never overlap — the guarantee `serial()` gives the JavaScript worker.

use std::collections::BTreeMap;
use std::future::Future;
use std::net::Ipv4Addr;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};

use serde_json::{Map, Value, json};
use tokio::sync::mpsc::UnboundedSender;

use crate::app_settings::{self, AppSettings};
use crate::ipc::{self, Incoming};
use crate::node::Node;
use crate::node::api::{
    ApiServer, Cast, CastFactory, CastOptions, Clock, LanFactory, NodeFactory, ReceiverStart,
    Source, Swarm,
};
use crate::protocol::{self, ParsedUri};

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

type Sink = Arc<dyn Fn(String) + Send + Sync>;

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

/// State the adapters reach back into while the worker is inside an adapter call.
struct Shared<N: Node, A> {
    node: Mutex<Option<Arc<N>>>,
    api: Mutex<Option<Arc<A>>>,
    cast: Mutex<Option<Arc<dyn Cast>>>,
    suspended: AtomicBool,
    live_ip: Mutex<Option<String>>,
}

pub(crate) struct Worker<N, F, C, L, A, K>
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
    send: Sink,
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

impl<N, F, C, L, A, K> Worker<N, F, C, L, A, K>
where
    N: Node + 'static,
    F: NodeFactory<Node = N> + 'static,
    C: Clock + 'static,
    L: LanFactory<N> + 'static,
    A: ApiServer<N> + 'static,
    K: CastFactory<N> + 'static,
{
    pub(crate) fn new(
        factory: Arc<F>,
        clock: Arc<C>,
        lan: Arc<L>,
        api: Option<Arc<A>>,
        cast: Option<Arc<K>>,
        send: Sink,
        changes: UnboundedSender<()>,
    ) -> Self {
        let shared = Arc::new(Shared {
            node: Mutex::new(None),
            api: Mutex::new(None),
            cast: Mutex::new(None),
            suspended: AtomicBool::new(false),
            live_ip: Mutex::new(None),
        });
        let mut worker = Self {
            factory,
            clock,
            lan,
            api_factory: api,
            cast_factory: cast,
            send,
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
        worker.schedule(LAN_POLL_MS, Timer::LanPoll);
        worker.schedule(TRAFFIC_POLL_MS, Timer::TrafficPoll);
        worker
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

    fn emit(&self, message: Value) {
        emit(&self.send, message);
    }

    fn line(&self, line: String) {
        (self.send)(line);
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

    fn fatal(&self, error: impl std::fmt::Display) {
        self.emit(json!({"event": "fatal", "error": error.to_string()}));
    }

    async fn report(&self) -> Value {
        let status = match self.node() {
            Some(node) => serde_json::to_value(node.status().await).unwrap_or(Value::Null),
            None => idle_status(),
        };
        let mut fields = match status {
            Value::Object(fields) => fields,
            other => {
                let mut fields = Map::new();
                fields.insert("status".into(), other);
                fields
            }
        };
        fields.insert("lan".into(), option_json(&self.lan_note));
        fields.insert("apiUrl".into(), option_json(&self.api_url));
        Value::Object(fields)
    }

    async fn with_report(&self, discovered: Value) -> Value {
        let mut value = self.report().await;
        if let Value::Object(fields) = &mut value {
            fields.insert("discoveredSources".into(), discovered);
        }
        value
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
        let send = self.send.clone();
        let api_cell = self.shared.clone();
        let options = CastOptions {
            storage,
            node: Arc::new(move || lock(&shared_node.node).clone()),
            get_lan_host: Arc::new(move || lock(&live_ip.live_ip).clone()),
            on_change: Arc::new(move || {
                if let Some(api) = lock(&api_cell.api).clone() {
                    api.notify("cast");
                }
                send_cast_event(&send, &lock(&shared_cast.cast).clone());
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

    async fn call(&mut self, method: &str, params: Value) -> Result<Value, String> {
        match method {
            "init" => {
                self.cast_storage = params
                    .get("castStorage")
                    .and_then(Value::as_str)
                    .filter(|dir| !dir.is_empty())
                    .map(str::to_owned);
                Ok(json!({"ok": true}))
            }
            "start" => self.start(params).await,
            "normalizeSettings" => {
                let previous = match self.node() {
                    Some(node) => Some(json!({"sources": node.list_sources().await})),
                    None => self
                        .last_settings
                        .as_ref()
                        .map(|settings| serde_json::to_value(settings).unwrap_or(Value::Null)),
                };
                app_settings::normalize_app_settings(
                    params.get("settings").unwrap_or(&Value::Null),
                    params
                        .get("requireChannel")
                        .is_some_and(app_settings::js_truthy),
                    previous.as_ref(),
                )
                .map(|settings| serde_json::to_value(settings).unwrap_or(Value::Null))
                .map_err(|error| error.to_string())
            }
            "status" => {
                if self.node().is_none() {
                    return Ok(self.with_report(json!([])).await);
                }
                let node = self.current()?;
                let discovered = node
                    .discovered_sources()
                    .await
                    .map_err(|error| error.to_string())?;
                let discovered = serde_json::to_value(discovered).unwrap_or_else(|_| json!([]));
                Ok(self.with_report(discovered).await)
            }
            "castStatus" => Ok(self.ensure_cast().await?.status()),
            "castPair" => {
                let cast = self.ensure_cast().await?;
                let receiver = cast.pair_receiver(&params).await?;
                let paired_id = receiver
                    .get("id")
                    .and_then(Value::as_str)
                    .filter(|id| !id.is_empty())
                    .unwrap_or("")
                    .to_owned();
                let mut status = cast.status();
                if let Value::Object(fields) = &mut status {
                    fields.insert("pairedId".into(), Value::String(paired_id));
                }
                Ok(status)
            }
            "castRemove" => {
                let cast = self.ensure_cast().await?;
                cast.remove_receiver(params.get("id").and_then(Value::as_str).unwrap_or(""));
                Ok(cast.status())
            }
            "castPlay" => {
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
            "castAttach" => self.ensure_cast().await?.cast_attach(&params).await,
            "castControl" => self.ensure_cast().await?.cast_control(&params).await,
            "castStop" => self.ensure_cast().await?.cast_stop(&params).await,
            "receiverStart" => self.receiver_start(params).await,
            "receiverReportState" => {
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
            "openUri" => self.open_uri(params).await,
            other => self.node_call(other, params).await,
        }
    }

    async fn node_call(&mut self, method: &str, params: Value) -> Result<Value, String> {
        let node = self.node();
        let api_url = self.api_url.clone();
        let update_now = std::cell::Cell::new(false);
        let result = crate::ipc::node_method(node.as_deref(), method, &params, || {
            update_now.set(true);
        })
        .await;
        let Some(result) = result else {
            return Err(format!("Unknown method {method}"));
        };
        if update_now.get() {
            self.update();
        }
        let mut value = result?;
        if method == "search"
            && let Value::Array(entries) = &mut value
        {
            for entry in entries {
                with_download(entry, api_url.as_deref())?;
            }
        }
        Ok(value)
    }

    async fn open_uri(&mut self, params: Value) -> Result<Value, String> {
        let uri = params.get("uri").and_then(Value::as_str).unwrap_or("");
        let parsed = protocol::parse_uri(uri).map_err(|error| error.to_string())?;
        if matches!(&parsed, ParsedUri::Receiver { .. }) {
            let cast = self.ensure_cast().await?;
            let receiver = cast
                .pair_receiver(&json!({"uri": protocol::js_trim(uri)}))
                .await?;
            return Ok(json!({"action": "receiver", "receiver": receiver}));
        }
        if self.node().is_none()
            && let ParsedUri::Source { key, kind, alias } = parsed
        {
            return Ok(
                json!({"action": "source", "source": {"key": key, "kind": kind, "alias": alias}}),
            );
        }
        let node = self.current()?;
        let opened = node
            .open_uri(uri, false)
            .await
            .map_err(|error| error.to_string())?;
        let mut value = serde_json::to_value(opened).map_err(|error| error.to_string())?;
        if let Value::Object(fields) = &mut value
            && let Some(entry) = fields.get_mut("entry")
        {
            with_download(entry, self.api_url.as_deref())?;
        }
        Ok(value)
    }

    async fn start(&mut self, params: Value) -> Result<Value, String> {
        let trackers_dir = params
            .get("trackersDir")
            .and_then(Value::as_str)
            .unwrap_or("");
        let default_folder = params
            .get("defaultFolder")
            .and_then(Value::as_str)
            .unwrap_or("");
        self.close_worker().await;
        let settings = app_settings::normalize_app_settings(
            params.get("settings").unwrap_or(&Value::Null),
            false,
            None,
        )
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
        let send = self.send.clone();
        let on_sources = Arc::new(move |list: Vec<Source>| {
            emit(
                &send,
                json!({"event": "sources", "sources": followed_sources(&list)}),
            );
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
        let discovered = serde_json::to_value(discovered).unwrap_or_else(|_| json!([]));
        Ok(self.with_report(discovered).await)
    }

    async fn receiver_start(&mut self, params: Value) -> Result<Value, String> {
        let peer_storage = params
            .get("peerStorage")
            .and_then(Value::as_str)
            .map(str::to_owned);
        let name = params
            .get("name")
            .and_then(Value::as_str)
            .unwrap_or("PearTube TV")
            .to_owned();
        let open_auth = params.get("openAuth").is_some_and(app_settings::js_truthy);
        let next_peer = peer_storage
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
        let send = self.send.clone();
        let options = ReceiverStart {
            name,
            open_auth,
            peer_storage: self.cast_peer_storage.clone(),
        };
        cast.start_local_receiver(
            options,
            Arc::new(move |command| {
                emit(
                    &send,
                    json!({"event": "receiverCommand", "command": command}),
                )
            }),
        )
        .await
    }

    /// Handle one input line; the loop awaits this so inputs never overlap.
    pub(crate) async fn input(&mut self, line: &str) {
        match ipc::decode(line) {
            Incoming::Malformed => {
                self.emit(json!({"event": "fatal", "error": "Malformed message from app"}));
            }
            Incoming::Event(event) => self.lan.host_event(event).await,
            Incoming::Request { id, method, params } => {
                let name = method_name(&method);
                let response = match self.call(&name, params).await {
                    Ok(value) => ipc::response(id, Ok(value)),
                    Err(error) => ipc::response(id, Err(error)),
                };
                self.line(response);
            }
        }
    }

    pub(crate) async fn timer(&mut self, timer: Timer) {
        match timer {
            Timer::Update => {
                self.last_update_at = self.now();
                self.emit(json!({"event": "update"}));
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

pub(crate) fn emit(send: &Sink, message: Value) {
    if let Ok(text) = serde_json::to_string(&message) {
        send(format!("{text}\n"));
    }
}

fn send_cast_event(send: &Sink, cast: &Option<Arc<dyn Cast>>) {
    if let Some(cast) = cast {
        emit(send, json!({"event": "cast", "cast": cast.status()}));
    }
}

fn option_json(value: &Option<String>) -> Value {
    value.clone().map(Value::String).unwrap_or(Value::Null)
}

/// The method name as the app sent it, as `String(msg.method)` would print it.
fn method_name(method: &Value) -> String {
    match method {
        Value::String(name) => name.clone(),
        Value::Null => "undefined".to_owned(),
        other => other.to_string(),
    }
}

/// `idleStatus()`: the empty node the app shows before (or without) a start.
pub(crate) fn idle_status() -> Value {
    json!({
        "tracker": "", "channel": "", "writer": "", "writable": false, "blobs": "", "blobBytes": 0,
        "cachedBytes": 0, "cacheMaxBytes": 0, "peers": 0, "lanPeers": 0, "downBps": 0, "upBps": 0,
        "rxBytes": 0, "txBytes": 0,
        "routes": {
            "private": {"peers": 0, "rxBytes": 0, "txBytes": 0, "downBps": 0, "upBps": 0},
            "public": {"peers": 0, "rxBytes": 0, "txBytes": 0, "downBps": 0, "upBps": 0},
            "lan": {"peers": 0, "rxBytes": 0, "txBytes": 0, "downBps": 0, "upBps": 0}
        },
        "inflightBlocks": 0, "rtt": 0, "retransmits": 0, "sources": [],
        "reciprocity": {"tier": "leecher", "seededBytes": 0, "contributingPeers": 0, "ratio": 0, "inflightMax": 0}
    })
}

/// `followedSources()`: what settings.json saves, the node's own channel excluded.
pub(crate) fn followed_sources(list: &[Source]) -> Vec<Value> {
    list.iter()
        .filter(|source| !source.writable && (!source.primary || !source.alias.is_empty()))
        .map(|source| json!({"key": source.key, "kind": source.kind, "alias": source.alias}))
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
fn with_download(entry: &mut Value, api_url: Option<&str>) -> Result<(), String> {
    let Value::Object(fields) = entry else {
        return Ok(());
    };
    let Some(api_url) = api_url else {
        return Ok(());
    };
    let Some(stream_url) = fields.get("streamUrl").and_then(Value::as_str) else {
        return Ok(());
    };
    let url = url::Url::parse(stream_url).map_err(|_| "Invalid URL".to_owned())?;
    let query = match url.query() {
        Some(query) => format!("?{query}"),
        None => String::new(),
    };
    let title = fields
        .get("title")
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_owned();
    fields.insert(
        "downloadUrl".into(),
        Value::String(format!(
            "{api_url}/v1/blob{query}&download={}",
            encode_uri_component(&title)
        )),
    );
    Ok(())
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

/// Clocks the worker itself: the process clock and a tokio sleep.
pub(crate) struct SystemClock;

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
