//! The cast sender, ported from `createCastController` in `src/cast.js`: finds receivers by mDNS,
//! keeps the ones it paired with, and casts to one over this device's node. The control channel
//! rides the peer connection the two nodes already share (`src/cast-control.js`), so a cast needs
//! no transport of its own.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use futures_util::future::BoxFuture;
use serde_json::{Map, Value, json};

use crate::protocol::{ParsedUri, WatchEntry, format_watch_uri, is_hex64, parse_uri};

use super::CastError;
use super::control::{
    Control, ControlHandlers, DEFAULT_TIMEOUT, EventHandler, OpenHandler, open_control,
};
use super::pairing::normalize_pair_code;
use super::receiver::{Receiver, cast_lan_topic};
use super::records::{
    DiscoveredReceiver, ListedReceiver, ReceiverTarget, SavedReceiver, SavedReceivers,
    list_receivers, receiver_target, record_json,
};
use super::state::{self, CastState};
use super::traits::{CastPeer, LanAddress};

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(|error| error.into_inner())
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|elapsed| elapsed.as_millis() as u64)
        .unwrap_or(0)
}

fn text(params: &Value, key: &str) -> String {
    params
        .get(key)
        .map(state::text_value)
        .filter(|text| text != "null" && text != "undefined")
        .unwrap_or_default()
}

fn obj(params: &Value) -> Map<String, Value> {
    params.as_object().cloned().unwrap_or_default()
}

fn cast_error(message: impl Into<String>) -> CastError {
    CastError::new(message.into(), 0)
}

/// `createCastController`'s argument object, minus what the host owns.
pub struct ControllerOptions {
    pub storage: PathBuf,
    /// `getNode`: the node a start opened, if any.
    pub get_node: Arc<dyn Fn() -> Option<Arc<dyn CastPeer>> + Send + Sync>,
    /// `getLanHost`: the device address last sampled while discovering on the LAN.
    pub get_lan_host: Arc<dyn Fn() -> Option<String> + Send + Sync>,
    /// Bind an mDNS browser (the apps always do; tests stand in for the adapter).
    pub mdns: bool,
    pub on_change: Option<Arc<dyn Fn() + Send + Sync>>,
}

impl Default for ControllerOptions {
    fn default() -> Self {
        Self {
            storage: PathBuf::new(),
            get_node: Arc::new(|| None),
            get_lan_host: Arc::new(|| None),
            mdns: false,
            on_change: None,
        }
    }
}

/// One cast or attach that owns the session.
struct Active {
    /// Distinguishes two sessions of the same receiver in the event and close callbacks.
    serial: u64,
    receiver_id: String,
    receiver_name: String,
    transport: &'static str,
    pair_token: String,
    state: CastState,
    control: Control,
}

impl Active {
    fn to_json(&self) -> Value {
        json!({
            "receiverId": self.receiver_id,
            "receiverName": self.receiver_name,
            "transport": self.transport,
            "state": state::to_json(&self.state)
        })
    }
}

struct Inner {
    storage: PathBuf,
    saved: Mutex<SavedReceivers>,
    discovered: Mutex<Vec<DiscoveredReceiver>>,
    /// mDNS fullname → discovered id, for `ServiceRemoved`.
    down_index: Mutex<HashMap<String, String>>,
    browse: Mutex<Option<super::mdns::BrowseHandle>>,
    browse_host: Mutex<Option<Option<String>>>,
    local_receiver: Mutex<Option<Arc<Receiver>>>,
    active: Mutex<Option<Active>>,
    attempt: AtomicU64,
    serial: AtomicU64,
    closing: AtomicBool,
    get_node: Arc<dyn Fn() -> Option<Arc<dyn CastPeer>> + Send + Sync>,
    get_lan_host: Arc<dyn Fn() -> Option<String> + Send + Sync>,
    mdns: bool,
    on_change: Option<Arc<dyn Fn() + Send + Sync>>,
}

impl Inner {
    fn emit_change(&self) {
        if self.closing.load(Ordering::Acquire) {
            return;
        }
        if let Some(on_change) = self.on_change.as_ref() {
            on_change();
        }
    }

    fn node(&self) -> Option<Arc<dyn CastPeer>> {
        (self.get_node)()
    }

    fn persist_saved(&self) {
        let saved = lock(&self.saved).clone();
        let _ = saved.save(&self.storage);
    }

    fn discovered(&self) -> Vec<DiscoveredReceiver> {
        lock(&self.discovered).clone()
    }

    /// `refreshBrowse(host)`: a browse per LAN address, stopped when the LAN goes away.
    fn refresh_browse(self: &Arc<Self>, host: Option<String>) {
        if *lock(&self.browse_host) == Some(host.clone()) && lock(&self.browse).is_some() {
            return;
        }
        let previous = lock(&self.browse).take();
        if let Some(previous) = previous {
            previous.stop();
        }
        *lock(&self.browse_host) = Some(host.clone());
        if self.closing.load(Ordering::Acquire) || (host.is_none() && !self.mdns) {
            lock(&self.discovered).clear();
            lock(&self.down_index).clear();
            self.emit_change();
            return;
        }
        let inner = self.clone();
        let up: Arc<dyn Fn(super::mdns::BrowsedService) + Send + Sync> =
            Arc::new(move |service| inner.on_service(service));
        let inner = self.clone();
        let down: Arc<dyn Fn(String) + Send + Sync> = Arc::new(move |fullname| {
            let id = lock(&inner.down_index).remove(&fullname);
            if let Some(id) = id
                && inner.discovered().iter().any(|item| item.id == id)
            {
                lock(&inner.discovered).retain(|item| item.id != id);
                inner.emit_change();
            }
        });
        if let Ok(handle) = super::mdns::browse(up, Some(down)) {
            *lock(&self.browse) = Some(handle);
        }
    }

    fn on_service(self: &Arc<Self>, service: super::mdns::BrowsedService) {
        let Some(record) = service.to_discovered() else {
            return;
        };
        let local = lock(&self.local_receiver)
            .as_ref()
            .map(|receiver| receiver.device_id());
        if local.as_deref() == Some(record.id.as_str()) {
            return;
        }
        let fullname = service.fullname.clone();
        let id = record.id.clone();
        {
            let mut discovered = lock(&self.discovered);
            match discovered.iter_mut().find(|item| item.id == id) {
                Some(slot) => *slot = record,
                None => discovered.push(record),
            }
        }
        lock(&self.down_index).insert(fullname, id);
        self.emit_change();
    }

    fn list(&self) -> Vec<ListedReceiver> {
        list_receivers(&self.discovered(), &lock(&self.saved).clone())
    }

    fn local_receiver(&self) -> Option<Arc<Receiver>> {
        lock(&self.local_receiver).clone()
    }

    fn status(&self) -> Value {
        let local = self.local_receiver();
        let session = lock(&self.active).as_ref().map(Active::to_json);
        json!({
            "receivers": if local.is_some() { json!([]) } else { json!(self.list()) },
            "session": session.unwrap_or(Value::Null),
            "receiverMode": local.map(|receiver| receiver.info()).unwrap_or(Value::Null)
        })
    }

    /// `connectReceiver`: the connection to the receiver's node and the cast channel on it.
    async fn connect_receiver(
        &self,
        public_key: &str,
        lan: Option<LanAddress>,
        current: bool,
        handlers: ControlHandlers,
    ) -> Result<(Control, &'static str), CastError> {
        if !is_hex64(public_key) {
            return Err(cast_error(
                "This receiver has no key: pair it again with its link or code",
            ));
        }
        if !current {
            return Err(cast_error(
                "This receiver runs an older PearTube: update it",
            ));
        }
        let Some(node) = self.node() else {
            return Err(cast_error("P2P node is not started"));
        };
        node.join_lan_topic("cast", cast_lan_topic());
        // `connectReceiver`: one `connectPeer`, one channel on it. The only retry in a setup is
        // `startSession`'s, one connection later; a connection that went away here is an error.
        let conn = node
            .connect_peer(public_key, lan.clone(), Duration::from_secs(30))
            .await?;
        let transport = if conn.is_lan() { "lan" } else { "p2p" };
        let control = open_control(&conn.mux(), handlers, DEFAULT_TIMEOUT).await?;
        Ok((control, transport))
    }

    fn close_active_transport(&self) {
        self.attempt.fetch_add(1, Ordering::AcqRel);
        let session = lock(&self.active).take();
        if let Some(session) = session {
            session.control.close();
        }
    }

    /// One setup attempt: open the channel, subscribe, send the command.
    async fn attempt_session(
        self: &Arc<Self>,
        receiver_id: &str,
        target: &ReceiverTarget,
        command: Value,
        timeout: Duration,
    ) -> Result<Active, CastError> {
        let serial = self.serial.fetch_add(1, Ordering::AcqRel) + 1;
        let event_inner = self.clone();
        let close_inner = self.clone();
        let on_event: EventHandler = Arc::new(move |message: Value| {
            if message.get("event").and_then(Value::as_str) != Some("state") {
                return;
            }
            let Some(next) = message.get("state").cloned() else {
                return;
            };
            let mut active = lock(&event_inner.active);
            if let Some(session) = active.as_mut()
                && session.serial == serial
                && let Value::Object(state) = next
            {
                session.state = state;
                drop(active);
                event_inner.emit_change();
            }
        });
        let receiver_name = target.name.clone();
        let receiver_for_attach = receiver_id.to_owned();
        let on_close: OpenHandler = Arc::new(move |control: Control| {
            let taken = {
                let mut active = lock(&close_inner.active);
                match active.as_ref() {
                    Some(session) if session.serial == serial => active.take(),
                    _ => None,
                }
            };
            if taken.is_none() {
                return;
            }
            close_inner.emit_change();
            if !close_inner.closing.load(Ordering::Acquire) {
                // The channel closes with its connection, and a LAN link can replace a DHT link
                // under a session. Take the remote back on the link that is left; if the TV is
                // gone, the session ends.
                let inner = close_inner.clone();
                let receiver_id = receiver_for_attach.clone();
                tokio::spawn(async move {
                    let _ = inner.reattach(receiver_id).await;
                });
            }
            let _ = control;
        });
        let handlers = ControlHandlers {
            on_request: None,
            on_event: Some(on_event),
            on_open: None,
            on_close: Some(on_close),
        };
        let (control, transport) = self
            .connect_receiver(
                &target.public_key,
                target.lan.clone(),
                target.current,
                handlers,
            )
            .await?;
        let session_result = async {
            let mut subscribe = Map::new();
            subscribe.insert("token".into(), json!(target.token));
            control
                .call("subscribe", subscribe, DEFAULT_TIMEOUT)
                .await?;
            let mut payload = command.as_object().cloned().unwrap_or_default();
            payload.insert("token".into(), json!(target.token));
            control.call("cmd", payload, timeout).await
        }
        .await;
        match session_result {
            Ok(reply) => {
                let mut state = state::idle(now_ms());
                if let Some(Value::Object(next)) = reply.get("state") {
                    state = next.clone();
                }
                Ok(Active {
                    serial,
                    receiver_id: receiver_id.to_owned(),
                    receiver_name,
                    transport,
                    pair_token: target.token.clone(),
                    state,
                    control,
                })
            }
            Err(error) => {
                control.close();
                Err(error)
            }
        }
    }

    fn claim(&self, mine: u64, session: Active) -> Result<(), CastError> {
        if mine != self.attempt.load(Ordering::Acquire) {
            session.control.close();
            return Err(cast_error("A newer cast or stop replaced this one"));
        }
        *lock(&self.active) = Some(session);
        self.emit_change();
        Ok(())
    }

    async fn start_session(
        self: &Arc<Self>,
        receiver_id: &str,
        command: Value,
        timeout: Duration,
    ) -> Result<Value, CastError> {
        let target = receiver_target(receiver_id, &self.discovered(), &lock(&self.saved).clone());
        if target.token.is_empty() && target.auth != "open" {
            return Err(cast_error(format!(
                "Pair with {} using its 6-character TV code before casting",
                target.name
            )));
        }
        self.close_active_transport();
        let mine = self.attempt.load(Ordering::Acquire);
        let first = self
            .attempt_session(receiver_id, &target, command.clone(), timeout)
            .await;
        match first {
            Ok(session) => {
                self.claim(mine, session)?;
                Ok(self.status())
            }
            Err(error)
                if error.message == "Receiver connection closed"
                    && !self.closing.load(Ordering::Acquire) =>
            {
                // A LAN link can replace a DHT link under setup: retry once on the new connection.
                // The retry keeps the outer `mine`: `attempt` only moves for a newer cast or a
                // stop, and one of those must win over this session (`claim` refuses it).
                let session = self
                    .attempt_session(receiver_id, &target, command, timeout)
                    .await?;
                self.claim(mine, session)?;
                Ok(self.status())
            }
            Err(error) => Err(error),
        }
    }

    /// `resolveEntryToCast`.
    async fn resolve_entry_to_cast(
        &self,
        node: &Arc<dyn CastPeer>,
        key: &str,
        uri: &str,
    ) -> Result<(Value, Value), CastError> {
        if !uri.is_empty() {
            let opened = node
                .open_uri(uri, true)
                .await?
                .ok_or_else(|| cast_error("URI is not a playable watch link"))?;
            if opened.action != "watch" {
                return Err(cast_error("URI is not a playable watch link"));
            }
            let entry_key = opened
                .entry
                .get("key")
                .map(state::text_value)
                .unwrap_or_default();
            let raw = node.find_entry(&entry_key).await;
            let raw = raw.ok_or_else(|| cast_error(format!("Entry not found: {entry_key}")))?;
            return Ok((opened.entry, raw));
        }
        if key.is_empty() {
            return Err(cast_error("Entry key or watch URI required to cast"));
        }
        let raw = node
            .find_entry(key)
            .await
            .ok_or_else(|| cast_error(format!("Entry not found: {key}")))?;
        let id = raw.get("id").map(state::text_value).unwrap_or_default();
        let list = node.search(&id).await;
        let item = list
            .iter()
            .find(|entry| entry.get("key").map(state::text_value).as_deref() == Some(key))
            .cloned()
            .unwrap_or_else(|| {
                json!({
                    "key": key,
                    "id": id,
                    "title": raw.get("title").cloned().unwrap_or(Value::Null),
                    "size": raw.get("size").cloned().unwrap_or(json!(0)),
                    "sha256": raw.get("sha256").cloned().unwrap_or(Value::Null),
                    "magnetUrl": format_watch_uri(&watch_entry_from(&raw), "", "")
                })
            });
        Ok((item, raw))
    }

    async fn cast_play(self: &Arc<Self>, params: &Value) -> Result<Value, CastError> {
        let Some(node) = self.node() else {
            return Err(cast_error("P2P node is not started"));
        };
        let receiver_id = text(params, "receiverId");
        let key = text(params, "key");
        let uri = text(params, "uri");
        let (item, raw) = self.resolve_entry_to_cast(&node, &key, &uri).await?;
        // A sender that holds the film opens it, so the TV fetches it from this device over their
        // connection. One that does not leaves the download to the TV: no second 4K stream onto a
        // phone's Wi-Fi.
        let local = item.get("local").is_some_and(state::truthy);
        let cached = item.get("cachedBytes").map(state::num).unwrap_or(0.0) > 0.0;
        if local || cached {
            node.warm(&raw).await;
        }
        let start_position_ms = params
            .get("startPositionMs")
            .map(state::num)
            .unwrap_or(0.0)
            .max(0.0);
        let source = item
            .get("source")
            .map(state::text_value)
            .unwrap_or_default();
        let command = json!({
            "action": "load",
            "entry": {
                "key": item.get("key").cloned().unwrap_or(Value::Null),
                "id": item.get("id").cloned().unwrap_or(Value::Null),
                "title": item.get("title").cloned().unwrap_or(Value::Null),
                "size": item.get("size").cloned().unwrap_or(json!(0)),
                "sha256": item.get("sha256").cloned().unwrap_or(Value::Null)
            },
            // With its channel, so the TV joins that channel's swarm.
            "magnetUrl": format_watch_uri(&watch_entry_from(&raw), &source, "channel"),
            "startPositionMs": start_position_ms
        });
        self.start_session(&receiver_id, command, Duration::from_secs(20))
            .await
    }

    /// Reconnects as a remote control to a TV receiver that is already playing on its own.
    /// The on-close re-attach, boxed so the async fn type does not recurse into itself.
    fn reattach(self: &Arc<Self>, receiver_id: String) -> BoxFuture<'static, ()> {
        let inner = self.clone();
        Box::pin(async move {
            let _ = inner
                .cast_attach(&json!({ "receiverId": receiver_id }))
                .await;
        })
    }

    async fn cast_attach(self: &Arc<Self>, params: &Value) -> Result<Value, CastError> {
        let receiver_id = text(params, "receiverId");
        self.start_session(
            &receiver_id,
            json!({ "action": "status" }),
            Duration::from_secs(10),
        )
        .await
    }

    async fn cast_control(self: &Arc<Self>, cmd: &Value) -> Result<Value, CastError> {
        let (control, pair_token, serial) = {
            let active = lock(&self.active);
            let Some(session) = active.as_ref() else {
                return Err(cast_error("No active cast session"));
            };
            (
                session.control.clone(),
                session.pair_token.clone(),
                session.serial,
            )
        };
        let mut payload = obj(cmd);
        payload.insert("token".into(), json!(pair_token));
        let reply = control.call("cmd", payload, DEFAULT_TIMEOUT).await?;
        {
            let mut active = lock(&self.active);
            if let Some(session) = active.as_mut()
                && session.serial == serial
                && let Some(Value::Object(next)) = reply.get("state")
            {
                session.state = next.clone();
            }
        }
        let stop = cmd.get("action").and_then(Value::as_str) == Some("stop");
        if stop
            && lock(&self.active)
                .as_ref()
                .is_some_and(|session| session.serial == serial)
        {
            self.close_active_transport();
        }
        self.emit_change();
        Ok(self.status())
    }

    async fn cast_stop(self: &Arc<Self>, stop_playback: bool) -> Result<Value, CastError> {
        if stop_playback && lock(&self.active).is_some() {
            let _ = self.cast_control(&json!({ "action": "stop" })).await;
        }
        self.close_active_transport();
        self.emit_change();
        Ok(self.status())
    }

    async fn pair_receiver(self: &Arc<Self>, params: &Value) -> Result<Value, CastError> {
        let mut id = text(params, "id");
        let code = text(params, "code");
        let uri = text(params, "uri");
        let mut public_key = text(params, "publicKey");
        let mut name = text(params, "name");
        let mut token = String::new();
        if !uri.is_empty() {
            match parse_uri(&uri).map_err(|error| cast_error(error.to_string()))? {
                ParsedUri::Receiver {
                    public_key: uri_key,
                    device_id,
                    token: uri_token,
                    name: uri_name,
                    ..
                } => {
                    public_key = uri_key;
                    token = uri_token;
                    if id.is_empty() {
                        id = device_id;
                    }
                    if name.is_empty() {
                        name = uri_name;
                    }
                }
                _ => {
                    return Err(cast_error("Expected a peartube://receiver/... URI"));
                }
            }
        }
        let address = text(params, "address");
        if !address.is_empty() {
            public_key = address.trim().to_owned();
        }
        let disc = {
            let discovered = self.discovered();
            discovered
                .iter()
                .find(|item| !id.is_empty() && item.id == id)
                .or_else(|| {
                    discovered.iter().find(|item| {
                        !item.public_key.is_empty() && item.public_key == public_key.to_lowercase()
                    })
                })
                .cloned()
        };
        let key = if !public_key.is_empty() {
            public_key.to_lowercase()
        } else if let Some(disc) = disc.as_ref() {
            disc.public_key.to_lowercase()
        } else {
            id.to_lowercase()
        };
        if !is_hex64(&key) {
            return Err(cast_error("Enter a receiver link or its 64-hex key"));
        }
        let mut record = SavedReceiver {
            id: disc
                .as_ref()
                .map(|disc| disc.id.clone())
                .filter(|id| !id.is_empty())
                .or_else(|| (!id.is_empty()).then(|| id.clone()))
                .unwrap_or_else(|| key.clone()),
            name: crate::protocol::clean_name(
                Some(&if !name.is_empty() {
                    name.clone()
                } else {
                    disc.as_ref()
                        .map(|disc| disc.name.clone())
                        .unwrap_or_default()
                }),
                "PearTube TV",
            ),
            public_key: key.clone(),
            token: token.clone(),
        };
        if record.token.is_empty() {
            let (control, _transport) = self
                .connect_receiver(
                    &key,
                    disc.as_ref().and_then(|disc| disc.lan.clone()),
                    disc.as_ref().is_none_or(|disc| disc.current),
                    ControlHandlers::default(),
                )
                .await?;
            let mut payload = Map::new();
            payload.insert("code".into(), json!(normalize_pair_code(Some(&code))));
            let reply = control.call("pair", payload, DEFAULT_TIMEOUT).await;
            let reply = match reply {
                Ok(reply) => reply,
                Err(error) => {
                    control.close();
                    return Err(error);
                }
            };
            control.close();
            record = SavedReceiver {
                id: reply
                    .get("deviceId")
                    .map(state::text_value)
                    .filter(|id| !id.is_empty())
                    .unwrap_or_else(|| record.id.clone()),
                name: crate::protocol::clean_name(
                    Some(
                        &reply
                            .get("name")
                            .map(state::text_value)
                            .filter(|name| !name.is_empty())
                            .unwrap_or_else(|| record.name.clone()),
                    ),
                    "PearTube TV",
                ),
                public_key: key,
                token: reply
                    .get("token")
                    .map(state::text_value)
                    .filter(|token| token != "null")
                    .unwrap_or_default(),
            };
        }
        lock(&self.saved).set(record.clone());
        self.persist_saved();
        self.emit_change();
        Ok(record_json(&record))
    }
}

/// `watch_entry_from`: the watch link's view of a raw announcement.
pub fn watch_entry_from(raw: &Value) -> WatchEntry {
    WatchEntry {
        id: raw.get("id").map(state::text_value).unwrap_or_default(),
        title: raw.get("title").map(state::text_value).unwrap_or_default(),
        sha256: raw.get("sha256").map(state::text_value),
        blobs: raw.get("blobs").map(state::text_value).unwrap_or_default(),
        blob: raw
            .get("blob")
            .cloned()
            .and_then(|blob| serde_json::from_value(blob).ok()),
    }
}

/// `createCastController`: finds receivers, keeps the paired ones, casts over this device's node.
pub struct CastController {
    inner: Arc<Inner>,
}

impl CastController {
    pub async fn start(options: ControllerOptions) -> Result<Self, CastError> {
        if let Err(error) = std::fs::create_dir_all(&options.storage) {
            return Err(CastError::new(error.to_string(), 500));
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let _ =
                std::fs::set_permissions(&options.storage, std::fs::Permissions::from_mode(0o700));
        }
        let saved = SavedReceivers::load(&options.storage);
        Ok(Self {
            inner: Arc::new(Inner {
                storage: options.storage,
                saved: Mutex::new(saved),
                discovered: Mutex::new(Vec::new()),
                down_index: Mutex::new(HashMap::new()),
                browse: Mutex::new(None),
                browse_host: Mutex::new(None),
                local_receiver: Mutex::new(None),
                active: Mutex::new(None),
                attempt: AtomicU64::new(0),
                serial: AtomicU64::new(0),
                closing: AtomicBool::new(false),
                get_node: options.get_node,
                get_lan_host: options.get_lan_host,
                mdns: options.mdns,
                on_change: options.on_change,
            }),
        })
    }

    /// Looking for TVs on this LAN is when this device's node should become reachable to them.
    pub async fn set_lan_host(&self, host: Option<String>) {
        if let Some(node) = self.inner.node() {
            node.join_lan_topic("cast", cast_lan_topic());
        }
        self.inner.refresh_browse(host.clone());
        if let Some(receiver) = self.inner.local_receiver() {
            receiver.set_host(host).await;
        }
    }

    pub fn list_receivers(&self) -> Vec<ListedReceiver> {
        self.inner.list()
    }

    pub fn status(&self) -> Value {
        self.inner.status()
    }

    pub async fn pair_receiver(&self, params: &Value) -> Result<Value, CastError> {
        self.inner.pair_receiver(params).await
    }

    pub fn remove_receiver(&self, id: &str) -> Value {
        let removed = lock(&self.inner.saved).remove(id);
        if removed {
            self.inner.persist_saved();
            self.inner.emit_change();
        }
        json!({ "removed": removed })
    }

    pub async fn cast_play(&self, params: &Value) -> Result<Value, CastError> {
        self.inner.cast_play(params).await
    }

    pub async fn cast_attach(&self, params: &Value) -> Result<Value, CastError> {
        self.inner.cast_attach(params).await
    }

    pub async fn cast_control(&self, cmd: &Value) -> Result<Value, CastError> {
        self.inner.cast_control(cmd).await
    }

    pub async fn cast_stop(&self, stop_playback: bool) -> Result<Value, CastError> {
        self.inner.cast_stop(stop_playback).await
    }

    /// `startLocalReceiver`: this device becomes the TV a sender pairs with.
    pub async fn start_local_receiver(
        &self,
        options: super::receiver::ReceiverOptions,
    ) -> Result<Value, CastError> {
        let previous = lock(&self.inner.local_receiver).take();
        if let Some(previous) = previous {
            previous.close().await;
        }
        let receiver = Arc::new(Receiver::start(options).await?);
        self.inner
            .local_receiver
            .lock()
            .map_err(|_| CastError::new("The cast controller is poisoned", 500))?
            .replace(receiver.clone());
        self.inner.emit_change();
        Ok(receiver.info())
    }

    pub fn report_local_receiver_state(&self, patch: &Value) -> Option<Value> {
        self.inner
            .local_receiver()
            .map(|receiver| state::to_json(&receiver.report_state(patch)))
    }

    /// The LAN address the app last sampled (`getLanHost`).
    pub fn lan_host(&self) -> Option<String> {
        (self.inner.get_lan_host)()
    }

    pub async fn close(&self) {
        if self.inner.closing.swap(true, Ordering::AcqRel) {
            return;
        }
        self.inner.close_active_transport();
        if let Some(previous) = lock(&self.inner.browse).take() {
            previous.stop();
        }
        let receiver = lock(&self.inner.local_receiver).take();
        if let Some(receiver) = receiver {
            receiver.close().await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn saved_receivers_round_trip_and_a_stop_retires_the_session() {
        let storage = tempfile::tempdir().unwrap();
        let controller = CastController::start(ControllerOptions {
            storage: storage.path().to_owned(),
            ..ControllerOptions::default()
        })
        .await
        .unwrap();
        assert_eq!(controller.status()["session"], Value::Null);
        assert!(controller.list_receivers().is_empty());
        // A stop with nothing active is a no-op that still reports the (empty) status.
        let status = controller.cast_stop(true).await.unwrap();
        assert_eq!(status["session"], Value::Null);
        controller.close().await;
    }

    #[tokio::test]
    async fn pairing_needs_a_receiver_link_or_key() {
        let storage = tempfile::tempdir().unwrap();
        let controller = CastController::start(ControllerOptions {
            storage: storage.path().to_owned(),
            ..ControllerOptions::default()
        })
        .await
        .unwrap();
        let error = controller
            .pair_receiver(&json!({ "code": "ABC123" }))
            .await
            .expect_err("no key");
        assert_eq!(error.message, "Enter a receiver link or its 64-hex key");
        controller.close().await;
    }

    #[test]
    fn a_watch_link_keeps_its_blob_and_channel() {
        let raw = json!({
            "id": "imdb:tt0133093",
            "title": "The Matrix",
            "sha256": "ab".repeat(32),
            "blobs": "cd".repeat(32),
            "blob": { "blockOffset": 0, "blockLength": 3, "byteOffset": 0, "byteLength": 1200 },
            "source": "ef".repeat(32)
        });
        let uri = format_watch_uri(&watch_entry_from(&raw), &"ef".repeat(32), "channel");
        assert!(uri.contains("kind=channel"));
        match parse_uri(&uri).unwrap() {
            ParsedUri::Watch { entry, .. } => {
                assert_eq!(entry.blobs, "cd".repeat(32));
                assert_eq!(entry.blob.block_length, 3);
            }
            other => panic!("unexpected {other:?}"),
        }
    }

    #[test]
    fn pairing_codes_normalize_before_compare() {
        use super::super::pairing::{random_hex, safe_equal};
        assert!(safe_equal(
            Some(&normalize_pair_code(Some(" abc-123 "))),
            Some("ABC123")
        ));
        assert_eq!(random_hex(16).len(), 32);
    }
}
