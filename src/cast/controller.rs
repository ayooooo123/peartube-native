//! The sender & hub controller, ported from `createCastController` in `src/cast.js`: LAN discovery,
//! pairing over HTTP or Noise, the token-gated LAN blob server, hosted Smart TV web receivers, and
//! the cast session's transport (web SSE, LAN HTTP or P2P RPC).

use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use futures_util::future::BoxFuture;
use http_body_util::BodyExt;
use hyper::body::Incoming;
use hyper::header::{self, HeaderValue};
use hyper::{Method, Request, Response, StatusCode};
use serde_json::{Map, Value, json};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use crate::protocol::{
    API_PORT, WatchEntry, format_receiver_uri, format_watch_uri, is_hex64, parse_uri,
};

use super::CastError;
use super::http::{
    Body, Handler, HttpClient, HttpServer, channel_body, collect_body, json_response, preflight,
    query_param, read_json_body, sse_frame,
};
use super::pairing::{random_hex, safe_equal};
use super::receiver::{Receiver, ReceiverOptions};
use super::records::{
    DiscoveredReceiver, SavedReceiver, SavedReceivers, WebListing, list_receivers, receiver_address,
};
use super::rpc::{self, ConnectOptions, RpcClient};
use super::state::{self, idle, text_value, truthy};
use super::traits::{BlobServerFactory, CastPeer, DhtConnect, LanBlobServer};

/// `ensureNode`: the controller's way to (re)open the sender's own node.
pub type EnsureNode =
    Arc<dyn Fn() -> BoxFuture<'static, Result<Option<Arc<dyn CastPeer>>, CastError>> + Send + Sync>;

/// The controller's construction options, `createCastController`'s argument object.
pub struct ControllerOptions {
    pub storage: PathBuf,
    pub peer: Option<Arc<dyn CastPeer>>,
    pub dht: Option<Arc<dyn DhtConnect>>,
    /// `ensureNode`: opens the receiver's own node when no `start` did.
    pub ensure_node: Option<EnsureNode>,
    pub get_lan_host: Option<Arc<dyn Fn() -> Option<String> + Send + Sync>>,
    pub blob_servers: Option<Arc<dyn BlobServerFactory>>,
    pub on_change: Option<Arc<dyn Fn() + Send + Sync>>,
    pub api_port: u16,
    pub mdns: bool,
}

impl Default for ControllerOptions {
    fn default() -> Self {
        Self {
            storage: PathBuf::new(),
            peer: None,
            dht: None,
            ensure_node: None,
            get_lan_host: None,
            blob_servers: None,
            on_change: None,
            api_port: API_PORT,
            mdns: true,
        }
    }
}

/// A live cast session.
#[derive(Clone)]
pub struct CastSession {
    pub receiver_id: String,
    pub receiver_name: String,
    /// `web`, `lan` or `p2p`.
    pub transport: &'static str,
    pub base_url: Option<String>,
    pub token: String,
    pub public_key: String,
    pub state: Value,
}

impl CastSession {
    fn to_json(&self) -> Value {
        json!({
            "receiverId": self.receiver_id,
            "receiverName": self.receiver_name,
            "transport": self.transport,
            "state": self.state,
        })
    }
}

/// A Smart TV browser the hub hosts itself.
struct WebReceiver {
    id: String,
    name: String,
    web_token: String,
    pair_token: String,
    seq: u64,
    /// The page's own event stream (hub → TV).
    clients: Vec<mpsc::Sender<Bytes>>,
    /// The sender's event streams (TV state → hub).
    sender_sse: Vec<mpsc::Sender<Bytes>>,
    state: Value,
}

struct Session {
    session: CastSession,
    sse: Option<JoinHandle<()>>,
    rpc: Option<Arc<RpcClient>>,
}

struct ControllerInner {
    storage: PathBuf,
    peer: Option<Arc<dyn CastPeer>>,
    dht: Option<Arc<dyn DhtConnect>>,
    ensure_node: Option<EnsureNode>,
    get_lan_host: Option<Arc<dyn Fn() -> Option<String> + Send + Sync>>,
    blob_servers: Option<Arc<dyn BlobServerFactory>>,
    on_change: Option<Arc<dyn Fn() + Send + Sync>>,
    api_port: u16,
    mdns: bool,
    client: Arc<HttpClient>,

    saved: Mutex<SavedReceivers>,
    discovered: Mutex<Vec<DiscoveredReceiver>>,
    web: Mutex<Vec<WebReceiver>>,
    allowed_blobs: Mutex<HashSet<String>>,
    blob_server: tokio::sync::Mutex<Option<Arc<dyn LanBlobServer>>>,
    hub: tokio::sync::Mutex<Option<HttpServer>>,
    hub_port: AtomicU64,
    browse: Mutex<Option<super::mdns::BrowseHandle>>,
    browse_host: Mutex<Option<String>>,
    service_ids: Mutex<HashMap<String, String>>,
    local_receiver: tokio::sync::Mutex<Option<Receiver>>,
    // Session updates never await; snapshots and stop must not mistake contention for absence.
    active: Mutex<Option<Session>>,
    attempt: AtomicU64,
    closing: AtomicBool,
}

impl ControllerInner {
    fn emit_change(&self) {
        if self.closing.load(Ordering::SeqCst) {
            return;
        }
        if let Some(sink) = self.on_change.as_ref() {
            sink();
        }
    }

    fn lan_host(&self) -> Option<String> {
        self.get_lan_host.as_ref().and_then(|f| f())
    }

    fn persist_saved(&self) {
        let saved = self.saved.lock().expect("cast saved").clone();
        let _ = saved.save(&self.storage);
    }

    fn discovered(&self) -> Vec<DiscoveredReceiver> {
        self.discovered.lock().expect("cast discovered").clone()
    }

    fn web_listings(&self, active_id: Option<&str>) -> Vec<WebListing> {
        self.web
            .lock()
            .expect("cast web")
            .iter()
            .filter(|rec| !rec.clients.is_empty() || active_id == Some(rec.id.as_str()))
            .map(|rec| WebListing {
                id: rec.id.clone(),
                name: rec.name.clone(),
                clients: rec.clients.len(),
            })
            .collect()
    }

    fn node(&self) -> Option<Arc<dyn CastPeer>> {
        self.peer.clone()
    }

    async fn ensure_lan_blob_server(&self) -> Result<Arc<dyn LanBlobServer>, CastError> {
        if let Some(server) = self.blob_server.lock().await.as_ref() {
            return Ok(Arc::clone(server));
        }
        let factory = self
            .blob_servers
            .as_ref()
            .ok_or_else(|| CastError::new("Node not started", 400))?;
        let host = self.lan_host();
        let server = factory.ensure(host.as_deref()).await?;
        *self.blob_server.lock().await = Some(Arc::clone(&server));
        Ok(server)
    }

    async fn ensure_web_hub_server(self: &Arc<Self>) -> u16 {
        if let Some(server) = self.hub.lock().await.as_ref() {
            return server.port();
        }
        let inner = Arc::clone(self);
        let handler: Handler = Arc::new(move |req, _peer| {
            let inner = Arc::clone(&inner);
            Box::pin(async move { inner.handle_hub(req).await })
                as BoxFuture<'static, Response<Body>>
        });
        let server = match HttpServer::bind(8177, "0.0.0.0", true, handler).await {
            Ok(server) => server,
            Err(_) => return 0,
        };
        if self.closing.load(Ordering::SeqCst) {
            server.close().await;
            return 0;
        }
        let port = server.port();
        self.hub_port.store(u64::from(port), Ordering::Relaxed);
        *self.hub.lock().await = Some(server);
        port
    }

    async fn handle_hub(self: Arc<Self>, req: Request<Incoming>) -> Response<Body> {
        let method = req.method().clone();
        let path = req.uri().path().to_owned();
        let query = req.uri().query().map(str::to_owned);
        if method == Method::OPTIONS {
            return preflight();
        }
        if method == Method::GET && (path == "/" || path == "/tv") {
            return super::http::html_response(super::ui::receiver_page("hub", "Smart TV Browser"));
        }
        if method == Method::GET && path == "/v1/cast/web-events" {
            return self.handle_web_events(&query);
        }
        if method == Method::GET && path == "/cast/v1/events" {
            return self.handle_hub_sender_events(&query);
        }
        let outcome: Result<Value, CastError> = async {
            match (&method, path.as_str()) {
                (&Method::POST, "/v1/cast/web-register") => {
                    let body = read_json_body(&collect_body(req).await?)?;
                    Ok(self.register_web_receiver(&body))
                }
                (&Method::POST, "/v1/cast/web-state") => {
                    let body = read_json_body(&collect_body(req).await?)?;
                    Ok(self.update_web_receiver_state(&body))
                }
                (_, p) if p.starts_with("/cast/v1/") => self.handle_hub_cast_json(req).await,
                _ => Err(CastError::not_found("Not found")),
            }
        }
        .await;
        match outcome {
            Ok(value) => json_response(200, &value),
            Err(err) => json_response(err.status, &json!({ "error": err.message })),
        }
    }

    fn register_web_receiver(&self, body: &Value) -> Value {
        let supplied_id = body.get("id").map(text_value).unwrap_or_default();
        let supplied_token = body.get("token").map(text_value).unwrap_or_default();
        let supplied_name = body.get("name").map(text_value).unwrap_or_default();
        let mut web = self.web.lock().expect("cast web");
        let known_index = web.iter().position(|r| {
            r.id == supplied_id && safe_equal(Some(&supplied_token), Some(&r.web_token))
        });
        let (id, name, web_token, pair_token) = match known_index {
            Some(index) => {
                if !supplied_name.is_empty() {
                    web[index].name =
                        crate::protocol::clean_name(Some(&supplied_name), &web[index].name);
                }
                let rec = &web[index];
                (
                    rec.id.clone(),
                    rec.name.clone(),
                    rec.web_token.clone(),
                    rec.pair_token.clone(),
                )
            }
            None => {
                let id = format!("web-{}", random_hex(8));
                let name = crate::protocol::clean_name(
                    (!supplied_name.is_empty()).then_some(supplied_name.as_str()),
                    "Smart TV Browser",
                );
                let web_token = random_hex(16);
                let pair_token = random_hex(16);
                web.push(WebReceiver {
                    id: id.clone(),
                    name: name.clone(),
                    web_token: web_token.clone(),
                    pair_token: pair_token.clone(),
                    seq: 0,
                    clients: Vec::new(),
                    sender_sse: Vec::new(),
                    state: Value::Object(idle(now_ms())),
                });
                self.saved.lock().expect("cast saved").set(SavedReceiver {
                    id: id.clone(),
                    name: name.clone(),
                    host: "127.0.0.1".to_owned(),
                    port: 0,
                    public_key: String::new(),
                    token: pair_token.clone(),
                    p2p: false,
                    web_hosted: true,
                });
                (id, name, web_token, pair_token)
            }
        };
        drop(web);
        self.persist_saved();
        self.emit_change();
        let hub_port = self
            .hub_port
            .load(Ordering::Relaxed)
            .max(u64::from(self.api_port)) as u16;
        let uri = format_receiver_uri(&crate::protocol::ReceiverUriInput {
            public_key: String::new(),
            device_id: id.clone(),
            name: name.clone(),
            token: pair_token,
            host: self.lan_host().unwrap_or_else(|| "127.0.0.1".to_owned()),
            port: f64::from(hub_port),
            p2p: false,
        });
        json!({
            "id": id,
            "name": name,
            "webToken": web_token,
            "pairCode": "OPEN",
            "pairingUri": uri,
            "qrSvg": super::qr::qr_svg(&uri),
        })
    }

    fn handle_web_events(self: &Arc<Self>, query: &Option<String>) -> Response<Body> {
        let id = query_param(query.as_deref(), "id").unwrap_or_default();
        let token = query_param(query.as_deref(), "token").unwrap_or_default();
        let mut web = self.web.lock().expect("cast web");
        let Some(rec) = web.iter_mut().find(|r| r.id == id) else {
            return json_response(401, &json!({ "error": "Unauthorized" }));
        };
        if !safe_equal(Some(&token), Some(&rec.web_token)) {
            return json_response(401, &json!({ "error": "Unauthorized" }));
        }
        let (tx, rx) = mpsc::channel(16);
        let _ = tx.try_send(Bytes::from(": connected\n\n"));
        rec.clients.push(tx);
        drop(web);
        self.emit_change();
        sse_response(rx)
    }

    fn handle_hub_sender_events(self: &Arc<Self>, query: &Option<String>) -> Response<Body> {
        let token = query_param(query.as_deref(), "token").unwrap_or_default();
        let mut web = self.web.lock().expect("cast web");
        let index = web
            .iter()
            .position(|r| safe_equal(Some(&token), Some(&r.pair_token)))
            .or_else(|| web.iter().position(|r| !r.clients.is_empty()));
        let Some(index) = index else {
            return json_response(401, &json!({ "error": "Unauthorized" }));
        };
        let state = web[index].state.clone();
        let (tx, rx) = mpsc::channel(16);
        let _ = tx.try_send(sse_frame(&json!({ "event": "state", "state": state })));
        web[index].sender_sse.push(tx);
        sse_response(rx)
    }

    fn update_web_receiver_state(&self, body: &Value) -> Value {
        let id = body.get("id").map(text_value).unwrap_or_default();
        let token = body.get("token").map(text_value).unwrap_or_default();
        let mut web = self.web.lock().expect("cast web");
        let Some(index) = web.iter().position(|r| r.id == id) else {
            return json!({ "error": "Unauthorized" });
        };
        if !safe_equal(Some(&token), Some(&web[index].web_token)) {
            return json!({ "error": "Unauthorized" });
        }
        if let Some(Value::Object(next)) = body.get("state") {
            let now = now_ms();
            let mut state = match &web[index].state {
                Value::Object(map) => map.clone(),
                _ => Map::new(),
            };
            for (key, value) in next {
                state.insert(key.clone(), value.clone());
            }
            state.insert("updatedAt".into(), json!(now));
            let state = Value::Object(state);
            web[index].state = state.clone();
            let frame = sse_frame(&json!({ "event": "state", "state": state }));
            let mut live = Vec::new();
            for sender in std::mem::take(&mut web[index].sender_sse) {
                if sender.try_send(frame.clone()).is_ok() {
                    live.push(sender);
                }
            }
            web[index].sender_sse = live;
        }
        drop(web);
        self.emit_change();
        json!({ "ok": true })
    }

    async fn handle_hub_cast_json(
        self: &Arc<Self>,
        req: Request<Incoming>,
    ) -> Result<Value, CastError> {
        let path = req.uri().path().to_owned();
        let query = req.uri().query().map(str::to_owned);
        let method = req.method().clone();
        let hub_port = self
            .hub_port
            .load(Ordering::Relaxed)
            .max(u64::from(self.api_port)) as u16;
        if method == Method::GET && path == "/cast/v1/info" {
            let id = query_param(query.as_deref(), "id").unwrap_or_default();
            let web = self.web.lock().expect("cast web");
            let rec = web
                .iter()
                .find(|r| r.id == id)
                .or_else(|| web.iter().find(|r| !r.clients.is_empty()))
                .or_else(|| web.first());
            let Some(rec) = rec else {
                return Err(CastError::not_found("No active Smart TV browser receiver"));
            };
            let mut out = Map::new();
            out.insert("protocol".into(), json!("peartube-cast/v1"));
            out.insert("deviceId".into(), json!(rec.id));
            out.insert("name".into(), json!(rec.name));
            out.insert("publicKey".into(), json!(""));
            out.insert("swarmKey".into(), json!(""));
            out.insert(
                "host".into(),
                json!(self.lan_host().unwrap_or_else(|| "127.0.0.1".to_owned())),
            );
            out.insert("port".into(), json!(hub_port));
            out.insert(
                "capabilities".into(),
                json!({ "p2p": false, "httpRange": true }),
            );
            out.insert("authRequired".into(), json!(false));
            out.insert("state".into(), rec.state.clone());
            return Ok(Value::Object(out));
        }
        if method == Method::POST && path == "/cast/v1/pair" {
            let body = read_json_body(&collect_body(req).await?)?;
            let clean =
                super::pairing::normalize_pair_code(body.get("code").and_then(Value::as_str));
            // A hosted browser receiver advertises `OPEN`, so a code never discriminates between
            // them; the app falls through to the active browser and so does the port.
            let _ = clean;
            let web = self.web.lock().expect("cast web");
            let rec = web
                .iter()
                .find(|r| !r.clients.is_empty())
                .or_else(|| web.first());
            let Some(rec) = rec else {
                return Err(CastError::not_found("No active Smart TV browser receiver"));
            };
            let mut out = Map::new();
            out.insert("deviceId".into(), json!(rec.id));
            out.insert("name".into(), json!(rec.name));
            out.insert("publicKey".into(), json!(""));
            out.insert("port".into(), json!(hub_port));
            out.insert("token".into(), json!(rec.pair_token));
            out.insert(
                "capabilities".into(),
                json!({ "p2p": false, "httpRange": true }),
            );
            return Ok(Value::Object(out));
        }
        if method == Method::POST && path == "/cast/v1/command" {
            let auth = bearer(req.headers());
            let cmd = read_json_body(&collect_body(req).await?)?;
            let mut web = self.web.lock().expect("cast web");
            let index = web
                .iter()
                .position(|r| safe_equal(Some(&auth), Some(&r.pair_token)))
                .or_else(|| web.iter().position(|r| !r.clients.is_empty()));
            let Some(index) = index else {
                return Err(CastError::not_found("No active Smart TV browser receiver"));
            };
            let stream_url = cmd
                .get("streamUrl")
                .or_else(|| cmd.get("httpStreamUrl"))
                .cloned();
            web[index].seq += 1;
            let seq = web[index].seq;
            let next_cmd = if cmd.get("action").and_then(Value::as_str) == Some("load") {
                let mut out = Map::new();
                out.insert("seq".into(), json!(seq));
                out.insert("action".into(), json!("load"));
                out.insert(
                    "entry".into(),
                    cmd.get("entry").cloned().unwrap_or(Value::Null),
                );
                out.insert("mode".into(), json!("http"));
                out.insert(
                    "streamUrl".into(),
                    stream_url.clone().unwrap_or(Value::Null),
                );
                out.insert("httpStreamUrl".into(), stream_url.unwrap_or(Value::Null));
                out.insert(
                    "startPositionMs".into(),
                    json!(
                        cmd.get("startPositionMs")
                            .map(|v| state::num(v).max(0.0) as u64)
                            .unwrap_or(0)
                    ),
                );
                Value::Object(out)
            } else {
                let mut out = Map::new();
                out.insert("seq".into(), json!(seq));
                if let Value::Object(fields) = &cmd {
                    for (key, value) in fields {
                        out.insert(key.clone(), value.clone());
                    }
                }
                Value::Object(out)
            };
            let frame = sse_frame(&json!({ "event": "command", "command": next_cmd }));
            let mut live = Vec::new();
            for client in std::mem::take(&mut web[index].clients) {
                if client.try_send(frame.clone()).is_ok() {
                    live.push(client);
                }
            }
            web[index].clients = live;
            if cmd.get("action").and_then(Value::as_str) == Some("load") {
                let mut loading = idle(now_ms());
                loading.insert("status".into(), json!("loading"));
                loading.insert("mode".into(), json!("http"));
                loading.insert(
                    "entry".into(),
                    cmd.get("entry").cloned().unwrap_or(Value::Null),
                );
                loading.insert(
                    "positionMs".into(),
                    json!(
                        cmd.get("startPositionMs")
                            .map(|v| state::num(v).max(0.0) as u64)
                            .unwrap_or(0)
                    ),
                );
                loading.insert("playing".into(), json!(true));
                loading.insert("buffering".into(), json!(true));
                web[index].state = Value::Object(loading);
            } else if cmd.get("action").and_then(Value::as_str) == Some("stop") {
                web[index].state = Value::Object(idle(now_ms()));
            }
            let state = web[index].state.clone();
            let state_frame = sse_frame(&json!({ "event": "state", "state": state }));
            let mut live = Vec::new();
            for sender in std::mem::take(&mut web[index].sender_sse) {
                if sender.try_send(state_frame.clone()).is_ok() {
                    live.push(sender);
                }
            }
            web[index].sender_sse = live;
            drop(web);
            self.emit_change();
            return Ok(json!({ "ok": true, "state": state }));
        }
        Err(CastError::not_found("Not found"))
    }

    async fn refresh_browse(self: &Arc<Self>, host: Option<String>) {
        {
            let same = self.browse_host.lock().expect("cast browse host").clone() == host;
            if same && self.browse.lock().expect("cast browse").is_some() {
                return;
            }
        }
        let previous = self.browse.lock().expect("cast browse").take();
        if let Some(previous) = previous {
            previous.stop();
        }
        *self.browse_host.lock().expect("cast browse host") = host.clone();
        if self.closing.load(Ordering::SeqCst) || !self.mdns || host.is_none() {
            self.discovered.lock().expect("cast discovered").clear();
            self.emit_change();
            return;
        }
        let inner = Arc::clone(self);
        let up: Arc<dyn Fn(super::mdns::BrowsedService) + Send + Sync> = Arc::new(move |service| {
            if let Some(mut record) = service.to_discovered() {
                record.seen_at = now_ms();
                let local_id = inner
                    .local_receiver
                    .try_lock()
                    .ok()
                    .and_then(|guard| guard.as_ref().map(|r| r.device_id().to_owned()));
                if local_id.as_deref() == Some(record.id.as_str()) {
                    return;
                }
                inner
                    .service_ids
                    .lock()
                    .expect("cast service ids")
                    .insert(service.fullname.clone(), record.id.clone());
                let existing = {
                    let saved = inner.saved.lock().expect("cast saved");
                    saved
                        .get(&record.id)
                        .cloned()
                        .or_else(|| saved.find_by_public_key(&record.public_key).cloned())
                };
                if let Some(mut saved) = existing {
                    saved.host = record.host.clone();
                    saved.port = record.port;
                    if !record.public_key.is_empty() {
                        saved.public_key = record.public_key.clone();
                    }
                    saved.p2p = saved.p2p || record.p2p;
                    inner.saved.lock().expect("cast saved").set(saved);
                    inner.persist_saved();
                }
                {
                    let mut discovered = inner.discovered.lock().expect("cast discovered");
                    match discovered.iter_mut().find(|d| d.id == record.id) {
                        Some(slot) => *slot = record,
                        None => discovered.push(record),
                    }
                }
                inner.emit_change();
            }
        });
        let down_inner = Arc::clone(self);
        let down: Arc<dyn Fn(String) + Send + Sync> = Arc::new(move |fullname| {
            // `ServiceRemoved` carries only the full name, so the id is remembered from the last
            // resolve, exactly the entry the app's `down` handler deletes.
            let id = down_inner
                .service_ids
                .lock()
                .expect("cast service ids")
                .remove(&fullname);
            if let Some(id) = id {
                let removed = {
                    let mut discovered = down_inner.discovered.lock().expect("cast discovered");
                    let before = discovered.len();
                    discovered.retain(|d| d.id != id);
                    before != discovered.len()
                };
                if removed {
                    down_inner.emit_change();
                }
            }
        });
        if let Ok(handle) = super::mdns::browse(up, Some(down)) {
            *self.browse.lock().expect("cast browse") = Some(handle);
        }
    }

    fn list_receivers(&self) -> Vec<super::records::ListedReceiver> {
        let active = self
            .active
            .lock()
            .expect("cast active")
            .as_ref()
            .map(|s| s.session.receiver_id.clone());
        let web = self.web_listings(active.as_deref());
        let discovered = self.discovered();
        let saved = self.saved.lock().expect("cast saved").clone();
        let lan_host = self.lan_host();
        let hub_port = self
            .hub_port
            .load(Ordering::Relaxed)
            .max(u64::from(self.api_port)) as u16;
        list_receivers(
            &web,
            &discovered,
            &saved,
            active.as_deref(),
            lan_host.as_deref(),
            hub_port,
        )
    }

    fn close_active_transport(&self) -> u64 {
        let attempt = self.attempt.fetch_add(1, Ordering::SeqCst) + 1;
        let session = self.active.lock().expect("cast active").take();
        if let Some(session) = session {
            if let Some(task) = session.sse {
                task.abort();
            }
            if let Some(rpc) = session.rpc {
                rpc.close();
            }
        }
        attempt
    }

    async fn claim(
        &self,
        mine: u64,
        session: CastSession,
        sse: Option<JoinHandle<()>>,
        rpc: Option<Arc<RpcClient>>,
    ) -> Result<CastSession, CastError> {
        if mine != self.attempt.load(Ordering::SeqCst) {
            if let Some(rpc) = rpc {
                rpc.close();
            }
            if let Some(task) = sse {
                task.abort();
            }
            return Err(CastError::bad_request(
                "A newer cast or stop replaced this one",
            ));
        }
        *self.active.lock().expect("cast active") = Some(Session {
            session: session.clone(),
            sse,
            rpc,
        });
        Ok(session)
    }

    fn status(&self) -> Value {
        let lan_host = self.lan_host();
        let hub_port = self.hub_port.load(Ordering::Relaxed) as u16;
        let web_receiver_url = if hub_port > 0 {
            Some(match lan_host.as_deref() {
                Some(host) => format!("http://{host}:{hub_port}/tv"),
                None => format!("http://127.0.0.1:{hub_port}/tv"),
            })
        } else {
            None
        };
        let receivers = if self
            .local_receiver
            .try_lock()
            .map(|r| r.is_some())
            .unwrap_or(false)
        {
            Vec::new()
        } else {
            self.list_receivers()
        };
        let session = self
            .active
            .lock()
            .expect("cast active")
            .as_ref()
            .map(|s| s.session.to_json());
        let receiver_mode = self
            .local_receiver
            .try_lock()
            .ok()
            .and_then(|slot| slot.as_ref().map(|r| r.info()))
            .unwrap_or(Value::Null);
        json!({
            "webReceiverUrl": web_receiver_url,
            "receivers": receivers,
            "session": session,
            "receiverMode": receiver_mode,
        })
    }
}

fn sse_response(rx: mpsc::Receiver<Bytes>) -> Response<Body> {
    let mapped = mpsc::channel::<Result<hyper::body::Frame<Bytes>, std::io::Error>>(1);
    let mut rx = rx;
    tokio::spawn(async move {
        while let Some(chunk) = rx.recv().await {
            if mapped
                .0
                .send(Ok(hyper::body::Frame::data(chunk)))
                .await
                .is_err()
            {
                break;
            }
        }
    });
    let mut res = Response::new(channel_body(mapped.1));
    *res.status_mut() = StatusCode::OK;
    super::http::cors_headers(res.headers_mut());
    res.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("text/event-stream"),
    );
    res.headers_mut()
        .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-cache"));
    res.headers_mut()
        .insert(header::CONNECTION, HeaderValue::from_static("keep-alive"));
    res
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

fn bearer(headers: &hyper::HeaderMap) -> String {
    headers
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .unwrap_or_default()
        .trim_start_matches("Bearer ")
        .trim()
        .to_owned()
}

/// One pairing target resolved from the saved list, discovery, or a typed address.
struct Target {
    host: String,
    port: u16,
    public_key: String,
    token: String,
    name: String,
    auth: String,
}

impl ControllerInner {
    fn target(&self, receiver_id: &str) -> Option<Target> {
        let disc = self
            .discovered
            .lock()
            .expect("cast discovered")
            .iter()
            .find(|d| d.id == receiver_id)
            .cloned();
        let saved = {
            let saved = self.saved.lock().expect("cast saved");
            saved.get(receiver_id).cloned().or_else(|| {
                disc.as_ref()
                    .and_then(|d| saved.find_by_public_key(&d.public_key).cloned())
            })
        };
        if disc.is_none() && saved.is_none() {
            return None;
        }
        let host = disc
            .as_ref()
            .map(|d| d.host.clone())
            .or_else(|| saved.as_ref().map(|s| s.host.clone()))
            .unwrap_or_default();
        let port = disc
            .as_ref()
            .map(|d| d.port as u16)
            .or_else(|| saved.as_ref().map(|s| s.port as u16))
            .unwrap_or(0);
        let public_key = disc
            .as_ref()
            .map(|d| d.public_key.clone())
            .filter(|k| !k.is_empty())
            .or_else(|| saved.as_ref().map(|s| s.public_key.clone()))
            .unwrap_or_default()
            .to_lowercase();
        let token = saved.as_ref().map(|s| s.token.clone()).unwrap_or_default();
        let name = disc
            .as_ref()
            .map(|d| d.name.clone())
            .or_else(|| saved.as_ref().map(|s| s.name.clone()))
            .unwrap_or_else(|| "PearTube TV".to_owned());
        Some(Target {
            host,
            port,
            public_key,
            token,
            name,
            auth: disc.as_ref().map(|d| d.auth.clone()).unwrap_or_default(),
        })
    }
}

/// The cast controller, `createCastController`'s returned object.
pub struct CastController {
    inner: Arc<ControllerInner>,
}

impl CastController {
    /// `createCastController(...)`.
    pub async fn start(options: ControllerOptions) -> Result<Self, CastError> {
        std::fs::create_dir_all(&options.storage)
            .map_err(|err| CastError::new(err.to_string(), 500))?;
        let saved = SavedReceivers::load(&options.storage);
        let inner = Arc::new(ControllerInner {
            storage: options.storage.clone(),
            peer: options.peer.clone(),
            dht: options.dht.clone(),
            ensure_node: options.ensure_node.clone(),
            get_lan_host: options.get_lan_host.clone(),
            blob_servers: options.blob_servers.clone(),
            on_change: options.on_change.clone(),
            api_port: options.api_port,
            mdns: options.mdns,
            client: Arc::new(HttpClient::new()),
            saved: Mutex::new(saved),
            discovered: Mutex::new(Vec::new()),
            web: Mutex::new(Vec::new()),
            allowed_blobs: Mutex::new(HashSet::new()),
            blob_server: tokio::sync::Mutex::new(None),
            hub: tokio::sync::Mutex::new(None),
            hub_port: AtomicU64::new(0),
            browse: Mutex::new(None),
            browse_host: Mutex::new(None),
            service_ids: Mutex::new(HashMap::new()),
            local_receiver: tokio::sync::Mutex::new(None),
            active: Mutex::new(None),
            attempt: AtomicU64::new(0),
            closing: AtomicBool::new(false),
        });
        Ok(Self { inner })
    }

    /// `initHub()`: bind the zero-install TV hub and answer its port.
    pub async fn init_hub(&self) -> u16 {
        self.inner.ensure_web_hub_server().await
    }

    /// `setLanHost(host)`: rebrowse and move the local receiver's advertisement.
    pub async fn set_lan_host(&self, host: Option<String>) {
        self.inner.refresh_browse(host.clone()).await;
        let receiver = self.inner.local_receiver.lock().await;
        if let Some(receiver) = receiver.as_ref() {
            receiver.set_host(host).await;
        }
    }

    /// `listReceivers()`.
    pub fn list_receivers(&self) -> Vec<super::records::ListedReceiver> {
        self.inner.list_receivers()
    }

    /// `status()`.
    pub fn status(&self) -> Value {
        self.inner.status()
    }

    /// `pairReceiver(params)`.
    pub async fn pair_receiver(&self, params: &Value) -> Result<Value, CastError> {
        self.inner.pair_receiver(params).await
    }

    /// `removeReceiver(id)`.
    pub fn remove_receiver(&self, id: &str) -> Value {
        let removed = self.inner.saved.lock().expect("cast saved").remove(id);
        if removed {
            self.inner.persist_saved();
            self.inner.emit_change();
        }
        json!({ "removed": removed })
    }

    /// `castPlay({ receiverId, key, uri, preferMode, startPositionMs })`.
    pub async fn cast_play(&self, params: &Value) -> Result<Value, CastError> {
        self.inner.cast_play(params).await
    }

    /// `castAttach({ receiverId })`.
    pub async fn cast_attach(&self, params: &Value) -> Result<Value, CastError> {
        self.inner.cast_attach(params).await
    }

    /// `castControl(cmd)`.
    pub async fn cast_control(&self, cmd: &Value) -> Result<Value, CastError> {
        self.inner.cast_control(cmd).await
    }

    /// `castStop({ stopPlayback })`.
    pub async fn cast_stop(&self, stop_playback: bool) -> Result<Value, CastError> {
        self.inner.cast_stop(stop_playback).await
    }

    /// `registerWebReceiver({ id, token, name })`.
    pub fn register_web_receiver(&self, params: &Value) -> Value {
        self.inner.register_web_receiver(params)
    }

    /// `updateWebReceiverState({ id, token, state })`.
    pub fn update_web_receiver_state(&self, params: &Value) -> Value {
        self.inner.update_web_receiver_state(params)
    }

    /// `handleHubCastJson(req, url)` for an API server that shares these routes.
    pub async fn hub_cast_json(&self, req: Request<Incoming>) -> Result<Value, CastError> {
        self.inner.handle_hub_cast_json(req).await
    }

    /// `handleWebEvents(req, res, url)`; `query` is the raw query string.
    pub fn web_events(&self, query: Option<&str>) -> Response<Body> {
        self.inner.handle_web_events(&query.map(str::to_owned))
    }

    /// `handleHubSenderEvents(req, res, url)`; `query` is the raw query string.
    pub fn hub_sender_events(&self, query: Option<&str>) -> Response<Body> {
        self.inner
            .handle_hub_sender_events(&query.map(str::to_owned))
    }

    /// `webSetupQrSvg(url)`.
    pub fn web_setup_qr_svg(&self, url: &str) -> String {
        if url.is_empty() {
            String::new()
        } else {
            super::qr::qr_svg(url)
        }
    }

    /// `startLocalReceiver(...)`.
    pub async fn start_local_receiver(&self, options: ReceiverOptions) -> Result<Value, CastError> {
        let previous = self.inner.local_receiver.lock().await.take();
        if let Some(previous) = previous {
            previous.close().await;
        }
        let peer = match options.peer.clone() {
            Some(peer) => Some(peer),
            None => match self.inner.ensure_node.as_ref() {
                Some(ensure) => ensure().await?,
                None => self.inner.node(),
            },
        };
        let mut options = options;
        options.peer = peer;
        let receiver = Receiver::start(options).await?;
        let info = receiver.info();
        *self.inner.local_receiver.lock().await = Some(receiver);
        self.inner.emit_change();
        Ok(info)
    }

    /// `reportLocalReceiverState(patch)`.
    pub async fn report_local_receiver_state(&self, patch: &Value) -> Option<Value> {
        let guard = self.inner.local_receiver.lock().await;
        guard.as_ref().map(|receiver| receiver.report_state(patch))
    }

    /// `close()`.
    pub async fn close(&self) {
        if self.inner.closing.swap(true, Ordering::SeqCst) {
            return;
        }
        self.inner.close_active_transport();
        self.inner.allowed_blobs.lock().expect("cast blobs").clear();
        if let Some(handle) = self.inner.browse.lock().expect("cast browse").take() {
            handle.stop();
        }
        let receiver = self.inner.local_receiver.lock().await.take();
        if let Some(receiver) = receiver {
            receiver.close().await;
        }
        let blob_server = self.inner.blob_server.lock().await.take();
        if let Some(server) = blob_server {
            server.close().await;
        }
        let hub = self.inner.hub.lock().await.take();
        if let Some(hub) = hub {
            hub.close().await;
        }
    }
}

impl ControllerInner {
    async fn pair_receiver(self: &Arc<Self>, params: &Value) -> Result<Value, CastError> {
        let mut host = params.get("host").map(text_value).unwrap_or_default();
        let mut port = params
            .get("port")
            .map(|v| state::num(v) as u16)
            .unwrap_or(0);
        let mut public_key = params
            .get("publicKey")
            .map(text_value)
            .unwrap_or_default()
            .to_lowercase();
        let id = params.get("id").map(text_value).unwrap_or_default();
        let code = params
            .get("code")
            .and_then(Value::as_str)
            .map(|raw| super::pairing::normalize_pair_code(Some(raw)))
            .unwrap_or_default();
        let name = params.get("name").map(text_value).unwrap_or_default();
        if let Some(address) = params.get("address").and_then(Value::as_str)
            && !address.is_empty()
        {
            let parsed = receiver_address(address).map_err(CastError::bad_request)?;
            host = parsed.host;
            port = parsed.port;
            public_key = parsed.public_key;
        }
        if let Some(uri) = params.get("uri").and_then(Value::as_str)
            && !uri.is_empty()
        {
            return Box::pin(self.pair_uri(uri, &code)).await;
        }
        let discovered = self.discovered();
        let disc = discovered.iter().find(|d| d.id == id).cloned().or_else(|| {
            discovered
                .iter()
                .find(|d| !public_key.is_empty() && d.public_key == public_key)
                .cloned()
        });
        let target_host = if host.is_empty() {
            disc.as_ref().map(|d| d.host.clone()).unwrap_or_default()
        } else {
            host
        };
        let target_port = if port == 0 {
            disc.as_ref().map(|d| d.port as u16).unwrap_or(0)
        } else {
            port
        };
        let target_pk = if public_key.is_empty() {
            disc.as_ref()
                .map(|d| d.public_key.clone())
                .unwrap_or_default()
                .to_lowercase()
        } else {
            public_key
        };
        let target_pk = if target_pk.is_empty() && is_hex64(&id.to_lowercase()) {
            id.to_lowercase()
        } else {
            target_pk
        };

        if !target_host.is_empty() && target_port > 0 {
            let url = format!("http://{target_host}:{target_port}/cast/v1/pair");
            match self
                .client
                .json(
                    Method::POST,
                    &url,
                    Vec::new(),
                    Some(&json!({ "code": code, "clientName": "PearTube" })),
                    Duration::from_secs(5),
                )
                .await
            {
                Ok(answer) => {
                    let record = SavedReceiver {
                        id: answer
                            .get("deviceId")
                            .map(text_value)
                            .filter(|s| !s.is_empty())
                            .unwrap_or_else(|| {
                                if !id.is_empty() {
                                    id.clone()
                                } else if !target_pk.is_empty() {
                                    target_pk.clone()
                                } else {
                                    format!("{target_host}:{target_port}")
                                }
                            }),
                        name: crate::protocol::clean_name(
                            answer
                                .get("name")
                                .map(text_value)
                                .filter(|s| !s.is_empty())
                                .or_else(|| disc.as_ref().map(|d| d.name.clone()))
                                .or_else(|| (!name.is_empty()).then(|| name.clone()))
                                .as_deref(),
                            "PearTube TV",
                        ),
                        host: target_host,
                        port: u64::from(
                            answer
                                .get("port")
                                .map(|v| state::num(v) as u16)
                                .filter(|p| *p > 0)
                                .unwrap_or(target_port),
                        ),
                        public_key: answer
                            .get("publicKey")
                            .map(text_value)
                            .filter(|s| !s.is_empty())
                            .unwrap_or_else(|| target_pk.clone()),
                        token: answer.get("token").map(text_value).unwrap_or_default(),
                        p2p: answer
                            .get("capabilities")
                            .and_then(|c| c.get("p2p"))
                            .is_some_and(truthy),
                        web_hosted: false,
                    };
                    let record = finish_record(record, target_pk);
                    self.saved.lock().expect("cast saved").set(record.clone());
                    self.persist_saved();
                    self.emit_change();
                    return Ok(serde_json::to_value(record).unwrap_or(Value::Null));
                }
                Err(err) => {
                    let code_related = err.message.to_lowercase().contains("pairing code");
                    if target_pk.is_empty() || code_related {
                        return Err(err);
                    }
                }
            }
        }

        if !target_pk.is_empty() {
            let dht = self.dht.as_ref().ok_or_else(|| {
                CastError::bad_request(
                    "Start PearTube P2P node before pairing over Noise public key",
                )
            })?;
            let key = hex_key(&target_pk)?;
            let relay_through = self
                .node()
                .and_then(|node| node.relay_through())
                .and_then(|value| serde_json::from_value::<Vec<String>>(value).ok());
            let rpc = rpc::connect_with_retry(
                dht.as_ref(),
                key,
                &ConnectOptions {
                    relay_through,
                    ..Default::default()
                },
            )
            .await?;
            let mut payload = Map::new();
            payload.insert("code".into(), json!(code));
            payload.insert("clientName".into(), json!("PearTube"));
            let answer = rpc.call("pair", payload, Duration::from_secs(10)).await;
            rpc.close();
            let answer = answer?;
            let record = SavedReceiver {
                id: answer
                    .get("deviceId")
                    .map(text_value)
                    .filter(|s| !s.is_empty())
                    .unwrap_or_else(|| target_pk.clone()),
                name: crate::protocol::clean_name(
                    answer
                        .get("name")
                        .map(text_value)
                        .filter(|s| !s.is_empty())
                        .or_else(|| (!name.is_empty()).then(|| name.clone()))
                        .as_deref(),
                    "PearTube TV",
                ),
                host: target_host,
                port: u64::from(target_port),
                public_key: answer
                    .get("publicKey")
                    .map(text_value)
                    .filter(|s| !s.is_empty())
                    .unwrap_or(target_pk),
                token: answer.get("token").map(text_value).unwrap_or_default(),
                p2p: true,
                web_hosted: false,
            };
            self.saved.lock().expect("cast saved").set(record.clone());
            self.persist_saved();
            self.emit_change();
            return Ok(serde_json::to_value(record).unwrap_or(Value::Null));
        }
        Err(CastError::bad_request(
            "Receiver not reachable on LAN or P2P public key",
        ))
    }

    async fn pair_uri(self: &Arc<Self>, uri: &str, code: &str) -> Result<Value, CastError> {
        let parsed = parse_uri(uri).map_err(|err| CastError::bad_request(err.to_string()))?;
        let crate::protocol::ParsedUri::Receiver {
            public_key,
            device_id,
            host,
            port,
            token,
            name,
            p2p,
        } = parsed
        else {
            return Err(CastError::bad_request(
                "Expected a peartube://receiver/... URI",
            ));
        };
        let port = port.0 as u16;
        let rec_id = if !device_id.is_empty() {
            device_id.clone()
        } else if !public_key.is_empty() {
            public_key.clone()
        } else {
            format!("{host}:{port}")
        };
        let mut record = SavedReceiver {
            id: rec_id.clone(),
            name: crate::protocol::clean_name(Some(&name), "Paired TV"),
            host: host.clone(),
            port: u64::from(port),
            public_key: public_key.clone(),
            token,
            p2p: p2p || !public_key.is_empty(),
            web_hosted: false,
        };
        if record.token.is_empty() && !code.is_empty() {
            let mut params = Map::new();
            params.insert("id".into(), json!(rec_id));
            params.insert("code".into(), json!(code));
            params.insert("host".into(), json!(record.host));
            params.insert("port".into(), json!(record.port));
            params.insert("publicKey".into(), json!(record.public_key));
            params.insert("name".into(), json!(record.name));
            return self.pair_receiver(&Value::Object(params)).await;
        }
        if !record.host.is_empty() && record.port > 0 {
            let url = format!("http://{}:{}/cast/v1/info", record.host, record.port);
            if let Ok(info) = self
                .client
                .json(
                    Method::GET,
                    &url,
                    Vec::new(),
                    None,
                    Duration::from_millis(2500),
                )
                .await
            {
                if let Some(device) = info
                    .get("deviceId")
                    .map(text_value)
                    .filter(|s| !s.is_empty())
                {
                    record.id = device;
                }
                if let Some(name) = info.get("name").map(text_value).filter(|s| !s.is_empty()) {
                    record.name = crate::protocol::clean_name(Some(&name), &record.name);
                }
                if let Some(public_key) = info
                    .get("publicKey")
                    .map(text_value)
                    .filter(|s| !s.is_empty())
                {
                    record.public_key = public_key;
                }
                record.p2p = info
                    .get("capabilities")
                    .and_then(|c| c.get("p2p"))
                    .is_some_and(truthy)
                    || !record.public_key.is_empty();
            }
        }
        self.saved.lock().expect("cast saved").set(record.clone());
        self.persist_saved();
        self.emit_change();
        Ok(serde_json::to_value(record).unwrap_or(Value::Null))
    }

    async fn resolve_entry_to_cast(
        &self,
        peer: &Arc<dyn CastPeer>,
        key: &str,
        uri: &str,
    ) -> Result<(Value, Value), CastError> {
        if !uri.is_empty() {
            let opened = peer
                .open_uri(uri, false)
                .await?
                .ok_or_else(|| CastError::bad_request("URI is not a playable watch link"))?;
            let entry_key = opened.entry.get("key").map(text_value).unwrap_or_default();
            if entry_key.is_empty() {
                return Err(CastError::bad_request("URI is not a playable watch link"));
            }
            let raw = peer
                .find_entry(&entry_key)
                .await
                .ok_or_else(|| CastError::new(format!("Entry not found: {entry_key}"), 404))?;
            let entry = opened.entry.clone();
            return Ok((entry, raw));
        }
        if key.is_empty() {
            return Err(CastError::bad_request(
                "Entry key or watch URI required to cast",
            ));
        }
        let raw = peer
            .find_entry(key)
            .await
            .ok_or_else(|| CastError::new(format!("Entry not found: {key}"), 404))?;
        let id = raw.get("id").map(text_value).unwrap_or_default();
        let list = peer.search(&id).await;
        let item = list
            .into_iter()
            .find(|entry| entry.get("key").map(text_value).as_deref() == Some(key))
            .unwrap_or_else(|| {
                json!({
                    "key": key,
                    "id": id,
                    "title": raw.get("title").cloned().unwrap_or(Value::Null),
                    "size": raw.get("size").cloned().unwrap_or(json!(0)),
                    "sha256": raw.get("sha256").cloned().unwrap_or(Value::Null),
                    "magnetUrl": format_watch_uri(&watch_entry_from(&raw), "", ""),
                })
            });
        Ok((item, raw))
    }

    async fn cast_play(self: &Arc<Self>, params: &Value) -> Result<Value, CastError> {
        let peer = match self.ensure_node.as_ref() {
            Some(ensure) => ensure().await?,
            None => self.node(),
        }
        .ok_or_else(|| CastError::bad_request("P2P node is not started"))?;
        let receiver_id = params.get("receiverId").map(text_value).unwrap_or_default();
        let key = params.get("key").map(text_value).unwrap_or_default();
        let uri = params.get("uri").map(text_value).unwrap_or_default();
        let prefer_mode = match params.get("preferMode").and_then(Value::as_str) {
            Some("http") => "http",
            Some("p2p") => "p2p",
            _ => "auto",
        };
        let start_position = params
            .get("startPositionMs")
            .map(|v| state::num(v).max(0.0) as u64)
            .unwrap_or(0);
        let (item, raw) = self.resolve_entry_to_cast(&peer, &key, &uri).await?;
        let disc = self
            .discovered
            .lock()
            .expect("cast discovered")
            .iter()
            .find(|d| d.id == receiver_id)
            .cloned();
        let pair = self
            .saved
            .lock()
            .expect("cast saved")
            .get(&receiver_id)
            .cloned();
        let target_p2p =
            disc.as_ref().is_some_and(|d| d.p2p) || pair.as_ref().is_some_and(|p| p.p2p);
        let blobs_hex = raw.get("blobs").map(text_value).unwrap_or_default();
        let local = item.get("local").is_some_and(truthy);
        let cached_bytes = item.get("cachedBytes").and_then(Value::as_u64).unwrap_or(0);
        if !target_p2p || local || cached_bytes > 0 {
            peer.warm(&raw).await;
        }
        self.allowed_blobs
            .lock()
            .expect("cast blobs")
            .insert(blobs_hex.clone());
        let lan_host = self.lan_host();
        let blob_server = self.ensure_lan_blob_server().await?;
        blob_server.allow(&blobs_hex);

        let web_target = self
            .web
            .lock()
            .expect("cast web")
            .iter()
            .find(|r| r.id == receiver_id)
            .map(|r| (r.name.clone(), r.pair_token.clone(), r.seq));
        let host_for_receiver = if web_target.is_some() {
            Some(lan_host.clone().unwrap_or_else(|| "127.0.0.1".to_owned()))
        } else {
            match lan_host.clone() {
                Some(host) => Some(host),
                None => {
                    let loopback = disc
                        .as_ref()
                        .map(|d| d.host == "127.0.0.1")
                        .unwrap_or(false)
                        || pair
                            .as_ref()
                            .map(|p| p.host == "127.0.0.1")
                            .unwrap_or(false);
                    loopback.then(|| "127.0.0.1".to_owned())
                }
            }
        };
        let blob = raw
            .get("blob")
            .cloned()
            .and_then(|value| serde_json::from_value::<crate::protocol::BlobPointer>(value).ok())
            .map(super::media::blob_from_pointer);
        let mime = super::http::guess_media_mime(&item);
        let http_stream_url = match (&host_for_receiver, blob) {
            (Some(host), Some(blob)) => Some(blob_server.get_link(&blobs_hex, host, &blob, &mime)),
            _ => None,
        };
        let mut entry = Map::new();
        entry.insert(
            "key".into(),
            item.get("key").cloned().unwrap_or(Value::Null),
        );
        entry.insert("id".into(), item.get("id").cloned().unwrap_or(Value::Null));
        entry.insert(
            "title".into(),
            item.get("title").cloned().unwrap_or(Value::Null),
        );
        entry.insert("size".into(), item.get("size").cloned().unwrap_or(json!(0)));
        entry.insert(
            "sha256".into(),
            item.get("sha256").cloned().unwrap_or(Value::Null),
        );
        let source = item.get("source").map(text_value).unwrap_or_default();
        let source_kind = if item.get("sourceKind").and_then(Value::as_str) == Some("channel") {
            "channel"
        } else {
            ""
        };
        let mut load = Map::new();
        load.insert("action".into(), json!("load"));
        load.insert("entry".into(), Value::Object(entry));
        load.insert(
            "httpStreamUrl".into(),
            match &http_stream_url {
                Some(url) => json!(url),
                None => Value::Null,
            },
        );
        load.insert(
            "magnetUrl".into(),
            json!(format_watch_uri(
                &watch_entry_from(&raw),
                &source,
                source_kind
            )),
        );
        load.insert("senderPeerKey".into(), json!(peer.peer_key()));
        load.insert("senderPrivateRouting".into(), json!(peer.private_routing()));
        load.insert(
            "senderLan".into(),
            if peer.private_routing() {
                Value::Null
            } else {
                peer.lan_endpoint().unwrap_or(Value::Null)
            },
        );
        let mut network = Map::new();
        network.insert("privateRouting".into(), json!(peer.private_routing()));
        network.insert("bootstrap".into(), peer.bootstrap().unwrap_or(Value::Null));
        network.insert(
            "relayThrough".into(),
            peer.relay_through().unwrap_or(Value::Null),
        );
        network.insert("lan".into(), json!(peer.has_lan()));
        network.insert(
            "sourceKey".into(),
            json!(if is_hex64(&source) {
                source.clone()
            } else {
                String::new()
            }),
        );
        network.insert(
            "sourceKind".into(),
            json!(if source_kind == "channel" {
                "channel"
            } else {
                "tracker"
            }),
        );
        load.insert("network".into(), Value::Object(network));
        load.insert("preferMode".into(), json!(prefer_mode));
        load.insert("startPositionMs".into(), json!(start_position));
        let load = Value::Object(load);

        if let Some((web_name, pair_token, _seq)) = web_target {
            let Some(http_stream_url) = http_stream_url.clone() else {
                return Err(CastError::bad_request(
                    "Smart TV Web Receiver needs a LAN IPv4 address for HTTP streaming",
                ));
            };
            self.close_active_transport();
            let loading = {
                let mut web = self.web.lock().expect("cast web");
                let Some(index) = web.iter().position(|r| r.id == receiver_id) else {
                    return Err(CastError::bad_request("Web TV disconnected"));
                };
                web[index].seq += 1;
                let seq = web[index].seq;
                let cmd = json!({
                    "seq": seq,
                    "action": "load",
                    "entry": load.get("entry").cloned().unwrap_or(Value::Null),
                    "mode": "http",
                    "streamUrl": http_stream_url,
                    "httpStreamUrl": http_stream_url,
                    "startPositionMs": start_position,
                });
                let frame = sse_frame(&json!({ "event": "command", "command": cmd }));
                let mut live = Vec::new();
                for client in std::mem::take(&mut web[index].clients) {
                    if client.try_send(frame.clone()).is_ok() {
                        live.push(client);
                    }
                }
                web[index].clients = live;
                let mut loading = idle(now_ms());
                loading.insert("status".into(), json!("loading"));
                loading.insert("mode".into(), json!("http"));
                loading.insert(
                    "entry".into(),
                    load.get("entry").cloned().unwrap_or(Value::Null),
                );
                loading.insert("positionMs".into(), json!(start_position));
                loading.insert("playing".into(), json!(true));
                loading.insert("buffering".into(), json!(true));
                let loading = Value::Object(loading);
                web[index].state = loading.clone();
                loading
            };
            *self.active.lock().expect("cast active") = Some(Session {
                session: CastSession {
                    receiver_id,
                    receiver_name: web_name,
                    transport: "web",
                    base_url: None,
                    token: pair_token,
                    public_key: String::new(),
                    state: loading,
                },
                sse: None,
                rpc: None,
            });
            self.emit_change();
            return Ok(self.status());
        }

        let target = self
            .target(&receiver_id)
            .ok_or_else(|| CastError::new("Cannot reach receiver PearTube TV", 404))?;
        if target.token.is_empty() && target.auth != "open" {
            return Err(CastError::bad_request(format!(
                "Pair with {} using its 6-character TV code before casting",
                target.name
            )));
        }
        self.close_active_transport();
        let mine = self.attempt.load(Ordering::SeqCst);

        if !target.host.is_empty() && target.port > 0 {
            let base_url = format!("http://{}:{}", target.host, target.port);
            let local_load = if http_stream_url.is_none() && target.host == "127.0.0.1" {
                blob.map(|blob| {
                    let mut load = load.clone();
                    if let Value::Object(map) = &mut load {
                        map.insert(
                            "httpStreamUrl".into(),
                            json!(blob_server.get_link(&blobs_hex, "127.0.0.1", &blob, &mime)),
                        );
                    }
                    load
                })
            } else {
                None
            };
            let payload = local_load.unwrap_or_else(|| load.clone());
            let attempt: Result<Value, CastError> = async {
                let info = self
                    .client
                    .json(
                        Method::GET,
                        &format!("{base_url}/cast/v1/info"),
                        Vec::new(),
                        None,
                        Duration::from_millis(2500),
                    )
                    .await
                    .ok();
                if let Some(swarm) = info
                    .as_ref()
                    .and_then(|i| i.get("swarmKey"))
                    .map(text_value)
                    .filter(|s| !s.is_empty())
                {
                    peer.join_peer(&swarm, false);
                }
                let headers = if target.token.is_empty() {
                    Vec::new()
                } else {
                    vec![(header::AUTHORIZATION, format!("Bearer {}", target.token))]
                };
                let answer = self
                    .client
                    .json(
                        Method::POST,
                        &format!("{base_url}/cast/v1/command"),
                        headers,
                        Some(&payload),
                        Duration::from_secs(12),
                    )
                    .await?;
                let state = answer
                    .get("state")
                    .cloned()
                    .unwrap_or_else(|| Value::Object(idle(now_ms())));
                if !peer.private_routing()
                    && let Some(endpoint) = state.get("peerLan").filter(|v| truthy(v))
                {
                    peer.dial_lan_peer(endpoint);
                }
                if let Some(swarm) = state
                    .get("peerSwarmKey")
                    .map(text_value)
                    .filter(|s| !s.is_empty())
                {
                    peer.join_peer(&swarm, target.host != "127.0.0.1");
                }
                let session = CastSession {
                    receiver_id: receiver_id.clone(),
                    receiver_name: target.name.clone(),
                    transport: "lan",
                    base_url: Some(base_url.clone()),
                    token: target.token.clone(),
                    public_key: target.public_key.clone(),
                    state,
                };
                let sse = self
                    .subscribe_http_sse(&base_url, &target.token, mine)
                    .await;
                let claimed = self.claim(mine, session, sse, None).await?;
                let _ = claimed;
                self.emit_change();
                Ok(self.status())
            }
            .await;
            match attempt {
                Ok(status) => return Ok(status),
                Err(err) => {
                    let code_related = err.message.to_lowercase().contains("unauthorized")
                        || err.message.to_lowercase().contains("pair");
                    if target.public_key.is_empty()
                        || mine != self.attempt.load(Ordering::SeqCst)
                        || code_related
                    {
                        return Err(err);
                    }
                }
            }
        }

        if !target.public_key.is_empty() {
            let dht = self
                .dht
                .as_ref()
                .ok_or_else(|| CastError::bad_request("Start PearTube P2P node before casting"))?;
            let key = hex_key(&target.public_key)?;
            let session_state: Arc<Mutex<Option<Value>>> = Arc::new(Mutex::new(None));
            let state_sink = Arc::clone(&session_state);
            let rpc = rpc::connect_with_retry(
                dht.as_ref(),
                key,
                &ConnectOptions {
                    relay_through: peer
                        .relay_through()
                        .and_then(|value| serde_json::from_value::<Vec<String>>(value).ok()),
                    on_state: Some(Arc::new(move |state| {
                        *state_sink.lock().expect("cast session state") = Some(state);
                    })),
                    ..Default::default()
                },
            )
            .await?;
            let mut subscribe = Map::new();
            subscribe.insert("token".into(), json!(target.token));
            rpc.call("subscribe", subscribe, Duration::from_secs(10))
                .await?;
            let mut payload = match load.clone() {
                Value::Object(map) => map,
                _ => Map::new(),
            };
            payload.insert("token".into(), json!(target.token));
            let answer = rpc.call("cmd", payload, Duration::from_secs(20)).await;
            let answer = match answer {
                Ok(answer) => answer,
                Err(err) => {
                    rpc.close();
                    return Err(err);
                }
            };
            let state = answer
                .get("state")
                .cloned()
                .or_else(|| session_state.lock().expect("cast session state").clone())
                .unwrap_or_else(|| Value::Object(idle(now_ms())));
            if let Some(swarm) = state
                .get("peerSwarmKey")
                .map(text_value)
                .filter(|s| !s.is_empty())
            {
                peer.join_peer(&swarm, false);
            }
            let session = CastSession {
                receiver_id,
                receiver_name: target.name,
                transport: "p2p",
                base_url: None,
                token: target.token,
                public_key: target.public_key,
                state,
            };
            let claimed = self.claim(mine, session, None, Some(Arc::new(rpc))).await?;
            let _ = claimed;
            return Ok(self.status());
        }
        Err(CastError::new(
            format!("Cannot reach receiver {}", target.name),
            404,
        ))
    }

    async fn subscribe_http_sse(
        self: &Arc<Self>,
        base_url: &str,
        token: &str,
        mine: u64,
    ) -> Option<JoinHandle<()>> {
        let url = if token.is_empty() {
            format!("{base_url}/cast/v1/events")
        } else {
            format!(
                "{base_url}/cast/v1/events?token={}",
                crate::protocol::encode_uri_component(token)
            )
        };
        let response = match self
            .client
            .request(Method::GET, &url, Vec::new(), None, Duration::from_secs(5))
            .await
        {
            Ok(response) if response.status() == StatusCode::OK => response,
            _ => return None,
        };
        let inner = Arc::clone(self);
        let poll_url = format!("{base_url}/cast/v1/info");
        Some(tokio::spawn(async move {
            let mut body = response.into_body();
            let mut buffer = String::new();
            let mut last_event = tokio::time::Instant::now();
            let mut tick = tokio::time::interval(Duration::from_secs(1));
            loop {
                tokio::select! {
                    frame = body.frame() => {
                        let Some(frame) = frame else { break };
                        let Ok(frame) = frame else { break };
                        let Some(data) = frame.into_data().ok() else { continue };
                        last_event = tokio::time::Instant::now();
                        buffer.push_str(&String::from_utf8_lossy(&data));
                        while let Some(index) = buffer.find("\n\n") {
                            let block: String = buffer.drain(..index + 2).collect();
                            for line in block.lines() {
                                let Some(payload) = line.strip_prefix("data:") else { continue };
                                let Ok(event) = serde_json::from_str::<Value>(payload.trim()) else { continue };
                                if event.get("event").and_then(Value::as_str) != Some("state") {
                                    continue;
                                }
                                let Some(state) = event.get("state").cloned() else { continue };
                                if inner.attempt.load(Ordering::SeqCst) != mine { continue }
                                if let Some(session) = inner.active.lock().expect("cast active").as_mut() {
                                    session.session.state = state;
                                }
                                inner.emit_change();
                            }
                        }
                    }
                    _ = tick.tick() => {
                        // A silent SSE stream is polled, like the app's 1 s fallback.
                        if last_event.elapsed() < Duration::from_millis(1200) { continue }
                        let Ok(info) = inner.client.json(Method::GET, &poll_url, Vec::new(), None, Duration::from_millis(1500)).await else { continue };
                        let Some(state) = info.get("state").cloned() else { continue };
                        if inner.attempt.load(Ordering::SeqCst) != mine { continue }
                        if let Some(session) = inner.active.lock().expect("cast active").as_mut() {
                            session.session.state = state;
                        }
                        inner.emit_change();
                    }
                }
            }
        }))
    }

    async fn cast_attach(self: &Arc<Self>, params: &Value) -> Result<Value, CastError> {
        let receiver_id = params.get("receiverId").map(text_value).unwrap_or_default();
        let target = self
            .target(&receiver_id)
            .ok_or_else(|| CastError::new("Cannot reach receiver PearTube TV", 404))?;
        self.close_active_transport();
        let mine = self.attempt.load(Ordering::SeqCst);
        if !target.host.is_empty() && target.port > 0 {
            let base_url = format!("http://{}:{}", target.host, target.port);
            let headers = if target.token.is_empty() {
                Vec::new()
            } else {
                vec![(header::AUTHORIZATION, format!("Bearer {}", target.token))]
            };
            let answer = self
                .client
                .json(
                    Method::POST,
                    &format!("{base_url}/cast/v1/command"),
                    headers,
                    Some(&json!({ "action": "status" })),
                    Duration::from_secs(5),
                )
                .await?;
            let state = answer
                .get("state")
                .cloned()
                .unwrap_or_else(|| Value::Object(idle(now_ms())));
            let session = CastSession {
                receiver_id,
                receiver_name: target.name.clone(),
                transport: "lan",
                base_url: Some(base_url.clone()),
                token: target.token.clone(),
                public_key: target.public_key.clone(),
                state,
            };
            let sse = self
                .subscribe_http_sse(&base_url, &target.token, mine)
                .await;
            self.claim(mine, session, sse, None).await?;
            self.emit_change();
            return Ok(self.status());
        }
        if !target.public_key.is_empty() {
            let dht = self.dht.as_ref().ok_or_else(|| {
                CastError::bad_request(
                    "Start PearTube P2P node before attaching over Noise public key",
                )
            })?;
            let key = hex_key(&target.public_key)?;
            let session_state: Arc<Mutex<Option<Value>>> = Arc::new(Mutex::new(None));
            let state_sink = Arc::clone(&session_state);
            let rpc = rpc::connect_with_retry(
                dht.as_ref(),
                key,
                &ConnectOptions {
                    on_state: Some(Arc::new(move |state| {
                        *state_sink.lock().expect("cast session state") = Some(state);
                    })),
                    ..Default::default()
                },
            )
            .await?;
            let mut subscribe = Map::new();
            subscribe.insert("token".into(), json!(target.token));
            rpc.call("subscribe", subscribe, Duration::from_secs(10))
                .await?;
            let mut status = Map::new();
            status.insert("token".into(), json!(target.token));
            status.insert("action".into(), json!("status"));
            let answer = rpc.call("cmd", status, Duration::from_secs(10)).await?;
            let state = answer
                .get("state")
                .cloned()
                .unwrap_or_else(|| Value::Object(idle(now_ms())));
            let session = CastSession {
                receiver_id,
                receiver_name: target.name,
                transport: "p2p",
                base_url: None,
                token: target.token,
                public_key: target.public_key,
                state,
            };
            self.claim(mine, session, None, Some(Arc::new(rpc))).await?;
            self.emit_change();
            return Ok(self.status());
        }
        Err(CastError::new(
            format!("Cannot reach receiver {}", target.name),
            404,
        ))
    }

    async fn cast_control(self: &Arc<Self>, cmd: &Value) -> Result<Value, CastError> {
        let current = self
            .active
            .lock()
            .expect("cast active")
            .as_ref()
            .map(|s| s.session.clone());
        let Some(session) = current else {
            return Err(CastError::bad_request("No active cast session"));
        };
        let stop = cmd.get("action").and_then(Value::as_str) == Some("stop");
        match session.transport {
            "web" => {
                let mut web = self.web.lock().expect("cast web");
                let Some(index) = web.iter().position(|r| r.id == session.receiver_id) else {
                    return Err(CastError::bad_request("Web TV disconnected"));
                };
                web[index].seq += 1;
                let seq = web[index].seq;
                let mut full = Map::new();
                full.insert("seq".into(), json!(seq));
                if let Value::Object(fields) = cmd {
                    for (key, value) in fields {
                        full.insert(key.clone(), value.clone());
                    }
                }
                let frame =
                    sse_frame(&json!({ "event": "command", "command": Value::Object(full) }));
                let mut live = Vec::new();
                for client in std::mem::take(&mut web[index].clients) {
                    if client.try_send(frame.clone()).is_ok() {
                        live.push(client);
                    }
                }
                web[index].clients = live;
                if stop {
                    web[index].state = Value::Object(idle(now_ms()));
                }
                drop(web);
                if stop {
                    self.close_active_transport();
                }
                self.emit_change();
                Ok(self.status())
            }
            "lan" => {
                let base_url = session.base_url.clone().unwrap_or_default();
                let headers = if session.token.is_empty() {
                    Vec::new()
                } else {
                    vec![(header::AUTHORIZATION, format!("Bearer {}", session.token))]
                };
                let answer = self
                    .client
                    .json(
                        Method::POST,
                        &format!("{base_url}/cast/v1/command"),
                        headers,
                        Some(cmd),
                        Duration::from_secs(5),
                    )
                    .await?;
                if let Some(state) = answer.get("state").cloned()
                    && let Some(active) = self.active.lock().expect("cast active").as_mut()
                {
                    active.session.state = state;
                }
                if stop {
                    self.close_active_transport();
                }
                self.emit_change();
                Ok(self.status())
            }
            _ => {
                let rpc = self
                    .active
                    .lock()
                    .expect("cast active")
                    .as_ref()
                    .and_then(|s| s.rpc.clone())
                    .ok_or_else(|| CastError::bad_request("Cast session transport disconnected"))?;
                if rpc.closed() {
                    return Err(CastError::bad_request(
                        "Cast session transport disconnected",
                    ));
                }
                let mut payload = match cmd {
                    Value::Object(map) => map.clone(),
                    _ => Map::new(),
                };
                payload.insert("token".into(), json!(session.token));
                let answer = rpc.call("cmd", payload, Duration::from_secs(10)).await?;
                if let Some(state) = answer.get("state").cloned()
                    && let Some(active) = self.active.lock().expect("cast active").as_mut()
                {
                    active.session.state = state;
                }
                if stop {
                    self.close_active_transport();
                }
                self.emit_change();
                Ok(self.status())
            }
        }
    }

    async fn cast_stop(self: &Arc<Self>, stop_playback: bool) -> Result<Value, CastError> {
        let has_session = self.active.lock().expect("cast active").is_some();
        if !has_session {
            self.close_active_transport();
            return Ok(self.status());
        }
        if stop_playback {
            let _ = self.cast_control(&json!({ "action": "stop" })).await;
        }
        self.close_active_transport();
        self.allowed_blobs.lock().expect("cast blobs").clear();
        self.emit_change();
        Ok(self.status())
    }
}

fn finish_record(mut record: SavedReceiver, target_pk: String) -> SavedReceiver {
    if record.public_key.is_empty() {
        record.public_key = target_pk;
    }
    record.p2p = record.p2p || !record.public_key.is_empty();
    record
}

fn watch_entry_from(raw: &Value) -> WatchEntry {
    WatchEntry {
        id: raw.get("id").map(text_value).unwrap_or_default(),
        title: raw.get("title").map(text_value).unwrap_or_default(),
        sha256: raw.get("sha256").map(text_value),
        blobs: raw.get("blobs").map(text_value).unwrap_or_default(),
        blob: raw
            .get("blob")
            .cloned()
            .and_then(|v| serde_json::from_value(v).ok()),
    }
}

fn hex_key(text: &str) -> Result<[u8; 32], CastError> {
    let text = text.to_lowercase();
    if text.len() != 64 {
        return Err(CastError::bad_request(
            "Public key must be 64 hex characters",
        ));
    }
    let mut out = [0u8; 32];
    for (i, byte) in out.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&text[i * 2..i * 2 + 2], 16)
            .map_err(|_| CastError::bad_request("Public key must be 64 hex characters"))?;
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn active_session_survives_contended_status_and_stop() {
        let storage = tempfile::tempdir().unwrap();
        let controller = CastController::start(ControllerOptions {
            storage: storage.path().to_owned(),
            mdns: false,
            ..Default::default()
        })
        .await
        .unwrap();
        *controller.inner.active.try_lock().unwrap() = Some(Session {
            session: CastSession {
                receiver_id: "tv".into(),
                receiver_name: "TV".into(),
                transport: "lan",
                base_url: None,
                token: String::new(),
                public_key: String::new(),
                state: json!({ "playing": false }),
            },
            sse: None,
            rpc: None,
        });
        for stop in [false, true] {
            let (locked, held) = std::sync::mpsc::sync_channel(0);
            let (release, released) = std::sync::mpsc::sync_channel(0);
            let writer = Arc::clone(&controller.inner);
            let writer = std::thread::spawn(move || {
                let _guard = writer.active.try_lock().unwrap();
                locked.send(()).unwrap();
                released.recv().unwrap();
            });
            held.recv().unwrap();
            let reader = Arc::clone(&controller.inner);
            let (sent, received) = std::sync::mpsc::sync_channel(1);
            let reader = std::thread::spawn(move || {
                if stop {
                    reader.close_active_transport();
                }
                sent.send(reader.status()).unwrap();
            });
            let early = received.recv_timeout(Duration::from_millis(100));
            release.send(()).unwrap();
            writer.join().unwrap();
            reader.join().unwrap();
            assert!(early.is_err(), "contention must wait, not omit the session");
            let status = received.recv().unwrap();
            if stop {
                assert_eq!(status["session"], Value::Null);
                assert!(controller.inner.active.try_lock().unwrap().is_none());
            } else {
                assert_eq!(status["session"]["state"]["playing"], json!(false));
            }
        }
        controller.close().await;
    }
}
