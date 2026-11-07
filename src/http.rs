//! The app's single HTTP surface (`src/http.js`), ported for relays and local client daemons.
//!
//! One server answers the `/v1` JSON API, the SSE event stream, the same-origin blob proxy, the
//! Stremio v3 addon documents and the M3U playlist. Node work goes through the [`Node`] trait;
//! everything the app keeps outside a node (settings, acquire jobs, the mirror host, cast, art
//! lookups) arrives as hooks on [`Options`].

use std::collections::HashMap;
use std::convert::Infallible;
use std::io;
use std::net::{IpAddr, SocketAddr};
use std::path::PathBuf;
use std::sync::{Arc, LazyLock, Mutex};
use std::time::{Duration, Instant};

use bytes::Bytes;
use futures_util::FutureExt;
use futures_util::future::BoxFuture;
use http_body_util::combinators::UnsyncBoxBody;
use http_body_util::{BodyExt, Empty, Full, StreamBody};
use hyper::body::{Frame, Incoming};
use hyper::header::{self, HeaderMap, HeaderValue};
use hyper::service::service_fn;
use hyper::{Method, Request, Response, StatusCode};
use hyper_util::client::legacy::Client;
use hyper_util::client::legacy::connect::HttpConnector;
use hyper_util::rt::{TokioExecutor, TokioIo};
use serde_json::{Map, Number, Value, json};
use tokio::net::TcpListener;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinSet;
use url::Url;

use crate::art::{self, CINEMETA_URL};
use crate::node::Node;
use crate::node::api::{Entry, SearchOptions, Source, SourceSpec, Status};
use crate::protocol::{clean_text, encode_uri_component, is_media_file, parse_uri};
use crate::resolution::{resolution_label, video_size};
use crate::settings::Settings;

/// `MAX_BODY`: the largest JSON request body the app reads.
pub const MAX_BODY: usize = 64 * 1024;
/// A stream whose header gave no resolution is tried again after ten minutes.
const RESOLUTION_RETRY: Duration = Duration::from_secs(600);
/// Sniffed download formats kept before the map is cleared.
const DOWNLOAD_CACHE_MAX: usize = 512;
/// `readStream`'s budget for the bytes it asks the blob server for.
const STREAM_TIMEOUT: Duration = Duration::from_secs(4);

/// Every response body the API writes; a channel body keeps SSE and proxied video streaming.
pub type Body = UnsyncBoxBody<Bytes, io::Error>;

type HttpClient = Client<HttpConnector, Full<Bytes>>;

static CLIENT: LazyLock<HttpClient> =
    LazyLock::new(|| Client::builder(TokioExecutor::new()).build_http());

/// A failure that answers with `{"error": message}`; the status is the app's `err.status || 400`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HttpError {
    status: u16,
    message: String,
}

impl HttpError {
    pub fn new(status: u16, message: impl Into<String>) -> Self {
        Self {
            status,
            message: message.into(),
        }
    }

    pub fn status(&self) -> u16 {
        self.status
    }

    pub fn message(&self) -> &str {
        &self.message
    }
}

impl std::fmt::Display for HttpError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for HttpError {}

/// Where the node's blob server listens. `host` is what the node advertises; `0.0.0.0`/`::`
/// mean loopback, the way the app rewrites them.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlobAddr {
    pub host: String,
    pub port: u16,
}

impl BlobAddr {
    pub fn new(host: impl Into<String>, port: u16) -> Self {
        Self {
            host: host.into(),
            port,
        }
    }

    fn host(&self) -> &str {
        match self.host.trim() {
            "" | "0.0.0.0" | "::" => "127.0.0.1",
            host => host,
        }
    }
}

/// Which Cinemeta image an art lookup wants.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Art {
    Poster,
    Backdrop,
}

/// `state.posterUrl` / `state.backdropUrl`: an IMDb id to a Cinemeta image URL.
pub type ArtLookup = Arc<dyn Fn(String) -> BoxFuture<'static, Option<String>> + Send + Sync>;
/// `state.applySettings(changed)`.
pub type ApplySettings =
    Arc<dyn Fn(Vec<String>) -> BoxFuture<'static, Result<(), String>> + Send + Sync>;
/// `state.recoverMnemonic(mnemonic, alias)`.
pub type RecoverMnemonic =
    Arc<dyn Fn(String, String) -> BoxFuture<'static, Result<Value, HttpError>> + Send + Sync>;
/// `state.identify(link)`.
pub type Identify =
    Arc<dyn Fn(String) -> BoxFuture<'static, Result<Value, HttpError>> + Send + Sync>;

/// The app's `metahub(kind, size)` lookups, which read Cinemeta through [`crate::art`].
pub fn cinemeta_lookup(base: &str, art: Art) -> ArtLookup {
    let base = base.to_owned();
    Arc::new(move |imdb: String| {
        let base = base.clone();
        Box::pin(async move {
            match art {
                Art::Poster => art::poster_url(&base, &imdb).await,
                Art::Backdrop => art::backdrop_url(&base, &imdb).await,
            }
        })
    })
}

/// The default art lookups: Cinemeta's small poster and medium backdrop.
pub fn metahub_lookups(art_base: &str) -> (ArtLookup, ArtLookup) {
    (
        cinemeta_lookup(art_base, Art::Poster),
        cinemeta_lookup(art_base, Art::Backdrop),
    )
}

/// A Cinemeta base URL, exported for callers that do not carry their own.
pub fn cinemeta_base() -> &'static str {
    CINEMETA_URL
}

/// `state.acquirer`: background downloads the `/v1/acquire` and `/v1/jobs` routes drive.
pub trait Acquirer: Send + Sync {
    fn add<'a>(&'a self, request: Value) -> BoxFuture<'a, Result<Value, HttpError>>;
    /// The job list, as `acquirer.list()` returns it.
    fn list(&self) -> Value;
    /// One job, or `null`.
    fn get(&self, id: &str) -> Value;
    /// The cancel result, or `null`.
    fn cancel(&self, id: &str) -> Value;
}

/// `state.mirror`: the plugin host behind `/v1/plugins` and `/v1/mirror`.
pub trait Mirror: Send + Sync {
    fn list(&self) -> Value;
    fn run<'a>(
        &'a self,
        plugin: &'a str,
        payload: Value,
    ) -> BoxFuture<'a, Result<Value, HttpError>>;
}

/// The cast hub the API server forwards to. The cast stack owns every one of these; the API
/// server only decides which routes reach it.
pub trait CastBridge: Send + Sync {
    fn status(&self) -> Value;
    fn web_setup_qr_svg(&self, url: &str) -> String;
    fn register_web_receiver(&self, params: &Value) -> Value;
    fn update_web_receiver_state(&self, params: &Value) -> Value;
    fn pair_receiver<'a>(&'a self, params: &'a Value) -> BoxFuture<'a, Result<Value, HttpError>>;
    fn remove_receiver(&self, id: &str) -> Value;
    fn cast_play<'a>(&'a self, params: &'a Value) -> BoxFuture<'a, Result<Value, HttpError>>;
    fn cast_control<'a>(&'a self, params: &'a Value) -> BoxFuture<'a, Result<Value, HttpError>>;
    fn cast_stop<'a>(&'a self, params: &'a Value) -> BoxFuture<'a, Result<Value, HttpError>>;
    /// `/cast/v1/*`: `None` is the app's `handleHubCastJson` miss.
    fn hub_json<'a>(
        &'a self,
        method: &'a Method,
        url: &'a Url,
        body: Value,
    ) -> BoxFuture<'a, Option<Result<Value, HttpError>>>;
    fn web_events(&self, url: &Url) -> Response<Body>;
    fn hub_sender_events(&self, url: &Url) -> Response<Body>;
}

/// Everything the app reads off `state` besides the node.
#[derive(Default)]
pub struct Options {
    /// `PEARTUBE_API_TOKEN`: when set, writes and settings reads need `Authorization: Bearer`.
    pub api_token: Option<String>,
    /// `state.fallbackStatus`: the status answered with before a node exists.
    pub fallback_status: Option<Value>,
    /// `state.extraStatus`: fields merged over the node status (`lan`, `apiUrl`, `lanStats`).
    pub extra_status: Option<Arc<dyn Fn() -> Value + Send + Sync>>,
    /// `state.fallbackJobs`: jobs answered with when no acquirer is attached.
    pub fallback_jobs: Option<Arc<dyn Fn() -> Value + Send + Sync>>,
    /// `state.settings`: the relay settings the `/v1/settings` routes read and write.
    pub settings: Option<Settings>,
    /// `state.applySettings(changed)`: applies live settings in place; a message becomes `warning`.
    pub apply_settings: Option<ApplySettings>,
    /// `state.restart`: reopens the node for the settings a restart applies.
    pub restart: Option<Arc<dyn Fn() -> BoxFuture<'static, ()> + Send + Sync>>,
    /// `state.recoverMnemonic(mnemonic, alias)`.
    pub recover_mnemonic: Option<RecoverMnemonic>,
    /// `state.identify(link)`.
    pub identify: Option<Identify>,
    /// `state.onSourcesChanged(sources)`.
    pub on_sources_changed: Option<Arc<dyn Fn(Vec<Source>) + Send + Sync>>,
    /// `state.posterUrl` and `state.backdropUrl`.
    pub poster: Option<ArtLookup>,
    pub backdrop: Option<ArtLookup>,
    /// The node's blob server, for `/v1/blob`, artwork-adjacent stream reads and resolutions.
    pub blob: Option<BlobAddr>,
    /// `state.tls.ca`, served at `/ca.crt`.
    pub tls_ca: Option<Bytes>,
    /// Where the packaged player lives: `index.html` and the two `/web/` assets.
    pub web_dir: Option<PathBuf>,
    pub acquirer: Option<Arc<dyn Acquirer>>,
    pub mirror: Option<Arc<dyn Mirror>>,
    pub cast: Option<Arc<dyn CastBridge>>,
}

/// A running API server: its address, its event subscribers and its accept task.
pub struct ApiServer {
    addr: SocketAddr,
    sse: Arc<Mutex<Vec<mpsc::Sender<Bytes>>>>,
    shutdown: Option<oneshot::Sender<()>>,
    accept: tokio::task::JoinHandle<()>,
}

impl ApiServer {
    /// The URL the app hands to its UI and its log line.
    pub fn url(&self) -> String {
        format!("http://{}", self.addr)
    }

    pub fn addr(&self) -> SocketAddr {
        self.addr
    }

    /// `srv.notify(event)`: one `data: {"event":…}` frame to every open `/v1/events` client.
    pub fn notify(&self, event: &str) {
        broadcast(&self.sse, event);
    }

    pub async fn close(mut self) {
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }
        self.accept.abort();
        let _ = self.accept.await;
        let mut clients = self.sse.lock().unwrap();
        for client in clients.drain(..) {
            let _ = client.try_send(Bytes::new());
        }
    }
}

/// One cached stream resolution: the label, or the retry deadline for a file that gave none.
struct Resolution {
    at: Instant,
    label: Option<Option<String>>,
    shared: futures_util::future::Shared<BoxFuture<'static, Option<String>>>,
}

/// The API server and its request state.
pub struct Api<N: Node> {
    node: Mutex<Option<Arc<N>>>,
    options: Options,
    settings: Option<tokio::sync::Mutex<Settings>>,
    resolutions: Mutex<HashMap<String, Resolution>>,
    downloads: Mutex<HashMap<String, (String, String)>>,
    web_cache: Mutex<HashMap<String, Bytes>>,
    sse: Arc<Mutex<Vec<mpsc::Sender<Bytes>>>>,
}

impl<N: Node> Api<N> {
    /// `createApi(state)` with no node yet; [`Api::set_node`] attaches one when it opens.
    pub fn new(mut options: Options) -> Self {
        let settings = options.settings.take().map(tokio::sync::Mutex::new);
        Self {
            node: Mutex::new(None),
            options,
            settings,
            resolutions: Mutex::new(HashMap::new()),
            downloads: Mutex::new(HashMap::new()),
            web_cache: Mutex::new(HashMap::new()),
            sse: Arc::new(Mutex::new(Vec::new())),
        }
    }

    pub fn with_node(options: Options, node: Arc<N>) -> Self {
        let api = Self::new(options);
        api.set_node(node);
        api
    }

    /// The node once it is open; `None` answers the pre-start status and empty reads.
    pub fn set_node(&self, node: Arc<N>) {
        *self.node.lock().unwrap() = Some(node);
    }

    pub fn node(&self) -> Option<Arc<N>> {
        self.node.lock().unwrap().clone()
    }

    /// Binds `addr` and serves until [`ApiServer::close`].
    pub async fn bind(self, addr: SocketAddr) -> io::Result<ApiServer> {
        let listener = TcpListener::bind(addr).await?;
        let addr = listener.local_addr()?;
        let api = Arc::new(self);
        let sse = Arc::clone(&api.sse);
        let (shutdown, mut stop) = oneshot::channel::<()>();
        let accept = tokio::spawn(async move {
            let mut connections = JoinSet::new();
            loop {
                tokio::select! {
                    _ = &mut stop => break,
                    accepted = listener.accept() => {
                        let Ok((stream, remote)) = accepted else { continue };
                        let api = Arc::clone(&api);
                        connections.spawn(async move {
                            let service = service_fn(move |req| {
                                let api = Arc::clone(&api);
                                async move {
                                    Ok::<_, Infallible>(api.handle(req, remote).await)
                                }
                            });
                            let _ = hyper::server::conn::http1::Builder::new()
                                .serve_connection(TokioIo::new(stream), service)
                                .await;
                        });
                    }
                    Some(_) = connections.join_next(), if !connections.is_empty() => {}
                }
            }
            connections.abort_all();
            while connections.join_next().await.is_some() {}
        });
        Ok(ApiServer {
            addr,
            sse,
            shutdown: Some(shutdown),
            accept,
        })
    }

    fn token(&self) -> Option<&str> {
        self.options
            .api_token
            .as_deref()
            .filter(|token| !token.is_empty())
    }

    async fn handle(self: Arc<Self>, req: Request<Incoming>, remote: SocketAddr) -> Response<Body> {
        let (parts, body) = req.into_parts();
        let mut body = Some(body);
        let method = parts.method;
        let url = Url::parse(&format!("http://127.0.0.1{}", parts.uri))
            .unwrap_or_else(|_| Url::parse("http://127.0.0.1/").unwrap());
        let path = url.path().to_owned();
        if method == Method::OPTIONS {
            let mut response = Response::new(empty_body());
            *response.status_mut() = StatusCode::NO_CONTENT;
            cors(response.headers_mut());
            return response;
        }
        if method == Method::GET && path == "/" {
            let Some(page) = self.web_asset("index.html") else {
                return plain(StatusCode::NOT_FOUND, "not found");
            };
            let mut response = full(page);
            let headers = response.headers_mut();
            cors(headers);
            isolation(headers);
            headers.insert(header::CACHE_CONTROL, HeaderValue::from_static("no-cache"));
            headers.insert(
                header::CONTENT_TYPE,
                HeaderValue::from_static("text/html; charset=utf-8"),
            );
            return response;
        }
        if let Some(ca) = &self.options.tls_ca
            && matches!(method, Method::GET | Method::HEAD)
            && path == "/ca.crt"
        {
            let mut response = Response::new(empty_body());
            let headers = response.headers_mut();
            cors(headers);
            headers.insert(
                header::CONTENT_TYPE,
                HeaderValue::from_static("application/x-x509-ca-cert"),
            );
            headers.insert(
                header::CONTENT_DISPOSITION,
                HeaderValue::from_static("attachment; filename=\"peartube-ca.crt\""),
            );
            headers.insert(
                header::CONTENT_LENGTH,
                HeaderValue::from_str(&ca.len().to_string()).unwrap(),
            );
            if method == Method::GET {
                *response.body_mut() = Full::new(ca.clone()).map_err(|e| match e {}).boxed_unsync();
            }
            return response;
        }
        if matches!(method, Method::GET | Method::HEAD)
            && (path == "/web/peartube_web.wasm" || path == "/web/player-worker.js")
        {
            return self.web_route(
                &method,
                &path,
                parts.headers.get(header::ACCEPT_ENCODING),
                parts.headers.get(header::IF_NONE_MATCH),
            );
        }
        if matches!(method, Method::GET | Method::HEAD)
            && path == "/v1/blob"
            && self.options.blob.is_some()
        {
            return self
                .blob_route(&method, &url, parts.headers.get(header::RANGE))
                .await;
        }
        if method == Method::GET
            && (path == "/tv" || path == "/cast/receiver")
            && self.options.cast.is_some()
        {
            let mut response = full(Bytes::from(crate::cast::ui::receiver_page(
                "hub",
                "PearTube Smart TV",
            )));
            let headers = response.headers_mut();
            cors(headers);
            isolation(headers);
            headers.insert(header::CACHE_CONTROL, HeaderValue::from_static("no-cache"));
            headers.insert(
                header::CONTENT_TYPE,
                HeaderValue::from_static("text/html; charset=utf-8"),
            );
            return response;
        }
        if method == Method::GET
            && let Some(cast) = &self.options.cast
            && (path == "/v1/cast/web-events" || path == "/cast/v1/events")
        {
            return if path == "/v1/cast/web-events" {
                cast.web_events(&url)
            } else {
                cast.hub_sender_events(&url)
            };
        }
        if method == Method::GET && path == "/v1/events" {
            return self.events_route();
        }
        if method == Method::GET
            && (self.options.poster.is_some() || self.options.backdrop.is_some())
            && let Some(captures) = art_match(&path)
        {
            let lookup = if captures.0 == "poster" {
                &self.options.poster
            } else {
                &self.options.backdrop
            };
            let art = match lookup {
                Some(lookup) => lookup(captures.1.to_owned()).await,
                None => None,
            };
            let mut response = Response::new(empty_body());
            *response.status_mut() = if art.is_some() {
                StatusCode::FOUND
            } else {
                StatusCode::NOT_FOUND
            };
            let headers = response.headers_mut();
            cors(headers);
            if let Some(location) = art {
                headers.insert(
                    header::LOCATION,
                    HeaderValue::from_str(&location)
                        .unwrap_or_else(|_| HeaderValue::from_static("")),
                );
                headers.insert(
                    header::CACHE_CONTROL,
                    HeaderValue::from_static("max-age=86400"),
                );
            }
            return response;
        }
        if method == Method::GET && path == "/v1/playlist.m3u" {
            let entries = match self.node() {
                Some(node) => node
                    .search(None, SearchOptions::default())
                    .await
                    .unwrap_or_default(),
                None => Vec::new(),
            };
            let mut lines = vec!["#EXTM3U".to_owned()];
            for entry in &entries {
                let title: String = entry
                    .title
                    .chars()
                    .map(|c| {
                        if matches!(c, '\r' | '\n' | ',') {
                            ' '
                        } else {
                            c
                        }
                    })
                    .collect();
                let source = first_units(&entry.source, 8);
                lines.push(format!(
                    "#EXTINF:-1 tvg-id=\"{}\" group-title=\"{}:{}\",{} ({})",
                    entry.id, entry.source_kind, source, title, entry.id
                ));
                lines.push(entry.stream_url.clone());
            }
            let body = format!("{}\n", lines.join("\n"));
            let mut response = full(Bytes::from(body));
            cors(response.headers_mut());
            response.headers_mut().insert(
                header::CONTENT_TYPE,
                HeaderValue::from_static("audio/x-mpegurl; charset=utf-8"),
            );
            return response;
        }

        let mut request = RequestContext {
            method: &method,
            url: &url,
            headers: &parts.headers,
            remote,
            body: &mut body,
        };
        let payload = match self.route(&mut request).await {
            Ok(Some(Value::Null)) | Ok(None) => {
                (StatusCode::NOT_FOUND, json!({"error": "not found"}))
            }
            Ok(Some(payload)) => (StatusCode::OK, payload),
            Err(error) => (status(error.status()), json!({"error": error.message})),
        };
        json_response(payload.0, &payload.1)
    }

    /// One `/v1` request: `None` is the app's `route` miss (a 404 `{"error":"not found"}`).
    async fn route(&self, req: &mut RequestContext<'_>) -> Result<Option<Value>, HttpError> {
        let method = req.method;
        let url = req.url;
        let path = url.path();
        let node = self.node();
        if let Some(cast) = &self.options.cast {
            if *method == Method::POST && path == "/v1/cast/web-register" {
                let body = self.read_body(req.body).await?;
                return Ok(Some(cast.register_web_receiver(&body)));
            }
            if *method == Method::POST && path == "/v1/cast/web-state" {
                let body = self.read_body(req.body).await?;
                return Ok(Some(cast.update_web_receiver_state(&body)));
            }
        }
        // Writes, and the settings themselves, need the token; reads of public state do not.
        if let Some(token) = self.token()
            && (matches!(*method, Method::POST | Method::PUT | Method::DELETE)
                || path == "/v1/settings"
                || path.starts_with("/v1/identity"))
        {
            let expected = format!("Bearer {token}");
            if req
                .headers
                .get(header::AUTHORIZATION)
                .and_then(|v| v.to_str().ok())
                != Some(&expected)
            {
                return Err(HttpError::new(401, "Unauthorized"));
            }
        }
        // An open relay already trusts its local network, so its settings do too.
        let settings_open = self.token().is_some() || is_private(req.remote);
        if *method == Method::GET && path == "/v1/status" {
            let mut status = match &node {
                Some(node) => status_object(node.status().await),
                None => object(self.options.fallback_status.clone().unwrap_or(Value::Null)),
            };
            if let Some(extra) = &self.options.extra_status {
                for (key, value) in object(extra()) {
                    status.insert(key, value);
                }
            }
            status.insert(
                "tokenRequired".to_owned(),
                Value::Bool(self.token().is_some()),
            );
            status.insert("settingsOpen".to_owned(), Value::Bool(settings_open));
            return Ok(Some(Value::Object(status)));
        }
        if (path == "/v1/settings" || path == "/v1/restart")
            && let Some(settings) = self.settings.as_ref()
        {
            if !settings_open {
                return Err(HttpError::new(
                    403,
                    "Set PEARTUBE_API_TOKEN to change settings over the internet",
                ));
            }
            // Other websites can reach a LAN relay through the visitor's browser; settings only
            // from its own page.
            if !same_origin(req.headers) {
                return Err(HttpError::new(
                    403,
                    "Settings can only be changed from the relay’s own page",
                ));
            }
            if *method == Method::GET && path == "/v1/settings" {
                return Ok(Some(settings.lock().await.view()));
            }
            if *method == Method::PUT && path == "/v1/settings" {
                let patch = self.read_body(req.body).await?;
                let mut settings = settings.lock().await;
                let updated = settings
                    .update(&patch)
                    .map_err(|error| HttpError::new(error.status(), error.message()))?;
                let changed: Vec<String> = updated
                    .get("changed")
                    .and_then(Value::as_array)
                    .map(|keys| {
                        keys.iter()
                            .filter_map(|key| key.as_str().map(str::to_owned))
                            .collect()
                    })
                    .unwrap_or_default();
                let mut warning = None;
                if !changed.is_empty()
                    && let Some(apply) = &self.options.apply_settings
                    && let Err(message) = apply(changed.clone()).await
                {
                    warning = Some(message);
                }
                drop(settings);
                let mut out = object(updated);
                if let Some(warning) = warning {
                    out.insert("warning".to_owned(), Value::String(warning));
                }
                return Ok(Some(Value::Object(out)));
            }
            if *method == Method::POST
                && path == "/v1/restart"
                && let Some(restart) = &self.options.restart
            {
                restart().await;
                return Ok(Some(settings.lock().await.view()));
            }
        }
        if path == "/v1/identity" || path.starts_with("/v1/identity/") {
            if !settings_open {
                return Err(HttpError::new(
                    403,
                    "Set PEARTUBE_API_TOKEN to manage channel identity over the internet",
                ));
            }
            if !same_origin(req.headers) {
                return Err(HttpError::new(
                    403,
                    "Channel identity can only be managed from the relay’s own page",
                ));
            }
            let node = node
                .clone()
                .ok_or_else(|| HttpError::new(503, "Node still starting"))?;
            if *method == Method::GET && path == "/v1/identity" {
                let reveal = query(url, "reveal").as_deref() == Some("1");
                return Ok(Some(value(
                    node.identity(reveal).await.map_err(node_error)?,
                )?));
            }
            if *method == Method::POST && path == "/v1/identity/writers" {
                let body = self.read_body(req.body).await?;
                let writer = text(&body, "writer");
                let role = text_or(&body, "role", "writer");
                let alias = text(&body, "alias");
                let granted = value(
                    node.grant_writer(&writer, &role, &alias)
                        .await
                        .map_err(node_error)?,
                )?;
                broadcast(&self.sse, "update");
                let identity = value(node.identity(false).await.map_err(node_error)?)?;
                return Ok(Some(merge(granted, identity)));
            }
            if *method == Method::DELETE
                && let Some(writer) = hex_tail(path, "/v1/identity/writers/", 64)
            {
                let purge = query(url, "purge").as_deref() == Some("1");
                let revoked = value(
                    node.revoke_writer(&writer, purge)
                        .await
                        .map_err(node_error)?,
                )?;
                broadcast(&self.sse, "update");
                let identity = value(node.identity(false).await.map_err(node_error)?)?;
                return Ok(Some(merge(revoked, identity)));
            }
            if *method == Method::POST && path == "/v1/identity/epoch" {
                let epoch = value(node.rotate_epoch().await.map_err(node_error)?)?;
                broadcast(&self.sse, "update");
                let identity = value(node.identity(false).await.map_err(node_error)?)?;
                return Ok(Some(merge(epoch, identity)));
            }
            if *method == Method::POST && path == "/v1/identity/recover" {
                let body = self.read_body(req.body).await?;
                let mnemonic = text(&body, "mnemonic");
                let alias = text(&body, "alias");
                if let Some(recover) = &self.options.recover_mnemonic {
                    return recover(mnemonic, alias).await.map(Some);
                }
                let claimed = value(
                    node.claim_with_mnemonic(&mnemonic, &alias)
                        .await
                        .map_err(node_error)?,
                )?;
                broadcast(&self.sse, "update");
                let identity = value(node.identity(false).await.map_err(node_error)?)?;
                return Ok(Some(merge(claimed, identity)));
            }
        }
        if *method == Method::POST
            && path == "/v1/identify"
            && let Some(identify) = &self.options.identify
        {
            let body = self.read_body(req.body).await?;
            let link = body
                .get("url")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .trim()
                .to_owned();
            if !is_http_link(&link) {
                return Err(HttpError::new(400, "Paste an http(s) link"));
            }
            return identify(link).await.map(Some);
        }
        if *method == Method::GET && path == "/v1/search" {
            let Some(id) = query(url, "id").filter(|id| !id.is_empty()) else {
                return Err(HttpError::new(400, "id is required"));
            };
            let options = SearchOptions {
                source: Some(query(url, "source").filter(|value| !value.is_empty())),
                query: None,
                limit: Some(js_number(query(url, "limit").as_deref())),
                offset: Some(js_number(query(url, "offset").as_deref())),
            };
            let results = match &node {
                Some(node) => value(node.search(Some(&id), options).await.map_err(node_error)?)?,
                None => Value::Array(Vec::new()),
            };
            return Ok(Some(json!({ "results": results })));
        }
        if *method == Method::GET && path == "/v1/entries" {
            let options = SearchOptions {
                source: Some(query(url, "source").filter(|value| !value.is_empty())),
                query: Some(query(url, "q").filter(|value| !value.is_empty())),
                limit: Some(js_number(query(url, "limit").as_deref())),
                offset: Some(js_number(query(url, "offset").as_deref())),
            };
            let results = match &node {
                Some(node) => value(node.search(None, options).await.map_err(node_error)?)?,
                None => Value::Array(Vec::new()),
            };
            return Ok(Some(json!({ "results": results })));
        }
        if *method == Method::POST && path == "/v1/entries/remove" {
            let node = node
                .clone()
                .ok_or_else(|| HttpError::new(503, "Node still starting"))?;
            let body = self.read_body(req.body).await?;
            let key = body
                .get("key")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .trim()
                .to_owned();
            if key.is_empty() {
                return Err(HttpError::new(400, "key is required"));
            }
            node.remove(&key).await.map_err(node_error)?;
            broadcast(&self.sse, "update");
            return Ok(Some(json!({ "removed": true, "key": key })));
        }
        if *method == Method::GET && path == "/v1/sources" {
            let Some(node) = &node else {
                let tracker = self
                    .options
                    .fallback_status
                    .as_ref()
                    .and_then(|status| status.get("tracker"))
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_owned();
                return Ok(Some(json!({
                    "tracker": tracker,
                    "channel": "",
                    "sources": [],
                    "discovered": [],
                    "published": [],
                })));
            };
            let status = node.status().await;
            let sources = value(node.list_sources().await)?;
            let discovered = value(node.discovered_sources().await.map_err(node_error)?)?;
            let published = value(node.published_sources().await.map_err(node_error)?)?;
            return Ok(Some(json!({
                "tracker": status.tracker,
                "channel": status.channel,
                "sources": sources,
                "discovered": discovered,
                "published": published,
            })));
        }
        if *method == Method::POST && path == "/v1/sources" {
            let node = node
                .clone()
                .ok_or_else(|| HttpError::new(503, "Node still starting"))?;
            let payload = self.read_body(req.body).await?;
            let spec: SourceSpec = serde_json::from_value(payload)
                .map_err(|error| HttpError::new(400, error.to_string()))?;
            let source = value(node.add_source(spec, false).await.map_err(node_error)?)?;
            self.sources_changed(&node).await;
            let sources = value(node.list_sources().await)?;
            return Ok(Some(json!({ "source": source, "sources": sources })));
        }
        if *method == Method::DELETE
            && let Some(key) = hex_tail(path, "/v1/sources/", 64)
        {
            let node = node
                .clone()
                .ok_or_else(|| HttpError::new(503, "Node still starting"))?;
            let removed = value(node.remove_source(&key).await.map_err(node_error)?)?;
            self.sources_changed(&node).await;
            let mut out = object(removed);
            out.insert("sources".to_owned(), value(node.list_sources().await)?);
            return Ok(Some(Value::Object(out)));
        }
        if *method == Method::POST && path == "/v1/channel/publish" {
            let node = node
                .clone()
                .ok_or_else(|| HttpError::new(503, "Node still starting"))?;
            let payload = self.read_body(req.body).await?;
            let source_key = payload.get("sourceKey");
            let is_source = payload.get("type").and_then(Value::as_str) == Some("source")
                || source_key.is_some_and(js_truthy);
            if is_source {
                let key = source_key
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_owned();
                let title = clean_text(
                    payload.get("title").and_then(Value::as_str),
                    &first_units(&key, 12),
                );
                return node
                    .publish_source(&key, &title)
                    .await
                    .map(Some)
                    .map_err(node_error);
            }
            let published = node
                .publish_channel(payload, None)
                .await
                .map_err(node_error)?;
            return Ok(Some(value(published)?));
        }
        if *method == Method::DELETE
            && let Some(key) = hex_tail(path, "/v1/channel/sources/", 64)
        {
            let node = node
                .clone()
                .ok_or_else(|| HttpError::new(503, "Node still starting"))?;
            return node
                .unpublish_source(&key)
                .await
                .map(Some)
                .map_err(node_error);
        }
        if (*method == Method::POST && path == "/v1/open")
            || (*method == Method::GET && path == "/v1/resolve")
        {
            let uri = if *method == Method::GET {
                query(url, "uri").unwrap_or_default()
            } else {
                self.read_body(req.body)
                    .await?
                    .get("uri")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_owned()
            };
            if let Some(cast) = &self.options.cast {
                // The app evaluates `parseUri(uri).action` only when cast is up, so a broken link
                // fails here rather than reaching the node.
                match parse_uri(&uri) {
                    Ok(crate::protocol::ParsedUri::Receiver { .. }) => {
                        let receiver = cast.pair_receiver(&json!({ "uri": uri })).await?;
                        return Ok(Some(json!({ "action": "receiver", "receiver": receiver })));
                    }
                    Ok(_) => {}
                    Err(error) => return Err(HttpError::new(400, error.to_string())),
                }
            }
            let node = node
                .clone()
                .ok_or_else(|| HttpError::new(503, "Node still starting"))?;
            let opened = node.open_uri(&uri, true).await.map_err(node_error)?;
            let action = match &opened {
                crate::node::api::OpenUri::Source { .. } => "source",
                crate::node::api::OpenUri::Access { .. } => "access",
                crate::node::api::OpenUri::Watch { .. } => "watch",
            };
            if action != "watch" {
                self.sources_changed(&node).await;
            }
            return Ok(Some(value(opened)?));
        }
        if *method == Method::POST && path == "/v1/cache/clear" {
            let node = node
                .clone()
                .ok_or_else(|| HttpError::new(503, "Node still starting"))?;
            // A body the app cannot parse reads as `{}`: clearing is the common case.
            let body = self.read_body(req.body).await.unwrap_or(Value::Null);
            let key = body.get("key").and_then(Value::as_str);
            let cleared = node.clear_cache(key, None).await.map_err(node_error)?;
            return Ok(Some(value(cleared)?));
        }
        if *method == Method::POST && path == "/v1/acquire" {
            let acquirer = self
                .options
                .acquirer
                .as_ref()
                .ok_or_else(|| HttpError::new(404, "Acquire unavailable on read-only peer"))?;
            let body = self.read_body(req.body).await?;
            let request = json!({
                "id": body.get("id").cloned().unwrap_or(Value::Null),
                "title": body.get("title").cloned().unwrap_or(Value::Null),
                "source": body.get("source").cloned().unwrap_or(Value::Null),
                "channel": body.get("channel").cloned().unwrap_or(Value::Null),
                "private": body.get("private").cloned().unwrap_or(Value::Bool(false)),
            });
            return acquirer.add(request).await.map(Some);
        }
        if *method == Method::GET && path == "/v1/jobs" {
            let jobs = match &self.options.acquirer {
                Some(acquirer) => acquirer.list(),
                None => self
                    .options
                    .fallback_jobs
                    .as_ref()
                    .map(|jobs| jobs())
                    .unwrap_or_else(|| Value::Array(Vec::new())),
            };
            return Ok(Some(json!({ "jobs": jobs })));
        }
        if let Some(id) = job_id(path)
            && matches!(*method, Method::GET | Method::DELETE)
        {
            let Some(acquirer) = &self.options.acquirer else {
                return Ok(None);
            };
            let job = if *method == Method::GET {
                acquirer.get(&id)
            } else {
                acquirer.cancel(&id)
            };
            return Ok(Some(job));
        }
        if *method == Method::GET && path == "/v1/plugins" {
            let plugins = self
                .options
                .mirror
                .as_ref()
                .map(|mirror| mirror.list())
                .unwrap_or_else(|| Value::Array(Vec::new()));
            return Ok(Some(json!({ "plugins": plugins })));
        }
        if *method == Method::POST
            && let Some(name) = plugin_name(path)
        {
            let mirror =
                self.options.mirror.as_ref().ok_or_else(|| {
                    HttpError::new(404, "Mirror host unavailable on read-only peer")
                })?;
            let payload = self.read_body(req.body).await?;
            return mirror.run(&percent_decode(&name), payload).await.map(Some);
        }
        if *method == Method::POST && path == "/v1/mirror" {
            let mirror =
                self.options.mirror.as_ref().ok_or_else(|| {
                    HttpError::new(404, "Mirror host unavailable on read-only peer")
                })?;
            let mut payload = object(self.read_body(req.body).await?);
            let plugin = payload
                .remove("plugin")
                .and_then(|plugin| plugin.as_str().map(str::to_owned))
                .unwrap_or_else(|| "http-acquire".to_owned());
            return mirror.run(&plugin, Value::Object(payload)).await.map(Some);
        }

        if let Some(cast) = &self.options.cast {
            if path.starts_with("/cast/v1/") {
                let body = if matches!(*method, Method::POST | Method::PUT) {
                    self.read_body(req.body).await.unwrap_or(Value::Null)
                } else {
                    Value::Null
                };
                return match cast.hub_json(method, url, body).await {
                    Some(result) => result.map(Some),
                    None => Ok(None),
                };
            }
            if path.starts_with("/v1/cast/") {
                if *method == Method::GET && path == "/v1/cast/status" {
                    let status = cast.status();
                    let qr = status
                        .get("webReceiverUrl")
                        .and_then(Value::as_str)
                        .map(|url| cast.web_setup_qr_svg(url))
                        .unwrap_or_default();
                    let mut out = object(status);
                    out.insert("webReceiverQrSvg".to_owned(), Value::String(qr));
                    return Ok(Some(Value::Object(out)));
                }
                if *method == Method::POST && path == "/v1/cast/pair" {
                    return cast
                        .pair_receiver(&self.read_body(req.body).await?)
                        .await
                        .map(Some);
                }
                if *method == Method::DELETE
                    && let Some(id) = rest(path, "/v1/cast/receivers/")
                {
                    return Ok(Some(cast.remove_receiver(&percent_decode(&id))));
                }
                if *method == Method::POST && path == "/v1/cast/play" {
                    return cast
                        .cast_play(&self.read_body(req.body).await?)
                        .await
                        .map(Some);
                }
                if *method == Method::POST && path == "/v1/cast/control" {
                    return cast
                        .cast_control(&self.read_body(req.body).await?)
                        .await
                        .map(Some);
                }
                if *method == Method::POST && path == "/v1/cast/stop" {
                    return cast
                        .cast_stop(&self.read_body(req.body).await?)
                        .await
                        .map(Some);
                }
            }
        }
        if *method == Method::GET && path == "/manifest.json" {
            return Ok(Some(json!({
                "id": "org.peartube.p2p",
                "version": "1.0.0",
                "name": "PearTube P2P",
                "description": "Decentralized Holepunch P2P channels & trackers",
                "resources": ["catalog", "stream"],
                "types": ["movie", "series"],
                "idPrefixes": ["tt", "imdb:", "clip:"],
                "catalogs": [
                    { "type": "movie", "id": "peartube-movies", "name": "PearTube Movies" },
                    { "type": "series", "id": "peartube-series", "name": "PearTube Series" }
                ]
            })));
        }
        if *method == Method::GET
            && let Some(kind) = catalog_kind(path)
        {
            let entries = match &node {
                Some(node) => node
                    .search(None, SearchOptions::default())
                    .await
                    .unwrap_or_default(),
                None => Vec::new(),
            };
            let mut seen: Vec<(String, Value)> = Vec::new();
            for entry in entries {
                let Some(meta) = stremio_meta(&entry) else {
                    continue;
                };
                if meta.get("type").and_then(Value::as_str) != Some(kind) {
                    continue;
                }
                let id = meta
                    .get("id")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_owned();
                if !seen.iter().any(|(seen, _)| *seen == id) {
                    seen.push((id, meta));
                }
            }
            let host = req.headers.get(header::HOST).and_then(|v| v.to_str().ok());
            let mut metas = Vec::with_capacity(seen.len());
            for (_, mut meta) in seen {
                if self.options.poster.is_some()
                    && let Some(host) = host
                    && let Value::Object(map) = &mut meta
                    && let Some(id) = map.get("id").and_then(Value::as_str).map(str::to_owned)
                {
                    map.insert(
                        "poster".to_owned(),
                        Value::String(format!("{}://{host}/v1/poster/{id}", proto(req))),
                    );
                }
                metas.push(meta);
            }
            return Ok(Some(json!({ "metas": metas })));
        }
        if *method == Method::GET
            && let Some(raw) = stream_id(path)
        {
            let id = stremio_to_pear_id(&raw);
            let entries = match &node {
                Some(node) => node
                    .search(Some(&id), SearchOptions::default())
                    .await
                    .unwrap_or_default(),
                None => Vec::new(),
            };
            let labels = futures_util::future::join_all(
                entries.iter().map(|entry| self.resolution_of(entry)),
            )
            .await;
            let origin = match (
                req.headers.get(header::HOST).and_then(|v| v.to_str().ok()),
                &self.options.blob,
            ) {
                (Some(host), Some(_)) => Some(format!("{}://{host}/v1/blob", proto(req))),
                _ => None,
            };
            let mut streams = Vec::with_capacity(entries.len());
            for (index, entry) in entries.iter().enumerate() {
                let label = match entry.id.rsplit_once(':') {
                    Some((_, suffix))
                        if is_episode_suffix(suffix) && !has_episode_tag(&entry.title) =>
                    {
                        format!("{} {}", entry.title, suffix.to_uppercase())
                    }
                    _ => entry.title.clone(),
                };
                let name = if entry.local {
                    "PearTube (Local)".to_owned()
                } else {
                    format!("PearTube ({})", entry.source_kind)
                };
                let name = match labels.get(index).and_then(Option::as_ref) {
                    Some(resolution) => format!("{name}\n{resolution}"),
                    None => name,
                };
                let source = first_units(&entry.source, 8);
                let url = match (&origin, Url::parse(&entry.stream_url)) {
                    (Some(origin), Ok(stream)) => format!(
                        "{origin}{}",
                        stream.query().map(|q| format!("?{q}")).unwrap_or_default()
                    ),
                    _ => entry.stream_url.clone(),
                };
                streams.push(json!({
                    "name": name,
                    "title": format!("{label}\n{:.1} MB · {source}…", entry.size as f64 / 1_048_576.0),
                    "url": url,
                    "behaviorHints": {
                        "bingeGroup": format!("peartube-{}", first_units(&entry.source, 12)),
                        "notWebReady": true,
                        "videoSize": entry.size,
                    }
                }));
            }
            return Ok(Some(json!({ "streams": streams })));
        }
        Ok(None)
    }

    /// The bytes of one request body, as the app parses it.
    async fn read_body(&self, body: &mut Option<Incoming>) -> Result<Value, HttpError> {
        let Some(mut incoming) = body.take() else {
            return Ok(Value::Object(Map::new()));
        };
        let mut data = Vec::new();
        while let Some(frame) = incoming.frame().await {
            let frame = frame.map_err(|error| HttpError::new(400, error.to_string()))?;
            let Some(chunk) = frame.data_ref() else {
                continue;
            };
            if data.len() + chunk.len() > MAX_BODY {
                return Err(HttpError::new(413, "Body too large"));
            }
            data.extend_from_slice(chunk);
        }
        if data.is_empty() {
            return Ok(Value::Object(Map::new()));
        }
        serde_json::from_slice(&data).map_err(|_| HttpError::new(400, "Body must be JSON"))
    }

    async fn sources_changed(&self, node: &N) {
        if let Some(changed) = &self.options.on_sources_changed {
            changed(node.list_sources().await);
        }
    }

    /// `resolutionOf`: the stream's resolution label, read from the blob server and cached.
    async fn resolution_of(&self, entry: &Entry) -> Option<String> {
        let blob = self.options.blob.as_ref()?;
        // The app answers `null` without a bound port, before asking the blob server anything.
        if blob.port == 0 {
            return None;
        }
        let search = Url::parse(&entry.stream_url).ok()?.query()?.to_owned();
        let shared = {
            let mut resolutions = self.resolutions.lock().unwrap();
            match resolutions.get(&search) {
                Some(known)
                    if known.label != Some(None) || known.at.elapsed() < RESOLUTION_RETRY =>
                {
                    known.shared.clone()
                }
                _ => {
                    let (host, port) = (blob.host().to_owned(), blob.port);
                    let probe = search.clone();
                    let shared: futures_util::future::Shared<BoxFuture<'static, Option<String>>> =
                        Box::pin(async move {
                            let read = |start: u64, len: usize| {
                                let host = host.clone();
                                let probe = probe.clone();
                                async move {
                                    read_stream(&host, port, &probe, start, len as u64)
                                        .await
                                        .map_err(|error| {
                                            io::Error::other(error.message().to_owned())
                                        })
                                }
                            };
                            let size = video_size(read).await.ok().flatten();
                            size.map(resolution_label)
                        })
                        .boxed()
                        .shared();
                    resolutions.insert(
                        search.clone(),
                        Resolution {
                            at: Instant::now(),
                            label: None,
                            shared: shared.clone(),
                        },
                    );
                    shared
                }
            }
        };
        let label = shared.await;
        if let Some(known) = self.resolutions.lock().unwrap().get_mut(&search) {
            known.label = Some(label.clone());
            known.at = Instant::now();
        }
        label
    }

    /// `/v1/blob`: the same-origin path to the node's blob server, so browsers and clients that
    /// only admit private addresses for their configured add-on can still play.
    async fn blob_route(
        &self,
        method: &Method,
        url: &Url,
        range: Option<&HeaderValue>,
    ) -> Response<Body> {
        let blob = self.options.blob.as_ref().unwrap();
        let download = query(url, "download");
        let up = UriQuery::without(url, "download").unwrap_or_default();
        let mut meta = None;
        if let Some(title) = &download {
            let cached = self.downloads.lock().unwrap().get(&up).cloned();
            let format = match cached {
                Some(format) => format,
                None => {
                    let head = read_stream(blob.host(), blob.port, &up, 0, 255).await.ok();
                    let format = sniff_download_format(head.as_deref());
                    if head.as_ref().is_some_and(|head| head.len() >= 4) {
                        let mut formats = self.downloads.lock().unwrap();
                        if formats.len() >= DOWNLOAD_CACHE_MAX {
                            formats.clear();
                        }
                        formats.insert(up.clone(), format.clone());
                    }
                    format
                }
            };
            meta = Some((format.1, content_disposition(title, &format.0)));
        }
        let target = if up.is_empty() {
            "/".to_owned()
        } else {
            format!("/?{up}")
        };
        let mut request = Request::builder().method(method.clone()).uri(format!(
            "http://{}:{}{target}",
            blob.host(),
            blob.port
        ));
        if let Some(range) = range {
            request = request.header(header::RANGE, range.clone());
        }
        let Ok(request) = request.body(Full::new(Bytes::new())) else {
            return error_response(StatusCode::BAD_GATEWAY);
        };
        let upstream = match CLIENT.request(request).await {
            Ok(upstream) => upstream,
            Err(_) => return error_response(StatusCode::BAD_GATEWAY),
        };
        let status = upstream.status();
        let copied: Vec<(hyper::header::HeaderName, HeaderValue)> = [
            "content-type",
            "content-length",
            "content-range",
            "accept-ranges",
        ]
        .iter()
        .filter_map(|name| {
            let name = hyper::header::HeaderName::from_bytes(name.as_bytes()).ok()?;
            let value = upstream.headers().get(&name)?.clone();
            Some((name, value))
        })
        .collect();
        let mut response = Response::new(
            upstream
                .into_body()
                .map_err(io::Error::other)
                .boxed_unsync(),
        );
        *response.status_mut() = status;
        let headers = response.headers_mut();
        cors(headers);
        for (name, value) in copied {
            headers.insert(name, value);
        }
        if let Some((mime, disposition)) = meta {
            headers.insert(
                header::CONTENT_TYPE,
                HeaderValue::from_str(&mime)
                    .unwrap_or_else(|_| HeaderValue::from_static("application/octet-stream")),
            );
            headers.insert(
                header::CONTENT_DISPOSITION,
                HeaderValue::from_str(&disposition)
                    .unwrap_or_else(|_| HeaderValue::from_static("attachment")),
            );
        }
        response
    }

    /// `/web/peartube_web.wasm` and `/web/player-worker.js`, with gzip and an ETag.
    fn web_route(
        &self,
        method: &Method,
        path: &str,
        accept_encoding: Option<&HeaderValue>,
        if_none_match: Option<&HeaderValue>,
    ) -> Response<Body> {
        let name = &path[5..];
        let gzip_wanted = name.ends_with(".wasm")
            && accept_encoding
                .and_then(|value| value.to_str().ok())
                .is_some_and(|value| value.contains("gzip"));
        let gz = gzip_wanted
            .then(|| self.web_asset(&format!("{name}.gz")))
            .flatten();
        let asset = gz.clone().or_else(|| self.web_asset(name));
        let Some(asset) = asset else {
            return plain(StatusCode::NOT_FOUND, "not found");
        };
        let etag = format!(
            "\"{}-{}-{}{}\"",
            asset.len(),
            asset
                .first()
                .map_or("undefined".to_owned(), |b| b.to_string()),
            asset
                .last()
                .map_or("undefined".to_owned(), |b| b.to_string()),
            if gz.is_some() { "-gz" } else { "" },
        );
        let mut response = Response::new(if *method == Method::HEAD {
            empty_body()
        } else {
            Full::new(asset.clone())
                .map_err(|e| match e {})
                .boxed_unsync()
        });
        cors(response.headers_mut());
        isolation(response.headers_mut());
        response.headers_mut().insert(
            header::ETAG,
            HeaderValue::from_str(&etag).unwrap_or_else(|_| HeaderValue::from_static("\"\"")),
        );
        response
            .headers_mut()
            .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-cache"));
        if if_none_match.and_then(|value| value.to_str().ok()) == Some(etag.as_str()) {
            *response.status_mut() = StatusCode::NOT_MODIFIED;
            return response;
        }
        let headers = response.headers_mut();
        headers.insert(header::VARY, HeaderValue::from_static("accept-encoding"));
        headers.insert(
            header::CONTENT_TYPE,
            HeaderValue::from_static(if name.ends_with(".wasm") {
                "application/wasm"
            } else {
                "text/javascript; charset=utf-8"
            }),
        );
        headers.insert(
            header::CONTENT_LENGTH,
            HeaderValue::from_str(&asset.len().to_string()).unwrap(),
        );
        if gz.is_some() {
            headers.insert(header::CONTENT_ENCODING, HeaderValue::from_static("gzip"));
        }
        response
    }

    fn web_asset(&self, name: &str) -> Option<Bytes> {
        if let Some(cached) = self.web_cache.lock().unwrap().get(name) {
            return Some(cached.clone());
        }
        let dir = self.options.web_dir.as_ref()?;
        let bytes = Bytes::from(std::fs::read(dir.join(name)).ok()?);
        self.web_cache
            .lock()
            .unwrap()
            .insert(name.to_owned(), bytes.clone());
        Some(bytes)
    }

    /// `/v1/events`: `srv.notify(event)` frames, one subscriber per response.
    fn events_route(&self) -> Response<Body> {
        let (sender, receiver) = mpsc::channel::<Bytes>(16);
        let _ = sender.try_send(Bytes::from_static(b": connected\n\n"));
        self.sse.lock().unwrap().push(sender);
        let stream = futures_util::stream::unfold(receiver, |mut receiver| async move {
            receiver
                .recv()
                .await
                .map(|bytes| (Ok::<_, io::Error>(Frame::data(bytes)), receiver))
        });
        let mut response = Response::new(StreamBody::new(stream).boxed_unsync());
        let headers = response.headers_mut();
        cors(headers);
        headers.insert(
            header::CONTENT_TYPE,
            HeaderValue::from_static("text/event-stream"),
        );
        headers.insert(header::CACHE_CONTROL, HeaderValue::from_static("no-cache"));
        headers.insert(header::CONNECTION, HeaderValue::from_static("keep-alive"));
        response
    }
}

/// The parts of one request `route` reads.
struct RequestContext<'a> {
    method: &'a Method,
    url: &'a Url,
    headers: &'a HeaderMap,
    remote: SocketAddr,
    body: &'a mut Option<Incoming>,
}

fn broadcast(sse: &Mutex<Vec<mpsc::Sender<Bytes>>>, event: &str) {
    let frame = Bytes::from(format!("data: {}\n\n", json!({ "event": event })));
    let mut clients = sse.lock().unwrap();
    clients.retain(|sender| sender.try_send(frame.clone()).is_ok());
}

/// The app's `CORS`: every route answers preflights and exposes the Range headers a player reads.
fn cors(headers: &mut HeaderMap) {
    for (name, value) in [
        (
            header::HeaderName::from_static("access-control-allow-origin"),
            "*",
        ),
        (
            header::HeaderName::from_static("access-control-allow-methods"),
            "GET, HEAD, POST, PUT, DELETE, OPTIONS",
        ),
        (
            header::HeaderName::from_static("access-control-allow-headers"),
            "content-type, authorization, range",
        ),
        (
            header::HeaderName::from_static("access-control-expose-headers"),
            "content-length, content-range, accept-ranges, content-disposition",
        ),
        (
            header::HeaderName::from_static("cross-origin-resource-policy"),
            "cross-origin",
        ),
    ] {
        headers.insert(name, HeaderValue::from_static(value));
    }
}

/// The app's `ISOLATION`, for the pages and assets it serves itself.
fn isolation(headers: &mut HeaderMap) {
    headers.insert(
        header::HeaderName::from_static("cross-origin-opener-policy"),
        HeaderValue::from_static("same-origin"),
    );
    headers.insert(
        header::HeaderName::from_static("cross-origin-embedder-policy"),
        HeaderValue::from_static("credentialless"),
    );
}

fn empty_body() -> Body {
    Empty::new().map_err(|e| match e {}).boxed_unsync()
}

fn full(bytes: Bytes) -> Response<Body> {
    Response::new(Full::new(bytes).map_err(|e| match e {}).boxed_unsync())
}

fn json_response(status: StatusCode, value: &Value) -> Response<Body> {
    let text = serde_json::to_string(value).unwrap_or_else(|_| "{}".to_owned());
    let mut response = full(Bytes::from(text));
    *response.status_mut() = status;
    cors(response.headers_mut());
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/json"),
    );
    response
}

/// `res.writeHead(status, CORS); res.end(text)`: no content type, as the app writes it.
fn plain(status: StatusCode, body: &str) -> Response<Body> {
    let mut response = full(Bytes::from(body.to_owned()));
    *response.status_mut() = status;
    cors(response.headers_mut());
    response
}

fn error_response(status: StatusCode) -> Response<Body> {
    let mut response = Response::new(empty_body());
    *response.status_mut() = status;
    cors(response.headers_mut());
    response
}

fn status(code: u16) -> StatusCode {
    StatusCode::from_u16(code).unwrap_or(StatusCode::BAD_REQUEST)
}

fn object(value: Value) -> Map<String, Value> {
    match value {
        Value::Object(map) => map,
        _ => Map::new(),
    }
}

/// `{...a, ...b}`: `b`'s keys overwrite `a`'s, keeping the position the first one had.
fn merge(a: Value, b: Value) -> Value {
    let mut map = object(a);
    for (key, value) in object(b) {
        map.insert(key, value);
    }
    Value::Object(map)
}

fn value<T: serde::Serialize>(value: T) -> Result<Value, HttpError> {
    serde_json::to_value(value).map_err(|error| HttpError::new(400, error.to_string()))
}

fn node_error<E: std::fmt::Display>(error: E) -> HttpError {
    HttpError::new(400, error.to_string())
}

/// A node status, with `Number` fields printed as JavaScript prints them.
fn status_object(status: Status) -> Map<String, Value> {
    object(serde_json::to_value(status).unwrap_or(Value::Null))
}

fn text(body: &Value, key: &str) -> String {
    body.get(key)
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_owned()
}

fn text_or(body: &Value, key: &str, fallback: &str) -> String {
    body.get(key)
        .and_then(Value::as_str)
        .unwrap_or(fallback)
        .to_owned()
}

fn js_truthy(value: &Value) -> bool {
    match value {
        Value::Null => false,
        Value::Bool(flag) => *flag,
        Value::Number(number) => number.as_f64().is_some_and(|n| n != 0.0 && !n.is_nan()),
        Value::String(text) => !text.is_empty(),
        Value::Array(_) | Value::Object(_) => true,
    }
}

fn is_http_link(link: &str) -> bool {
    let lower = link.to_lowercase();
    lower.starts_with("http://") || lower.starts_with("https://")
}

/// `Number(raw) || 0`: NaN, empty and missing read as zero; integers keep their integer form.
fn js_number(raw: Option<&str>) -> Number {
    let parsed = raw
        .map(str::trim)
        .and_then(|raw| raw.parse::<f64>().ok())
        .filter(|value| value.is_finite())
        .unwrap_or(0.0);
    if parsed.fract() == 0.0 && parsed.abs() <= 9_007_199_254_740_991.0 {
        Number::from(parsed as i64)
    } else {
        Number::from_f64(parsed).unwrap_or_else(|| Number::from(0))
    }
}

fn query(url: &Url, name: &str) -> Option<String> {
    url.query_pairs()
        .find(|(key, _)| key == name)
        .map(|(_, value)| value.into_owned())
}

/// `PRIVATE_ADDR`: loopback, RFC 1918, CGNAT/Tailscale, link-local and IPv6 ULA/link-local.
fn is_private(remote: SocketAddr) -> bool {
    fn v4(ip: std::net::Ipv4Addr) -> bool {
        let [a, b, ..] = ip.octets();
        ip.is_loopback()
            || a == 10
            || (a == 192 && b == 168)
            || (a == 172 && (16..=31).contains(&b))
            || (a == 100 && (64..=127).contains(&b))
            || (a == 169 && b == 254)
    }
    match remote.ip() {
        IpAddr::V4(ip) => v4(ip),
        IpAddr::V6(ip) => {
            if let Some(mapped) = ip.to_ipv4_mapped() {
                return v4(mapped);
            }
            ip.is_loopback()
                || (ip.segments()[0] & 0xffc0) == 0xfe80
                || (ip.segments()[0] & 0xfe00) == 0xfc00
        }
    }
}

/// `reqProto`: `https` behind a local TLS terminator, `http` otherwise.
fn proto(req: &RequestContext<'_>) -> &'static str {
    let forwarded = req
        .headers
        .get("x-forwarded-proto")
        .and_then(|value| value.to_str().ok());
    if is_private(req.remote) && forwarded == Some("https") {
        "https"
    } else {
        "http"
    }
}

/// Whether an `Origin` names this relay; a browser on another site is refused settings writes.
fn same_origin(headers: &HeaderMap) -> bool {
    let Some(origin) = headers
        .get(header::ORIGIN)
        .and_then(|value| value.to_str().ok())
    else {
        return true;
    };
    let host = headers
        .get(header::HOST)
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default();
    let Ok(url) = Url::parse(origin) else {
        return false;
    };
    let Some(name) = url.host_str() else {
        return false;
    };
    let name = if name.contains(':') {
        format!("[{name}]")
    } else {
        name.to_owned()
    };
    let own = match url.port() {
        Some(port) => format!("{name}:{port}"),
        None => name,
    };
    own == host
}

/// The first `len` UTF-16 units, as the app's `slice` counts.
fn first_units(text: &str, len: usize) -> String {
    let units: Vec<u16> = text.encode_utf16().take(len).collect();
    String::from_utf16_lossy(&units)
}

/// `path` without `prefix` and with exactly `len` hexadecimal units, case preserved.
fn hex_tail(path: &str, prefix: &str, len: usize) -> Option<String> {
    let tail = path.strip_prefix(prefix)?;
    (tail.len() == len && tail.bytes().all(|byte| byte.is_ascii_hexdigit()))
        .then(|| tail.to_owned())
}

fn rest(path: &str, prefix: &str) -> Option<String> {
    let tail = path.strip_prefix(prefix)?;
    (!tail.is_empty() && !tail.contains('/')).then(|| tail.to_owned())
}

fn job_id(path: &str) -> Option<String> {
    let tail = path.strip_prefix("/v1/jobs/")?;
    (tail.len() == 36
        && tail
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() || byte == b'-'))
    .then(|| tail.to_owned())
}

fn plugin_name(path: &str) -> Option<String> {
    let tail = path.strip_prefix("/v1/plugins/")?.strip_suffix("/run")?;
    (!tail.is_empty() && !tail.contains('/')).then(|| tail.to_owned())
}

/// `/v1/(poster|backdrop)/(tt` + five to ten digits + `)`.
fn art_match(path: &str) -> Option<(&'static str, &str)> {
    let tail = path.strip_prefix("/v1/")?;
    let (kind, id) = tail.split_once('/')?;
    let kind = match kind {
        "poster" => "poster",
        "backdrop" => "backdrop",
        _ => return None,
    };
    crate::art::is_imdb_id(id).then_some((kind, id))
}

/// `/catalog/(movie|series)/<anything>.json`.
fn catalog_kind(path: &str) -> Option<&'static str> {
    let tail = path.strip_prefix("/catalog/")?;
    let (kind, name) = tail.split_once('/')?;
    let kind = match kind {
        "movie" => "movie",
        "series" => "series",
        _ => return None,
    };
    (name.ends_with(".json") && !name.contains('/')).then_some(kind)
}

/// `/stream/<type>/<id>.json`, the id still percent-encoded.
fn stream_id(path: &str) -> Option<String> {
    let tail = path.strip_prefix("/stream/")?;
    let (_, id) = tail.split_once('/')?;
    let id = id.strip_suffix(".json")?;
    (!id.is_empty()).then(|| id.to_owned())
}

fn percent_decode(text: &str) -> String {
    let bytes = text.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut index = 0;
    while index < bytes.len() {
        let hex = |at: usize| {
            bytes
                .get(at)
                .and_then(|byte| (*byte as char).to_digit(16))
                .map(|digit| digit as u8)
        };
        if bytes[index] == b'%'
            && let (Some(high), Some(low)) = (hex(index + 1), hex(index + 2))
        {
            out.push((high << 4) | low);
            index += 3;
            continue;
        }
        out.push(bytes[index]);
        index += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// The query string without one parameter, re-serialized the way `URLSearchParams` writes it.
struct UriQuery;

impl UriQuery {
    fn without(url: &Url, name: &str) -> Option<String> {
        let pairs: Vec<(String, String)> = url
            .query_pairs()
            .filter(|(key, _)| key != name)
            .map(|(key, value)| (key.into_owned(), value.into_owned()))
            .collect();
        if pairs.is_empty() {
            return None;
        }
        let mut form = url::form_urlencoded::Serializer::new(String::new());
        form.extend_pairs(pairs);
        Some(form.finish())
    }
}

/// `stremioToPearId`.
fn stremio_to_pear_id(raw: &str) -> String {
    let clean = percent_decode(raw);
    let clean = match clean
        .strip_suffix(".json")
        .or_else(|| clean.strip_suffix(".JSON"))
    {
        Some(stripped) => stripped.to_owned(),
        None => {
            let lower = clean.to_lowercase();
            if lower.ends_with(".json") {
                clean[..clean.len() - 5].to_owned()
            } else {
                clean
            }
        }
    };
    if let Some(imdb) = imdb_only(&clean) {
        return format!("imdb:{}", imdb.to_lowercase());
    }
    if let Some((imdb, season, episode)) = episode_link(&clean) {
        return format!("imdb:{}:s{:02}e{:02}", imdb.to_lowercase(), season, episode);
    }
    clean.to_lowercase()
}

fn digits(value: &str) -> Option<u64> {
    (!value.is_empty() && value.bytes().all(|byte| byte.is_ascii_digit()))
        .then(|| value.parse().ok())
        .flatten()
}

/// `tt` and digits only.
fn imdb_only(value: &str) -> Option<&str> {
    let rest = value
        .strip_prefix("tt")
        .or_else(|| value.strip_prefix("TT"))?;
    (rest.bytes().all(|byte| byte.is_ascii_digit()) && !rest.is_empty()).then_some(value)
}

/// `<tt-id>:<season>:<episode>`.
fn episode_link(value: &str) -> Option<(&str, u64, u64)> {
    let mut parts = value.split(':');
    let imdb = parts.next()?;
    imdb_only(imdb)?;
    let season = digits(parts.next()?)?;
    let episode = digits(parts.next()?)?;
    if parts.next().is_some() {
        return None;
    }
    Some((imdb, season, episode))
}

/// `\bS\d{2}E\d{2,3}\b`, case-insensitive: a release name states the episode.
fn has_episode_tag(title: &str) -> bool {
    let bytes = title.as_bytes();
    for index in 0..bytes.len() {
        if bytes[index] != b'S' && bytes[index] != b's' {
            continue;
        }
        if index > 0 {
            let previous = bytes[index - 1];
            if previous.is_ascii_alphanumeric() || previous == b'_' {
                continue;
            }
        }
        if let Some(end) = episode_tag_end(&bytes[index..]) {
            let next = bytes.get(index + end);
            if next.is_none_or(|next| !next.is_ascii_alphanumeric() && *next != b'_') {
                return true;
            }
        }
    }
    false
}

/// The length of an `S\d{2}E\d{2,3}` tag at the start of `tag`, upper or lower case.
fn episode_tag_end(tag: &[u8]) -> Option<usize> {
    if tag.len() < 6 || !matches!(tag[0], b'S' | b's') || !matches!(tag[3], b'E' | b'e') {
        return None;
    }
    if !tag[1..3].iter().all(u8::is_ascii_digit) {
        return None;
    }
    let digits = tag[4..]
        .iter()
        .take_while(|byte| byte.is_ascii_digit())
        .count();
    (2..=3).contains(&digits).then_some(4 + digits)
}

/// A trailing ` S\d{2}E\d{2,3}` on a series title, which names the show, not the episode.
fn strip_episode_tag(title: &str) -> String {
    let trimmed = title.trim_end();
    let Some(space) = trimmed.rfind(' ') else {
        return title.to_owned();
    };
    let tail = &trimmed[space + 1..];
    match episode_tag_end(tail.as_bytes()) {
        Some(end) if end == tail.len() => trimmed[..space].to_owned(),
        _ => title.to_owned(),
    }
}

/// `s\d{2}e\d{2,3}` lower-case, as the stream label matcher spells it.
fn is_episode_suffix(tag: &str) -> bool {
    let bytes = tag.as_bytes();
    (6..=7).contains(&bytes.len())
        && matches!(episode_tag_end(bytes), Some(end) if end == bytes.len())
}

/// `pearToStremioMeta`: catalog tiles must be ids another addon can describe.
fn stremio_meta(entry: &Entry) -> Option<Value> {
    let id = entry.id.as_str();
    if let Some(imdb) = catalog_series_id(id) {
        return Some(json!({
            "id": imdb,
            "type": "series",
            "name": strip_episode_tag(&entry.title),
        }));
    }
    let imdb = id.strip_prefix("imdb:")?;
    imdb_only(imdb)?;
    Some(json!({ "id": imdb, "type": "movie", "name": entry.title }))
}

/// `imdb:<tt-id>:s\d{2}e\d{2,3}`.
fn catalog_series_id(id: &str) -> Option<&str> {
    let rest = id.strip_prefix("imdb:")?;
    let mut parts = rest.split(':');
    let imdb = parts.next()?;
    imdb_only(imdb)?;
    let episode = parts.next()?;
    if parts.next().is_some() || !episode.starts_with('s') {
        return None;
    }
    match episode_tag_end(episode.as_bytes()) {
        Some(end) if end == episode.len() => Some(imdb),
        _ => None,
    }
}

/// `readStream`: `start` and `end` through the node's own blob server, only a 206 accepted.
async fn read_stream(
    host: &str,
    port: u16,
    search: &str,
    start: u64,
    end: u64,
) -> Result<Vec<u8>, HttpError> {
    // The app hands the read *length* as the range's last byte; the request is kept identical.
    let target = if search.is_empty() {
        "/".to_owned()
    } else {
        format!("/?{search}")
    };
    let request = Request::builder()
        .uri(format!("http://{host}:{port}{target}"))
        .header(header::RANGE, format!("bytes={start}-{end}"))
        .body(Full::new(Bytes::new()))
        .map_err(|error| HttpError::new(400, error.to_string()))?;
    let exchange = async {
        let response = CLIENT
            .request(request)
            .await
            .map_err(|error| HttpError::new(400, error.to_string()))?;
        if response.status() != StatusCode::PARTIAL_CONTENT {
            return Err(HttpError::new(
                400,
                format!("Stream read returned {}", response.status().as_u16()),
            ));
        }
        let mut body = response.into_body();
        let mut bytes = Vec::new();
        while let Some(frame) = body.frame().await {
            let frame = frame.map_err(|error| HttpError::new(400, error.to_string()))?;
            if let Some(chunk) = frame.data_ref() {
                bytes.extend_from_slice(chunk);
            }
        }
        Ok(bytes)
    };
    match tokio::time::timeout(STREAM_TIMEOUT, exchange).await {
        Ok(result) => result,
        Err(_) => Err(HttpError::new(400, "Stream read timed out")),
    }
}

/// `sniffDownloadFormat`: the container's extension and MIME from its first bytes.
fn sniff_download_format(head: Option<&[u8]>) -> (String, String) {
    let Some(head) = head else {
        return (".mkv".to_owned(), "application/octet-stream".to_owned());
    };
    if head.len() < 4 {
        return (".mkv".to_owned(), "application/octet-stream".to_owned());
    }
    let at = |offset: usize| -> Option<&[u8]> { head.get(offset..offset + 4) };
    if head[..4] == [0x1a, 0x45, 0xdf, 0xa3] {
        let window: Vec<u8> = head[..head.len().min(128)]
            .iter()
            .map(u8::to_ascii_lowercase)
            .collect();
        if window.windows(4).any(|window| window == b"webm") {
            return (".webm".to_owned(), "video/webm".to_owned());
        }
        return (".mkv".to_owned(), "video/x-matroska".to_owned());
    }
    if let Some(kind) = at(4)
        && [&b"ftyp"[..], b"moov", b"mdat", b"free", b"wide", b"skip"].contains(&kind)
    {
        return if at(8) == Some(&b"qt  "[..]) {
            (".mov".to_owned(), "video/quicktime".to_owned())
        } else {
            (".mp4".to_owned(), "video/mp4".to_owned())
        };
    }
    if at(0) == Some(&b"RIFF"[..]) && at(8) == Some(&b"AVI "[..]) {
        return (".avi".to_owned(), "video/x-msvideo".to_owned());
    }
    if at(0) == Some(&b"OggS"[..]) {
        return (".ogv".to_owned(), "video/ogg".to_owned());
    }
    if at(0) == Some(&b"fLaC"[..]) {
        return (".flac".to_owned(), "audio/flac".to_owned());
    }
    if head[0] == 0x49 && head[1] == 0x44 && head[2] == 0x33 {
        return (".mp3".to_owned(), "audio/mpeg".to_owned());
    }
    if head[0] == 0x47 && (head.len() < 189 || head[188] == 0x47) {
        return (".ts".to_owned(), "video/mp2t".to_owned());
    }
    (".mkv".to_owned(), "application/octet-stream".to_owned())
}

/// `contentDispositionFor`: a safe ASCII name plus the full one, so saves keep their title.
fn content_disposition(raw: &str, ext: &str) -> String {
    let cleaned: String = raw
        .chars()
        .map(|c| {
            if matches!(c, '/' | '\\' | ':' | '*' | '?' | '"' | '<' | '>' | '|')
                || (c as u32) < 0x20
            {
                ' '
            } else {
                c
            }
        })
        .collect();
    let mut base = cleaned.split_whitespace().collect::<Vec<_>>().join(" ");
    base = first_units(&base, 180);
    if base.is_empty() {
        base = "video".to_owned();
    }
    let full = if is_media_file(&base) {
        base
    } else {
        format!("{base}{ext}")
    };
    let ascii: String = full
        .chars()
        .map(|c| {
            if (' '..='~').contains(&c) && c != '"' && c != '\\' {
                c
            } else {
                '_'
            }
        })
        .collect();
    format!(
        "attachment; filename=\"{ascii}\"; filename*=UTF-8''{}",
        encode_uri_component(&full)
    )
}
