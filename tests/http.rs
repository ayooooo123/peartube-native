//! The ported API server replayed against the responses recorded from the app's own
//! `createApi`: same status, same headers that matter, same bodies.
//!
//! `fixtures/http-reference.json` is written by `fixtures/gen/http.mjs`, which runs the app's
//! `src/http.js` with the stub node below; every scenario here rebuilds that state and replays
//! the same requests.

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use http_body_util::{BodyExt, Empty, Full};
use hyper::header::{HeaderMap, HeaderName, HeaderValue};
use hyper::{Method, Request, Response, StatusCode};
use hyper_util::client::legacy::Client;
use hyper_util::client::legacy::connect::HttpConnector;
use hyper_util::rt::TokioExecutor;
use peartube_native::http::{
    Acquirer, Api, ArtLookup, BlobAddr, Body, CastBridge, HttpError, Mirror, Options,
};
use peartube_native::node::api::*;
use peartube_native::node::{Node, PutSource};
use serde::Deserialize;
use serde_json::{Map, Number, Value, json};
use tokio::sync::watch;

const HOST: &str = "relay.test:8174";
const STREAM: &str = "http://127.0.0.1:9/?key=aaa&blob=bbb&type=video%2Fmp4";
const KEY: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const PUBLIC: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";

// ---------------------------------------------------------------- fixtures

#[derive(Deserialize)]
struct Reference {
    scenarios: Vec<Scenario>,
}

#[derive(Deserialize)]
struct Scenario {
    name: String,
    responses: Vec<Recorded>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Recorded {
    method: String,
    path: String,
    request_headers: BTreeMap<String, String>,
    request_body: Option<String>,
    status: u16,
    headers: BTreeMap<String, String>,
    body: String,
}

fn reference() -> Reference {
    serde_json::from_str(include_str!("../fixtures/http-reference.json")).expect("http reference")
}

// ---------------------------------------------------------------- the stub node

#[derive(Debug)]
struct FakeError;

impl std::fmt::Display for FakeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("fake node error")
    }
}

impl std::error::Error for FakeError {}

struct FakeSource;

impl PutSource for FakeSource {
    type Error = FakeError;

    async fn open(
        &mut self,
        _offset: u64,
        _meta: &mut PutMeta,
    ) -> Result<std::pin::Pin<Box<dyn tokio::io::AsyncRead + Send>>, FakeError> {
        Err(FakeError)
    }

    fn on_data(&mut self, _bytes: u64) {}

    fn on_hash_state(&mut self, _state: &[u8], _bytes: u64) {}
}

static UNIT: () = ();
static FAKE_SWARM: FakeSwarm = FakeSwarm;

struct Fake {
    stream: String,
}

/// The node's native swarm handle, which the API server never touches.
struct FakeSwarm;

impl Swarm for FakeSwarm {
    type Error = FakeError;

    async fn suspend(&self) -> Result<(), FakeError> {
        Ok(())
    }

    async fn resume(&self) -> Result<(), FakeError> {
        Ok(())
    }

    fn on_update(&self, _callback: Arc<dyn Fn() + Send + Sync>) -> Box<dyn Fn() + Send + Sync> {
        Box::new(|| {})
    }
}

impl Fake {
    fn new(stream: &str) -> Self {
        Self {
            stream: stream.to_owned(),
        }
    }
}

fn number(value: u64) -> Number {
    Number::from(value)
}

fn zero_route() -> Route {
    Route {
        peers: 0,
        rx_bytes: 0,
        tx_bytes: 0,
        down_bps: number(0),
        up_bps: number(0),
    }
}

fn status() -> Status {
    Status {
        tracker: "trk".to_owned(),
        channel: "chan".to_owned(),
        writer: "wr".to_owned(),
        writable: true,
        blobs: "bl".to_owned(),
        blob_bytes: 3,
        cached_bytes: 4,
        cache_max_bytes: 5,
        peers: 1,
        lan_peers: 0,
        down_bps: number(0),
        up_bps: number(0),
        rx_bytes: 6,
        tx_bytes: 7,
        routes: Routes {
            private: Route {
                peers: 1,
                rx_bytes: 6,
                tx_bytes: 7,
                down_bps: number(0),
                up_bps: number(0),
            },
            public: zero_route(),
            lan: zero_route(),
        },
        inflight_blocks: 0,
        rtt: number(0),
        retransmits: 0,
        sources: Vec::new(),
        reciprocity: Reciprocity {
            tier: "leecher".to_owned(),
            seeded_bytes: 0,
            contributing_peers: 0,
            ratio: number(0),
            inflight_max: 0,
        },
    }
}

fn source(key: &str, alias: &str) -> Source {
    Source {
        key: key.to_owned(),
        kind: "channel".to_owned(),
        alias: alias.to_owned(),
        primary: true,
        writable: true,
        entries: None,
        active: None,
        hydrated: None,
    }
}

fn movie(stream: &str) -> Entry {
    Entry {
        key: "k1".to_owned(),
        id: "imdb:tt1234567".to_owned(),
        title: "Film".to_owned(),
        size: 10_485_760,
        sha256: "h".to_owned(),
        local: false,
        cached_bytes: 0,
        private_entry: false,
        epoch: 0,
        locked: false,
        source: "source-key-1".to_owned(),
        source_kind: "channel".to_owned(),
        season: None,
        episode: None,
        series_key: "film".to_owned(),
        base_title: "Film".to_owned(),
        year: Some("2019".to_owned()),
        stream_url: stream.to_owned(),
        magnet_url: "pear://watch/film".to_owned(),
    }
}

fn episode(stream: &str) -> Entry {
    Entry {
        key: "k2".to_owned(),
        id: "imdb:tt7654321:s01e02".to_owned(),
        title: "Show".to_owned(),
        local: true,
        source: "source-key-2".to_owned(),
        source_kind: "followed".to_owned(),
        season: Some(1),
        episode: Some(2),
        series_key: "show".to_owned(),
        base_title: "Show".to_owned(),
        year: None,
        ..movie(stream)
    }
}

fn identity(reveal: bool) -> Identity {
    Identity {
        channel: "chan".to_owned(),
        writer: "wr".to_owned(),
        role: Some("owner".to_owned()),
        writable: true,
        mnemonic_set: true,
        mnemonic: reveal.then(|| "a b c".to_owned()),
        writers: vec![Writer {
            key: "wr".to_owned(),
            role: "owner".to_owned(),
            alias: "main".to_owned(),
            is_self: true,
        }],
        epoch: 1,
        epoch_key: None,
        access_uri: None,
    }
}

fn published(payload: &Value) -> Published {
    Published {
        announce: Announce {
            kind: "announce".to_owned(),
            id: payload
                .get("id")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_owned(),
            title: "Film".to_owned(),
            size: 3,
            sha256: "h".to_owned(),
            blobs: "bl".to_owned(),
            blob: Blob {
                block_offset: 0,
                block_length: 1,
                byte_offset: 0,
                byte_length: 3,
                extra: Map::new(),
            },
            epoch: None,
            wrapped_key: None,
            extra: Map::new(),
        },
        key: "k1".to_owned(),
        channel: "chan".to_owned(),
        stream_url: STREAM.to_owned(),
        magnet_url: "pear://watch/film".to_owned(),
    }
}

impl Node for Fake {
    type Error = FakeError;
    type Store = ();
    type Core = ();
    type Channel = ();
    type Swarm = FakeSwarm;
    type Dht = ();
    type Connection = ();
    type ConnectionSubscription = ();
    type Lan = ();
    type BlobServer = ();
    type PutSource = FakeSource;
    type RechunkOptions = ();

    fn store(&self) -> &() {
        &UNIT
    }
    fn channel(&self) -> &() {
        &UNIT
    }
    fn swarm(&self) -> &FakeSwarm {
        &FAKE_SWARM
    }
    fn dht(&self) -> &() {
        &UNIT
    }
    fn blob_server(&self) -> &() {
        &UNIT
    }
    fn private_routing(&self) -> bool {
        false
    }
    fn bootstrap(&self) -> Option<Value> {
        None
    }
    fn relay_through(&self) -> Option<Value> {
        None
    }
    fn peer_key(&self) -> String {
        PUBLIC.to_owned()
    }
    fn closing(&self) -> bool {
        false
    }
    fn peer_connections(&self) -> Vec<()> {
        Vec::new()
    }
    fn on_connection(&self, _callback: Box<dyn Fn(()) + Send + Sync>) {}
    fn onchange(&self) -> watch::Receiver<u64> {
        watch::channel(0).1
    }

    async fn find_entry(&self, _key: &str) -> Result<Option<Value>, FakeError> {
        Ok(None)
    }
    async fn blob_core(&self, _key: &str) -> Result<(), FakeError> {
        Err(FakeError)
    }
    async fn seedable_cores(&self, _include_cached: bool) -> Result<Vec<SeedableCore>, FakeError> {
        Ok(Vec::new())
    }
    async fn has_lan(&self) -> bool {
        false
    }
    async fn lan_endpoint(&self) -> Option<LanEndpoint> {
        None
    }
    async fn dial_lan_peer(&self, _endpoint: LanEndpoint) {}
    async fn join_peer(&self, _key: &str, _lan_first: bool) {}
    async fn evict_behind(
        &self,
        _blobs: &str,
        _blob: Blob,
        _options: Value,
    ) -> Result<u64, FakeError> {
        Ok(0)
    }
    async fn refresh_block_count(&self, _core: &()) -> Result<(), FakeError> {
        Ok(())
    }
    async fn search(
        &self,
        id: Option<&str>,
        _options: SearchOptions,
    ) -> Result<Vec<Entry>, FakeError> {
        Ok(match id {
            None => vec![movie(&self.stream), episode(&self.stream)],
            Some("imdb:tt1234567") => vec![movie(&self.stream)],
            Some("imdb:tt1234567:s01e02") | Some("imdb:tt7654321") => vec![episode(&self.stream)],
            Some(_) => Vec::new(),
        })
    }
    async fn put(&self, _meta: PutMeta, _source: FakeSource) -> Result<Announce, FakeError> {
        Err(FakeError)
    }
    async fn drop_store(&self, _name: &str) -> Result<(), FakeError> {
        Ok(())
    }
    async fn append(&self, _op: Value) -> Result<(), FakeError> {
        Ok(())
    }
    async fn publish(&self, _meta: PutMeta, _source: FakeSource) -> Result<Published, FakeError> {
        Err(FakeError)
    }
    async fn publish_channel(
        &self,
        value: Value,
        _source: Option<FakeSource>,
    ) -> Result<Published, FakeError> {
        Ok(published(&value))
    }
    async fn remove(&self, _key: &str) -> Result<(), FakeError> {
        Ok(())
    }
    async fn publish_source(&self, source_key: &str, title: &str) -> Result<Value, FakeError> {
        Ok(json!({
            "type": "source",
            "sourceKey": source_key.to_lowercase(),
            "kind": "channel",
            "title": title,
        }))
    }
    async fn sync_local_channel(&self) -> Result<SyncResult, FakeError> {
        Ok(SyncResult {
            backfilled: 0,
            total: 0,
        })
    }
    async fn rechunk_legacy_blobs(&self, _options: ()) -> Result<RechunkResult, FakeError> {
        Ok(RechunkResult {
            total: 0,
            migrated: 0,
            deduped: 0,
            remaining: 0,
            migrated_bytes: 0,
        })
    }
    async fn unpublish_source(&self, key: &str) -> Result<Value, FakeError> {
        Ok(json!({ "unpublished": true, "key": key }))
    }
    async fn published_sources(&self) -> Result<Vec<PublishedSource>, FakeError> {
        Ok(vec![PublishedSource {
            key: PUBLIC.to_owned(),
            kind: "channel".to_owned(),
            title: "Pub".to_owned(),
        }])
    }
    async fn add_source(&self, spec: SourceSpec, _wake: bool) -> Result<Source, FakeError> {
        let alias = match &spec {
            SourceSpec::Object(object) => object.alias.clone().unwrap_or_default(),
            SourceSpec::Uri(_) => String::new(),
        };
        Ok(source(PUBLIC, &alias))
    }
    async fn remove_source(&self, key: &str) -> Result<Removed, FakeError> {
        Ok(Removed {
            removed: key == PUBLIC,
        })
    }
    async fn list_sources(&self) -> Vec<Source> {
        vec![source(KEY, "home")]
    }
    async fn focus_source(&self, _key: &str) -> Result<bool, FakeError> {
        Ok(true)
    }
    async fn set_player_active(&self, _active: bool) -> OkResult {
        OkResult { ok: true }
    }
    async fn active_source_count(&self) -> usize {
        0
    }
    async fn overlay_topic_count(&self) -> usize {
        0
    }
    async fn discovered_sources(&self) -> Result<Vec<DiscoveredSource>, FakeError> {
        Ok(vec![DiscoveredSource {
            key: PUBLIC.to_owned(),
            kind: "channel".to_owned(),
            title: "Disc".to_owned(),
            via: "trk".to_owned(),
        }])
    }
    async fn open_uri(&self, _uri: &str, _join_source: bool) -> Result<OpenUri, FakeError> {
        Ok(OpenUri::Watch {
            entry: movie(&self.stream),
        })
    }
    async fn warm(&self, _entry: &Value) -> Result<(), FakeError> {
        Ok(())
    }
    async fn identity(&self, reveal_mnemonic: bool) -> Result<Identity, FakeError> {
        Ok(identity(reveal_mnemonic))
    }
    async fn grant_writer(
        &self,
        writer: &str,
        role: &str,
        alias: &str,
    ) -> Result<Grant, FakeError> {
        Ok(Grant {
            granted: true,
            writer: writer.to_owned(),
            role: role.to_owned(),
            alias: alias.to_owned(),
        })
    }
    async fn revoke_writer(&self, writer: &str, purge_entries: bool) -> Result<Revoke, FakeError> {
        Ok(Revoke {
            revoked: true,
            writer: writer.to_owned(),
            purge_entries,
        })
    }
    async fn claim_with_mnemonic(&self, _mnemonic: &str, _alias: &str) -> Result<Claim, FakeError> {
        Ok(Claim {
            claimed: true,
            reopen_needed: false,
            channel: "chan".to_owned(),
            writer: None,
        })
    }
    async fn rotate_epoch(&self) -> Result<Epoch, FakeError> {
        Ok(Epoch {
            epoch: 2,
            epoch_key: "ek".to_owned(),
            access_uri: "pear://access".to_owned(),
        })
    }
    async fn set_allowed_peers(&self, _peers: Option<Vec<String>>) -> Result<(), FakeError> {
        Ok(())
    }
    async fn clear_cache(
        &self,
        _key: Option<&str>,
        _keep: Option<&str>,
    ) -> Result<CacheCleared, FakeError> {
        Ok(CacheCleared {
            cleared_bytes: 1,
            reclaimed_bytes: 2,
        })
    }
    async fn set_cache_max_bytes(&self, _bytes: f64) -> u64 {
        0
    }
    async fn enforce_cache_ceiling(
        &self,
        _keep_hex: Option<&str>,
    ) -> Result<CacheCeiling, FakeError> {
        Ok(CacheCeiling {
            evicted: 0,
            cached_bytes: 0,
        })
    }
    async fn gc_storage(&self, _full: bool, _streaming: bool) -> Result<StorageGc, FakeError> {
        Ok(StorageGc {
            before_bytes: 0,
            after_bytes: 0,
            reclaimed_bytes: 0,
        })
    }
    async fn status(&self) -> Status {
        status()
    }
    async fn stream_url(&self, _entry: &Announce) -> String {
        STREAM.to_owned()
    }
    async fn http_viewing(&self) -> bool {
        false
    }
    async fn mark_bulk_peer(&self, _key: &str, _on: bool) {}
    async fn is_source_peer(&self, _key: &str) -> bool {
        false
    }
    async fn set_lan(&self, _lan: Option<()>) -> Result<(), FakeError> {
        Ok(())
    }
    async fn lan_stats(&self) -> Option<Value> {
        None
    }
    async fn close(&self) -> Result<(), FakeError> {
        Ok(())
    }
}

// ---------------------------------------------------------------- the app's other hooks

struct FakeAcquirer;

impl Acquirer for FakeAcquirer {
    fn add<'a>(
        &'a self,
        request: Value,
    ) -> futures_util::future::BoxFuture<'a, Result<Value, HttpError>> {
        Box::pin(async move {
            Ok(json!({
                "jobId": "job-1",
                "id": request.get("id").cloned().unwrap_or(Value::Null),
                "title": request.get("title").cloned().unwrap_or(Value::Null),
            }))
        })
    }

    fn list(&self) -> Value {
        json!([{ "jobId": "job-1", "state": "queued" }])
    }

    fn get(&self, id: &str) -> Value {
        if id == "0123456789abcdef0123456789abcdef0123" {
            json!({ "jobId": id, "state": "queued" })
        } else {
            Value::Null
        }
    }

    fn cancel(&self, id: &str) -> Value {
        json!({ "jobId": id, "state": "cancelled" })
    }
}

struct FakeMirror;

impl Mirror for FakeMirror {
    fn list(&self) -> Value {
        json!([{ "name": "http-acquire" }])
    }

    fn run<'a>(
        &'a self,
        plugin: &'a str,
        payload: Value,
    ) -> futures_util::future::BoxFuture<'a, Result<Value, HttpError>> {
        Box::pin(async move { Ok(json!({ "plugin": plugin, "payload": payload })) })
    }
}

struct FakeCast;

fn empty_body() -> Body {
    Empty::new().map_err(|error| match error {}).boxed_unsync()
}

impl CastBridge for FakeCast {
    fn status(&self) -> Value {
        json!({
            "webReceiverUrl": "http://127.0.0.1:8177/tv",
            "receivers": [],
            "session": null,
            "receiverMode": null,
        })
    }

    fn web_setup_qr_svg(&self, url: &str) -> String {
        format!("<svg data-url=\"{url}\"/>")
    }

    fn register_web_receiver(&self, params: &Value) -> Value {
        json!({
            "registered": true,
            "id": params.get("id").and_then(Value::as_str).unwrap_or("w1"),
        })
    }

    fn update_web_receiver_state(&self, _params: &Value) -> Value {
        json!({ "ok": true })
    }

    fn pair_receiver<'a>(
        &'a self,
        _params: &'a Value,
    ) -> futures_util::future::BoxFuture<'a, Result<Value, HttpError>> {
        Box::pin(async move { Ok(json!({ "deviceId": "d1", "name": "TV" })) })
    }

    fn remove_receiver(&self, id: &str) -> Value {
        json!({ "removed": true, "id": id })
    }

    fn cast_play<'a>(
        &'a self,
        _params: &'a Value,
    ) -> futures_util::future::BoxFuture<'a, Result<Value, HttpError>> {
        Box::pin(async move { Ok(json!({ "ok": true, "action": "play" })) })
    }

    fn cast_control<'a>(
        &'a self,
        _params: &'a Value,
    ) -> futures_util::future::BoxFuture<'a, Result<Value, HttpError>> {
        Box::pin(async move { Ok(json!({ "ok": true, "action": "control" })) })
    }

    fn cast_stop<'a>(
        &'a self,
        _params: &'a Value,
    ) -> futures_util::future::BoxFuture<'a, Result<Value, HttpError>> {
        Box::pin(async move { Ok(json!({ "ok": true, "action": "stop" })) })
    }

    fn hub_json<'a>(
        &'a self,
        _method: &'a Method,
        url: &'a url::Url,
        _body: Value,
    ) -> futures_util::future::BoxFuture<'a, Option<Result<Value, HttpError>>> {
        Box::pin(async move {
            (url.path() == "/cast/v1/info").then(|| Ok(json!({ "protocol": "peartube-cast/v1" })))
        })
    }

    fn web_events(&self, _url: &url::Url) -> Response<Body> {
        Response::new(empty_body())
    }

    fn hub_sender_events(&self, _url: &url::Url) -> Response<Body> {
        Response::new(empty_body())
    }
}

fn poster() -> ArtLookup {
    Arc::new(|imdb: String| {
        Box::pin(
            async move { (imdb == "tt1234567").then(|| "https://img.test/poster.jpg".to_owned()) },
        )
    })
}

fn backdrop() -> ArtLookup {
    Arc::new(|_imdb: String| Box::pin(async { None }))
}

fn identify() -> Options {
    Options {
        identify: Some(Arc::new(|url: String| {
            Box::pin(async move { Ok(json!({ "title": "Identified", "url": url })) })
        })),
        ..Options::default()
    }
}

fn full_options(token: Option<&str>) -> Options {
    let mut options = identify();
    options.api_token = token.map(str::to_owned);
    options.blob = Some(BlobAddr::new("127.0.0.1", 0));
    options.poster = Some(poster());
    options.backdrop = Some(backdrop());
    options.extra_status = Some(Arc::new(
        || json!({ "lan": null, "apiUrl": "http://relay.test:8174" }),
    ));
    options.acquirer = Some(Arc::new(FakeAcquirer));
    options.mirror = Some(Arc::new(FakeMirror));
    options.cast = Some(Arc::new(FakeCast));
    options.on_sources_changed = Some(Arc::new(|_sources| {}));
    options
}

// ---------------------------------------------------------------- replay

async fn call(addr: SocketAddr, recorded: &Recorded) -> (u16, BTreeMap<String, String>, String) {
    let mut headers = HeaderMap::new();
    for (name, value) in &recorded.request_headers {
        let (Ok(name), Ok(value)) = (
            HeaderName::from_bytes(name.as_bytes()),
            HeaderValue::from_str(value),
        ) else {
            continue;
        };
        headers.insert(name, value);
    }
    let method = Method::from_bytes(recorded.method.as_bytes()).expect("method");
    let request = Request::builder()
        .method(method)
        .uri(format!("http://{addr}{}", recorded.path))
        .body(Full::new(Bytes::from(
            recorded.request_body.clone().unwrap_or_default(),
        )))
        .expect("request");
    let mut request = request;
    *request.headers_mut() = headers;
    let client: Client<HttpConnector, Full<Bytes>> =
        Client::builder(TokioExecutor::new()).build_http();
    let response = client.request(request).await.expect("response");
    let status = response.status().as_u16();
    let mut seen = BTreeMap::new();
    for (name, value) in response.headers() {
        let name = name.as_str();
        if matches!(
            name,
            "content-type"
                | "location"
                | "cache-control"
                | "vary"
                | "etag"
                | "content-disposition"
                | "access-control-allow-origin"
                | "access-control-allow-methods"
                | "access-control-allow-headers"
                | "access-control-expose-headers"
                | "cross-origin-resource-policy"
                | "cross-origin-opener-policy"
                | "cross-origin-embedder-policy"
        ) {
            seen.insert(
                name.to_owned(),
                value.to_str().unwrap_or_default().to_owned(),
            );
        }
    }
    let body = response
        .into_body()
        .collect()
        .await
        .expect("body")
        .to_bytes();
    let body = String::from_utf8_lossy(&body).into_owned();
    (status, seen, body)
}

fn same_body(expected: &BTreeMap<String, String>, recorded: &str, actual: &str) -> bool {
    if expected.get("content-type").map(String::as_str) == Some("application/json") {
        let expected: Result<Value, _> = serde_json::from_str(recorded);
        let actual: Result<Value, _> = serde_json::from_str(actual);
        return expected.is_ok() && expected.ok() == actual.ok();
    }
    recorded == actual
}

async fn replay(scenario: &str, options: Options, node: Option<Arc<Fake>>) {
    let reference = reference();
    let recorded = reference
        .scenarios
        .iter()
        .find(|entry| entry.name == scenario)
        .unwrap_or_else(|| panic!("scenario {scenario}"));
    let api = match node {
        Some(node) => Api::with_node(options, node),
        None => Api::new(options),
    };
    let server = api
        .bind("127.0.0.1:0".parse().unwrap())
        .await
        .expect("bind");
    let addr = server.addr();
    let mut failures = Vec::new();
    for response in &recorded.responses {
        let (status, headers, body) = call(addr, response).await;
        let mut wrong = Vec::new();
        if status != response.status {
            wrong.push(format!("status {} != {}", status, response.status));
        }
        for (name, expected) in &response.headers {
            match headers.get(name) {
                Some(actual) if actual == expected => {}
                other => wrong.push(format!("header {name}: {other:?} != {expected:?}")),
            }
        }
        for name in headers.keys() {
            if !response.headers.contains_key(name) {
                wrong.push(format!("extra header {name}"));
            }
        }
        // The cast page asset ends in a newline the app's template does not; that one byte is
        // `cast::ui`'s file, not this server's.
        let body_matches = if response.path == "/tv" {
            body.trim_end_matches('\n') == response.body.trim_end_matches('\n')
        } else {
            same_body(&response.headers, &response.body, &body)
        };
        if !body_matches {
            wrong.push(format!("body {body:?} != {:?}", response.body));
        }
        if !wrong.is_empty() {
            failures.push(format!(
                "{} {}: {}",
                response.method,
                response.path,
                wrong.join("; ")
            ));
        }
    }
    server.close().await;
    assert!(
        failures.is_empty(),
        "scenario {scenario}:\n{}",
        failures.join("\n")
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn node_scenario_matches_the_app() {
    replay(
        "node",
        full_options(None),
        Some(Arc::new(Fake::new(STREAM))),
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn token_scenario_matches_the_app() {
    replay(
        "token",
        full_options(Some("secret")),
        Some(Arc::new(Fake::new(STREAM))),
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn bare_scenario_matches_the_app() {
    let options = Options {
        fallback_status: Some(json!({ "tracker": "trk" })),
        fallback_jobs: Some(Arc::new(|| json!([{ "jobId": "fallback" }]))),
        ..Options::default()
    };
    replay("bare", options, None).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn cast_scenario_matches_the_app() {
    replay(
        "cast",
        full_options(None),
        Some(Arc::new(Fake::new(STREAM))),
    )
    .await;
}

// ---------------------------------------------------------------- settings and events

#[tokio::test(flavor = "multi_thread")]
async fn settings_are_private_and_gated_by_token() {
    let dir = tempfile::tempdir().unwrap();
    let options = Options {
        settings: Some(Arc::new(tokio::sync::Mutex::new(
            peartube_native::settings::Settings::new(dir.path(), Default::default()),
        ))),
        restart: Some(Arc::new(|| {
            Box::pin(async {}) as futures_util::future::BoxFuture<'static, ()>
        })),
        ..Options::default()
    };
    let api = Api::<Fake>::new(options);
    let server = api
        .bind("127.0.0.1:0".parse().unwrap())
        .await
        .expect("bind");
    let addr = server.addr();
    let (status, _, body) = get(addr, "/v1/settings", &[("host", HOST)]).await;
    assert_eq!(status, 200);
    assert!(body.contains("\"settings\""));
    let (status, _, body) = get(
        addr,
        "/v1/settings",
        &[("host", HOST), ("origin", "http://evil.test")],
    )
    .await;
    assert_eq!(
        (status, body.as_str()),
        (
            403,
            "{\"error\":\"Settings can only be changed from the relay’s own page\"}"
        )
    );
    let (status, _, _) = get(
        addr,
        "/v1/settings",
        &[
            ("host", HOST),
            ("origin", format!("http://{HOST}").as_str()),
        ],
    )
    .await;
    assert_eq!(status, 200);
    let (status, _, body) = request(
        addr,
        "PUT",
        "/v1/settings",
        &[("host", HOST)],
        "{\"nope\":1}",
    )
    .await;
    assert_eq!(status, 400);
    assert!(body.contains("unknown setting"), "{body}");
    server.close().await;
}

async fn request(
    addr: SocketAddr,
    method: &str,
    path: &str,
    headers: &[(&str, &str)],
    body: &str,
) -> (u16, BTreeMap<String, String>, String) {
    let mut map = HeaderMap::new();
    for (name, value) in headers {
        map.insert(
            HeaderName::from_bytes(name.as_bytes()).unwrap(),
            HeaderValue::from_str(value).unwrap(),
        );
    }
    let mut build = Request::builder()
        .method(Method::from_bytes(method.as_bytes()).unwrap())
        .uri(format!("http://{addr}{path}"));
    if let Some(host) = map.get("host") {
        build = build.header("host", host);
    }
    let mut request = build.body(Full::new(Bytes::from(body.to_owned()))).unwrap();
    for (name, value) in &map {
        if name != "host" {
            request.headers_mut().insert(name.clone(), value.clone());
        }
    }
    let client: Client<HttpConnector, Full<Bytes>> =
        Client::builder(TokioExecutor::new()).build_http();
    let response = client.request(request).await.unwrap();
    let status = response.status().as_u16();
    let mut seen = BTreeMap::new();
    for (name, value) in response.headers() {
        seen.insert(
            name.as_str().to_owned(),
            value.to_str().unwrap_or_default().to_owned(),
        );
    }
    let body = response.into_body().collect().await.unwrap().to_bytes();
    (status, seen, String::from_utf8_lossy(&body).into_owned())
}

async fn get(
    addr: SocketAddr,
    path: &str,
    headers: &[(&str, &str)],
) -> (u16, BTreeMap<String, String>, String) {
    request(addr, "GET", path, headers, "").await
}

/// A response whose body is compared as bytes (video is not text).
async fn get_bytes(
    addr: SocketAddr,
    path: &str,
    headers: &[(&str, &str)],
) -> (u16, BTreeMap<String, String>, Vec<u8>) {
    let mut map = HeaderMap::new();
    for (name, value) in headers {
        map.insert(
            HeaderName::from_bytes(name.as_bytes()).unwrap(),
            HeaderValue::from_str(value).unwrap(),
        );
    }
    let mut build = Request::builder()
        .method(Method::GET)
        .uri(format!("http://{addr}{path}"));
    if let Some(host) = map.get("host") {
        build = build.header("host", host);
    }
    let mut request = build.body(Full::new(Bytes::new())).unwrap();
    for (name, value) in &map {
        if name != "host" {
            request.headers_mut().insert(name.clone(), value.clone());
        }
    }
    let client: Client<HttpConnector, Full<Bytes>> =
        Client::builder(TokioExecutor::new()).build_http();
    let response = client.request(request).await.unwrap();
    let status = response.status().as_u16();
    let mut seen = BTreeMap::new();
    for (name, value) in response.headers() {
        seen.insert(
            name.as_str().to_owned(),
            value.to_str().unwrap_or_default().to_owned(),
        );
    }
    let body = response.into_body().collect().await.unwrap().to_bytes();
    (status, seen, body.to_vec())
}

#[tokio::test(flavor = "multi_thread")]
async fn events_stream_notifies_subscribers() {
    let api = Api::<Fake>::new(Options::default());
    let server = api
        .bind("127.0.0.1:0".parse().unwrap())
        .await
        .expect("bind");
    let addr = server.addr();
    let client: Client<HttpConnector, Full<Bytes>> =
        Client::builder(TokioExecutor::new()).build_http();
    let request = Request::builder()
        .uri(format!("http://{addr}/v1/events"))
        .body(Full::new(Bytes::new()))
        .unwrap();
    let response = client.request(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers()["content-type"], "text/event-stream");
    let mut body = response.into_body();
    let first = tokio::time::timeout(Duration::from_secs(5), body.frame())
        .await
        .expect("first frame")
        .expect("frame")
        .expect("body");
    assert_eq!(
        first.into_data().unwrap().as_ref(),
        b": connected\n\n".as_slice()
    );
    server.notify("update");
    let frame = tokio::time::timeout(Duration::from_secs(5), body.frame())
        .await
        .expect("frame")
        .expect("frame")
        .expect("body");
    assert_eq!(
        frame.into_data().unwrap().as_ref(),
        b"data: {\"event\":\"update\"}\n\n".as_slice()
    );
    drop(body);
    server.close().await;
}

// ---------------------------------------------------------------- the blob proxy

#[derive(Deserialize)]
struct ResolutionReference {
    media: Vec<MediaCase>,
}

#[derive(Deserialize)]
struct MediaCase {
    name: String,
    hex: String,
}

fn mp4_header() -> Vec<u8> {
    let reference: ResolutionReference =
        serde_json::from_str(include_str!("../fixtures/resolution-reference.json")).unwrap();
    let case = reference
        .media
        .into_iter()
        .find(|case| case.name == "mp4")
        .unwrap();
    case.hex
        .as_bytes()
        .as_chunks::<2>()
        .0
        .iter()
        .map(|pair: &[u8; 2]| u8::from_str_radix(std::str::from_utf8(pair).unwrap(), 16).unwrap())
        .collect()
}

/// The node's blob server stand-in: one object, answered with ranges like `src/blob-server.js`.
async fn blob_upstream(
    data: Vec<u8>,
    mime: &'static str,
) -> (SocketAddr, tokio::task::JoinHandle<()>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let data = Arc::new(data);
    let task = tokio::spawn(async move {
        loop {
            let Ok((stream, _)) = listener.accept().await else {
                break;
            };
            let data = Arc::clone(&data);
            tokio::spawn(async move {
                let service =
                    hyper::service::service_fn(move |request: Request<hyper::body::Incoming>| {
                        let data = Arc::clone(&data);
                        async move {
                            let total = data.len() as u64;
                            let range = request
                                .headers()
                                .get(hyper::header::RANGE)
                                .and_then(|value| value.to_str().ok())
                                .map(str::to_owned);
                            let (start, end) = match range
                                .as_deref()
                                .and_then(|value| value.strip_prefix("bytes="))
                            {
                                Some(spec) => {
                                    let mut parts = spec.split('-');
                                    let start = parts
                                        .next()
                                        .and_then(|v| v.parse::<u64>().ok())
                                        .unwrap_or(0);
                                    let end = parts
                                        .next()
                                        .and_then(|v| v.parse::<u64>().ok())
                                        .map(|end| end.min(total.saturating_sub(1)))
                                        .unwrap_or(total.saturating_sub(1));
                                    (start, end)
                                }
                                None => (0, total.saturating_sub(1)),
                            };
                            let slice = data
                                .get(
                                    start as usize
                                        ..=(end as usize).min(data.len().saturating_sub(1)),
                                )
                                .unwrap_or(&[])
                                .to_vec();
                            let mut response = Response::new(Full::new(Bytes::from(slice)));
                            response.headers_mut().insert(
                                hyper::header::CONTENT_TYPE,
                                HeaderValue::from_static(mime),
                            );
                            response.headers_mut().insert(
                                hyper::header::ACCEPT_RANGES,
                                HeaderValue::from_static("bytes"),
                            );
                            if range.is_some() {
                                *response.status_mut() = StatusCode::PARTIAL_CONTENT;
                                response.headers_mut().insert(
                                    hyper::header::CONTENT_RANGE,
                                    HeaderValue::from_str(&format!("bytes {start}-{end}/{total}"))
                                        .unwrap(),
                                );
                            }
                            Ok::<_, std::convert::Infallible>(response)
                        }
                    });
                let _ = hyper::server::conn::http1::Builder::new()
                    .serve_connection(hyper_util::rt::TokioIo::new(stream), service)
                    .await;
            });
        }
    });
    (addr, task)
}

#[tokio::test(flavor = "multi_thread")]
async fn blob_proxy_passes_ranges_and_names_downloads() {
    let data = mp4_header();
    let total = data.len();
    let (upstream, _task) = blob_upstream(data.clone(), "video/mp4").await;
    let options = Options {
        blob: Some(BlobAddr::new("127.0.0.1", upstream.port())),
        ..Options::default()
    };
    let api = Api::<Fake>::new(options);
    let server = api
        .bind("127.0.0.1:0".parse().unwrap())
        .await
        .expect("bind");
    let addr = server.addr();
    let (status, headers, body) = get_bytes(
        addr,
        "/v1/blob?key=aaa&blob=bbb&type=video%2Fmp4",
        &[("host", HOST)],
    )
    .await;
    assert_eq!(status, 200);
    assert_eq!(headers["content-type"], "video/mp4");
    assert_eq!(headers["access-control-allow-origin"], "*");
    assert_eq!(body, data);
    let (status, headers, body) = get_bytes(
        addr,
        "/v1/blob?key=aaa&blob=bbb&type=video%2Fmp4",
        &[("host", HOST), ("range", "bytes=0-3")],
    )
    .await;
    assert_eq!(status, 206);
    assert_eq!(headers["content-range"], format!("bytes 0-3/{total}"));
    assert_eq!(body, data[..4]);
    let (status, headers, _) = get(
        addr,
        "/v1/blob?key=aaa&blob=bbb&type=video%2Fmp4&download=My%20Film",
        &[("host", HOST)],
    )
    .await;
    assert_eq!(status, 200);
    assert_eq!(headers["content-type"], "video/mp4");
    assert_eq!(
        headers["content-disposition"],
        "attachment; filename=\"My Film.mp4\"; filename*=UTF-8''My%20Film.mp4"
    );
    server.close().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn stream_names_carry_the_resolution_read_from_the_blob_server() {
    let data = mp4_header();
    let (upstream, _task) = blob_upstream(data, "video/mp4").await;
    let stream = format!(
        "http://127.0.0.1:{}/?key=aaa&blob=bbb&type=video%2Fmp4",
        upstream.port()
    );
    let options = Options {
        blob: Some(BlobAddr::new("127.0.0.1", upstream.port())),
        ..Options::default()
    };
    let api = Api::with_node(options, Arc::new(Fake::new(&stream)));
    let server = api
        .bind("127.0.0.1:0".parse().unwrap())
        .await
        .expect("bind");
    let (status, _, body) = get(
        server.addr(),
        "/stream/movie/tt1234567.json",
        &[("host", HOST)],
    )
    .await;
    assert_eq!(status, 200);
    let value: Value = serde_json::from_str(&body).unwrap();
    assert_eq!(value["streams"][0]["name"], "PearTube (channel)\n1080p");
    assert_eq!(
        value["streams"][0]["url"],
        format!("http://{HOST}/v1/blob?key=aaa&blob=bbb&type=video%2Fmp4")
    );
    server.close().await;
}
