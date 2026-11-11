//! Durable HTTP/HTTPS acquirer, ported from PearTube's `src/acquire.js`.
//!
//! Jobs hold private source URLs and auth headers, so they stay inside a 0600 `jobs.json` and never leak into
//! Corestore where replication would serve them to any stranger who names the core. What the outside may see of a
//! job is `Job::public_json`: no source secrets, no announce payload, no per-video key.
//!
//! The node is driven through `node::Node`. The two things that contract deliberately leaves non-JSON — the
//! resumable put source and the process that downloads a page — are small traits (`PutStreamFactory`,
//! `PageDownloader`) so a test can script them.

use std::collections::HashMap;
use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::task::{Context, Poll};
use std::time::Duration;

use bytes::Bytes;
use http_body_util::Full;
use hyper::body::{Body, Incoming};
use hyper_util::client::legacy::Client;
use hyper_util::client::legacy::connect::HttpConnector;
use hyper_util::rt::TokioExecutor;
use rand::RngCore;
use serde_json::{Map, Value, json};
use tokio::io::{AsyncRead, ReadBuf};
use tokio::sync::Notify;
use tokio::time::{Instant, Sleep};

use crate::identify::{self, DownloadProgress, Downloaded};
use crate::node::Node;
use crate::node::api::PutMeta;
use crate::node::hash_state::base64_encode;

/// A source that will never recover without a person: 400/401/403/404/410/451.
pub const TERMINAL_STATUS: [u16; 6] = [400, 401, 403, 404, 410, 451];
/// The headers `merge_headers` starts from.
pub const DEFAULT_HEADERS: [(&str, &str); 3] = [
    ("user-agent", "VLC/3.0.18 LibVLC/3.0.18"),
    ("accept", "*/*"),
    ("accept-encoding", "identity"),
];

/// Why a source could not proceed. `status` and `retry_after` are its HTTP answer, when it gave one.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct SourceError {
    pub message: String,
    pub status: Option<u16>,
    pub retry_after: Option<Duration>,
}

impl SourceError {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            status: None,
            retry_after: None,
        }
    }
}

impl fmt::Display for SourceError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for SourceError {}

/// An acquirer failure: a source problem, one of the app's own messages (`Cancelled`, `Closed`, a truncated
/// transfer), or a source that changed under a resumed core.
#[derive(Debug, Clone, PartialEq)]
pub enum AcquireError {
    Source(SourceError),
    Message(String),
    Restart(String),
}

impl AcquireError {
    pub fn message(&self) -> String {
        match self {
            Self::Source(error) => error.message.clone(),
            Self::Message(message) | Self::Restart(message) => message.clone(),
        }
    }

    pub fn status(&self) -> Option<u16> {
        match self {
            Self::Source(error) => error.status,
            Self::Message(_) | Self::Restart(_) => None,
        }
    }

    pub fn retry_after(&self) -> Option<Duration> {
        match self {
            Self::Source(error) => error.retry_after,
            Self::Message(_) | Self::Restart(_) => None,
        }
    }

    /// The bytes on the core belong to another encode: the backend must restart, not continue.
    pub fn restart(&self) -> bool {
        matches!(self, Self::Restart(_))
    }
}

impl From<SourceError> for AcquireError {
    fn from(error: SourceError) -> Self {
        Self::Source(error)
    }
}

impl fmt::Display for AcquireError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message())
    }
}

impl std::error::Error for AcquireError {}

/// `AbortSignal` as `createAcquirer` uses it: aborting drops every open source and stops the run.
#[derive(Debug, Default)]
pub struct Cancel {
    aborted: AtomicBool,
    notify: Notify,
}

impl Cancel {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn abort(&self) {
        self.aborted.store(true, Ordering::SeqCst);
        self.notify.notify_waiters();
    }

    pub fn is_aborted(&self) -> bool {
        self.aborted.load(Ordering::SeqCst)
    }

    pub async fn aborted(&self) {
        while !self.is_aborted() {
            self.notify.notified().await;
        }
    }
}

/// `crypto.randomUUID` / `crypto.randomBytes`; a fake makes job ids and store keys predictable.
pub trait RandomSource: Send + Sync {
    /// A lowercase dashed UUID v4.
    fn uuid_v4(&self) -> String;
    fn bytes(&self, len: usize) -> Vec<u8>;
}

/// The platform RNG.
pub struct OsRandom;

impl RandomSource for OsRandom {
    fn uuid_v4(&self) -> String {
        let mut bytes = [0u8; 16];
        rand::rngs::OsRng.fill_bytes(&mut bytes);
        bytes[6] = (bytes[6] & 0x0f) | 0x40;
        bytes[8] = (bytes[8] & 0x3f) | 0x80;
        let hex = hex(&bytes);
        format!(
            "{}-{}-{}-{}-{}",
            &hex[0..8],
            &hex[8..12],
            &hex[12..16],
            &hex[16..20],
            &hex[20..32]
        )
    }

    fn bytes(&self, len: usize) -> Vec<u8> {
        let mut bytes = vec![0u8; len];
        rand::rngs::OsRng.fill_bytes(&mut bytes);
        bytes
    }
}

/// `mergeHeaders`: the app's defaults, then each set in order, lowercased and stringified.
pub fn merge_headers(sets: &[&[(String, String)]]) -> Vec<(String, String)> {
    let mut out: Vec<(String, String)> = DEFAULT_HEADERS
        .iter()
        .map(|(key, value)| ((*key).to_owned(), (*value).to_owned()))
        .collect();
    for entry in sets.iter().flat_map(|set| set.iter()) {
        // Header names are case-insensitive: a user's `Authorization` must replace, not double, and must be
        // stripped on a cross-origin redirect like the app's lowercased `mergeHeaders`.
        let key = entry.0.to_ascii_lowercase();
        match out.iter_mut().find(|(kept, _)| *kept == key) {
            Some(slot) => slot.1 = entry.1.clone(),
            None => out.push((key, entry.1.clone())),
        }
    }
    out
}

/// Header lookup is case-insensitive, as Node hands headers over lowercased.
pub fn header<'a>(headers: &'a [(String, String)], name: &str) -> Option<&'a str> {
    headers
        .iter()
        .find(|(key, _)| key.eq_ignore_ascii_case(name))
        .map(|(_, value)| value.as_str())
}

/// The strong ETag a reply carries: a weak one (`W/`) cannot prove two halves are the same bytes.
fn strong_etag(headers: &[(String, String)]) -> Option<String> {
    header(headers, "etag")
        .filter(|etag| !etag.starts_with("W/"))
        .map(str::to_owned)
}

/// The offset a `content-range: bytes N-...` header answers with, the app's `/^bytes\s+(\d+)-/i`.
pub fn content_range_start(value: &str) -> Option<u64> {
    let head = value.get(..5)?;
    if !head.eq_ignore_ascii_case("bytes") {
        return None;
    }
    let spaced = &value[5..];
    let rest = spaced.trim_start_matches(char::is_whitespace);
    if rest.len() == spaced.len() {
        return None;
    }
    let digits: String = rest.chars().take_while(char::is_ascii_digit).collect();
    if digits.is_empty() || !rest[digits.len()..].starts_with('-') {
        return None;
    }
    digits.parse().ok()
}

fn hex(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push_str(&format!("{byte:02x}"));
    }
    out
}

fn js_number(text: &str) -> f64 {
    let trimmed = text.trim();
    if trimmed.is_empty() {
        return 0.0;
    }
    trimmed.parse().unwrap_or(f64::NAN)
}

/// Where a job streams from, plus its private headers.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct SourceSpec {
    pub url: String,
    pub headers: Vec<(String, String)>,
    /// A page link (`ubit get`) rather than a direct media URL.
    pub page: bool,
    pub private: bool,
}

/// The job states `jobs.json` records.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JobStatus {
    Queued,
    Running,
    Announcing,
    Done,
    Failed,
    Cancelled,
    Removed,
}

impl JobStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Queued => "queued",
            Self::Running => "running",
            Self::Announcing => "announcing",
            Self::Done => "done",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
            Self::Removed => "removed",
        }
    }

    fn parse(text: &str) -> Option<Self> {
        Some(match text {
            "queued" => Self::Queued,
            "running" => Self::Running,
            "announcing" => Self::Announcing,
            "done" => Self::Done,
            "failed" => Self::Failed,
            "cancelled" => Self::Cancelled,
            "removed" => Self::Removed,
            _ => return None,
        })
    }
}

/// One acquisition job. `total`, `error` and `hash_state` distinguish absent from JSON null, as the app does.
#[derive(Debug, Clone)]
pub struct Job {
    pub job_id: String,
    pub id: String,
    pub title: String,
    pub plugin: String,
    pub channel: bool,
    pub private: bool,
    pub status: JobStatus,
    pub created: u64,
    pub store: Option<String>,
    pub source: Option<SourceSpec>,
    pub total: Option<Option<u64>>,
    pub bytes: Option<u64>,
    pub etag: Option<Value>,
    pub last_modified: Option<Value>,
    pub hash_state: Option<Value>,
    pub hash_bytes: Option<u64>,
    pub store_key: Option<String>,
    pub announce: Option<Value>,
    pub error: Option<Value>,
    pub attempts: Option<u64>,
    pub size: Option<u64>,
    pub sha256: Option<String>,
    pub extra: Map<String, Value>,
}

impl Job {
    fn new(spec: NewJob) -> Self {
        Self {
            job_id: spec.job_id,
            id: spec.id,
            title: spec.title,
            plugin: spec.plugin,
            channel: spec.channel,
            private: spec.private,
            status: JobStatus::Queued,
            created: spec.created,
            store: Some(spec.store),
            source: Some(spec.source),
            total: None,
            bytes: None,
            etag: None,
            last_modified: None,
            hash_state: None,
            hash_bytes: None,
            store_key: spec.store_key,
            announce: None,
            error: None,
            attempts: None,
            size: None,
            sha256: None,
            extra: Map::new(),
        }
    }

    /// The whole job, as `jobs.json` holds it.
    pub fn to_json(&self) -> Value {
        let mut out = Map::new();
        out.insert("jobId".into(), json!(self.job_id));
        out.insert("id".into(), json!(self.id));
        out.insert("title".into(), json!(self.title));
        out.insert("plugin".into(), json!(self.plugin));
        out.insert("channel".into(), json!(self.channel));
        out.insert("private".into(), json!(self.private));
        out.insert("status".into(), json!(self.status.as_str()));
        out.insert("created".into(), json!(self.created));
        if let Some(store) = &self.store {
            out.insert("store".into(), json!(store));
        }
        // The app keeps the key after a job's source credentials are dropped (`source = null`).
        out.insert(
            "source".into(),
            self.source.as_ref().map_or(Value::Null, source_json),
        );
        if let Some(total) = self.total {
            out.insert("total".into(), total.map_or(Value::Null, |n| json!(n)));
        }
        if let Some(bytes) = self.bytes {
            out.insert("bytes".into(), json!(bytes));
        }
        if let Some(etag) = &self.etag {
            out.insert("etag".into(), etag.clone());
        }
        if let Some(last_modified) = &self.last_modified {
            out.insert("lastModified".into(), last_modified.clone());
        }
        if let Some(hash_state) = &self.hash_state {
            out.insert("hashState".into(), hash_state.clone());
        }
        if let Some(hash_bytes) = self.hash_bytes {
            out.insert("hashBytes".into(), json!(hash_bytes));
        }
        if let Some(store_key) = &self.store_key {
            out.insert("storeKey".into(), json!(store_key));
        }
        if let Some(announce) = &self.announce {
            out.insert("announce".into(), announce.clone());
        }
        if let Some(error) = &self.error {
            out.insert("error".into(), error.clone());
        }
        if let Some(attempts) = self.attempts {
            out.insert("attempts".into(), json!(attempts));
        }
        if let Some(size) = self.size {
            out.insert("size".into(), json!(size));
        }
        if let Some(sha256) = &self.sha256 {
            out.insert("sha256".into(), json!(sha256));
        }
        for (key, value) in &self.extra {
            out.entry(key.clone()).or_insert_with(|| value.clone());
        }
        Value::Object(out)
    }

    /// `publicJob`: the job without the source URL, its auth headers, the announce or the per-video key.
    pub fn public_json(&self) -> Value {
        let mut out = self.to_json();
        if let Some(map) = out.as_object_mut() {
            for key in [
                "source",
                "announce",
                "storeKey",
                "etag",
                "lastModified",
                "hashState",
                "hashBytes",
            ] {
                map.remove(key);
            }
        }
        out
    }

    fn from_json(value: &Value) -> Option<Self> {
        let map = value.as_object()?;
        let text = |key: &str| map.get(key).and_then(Value::as_str).map(str::to_owned);
        let status = text("status")
            .as_deref()
            .and_then(JobStatus::parse)
            .unwrap_or(JobStatus::Queued);
        let private = map.get("private").and_then(Value::as_bool).unwrap_or(false);
        let source = match map.get("source") {
            None | Some(Value::Null) => None,
            Some(source) => Some(SourceSpec {
                url: source
                    .get("url")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_owned(),
                headers: source
                    .get("headers")
                    .and_then(Value::as_object)
                    .map(|headers| {
                        headers
                            .iter()
                            .map(|(key, value)| (key.clone(), js_string(value)))
                            .collect()
                    })
                    .unwrap_or_default(),
                page: source.get("page").and_then(Value::as_bool).unwrap_or(false),
                private,
            }),
        };
        let total = match map.get("total") {
            None => None,
            Some(Value::Null) => Some(None),
            Some(value) => Some(value.as_u64()),
        };
        let known = [
            "jobId",
            "id",
            "title",
            "plugin",
            "channel",
            "private",
            "status",
            "created",
            "store",
            "source",
            "total",
            "bytes",
            "etag",
            "lastModified",
            "hashState",
            "hashBytes",
            "storeKey",
            "announce",
            "error",
            "attempts",
            "size",
            "sha256",
        ];
        Some(Self {
            job_id: text("jobId").unwrap_or_default(),
            id: text("id").unwrap_or_default(),
            title: text("title").unwrap_or_default(),
            plugin: text("plugin").unwrap_or_else(|| "http-acquire".to_owned()),
            channel: map.get("channel").and_then(Value::as_bool).unwrap_or(false),
            private,
            status,
            created: map.get("created").and_then(Value::as_u64).unwrap_or(0),
            store: text("store"),
            source,
            total,
            bytes: map.get("bytes").and_then(Value::as_u64),
            etag: map.get("etag").cloned(),
            last_modified: map.get("lastModified").cloned(),
            hash_state: map.get("hashState").cloned(),
            hash_bytes: map.get("hashBytes").and_then(Value::as_u64),
            store_key: text("storeKey"),
            announce: map.get("announce").cloned(),
            error: map.get("error").cloned(),
            attempts: map.get("attempts").and_then(Value::as_u64),
            size: map.get("size").and_then(Value::as_u64),
            sha256: text("sha256"),
            extra: map
                .iter()
                .filter(|(key, _)| !known.contains(&key.as_str()))
                .map(|(key, value)| (key.clone(), value.clone()))
                .collect(),
        })
    }
}

/// `publicJob(job)` over a raw `jobs.json` entry: what the API shows before an acquirer opens.
pub fn public_job(value: &Value) -> Option<Value> {
    Job::from_json(value).map(|job| job.public_json())
}

fn js_string(value: &Value) -> String {
    match value {
        Value::String(text) => text.clone(),
        other => other.to_string(),
    }
}

/// A `SourceSpec` back into the `source` object `jobs.json` holds.
pub fn source_json(source: &SourceSpec) -> Value {
    let mut headers = Map::new();
    for (key, value) in &source.headers {
        headers.insert(key.clone(), json!(value));
    }
    json!({"url": source.url, "headers": headers, "page": source.page})
}

struct NewJob {
    job_id: String,
    id: String,
    title: String,
    plugin: String,
    channel: bool,
    private: bool,
    created: u64,
    store: String,
    source: SourceSpec,
    store_key: Option<String>,
}

/// What `add` is asked for: `{ id, title, source, channel?, private?, plugin? }`.
#[derive(Debug, Clone)]
pub struct AddJob {
    pub id: String,
    pub title: String,
    pub source: SourceSpec,
    pub channel: Option<bool>,
    pub private: bool,
    pub plugin: Option<String>,
}

/// The files a job needs. `jobs.json` is always written through a 0600 temp file and renamed.
pub trait JobFiles: Send + Sync {
    fn read_jobs(&self) -> Option<String>;
    fn write_jobs(&self, json: &str) -> Result<(), String>;
    /// Remove `ubit-dl-*` work directories left by a previous boot.
    fn clean_temp(&self);
    fn make_temp_dir(&self) -> Result<String, String>;
    fn remove_dir(&self, path: &str);
    fn file_size(&self, path: &str) -> Option<u64>;
    fn open_read(&self, path: &str) -> Result<Box<dyn AsyncRead + Send + Unpin>, String>;
    /// `join(dirname(jobs.json), 'ubit')`.
    fn config_dir(&self) -> Option<String> {
        None
    }
}

/// The real layout: `jobs.json` beside its `ubit/` config and `ubit-dl-*` work directories.
pub struct LocalJobFiles {
    pub work_root: std::path::PathBuf,
    pub file: std::path::PathBuf,
}

impl LocalJobFiles {
    pub fn new(file: impl Into<std::path::PathBuf>) -> Self {
        let file = file.into();
        let work_root = file
            .parent()
            .map(std::path::Path::to_path_buf)
            .unwrap_or_default();
        Self { work_root, file }
    }
}

impl JobFiles for LocalJobFiles {
    fn read_jobs(&self) -> Option<String> {
        std::fs::read_to_string(&self.file).ok()
    }

    fn write_jobs(&self, json: &str) -> Result<(), String> {
        use std::io::Write;
        let mut tmp = self.file.clone().into_os_string();
        tmp.push(".tmp");
        let tmp = std::path::PathBuf::from(tmp);
        let mut options = std::fs::OpenOptions::new();
        options.write(true).create(true).truncate(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let mut handle = options.open(&tmp).map_err(|error| error.to_string())?;
        handle
            .write_all(json.as_bytes())
            .map_err(|error| error.to_string())?;
        std::fs::rename(&tmp, &self.file).map_err(|error| error.to_string())
    }

    fn clean_temp(&self) {
        let Ok(entries) = std::fs::read_dir(&self.work_root) else {
            return;
        };
        for entry in entries.flatten() {
            if entry.file_name().to_string_lossy().starts_with("ubit-dl-") {
                let _ = std::fs::remove_dir_all(entry.path());
            }
        }
    }

    fn make_temp_dir(&self) -> Result<String, String> {
        let mut bytes = [0u8; 6];
        rand::rngs::OsRng.fill_bytes(&mut bytes);
        let mut path = self.work_root.clone();
        path.push(format!("ubit-dl-{}", hex(&bytes)));
        std::fs::create_dir_all(&path).map_err(|error| error.to_string())?;
        Ok(path.to_string_lossy().into_owned())
    }

    fn remove_dir(&self, path: &str) {
        let _ = std::fs::remove_dir_all(path);
    }

    fn file_size(&self, path: &str) -> Option<u64> {
        std::fs::metadata(path).ok().map(|meta| meta.len())
    }

    fn open_read(&self, path: &str) -> Result<Box<dyn AsyncRead + Send + Unpin>, String> {
        let file = std::fs::File::open(path).map_err(|error| error.to_string())?;
        Ok(Box::new(tokio::fs::File::from_std(file)))
    }

    fn config_dir(&self) -> Option<String> {
        Some(self.work_root.join("ubit").to_string_lossy().into_owned())
    }
}

/// One HTTP request for a source; `timeout` covers the wait for a reply.
#[derive(Debug, Clone)]
pub struct SourceRequest {
    pub url: String,
    pub headers: Vec<(String, String)>,
    pub timeout: Duration,
}

/// A source's reply; the body streams so a resumed transfer never buffers the film.
pub struct SourceResponse {
    pub status: u16,
    pub headers: Vec<(String, String)>,
    pub url: String,
    pub body: Box<dyn AsyncRead + Send + Unpin>,
}

/// One hop of a source request. Redirects and status policy live in `open_source`, as in the app.
pub trait HttpOpen: Send + Sync {
    fn open(
        &self,
        request: SourceRequest,
    ) -> Pin<Box<dyn Future<Output = Result<SourceResponse, SourceError>> + Send>>;
}

/// The real client: GET, the given headers, a TLS root store.
pub struct HyperHttp;

impl HttpOpen for HyperHttp {
    fn open(
        &self,
        request: SourceRequest,
    ) -> Pin<Box<dyn Future<Output = Result<SourceResponse, SourceError>> + Send>> {
        Box::pin(async move {
            let Ok(uri) = url::Url::parse(&request.url) else {
                return Err(SourceError::new(format!("Invalid URL: {}", request.url)));
            };
            let mut builder = hyper::Request::builder()
                .method(hyper::Method::GET)
                .uri(uri.as_str());
            {
                let Some(headers) = builder.headers_mut() else {
                    return Err(SourceError::new("Invalid URL"));
                };
                for (name, value) in &request.headers {
                    let (Ok(name), Ok(value)) = (
                        hyper::header::HeaderName::from_bytes(name.as_bytes()),
                        hyper::header::HeaderValue::from_str(value),
                    ) else {
                        continue;
                    };
                    headers.insert(name, value);
                }
            }
            let timeout = request.timeout;
            let hyper_request = builder
                .body(Full::new(Bytes::new()))
                .map_err(|error| SourceError::new(error.to_string()))?;
            let response = tokio::time::timeout(timeout, HOP_CLIENT.request(hyper_request))
                .await
                .map_err(|_| SourceError::new("Source timed out"))?
                .map_err(|error| SourceError::new(error.to_string()))?;
            let status = response.status().as_u16();
            let headers = response
                .headers()
                .iter()
                .map(|(name, value)| {
                    (
                        name.as_str().to_ascii_lowercase(),
                        value.to_str().unwrap_or_default().to_owned(),
                    )
                })
                .collect();
            Ok(SourceResponse {
                status,
                headers,
                url: uri.as_str().to_owned(),
                body: Box::new(BodyReader {
                    body: response.into_body(),
                    pending: Bytes::new(),
                }),
            })
        })
    }
}

type Hops = hyper_rustls::HttpsConnector<HttpConnector>;

static HOP_CLIENT: std::sync::LazyLock<Client<Hops, Full<Bytes>>> =
    std::sync::LazyLock::new(|| {
        let https = hyper_rustls::HttpsConnectorBuilder::new()
            .with_webpki_roots()
            .https_or_http()
            .enable_http1()
            .build();
        Client::builder(TokioExecutor::new()).build(https)
    });

/// A hyper body as `AsyncRead`, copying each frame straight into the caller's buffer.
struct BodyReader<B = Incoming> {
    body: B,
    /// The tail of a frame larger than the caller's buffer; dropping it would lose bytes.
    pending: Bytes,
}

impl<B> AsyncRead for BodyReader<B>
where
    B: Body<Data = Bytes> + Unpin,
    B::Error: std::fmt::Display,
{
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        let this = self.get_mut();
        loop {
            if !this.pending.is_empty() {
                let take = this.pending.len().min(buf.remaining());
                buf.put_slice(&this.pending.split_to(take));
                return Poll::Ready(Ok(()));
            }
            if buf.remaining() == 0 {
                return Poll::Ready(Ok(()));
            }
            match Pin::new(&mut this.body).poll_frame(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(None) => return Poll::Ready(Ok(())),
                Poll::Ready(Some(Err(error))) => {
                    return Poll::Ready(Err(std::io::Error::other(error.to_string())));
                }
                Poll::Ready(Some(Ok(frame))) => {
                    if let Ok(data) = frame.into_data()
                        && !data.is_empty()
                    {
                        this.pending = data;
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod body_reader_tests {
    use super::*;
    use tokio::io::AsyncReadExt;

    /// A frame larger than the caller's buffer must be drained, never dropped: a lost tail reads
    /// as a truncated transfer and fails the job.
    #[tokio::test]
    async fn a_frame_larger_than_the_read_buffer_is_not_lost() {
        let data = Bytes::from(
            (0..1024 * 1024)
                .map(|index| index as u8)
                .collect::<Vec<u8>>(),
        );
        let mut reader = BodyReader {
            body: Full::new(data.clone()),
            pending: Bytes::new(),
        };
        let mut chunk = vec![0u8; 4096];
        let mut total = Vec::new();
        loop {
            let read = reader.read(&mut chunk).await.expect("read");
            if read == 0 {
                break;
            }
            total.extend_from_slice(&chunk[..read]);
        }
        assert_eq!(total, data.to_vec());
    }
}

#[cfg(test)]
mod header_tests {
    use super::*;
    use std::collections::VecDeque;

    /// One scripted hop: the status it answers with and the headers that ride it.
    type Hop = (u16, Vec<(String, String)>);

    /// A scripted source: records each hop's headers and answers from a queue.
    struct Script {
        seen: Mutex<Vec<Vec<(String, String)>>>,
        replies: Mutex<VecDeque<Hop>>,
    }

    impl Script {
        fn new(replies: Vec<Hop>) -> Arc<Self> {
            Arc::new(Self {
                seen: Mutex::new(Vec::new()),
                replies: Mutex::new(replies.into()),
            })
        }
    }

    impl HttpOpen for Script {
        fn open(
            &self,
            request: SourceRequest,
        ) -> Pin<Box<dyn Future<Output = Result<SourceResponse, SourceError>> + Send>> {
            self.seen
                .lock()
                .expect("seen")
                .push(request.headers.clone());
            let (status, headers) = self
                .replies
                .lock()
                .expect("replies")
                .pop_front()
                .unwrap_or((200, Vec::new()));
            let url = request.url;
            Box::pin(async move {
                Ok(SourceResponse {
                    status,
                    headers,
                    url,
                    body: Box::new(std::io::Cursor::new(Vec::<u8>::new())),
                })
            })
        }
    }

    /// A range resume accepts only the app's shape: `bytes N-...`, with the space and the dash.
    #[test]
    fn content_range_start_matches_the_app_regex() {
        assert_eq!(content_range_start("bytes 4-9/10"), Some(4));
        assert_eq!(content_range_start("BYTES 0-"), Some(0));
        assert_eq!(content_range_start("bytes  12-13/20"), Some(12));
        assert_eq!(content_range_start("bytes4-9/10"), None);
        assert_eq!(content_range_start("bytes"), None);
        assert_eq!(content_range_start("bytes -9/10"), None);
        assert_eq!(content_range_start("bytes 4"), None);
        assert_eq!(content_range_start("items 4-9/10"), None);
    }

    /// A user header replaces the default case-insensitively, as the app's `mergeHeaders` lowercases keys.
    #[test]
    fn a_user_header_replaces_the_default_case_insensitively() {
        let merged = merge_headers(&[&[("User-Agent".to_owned(), "VLC/4".to_owned())]]);
        assert_eq!(header(&merged, "user-agent"), Some("VLC/4"));
        assert_eq!(
            merged
                .iter()
                .filter(|(name, _)| name == "user-agent")
                .count(),
            1,
            "one user-agent, not the default plus the override"
        );
    }

    /// A cross-origin redirect must not carry an auth header a caller spelled with a capital A.
    #[tokio::test]
    async fn a_cross_origin_redirect_strips_the_authorization_header() {
        let script = Script::new(vec![
            (
                302,
                vec![(
                    "location".to_owned(),
                    "http://cdn.example/video.mkv".to_owned(),
                )],
            ),
            (200, Vec::new()),
        ]);
        let headers = merge_headers(&[&[(
            "Authorization".to_owned(),
            "Bearer one-shot-secret".to_owned(),
        )]]);
        open_source(
            &*script,
            "http://gateway.example/source",
            &headers,
            &Cancel::new(),
            0,
            Duration::from_secs(5),
        )
        .await
        .expect("the redirect resolves");
        let seen = script.seen.lock().expect("seen");
        assert_eq!(
            header(&seen[0], "authorization"),
            Some("Bearer one-shot-secret")
        );
        assert_eq!(
            header(&seen[1], "authorization"),
            None,
            "auth never crosses origins"
        );
    }
}

/// Resolve redirects (auth/cookie stripped across origins) and turn a non-2xx answer into a tagged error.
async fn open_source(
    http: &dyn HttpOpen,
    url: &str,
    headers: &[(String, String)],
    cancel: &Cancel,
    mut redirects: u8,
    timeout: Duration,
) -> Result<SourceResponse, AcquireError> {
    let mut current =
        url::Url::parse(url).map_err(|_| AcquireError::Message("Invalid URL".into()))?;
    let mut headers = headers.to_vec();
    loop {
        if cancel.is_aborted() {
            return Err(AcquireError::Message("Cancelled".into()));
        }
        let response = http
            .open(SourceRequest {
                url: current.as_str().to_owned(),
                headers: headers.clone(),
                timeout,
            })
            .await?;
        let location = header(&response.headers, "location").map(str::to_owned);
        if (300..400).contains(&response.status) && location.is_some() && redirects < 10 {
            let location = location.unwrap_or_default();
            let next = current
                .join(&location)
                .map_err(|_| AcquireError::Message("Invalid URL".into()))?;
            if next.origin() != current.origin() {
                headers.retain(|(key, _)| {
                    !key.eq_ignore_ascii_case("authorization")
                        && !key.eq_ignore_ascii_case("cookie")
                });
            }
            current = next;
            redirects += 1;
            continue;
        }
        if !(200..300).contains(&response.status) {
            let retry_after = header(&response.headers, "retry-after")
                .map(|value| js_number(value) * 1000.0)
                .filter(|ms| ms.is_finite() && *ms > 0.0)
                .map(|ms| Duration::from_millis(ms as u64));
            return Err(AcquireError::Source(SourceError {
                message: format!("Source answered {}", response.status),
                status: Some(response.status),
                retry_after,
            }));
        }
        return Ok(SourceResponse {
            url: current.as_str().to_owned(),
            ..response
        });
    }
}

/// One in-flight source open, awaited by the reader that needs its head.
type PendingResponse = Pin<Box<dyn Future<Output = Result<SourceResponse, AcquireError>> + Send>>;

/// A stalled or dropped mid-film stream, re-stitched with `Range: bytes=offset-`.
///
/// Range resumes may force upstream (e.g. a Usenet skipReader) to chew through several MiB before emitting byte 1,
/// so the pre-first-byte leash widens on each retry and snaps back to `stall` once bytes flow.
pub struct Resumable {
    http: Arc<dyn HttpOpen>,
    url: String,
    base_headers: Vec<(String, String)>,
    total: Option<u64>,
    etag: Option<String>,
    offset: u64,
    conn_start: u64,
    reconnects: u32,
    current: Option<Box<dyn AsyncRead + Send + Unpin>>,
    pending: Option<PendingResponse>,
    delay: Option<Pin<Box<Sleep>>>,
    deadline: Option<Pin<Box<Sleep>>>,
    last_error: Option<String>,
    retry_after: Option<Duration>,
    cancel: Arc<Cancel>,
    closed: Arc<AtomicBool>,
    stall: Duration,
    retry_delay: Duration,
    max_reconnects: u32,
    job_id: String,
    eof: bool,
}

/// What a resumable read needs before its first byte.
#[allow(clippy::too_many_arguments)]
struct ResumableConfig {
    http: Arc<dyn HttpOpen>,
    url: String,
    base_headers: Vec<(String, String)>,
    total: Option<u64>,
    etag: Option<String>,
    offset: u64,
    cancel: Arc<Cancel>,
    closed: Arc<AtomicBool>,
    stall: Duration,
    retry_delay: Duration,
    max_reconnects: u32,
    job_id: String,
}

impl Resumable {
    fn new(config: ResumableConfig, first: SourceResponse) -> Self {
        Self {
            http: config.http,
            url: config.url,
            base_headers: config.base_headers,
            total: config.total,
            etag: config.etag,
            offset: config.offset,
            conn_start: config.offset,
            reconnects: 0,
            current: Some(first.body),
            pending: None,
            delay: None,
            deadline: None,
            last_error: None,
            retry_after: None,
            cancel: config.cancel,
            closed: config.closed,
            stall: config.stall,
            retry_delay: config.retry_delay,
            max_reconnects: config.max_reconnects,
            job_id: config.job_id,
            eof: false,
        }
    }

    fn seek_timeout(&self) -> Duration {
        self.stall * (self.reconnects + 2).min(6)
    }

    fn cancelled(&self) -> bool {
        self.closed.load(Ordering::SeqCst) || self.cancel.is_aborted()
    }

    /// The stream ended or failed with bytes already on the core: reconnect, or give up loudly.
    fn after_connection(&mut self) -> Result<(), AcquireError> {
        if self.offset > self.conn_start {
            self.reconnects = 0;
            return Ok(());
        }
        self.reconnects += 1;
        if self.reconnects > self.max_reconnects {
            let total = self
                .total
                .map_or_else(|| "null".to_owned(), |n| n.to_string());
            let suffix = self
                .last_error
                .as_ref()
                .map(|error| format!(" ({error})"))
                .unwrap_or_default();
            return Err(AcquireError::Message(format!(
                "Truncated transfer for {}: expected {total} bytes, got {}{suffix}",
                self.job_id, self.offset
            )));
        }
        Ok(())
    }

    fn request_headers(&self) -> Vec<(String, String)> {
        let mut headers = self.base_headers.clone();
        headers.push(("range".into(), format!("bytes={}-", self.offset)));
        if let Some(etag) = &self.etag {
            headers.push(("if-range".into(), etag.clone()));
        }
        headers
    }

    fn backoff(&self) -> Duration {
        let base = if self.retry_delay.is_zero() {
            250u64
        } else {
            self.retry_delay.as_millis() as u64
        };
        let growth = 250u64.saturating_mul(1u64 << self.reconnects.min(6));
        Duration::from_millis(base.min(growth))
    }

    /// The pause before the next reconnect; a server's `retry-after` wins.
    fn wait_before_reconnect(&mut self) {
        let delay = self.retry_after.take().unwrap_or_else(|| self.backoff());
        arm(&mut self.delay, delay);
    }

    fn truncated(&self) -> std::io::Error {
        let total = self
            .total
            .map_or_else(|| "null".to_owned(), |n| n.to_string());
        std::io::Error::other(format!(
            "Truncated transfer for {}: expected {total} bytes, got {}",
            self.job_id, self.offset
        ))
    }
}

/// Re-arm a stored timer instead of allocating a new one for every chunk.
fn arm(slot: &mut Option<Pin<Box<Sleep>>>, after: Duration) {
    let at = Instant::now() + after;
    match slot {
        Some(sleep) => sleep.as_mut().reset(at),
        None => *slot = Some(Box::pin(tokio::time::sleep_until(at))),
    }
}

impl AsyncRead for Resumable {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        let this = self.get_mut();
        if this.eof {
            return Poll::Ready(Ok(()));
        }
        loop {
            if this.cancelled() {
                return Poll::Ready(Err(std::io::Error::other("Cancelled")));
            }
            if this.current.is_none() {
                if let Some(delay) = this.delay.as_mut() {
                    if delay.as_mut().poll(cx).is_pending() {
                        return Poll::Pending;
                    }
                    this.delay = None;
                }
                if this.pending.is_none() {
                    let http = this.http.clone();
                    let url = this.url.clone();
                    let headers = this.request_headers();
                    let cancel = this.cancel.clone();
                    let timeout = this.seek_timeout();
                    this.pending = Some(Box::pin(async move {
                        open_source(&*http, &url, &headers, &cancel, 0, timeout).await
                    }));
                }
                let pending = this.pending.as_mut().expect("set above");
                match pending.as_mut().poll(cx) {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(Ok(response)) => {
                        this.pending = None;
                        let start = header(&response.headers, "content-range")
                            .and_then(content_range_start);
                        if response.status != 206 || start != Some(this.offset) {
                            return Poll::Ready(Err(this.truncated()));
                        }
                        this.current = Some(response.body);
                        this.conn_start = this.offset;
                        let leash = this.seek_timeout();
                        arm(&mut this.deadline, leash);
                    }
                    Poll::Ready(Err(error)) => {
                        this.pending = None;
                        if this.cancelled() {
                            return Poll::Ready(Err(std::io::Error::other("Cancelled")));
                        }
                        this.reconnects += 1;
                        if error.status().is_some_and(|s| TERMINAL_STATUS.contains(&s))
                            || this.reconnects > this.max_reconnects
                        {
                            return Poll::Ready(Err(std::io::Error::other(error.message())));
                        }
                        this.last_error = Some(error.message());
                        this.retry_after = error.retry_after();
                        this.wait_before_reconnect();
                    }
                }
                continue;
            }
            if this.deadline.is_none() {
                let leash = if this.offset > 0 {
                    this.seek_timeout()
                } else {
                    this.stall
                };
                arm(&mut this.deadline, leash);
            }
            let current = this.current.as_mut().expect("checked above");
            match Pin::new(&mut **current).poll_read(cx, buf) {
                Poll::Pending => {
                    let stalled = this
                        .deadline
                        .as_mut()
                        .is_some_and(|deadline| deadline.as_mut().poll(cx).is_ready());
                    if !stalled {
                        return Poll::Pending;
                    }
                    this.current = None;
                    this.deadline = None;
                    if this.total.is_none() || this.offset == 0 {
                        return Poll::Ready(Err(std::io::Error::other("Source stalled")));
                    }
                    this.last_error = Some("Source stalled".into());
                    if let Err(error) = this.after_connection() {
                        return Poll::Ready(Err(std::io::Error::other(error.message())));
                    }
                    this.wait_before_reconnect();
                }
                Poll::Ready(Ok(())) => {
                    let read = buf.filled().len();
                    if read > 0 {
                        this.offset += read as u64;
                        this.reconnects = 0;
                        arm(&mut this.deadline, this.stall);
                        return Poll::Ready(Ok(()));
                    }
                    this.current = None;
                    this.deadline = None;
                    if this.total.is_none() || this.offset >= this.total.unwrap_or(0) {
                        this.eof = true;
                        return Poll::Ready(Ok(()));
                    }
                    if let Err(error) = this.after_connection() {
                        return Poll::Ready(Err(std::io::Error::other(error.message())));
                    }
                    this.wait_before_reconnect();
                }
                Poll::Ready(Err(error)) => {
                    this.current = None;
                    this.deadline = None;
                    if this.cancelled() {
                        return Poll::Ready(Err(std::io::Error::other("Cancelled")));
                    }
                    if this.total.is_none() || this.offset == 0 {
                        return Poll::Ready(Err(std::io::Error::other(error.to_string())));
                    }
                    this.last_error = Some(error.to_string());
                    if let Err(error) = this.after_connection() {
                        return Poll::Ready(Err(std::io::Error::other(error.message())));
                    }
                    this.wait_before_reconnect();
                }
            }
        }
    }
}

/// The job state a running source reports into.
pub trait JobSink: Send + Sync {
    fn on_bytes(&self, bytes: u64);
    /// The source's own failure, so a backend error that loses it still dead-letters a 404.
    fn on_source_error(&self, error: &AcquireError);
    fn on_hash_state(&self, state: &[u8], bytes: u64);
    /// The first response's size and validators, when a transfer starts from zero.
    fn set_response(&self, total: Option<u64>, etag: Option<String>, last_modified: Option<String>);
}

enum StreamKind {
    Http,
    File(String),
    Stream(Option<Box<dyn AsyncRead + Send + Unpin>>),
}

/// What `node.put` streams: a fresh or resumed source, as `node-api.md` describes.
pub struct JobSource<E> {
    sink: Arc<dyn JobSink>,
    kind: StreamKind,
    http: Option<Arc<dyn HttpOpen>>,
    files: Option<Arc<dyn JobFiles>>,
    spec: Option<SourceSpec>,
    etag: Option<String>,
    last_modified: Option<String>,
    cancel: Arc<Cancel>,
    closed: Arc<AtomicBool>,
    stall: Duration,
    retry_delay: Duration,
    max_reconnects: u32,
    job_id: String,
    _error: std::marker::PhantomData<E>,
}

impl<E: From<AcquireError> + Send> JobSource<E> {
    /// A resumable HTTP source.
    #[allow(clippy::too_many_arguments)]
    pub fn http(
        sink: Arc<dyn JobSink>,
        http: Arc<dyn HttpOpen>,
        files: Arc<dyn JobFiles>,
        job_id: String,
        spec: SourceSpec,
        etag: Option<String>,
        last_modified: Option<String>,
        cancel: Arc<Cancel>,
        closed: Arc<AtomicBool>,
        stall: Duration,
        retry_delay: Duration,
        max_reconnects: u32,
    ) -> Self {
        Self {
            sink,
            kind: StreamKind::Http,
            http: Some(http),
            files: Some(files),
            spec: Some(spec),
            etag,
            last_modified,
            cancel,
            closed,
            stall,
            retry_delay,
            max_reconnects,
            job_id,
            _error: std::marker::PhantomData,
        }
    }

    /// An already-open stream (`mirror.js`'s `ctx.ingestStream`).
    pub fn stream(reader: Box<dyn AsyncRead + Send + Unpin>) -> Self {
        Self {
            sink: Arc::new(NoopSink),
            kind: StreamKind::Stream(Some(reader)),
            http: None,
            files: None,
            spec: None,
            etag: None,
            last_modified: None,
            cancel: Arc::new(Cancel::new()),
            closed: Arc::new(AtomicBool::new(false)),
            stall: Duration::from_secs(30),
            retry_delay: Duration::ZERO,
            max_reconnects: 8,
            job_id: String::new(),
            _error: std::marker::PhantomData,
        }
    }

    /// A finished file (`ubit get` output) or any local stream.
    pub fn reader(
        sink: Arc<dyn JobSink>,
        files: Arc<dyn JobFiles>,
        path: impl Into<String>,
    ) -> Self {
        Self {
            sink,
            kind: StreamKind::File(path.into()),
            http: None,
            files: Some(files),
            spec: None,
            etag: None,
            last_modified: None,
            cancel: Arc::new(Cancel::new()),
            closed: Arc::new(AtomicBool::new(false)),
            stall: Duration::from_secs(30),
            retry_delay: Duration::ZERO,
            max_reconnects: 8,
            job_id: String::new(),
            _error: std::marker::PhantomData,
        }
    }

    /// `node.put`'s source factory: open at `offset`, updating `meta.expected_size` from the source.
    pub async fn open(
        &mut self,
        offset: u64,
        meta: &mut PutMeta,
    ) -> Result<Box<dyn AsyncRead + Send + Unpin>, E> {
        if let StreamKind::Stream(reader) = &mut self.kind {
            return Ok(reader.take().expect("opened once"));
        }
        match &self.kind {
            StreamKind::Stream(_) => unreachable!("handled above"),
            StreamKind::File(path) => self
                .files
                .as_ref()
                .expect("file source has a filesystem")
                .open_read(path)
                .map_err(|error| E::from(AcquireError::Message(error))),
            StreamKind::Http => {
                let spec = self.spec.clone().expect("http source has a spec");
                let http = self.http.clone().expect("http source has a client");
                let base_headers = merge_headers(&[&spec.headers]);
                if offset > 0 {
                    let mut resume_headers = base_headers.clone();
                    resume_headers.push(("range".into(), format!("bytes={offset}-")));
                    if let Some(validator) = self.etag.clone().or(self.last_modified.clone()) {
                        resume_headers.push(("if-range".into(), validator));
                    }
                    let first = match open_source(
                        &*http,
                        &spec.url,
                        &resume_headers,
                        &self.cancel,
                        0,
                        self.stall,
                    )
                    .await
                    {
                        Ok(first) => first,
                        Err(error) => {
                            self.sink.on_source_error(&error);
                            return Err(E::from(error));
                        }
                    };
                    let start =
                        header(&first.headers, "content-range").and_then(content_range_start);
                    let rotated = self.etag.as_ref().is_some_and(|etag| {
                        header(&first.headers, "etag").is_some_and(|value| value != etag)
                    });
                    if rotated || first.status != 206 || start != Some(offset) {
                        let error = AcquireError::Restart(format!(
                            "Source changed while resuming {}",
                            self.job_id
                        ));
                        // The job must drop the core holding the other encode: keeping it fails every retry.
                        self.sink.on_source_error(&error);
                        return Err(E::from(error));
                    }
                    self.sink.on_bytes(offset);
                    Ok(Box::new(Resumable::new(
                        ResumableConfig {
                            http,
                            url: spec.url,
                            base_headers,
                            total: meta.expected_size.flatten(),
                            // As `readResumable`, a reconnect revalidates with the reply's own strong ETag, not the
                            // one the job last saved: a rotated encode must fail the resume, not pass it.
                            etag: strong_etag(&first.headers),
                            offset,
                            cancel: self.cancel.clone(),
                            closed: self.closed.clone(),
                            stall: self.stall,
                            retry_delay: self.retry_delay,
                            max_reconnects: self.max_reconnects,
                            job_id: self.job_id.clone(),
                        },
                        first,
                    )))
                } else {
                    let first = match open_source(
                        &*http,
                        &spec.url,
                        &base_headers,
                        &self.cancel,
                        0,
                        self.stall,
                    )
                    .await
                    {
                        Ok(first) => first,
                        Err(error) => {
                            self.sink.on_source_error(&error);
                            return Err(E::from(error));
                        }
                    };
                    let total = header(&first.headers, "content-length")
                        .map(js_number)
                        .filter(|n| {
                            n.is_finite()
                                && *n >= 0.0
                                && n.fract() == 0.0
                                && *n <= 9_007_199_254_740_991.0
                        })
                        .map(|n| n as u64);
                    let etag = strong_etag(&first.headers);
                    let last_modified = header(&first.headers, "last-modified").map(str::to_owned);
                    // Durable at once, together with the zero progress: a crash before the first hash checkpoint
                    // resumes with the response it saw.
                    self.sink.on_bytes(0);
                    self.sink.set_response(total, etag.clone(), last_modified);
                    meta.expected_size = Some(total);
                    Ok(Box::new(Resumable::new(
                        ResumableConfig {
                            http,
                            url: spec.url,
                            base_headers,
                            total,
                            etag,
                            offset: 0,
                            cancel: self.cancel.clone(),
                            closed: self.closed.clone(),
                            stall: self.stall,
                            retry_delay: self.retry_delay,
                            max_reconnects: self.max_reconnects,
                            job_id: self.job_id.clone(),
                        },
                        first,
                    )))
                }
            }
        }
    }

    /// Progress from the backend: `job.bytes`.
    pub fn on_data(&mut self, bytes: u64) {
        self.sink.on_bytes(bytes);
    }

    /// A sha256 checkpoint the backend reports mid-stream.
    pub fn on_hash_state(&mut self, state: &[u8], bytes: u64) {
        self.sink.on_hash_state(state, bytes);
    }
}

/// Builds the backend's opaque `node.put` source from the acquirer's stream.
pub trait PutStreamFactory<N: Node>: Send + Sync {
    /// A resumable source: the backend calls `open` with what it already has.
    fn resumable(&self, source: JobSource<N::Error>) -> N::PutSource;
    /// A plain stream (`createReadStream`, mirror ingestion): no resume offset is offered.
    fn reader(&self, source: JobSource<N::Error>) -> N::PutSource;
}

/// `downloadPage`: how a page link becomes a file before `put`.
pub struct PageOptions {
    pub config_dir: String,
    pub stall_timeout: Duration,
    pub controller: Option<identify::Controller>,
    pub on_progress: Option<Box<dyn FnMut(DownloadProgress) + Send>>,
}

/// The process seam behind `ubit get`.
pub trait PageDownloader: Send + Sync {
    fn download(
        &self,
        url: &str,
        dir: &str,
        opts: PageOptions,
    ) -> Pin<Box<dyn Future<Output = Result<Downloaded, AcquireError>> + Send>>;
}

/// The real `ubit get` runner.
pub struct UbitPages {
    pub ubit: String,
}

impl PageDownloader for UbitPages {
    fn download(
        &self,
        url: &str,
        dir: &str,
        opts: PageOptions,
    ) -> Pin<Box<dyn Future<Output = Result<Downloaded, AcquireError>> + Send>> {
        let url = url.to_owned();
        let dir = dir.to_owned();
        let ubit = self.ubit.clone();
        Box::pin(async move {
            identify::download_page(
                &url,
                &dir,
                identify::DownloadOptions {
                    config_dir: opts.config_dir,
                    stall_timeout: opts.stall_timeout,
                    ubit,
                    controller: opts.controller,
                    on_progress: opts.on_progress,
                },
            )
            .await
            .map_err(|error| {
                AcquireError::Source(SourceError {
                    message: error.message().to_owned(),
                    status: error.status(),
                    retry_after: None,
                })
            })
        })
    }
}

/// `createAcquirer`'s options, with the app's defaults.
#[derive(Clone)]
pub struct AcquireOptions {
    pub concurrency: usize,
    pub retry_delay: Duration,
    pub stall_timeout: Duration,
    pub max_reconnects: u32,
    pub max_attempts: u32,
    pub default_channel: bool,
    pub ubit_config_dir: Option<String>,
    pub on_done: Option<Arc<dyn Fn(Value, Value) + Send + Sync>>,
    pub now_ms: Arc<dyn Fn() -> u64 + Send + Sync>,
}

impl Default for AcquireOptions {
    fn default() -> Self {
        Self {
            concurrency: 4,
            retry_delay: Duration::from_millis(15_000),
            stall_timeout: Duration::from_millis(30_000),
            max_reconnects: 8,
            max_attempts: 4,
            default_channel: false,
            ubit_config_dir: None,
            on_done: None,
            now_ms: Arc::new(|| {
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|d| d.as_millis() as u64)
                    .unwrap_or(0)
            }),
        }
    }
}

struct Inner<N: Node> {
    node: Arc<N>,
    files: Arc<dyn JobFiles>,
    pages: Arc<dyn PageDownloader>,
    http: Arc<dyn HttpOpen>,
    factory: Arc<dyn PutStreamFactory<N>>,
    random: Arc<dyn RandomSource>,
    opts: AcquireOptions,
    jobs: Mutex<Vec<Job>>,
    source_errors: Mutex<HashMap<String, AcquireError>>,
    concurrency: AtomicUsize,
    running: Mutex<HashMap<String, Arc<Cancel>>>,
    active: AtomicUsize,
    idle: Notify,
    closed: Arc<AtomicBool>,
}

impl<N: Node + 'static> Inner<N>
where
    N::Error: From<AcquireError>,
{
    fn now(&self) -> u64 {
        (self.opts.now_ms)()
    }

    fn concurrency(&self) -> usize {
        self.concurrency.load(Ordering::SeqCst)
    }

    /// Persist through the 0600 rename. A failed write is the disk's problem, not the job's.
    fn save(&self) {
        if self.closed.load(Ordering::SeqCst) {
            return;
        }
        let body = {
            let jobs = self.jobs.lock().expect("acquirer jobs");
            Value::Array(jobs.iter().map(Job::to_json).collect()).to_string()
        };
        let _ = self.files.write_jobs(&body);
    }

    fn with_job<R>(&self, id: &str, f: impl FnOnce(&mut Job) -> R) -> Option<R> {
        let mut jobs = self.jobs.lock().expect("acquirer jobs");
        jobs.iter_mut().find(|job| job.job_id == id).map(f)
    }

    fn snapshot(&self, id: &str) -> Option<Job> {
        self.jobs
            .lock()
            .expect("acquirer jobs")
            .iter()
            .find(|job| job.job_id == id)
            .cloned()
    }

    fn take_source_error(&self, id: &str) -> Option<AcquireError> {
        self.source_errors
            .lock()
            .expect("acquirer source errors")
            .remove(id)
    }

    fn new_store(&self) -> String {
        format!("video:{}", self.random.uuid_v4().replace('-', ""))
    }

    fn sink(self: &Arc<Self>, id: &str) -> Arc<dyn JobSink> {
        Arc::new(Sink {
            inner: Arc::downgrade(self),
            job_id: id.to_owned(),
        })
    }

    /// `node.dropStore(store)` for a job that will never run again; its failure is the store's business.
    fn drop_core(self: &Arc<Self>, store: String) {
        let inner = self.clone();
        tokio::spawn(async move {
            let _ = inner.node.drop_store(&store).await;
        });
    }

    /// Start every job that fits. The synchronous head of the app's `run` (durable store, status, error) happens
    /// here so `add` reports the same status the app does.
    fn next(self: &Arc<Self>) {
        loop {
            if self.closed.load(Ordering::SeqCst) {
                return;
            }
            let picked = {
                let mut jobs = self.jobs.lock().expect("acquirer jobs");
                let mut running = self.running.lock().expect("acquirer running");
                if running.len() >= self.concurrency() {
                    return;
                }
                let index = jobs
                    .iter()
                    .position(|job| {
                        job.status == JobStatus::Announcing && !running.contains_key(&job.job_id)
                    })
                    .or_else(|| {
                        jobs.iter().position(|job| {
                            job.status == JobStatus::Queued && !running.contains_key(&job.job_id)
                        })
                    });
                let Some(index) = index else {
                    return;
                };
                let job = &mut jobs[index];
                if job.source.as_ref().is_some_and(|source| !source.page) && job.store.is_none() {
                    job.store = Some(self.new_store());
                }
                // A private job from before per-job keys existed gets one now, or a restart would reopen its
                // core under a second key.
                if job.private && job.store.is_some() && job.store_key.is_none() {
                    job.store_key = Some(hex(&self.random.bytes(32)));
                }
                job.status = if job.announce.is_some() {
                    JobStatus::Announcing
                } else {
                    JobStatus::Running
                };
                job.error = Some(Value::Null);
                let id = job.job_id.clone();
                let cancel = Arc::new(Cancel::new());
                running.insert(id.clone(), cancel.clone());
                (id, cancel)
            };
            self.save();
            self.active.fetch_add(1, Ordering::SeqCst);
            let inner = self.clone();
            tokio::spawn(async move {
                Arc::clone(&inner).run(picked.0, picked.1).await;
                if inner.active.fetch_sub(1, Ordering::SeqCst) == 1 {
                    inner.idle.notify_waiters();
                }
            });
        }
    }

    async fn run(self: Arc<Self>, id: String, cancel: Arc<Cancel>) {
        let Some(loaded) = self.snapshot(&id) else {
            self.finish(&id);
            return;
        };
        if let Err(error) = self.run_job(&id, &loaded, &cancel).await {
            self.fail(&id, &loaded, error).await;
        }
        self.finish(&id);
    }

    fn finish(self: &Arc<Self>, id: &str) {
        self.running.lock().expect("acquirer running").remove(id);
        self.save();
        if !self.node.closing() && !self.closed.load(Ordering::SeqCst) {
            self.next();
        }
    }

    async fn run_job(
        self: &Arc<Self>,
        id: &str,
        loaded: &Job,
        cancel: &Arc<Cancel>,
    ) -> Result<(), AcquireError> {
        if loaded.announce.is_none() {
            match &loaded.source {
                Some(source) if source.page => {
                    self.transfer_page(id, loaded, cancel).await?;
                }
                Some(_) => {
                    self.transfer_http(id, loaded, cancel).await?;
                }
                // A job with nothing left to fetch and no announce can never run: fail it instead of reading
                // through a missing source, which would panic the task and leave it in `running` forever.
                None => return Err(AcquireError::Message("Job has no source".into())),
            }
            if self
                .snapshot(id)
                .is_some_and(|job| job.status == JobStatus::Cancelled)
            {
                return Err(AcquireError::Message("Cancelled".into()));
            }
            // Closed for a reopen: the next acquirer runs this job again from jobs.json, so this one must not
            // announce it.
            if self.closed.load(Ordering::SeqCst) {
                return Err(AcquireError::Message("Closed".into()));
            }
            let announce = self.snapshot(id).and_then(|job| job.announce);
            if announce.is_none() {
                return Ok(());
            }
            self.with_job(id, |job| {
                job.status = JobStatus::Announcing;
                job.source = None;
            });
            self.save();
        }
        let Some(announce) = self.snapshot(id).and_then(|job| job.announce) else {
            return Ok(());
        };
        self.node
            .append(announce.clone())
            .await
            .map_err(|error| AcquireError::Message(error.to_string()))?;
        let size = announce.get("size").and_then(Value::as_u64);
        let sha256 = announce
            .get("sha256")
            .and_then(Value::as_str)
            .map(str::to_owned);
        self.with_job(id, |job| {
            job.status = JobStatus::Done;
            job.size = size;
            job.sha256 = sha256;
            job.hash_state = Some(Value::Null);
            job.hash_bytes = Some(0);
        });
        if let Some(on_done) = &self.opts.on_done
            && let Some(job) = self.snapshot(id)
        {
            on_done(job.public_json(), announce);
        }
        Ok(())
    }

    /// The direct-source path: `node.put` with a resumable factory.
    async fn transfer_http(
        self: &Arc<Self>,
        id: &str,
        job: &Job,
        cancel: &Arc<Cancel>,
    ) -> Result<(), AcquireError> {
        let Some(source) = job.source.clone() else {
            return Err(AcquireError::Message("Job has no source".into()));
        };
        let mut extra = Map::new();
        extra.insert(
            "hashState".into(),
            job.hash_state.clone().unwrap_or(Value::Null),
        );
        extra.insert("hashBytes".into(), json!(job.hash_bytes.unwrap_or(0)));
        if job.private
            && let Some(store_key) = &job.store_key
        {
            extra.insert("storeKey".into(), json!(store_key));
        }
        let meta = PutMeta {
            id: job.id.clone(),
            title: job.title.clone(),
            private_entry: Some(job.private),
            epoch: None,
            store: job.store.clone(),
            expected_size: job.total,
            extra,
        };
        let stream = JobSource::<N::Error>::http(
            self.sink(id),
            self.http.clone(),
            self.files.clone(),
            id.to_owned(),
            source,
            job.etag.as_ref().and_then(Value::as_str).map(str::to_owned),
            job.last_modified
                .as_ref()
                .and_then(Value::as_str)
                .map(str::to_owned),
            cancel.clone(),
            self.closed.clone(),
            self.opts.stall_timeout,
            self.opts.retry_delay,
            self.opts.max_reconnects,
        );
        self.source_errors
            .lock()
            .expect("acquirer source errors")
            .remove(id);
        let announce = match self.node.put(meta, self.factory.resumable(stream)).await {
            Ok(announce) => announce,
            Err(error) => {
                return Err(self
                    .take_source_error(id)
                    .unwrap_or_else(|| AcquireError::Message(error.to_string())));
            }
        };
        self.with_job(id, |job| {
            job.announce = Some(serde_json::to_value(announce).unwrap_or(Value::Null));
        });
        Ok(())
    }

    /// The page path: `ubit get` into a temp directory, then `put` the file.
    async fn transfer_page(
        self: &Arc<Self>,
        id: &str,
        job: &Job,
        cancel: &Arc<Cancel>,
    ) -> Result<(), AcquireError> {
        let Some(source) = job.source.clone() else {
            return Err(AcquireError::Message("Job has no source".into()));
        };
        let tmp_dir = self.files.make_temp_dir().map_err(AcquireError::Message)?;
        let result = self
            .transfer_page_inner(id, job, &source, &tmp_dir, cancel)
            .await;
        self.files.remove_dir(&tmp_dir);
        result
    }

    async fn transfer_page_inner(
        self: &Arc<Self>,
        id: &str,
        job: &Job,
        source: &SourceSpec,
        tmp_dir: &str,
        cancel: &Arc<Cancel>,
    ) -> Result<(), AcquireError> {
        self.with_job(id, |job| {
            job.total = Some(None);
            job.bytes = Some(0);
        });
        let controller = identify::Controller::new();
        let watcher = {
            let controller = controller.clone();
            let cancel = cancel.clone();
            tokio::spawn(async move {
                cancel.aborted().await;
                controller.abort();
            })
        };
        let progress = {
            let inner = Arc::downgrade(self);
            let id = id.to_owned();
            move |progress: DownloadProgress| {
                if let Some(inner) = inner.upgrade() {
                    inner.with_job(&id, |job| {
                        job.bytes = Some(progress.bytes.0.max(0.0) as u64);
                        if let Some(total) = progress.total {
                            job.total = Some(Some(total.0.max(0.0) as u64));
                        }
                    });
                }
            }
        };
        let downloaded = self
            .pages
            .download(
                &source.url,
                tmp_dir,
                PageOptions {
                    config_dir: self
                        .opts
                        .ubit_config_dir
                        .clone()
                        .or_else(|| self.files.config_dir())
                        .unwrap_or_default(),
                    stall_timeout: self.opts.stall_timeout,
                    controller: Some(controller),
                    on_progress: Some(Box::new(progress)),
                },
            )
            .await;
        watcher.abort();
        let downloaded = downloaded?;
        if self
            .snapshot(id)
            .is_some_and(|job| job.status == JobStatus::Cancelled)
        {
            return Err(AcquireError::Message("Cancelled".into()));
        }
        if self.closed.load(Ordering::SeqCst) {
            return Err(AcquireError::Message("Closed".into()));
        }
        let total = downloaded
            .size
            .map(|size| size.max(0) as u64)
            .or_else(|| self.files.file_size(&downloaded.path))
            .unwrap_or(0);
        self.with_job(id, |job| job.total = Some(Some(total)));
        let meta = PutMeta {
            id: job.id.clone(),
            title: job.title.clone(),
            private_entry: Some(job.private),
            epoch: None,
            store: None,
            expected_size: Some(Some(total)),
            extra: Map::new(),
        };
        let stream =
            JobSource::<N::Error>::reader(self.sink(id), self.files.clone(), downloaded.path);
        let announce = self
            .node
            .put(meta, self.factory.reader(stream))
            .await
            .map_err(|error| AcquireError::Message(error.to_string()))?;
        self.with_job(id, |job| {
            job.announce = Some(serde_json::to_value(announce).unwrap_or(Value::Null));
        });
        Ok(())
    }

    /// `run`'s catch: a dead-lettered source, a dropped half-written core, and a backoff retry.
    async fn fail(self: &Arc<Self>, id: &str, loaded: &Job, error: AcquireError) {
        let closed = self.closed.load(Ordering::SeqCst);
        let job = self
            .with_job(id, |job| {
                if job.status != JobStatus::Cancelled {
                    job.status = if job.announce.is_some() {
                        JobStatus::Announcing
                    } else {
                        JobStatus::Failed
                    };
                }
                job.error = Some(json!(error.message()));
                if !closed && job.status == JobStatus::Failed {
                    job.attempts = Some(job.attempts.unwrap_or(0) + 1);
                    if error
                        .status()
                        .is_some_and(|status| TERMINAL_STATUS.contains(&status))
                        || job.attempts.unwrap_or(0) >= u64::from(self.opts.max_attempts)
                    {
                        job.source = None;
                    }
                }
                job.clone()
            })
            .unwrap_or_else(|| loaded.clone());
        // A job that will never run again must not leave a half-written core on the array, and a source that
        // changed under a resumed core leaves bytes of another encode that the next attempt must not stitch onto.
        if (job.status == JobStatus::Cancelled
            || (job.status == JobStatus::Failed && job.source.is_none())
            || error.restart())
            && let Some(store) = &job.store
        {
            let _ = self.node.drop_store(store).await;
        }
        if closed
            || job.status != JobStatus::Failed
            || job.source.is_none()
            || self.opts.retry_delay.is_zero()
        {
            return;
        }
        let attempts = job.attempts.unwrap_or(1).max(1);
        let delay = error.retry_after().unwrap_or_else(|| {
            let factor = 2u32.saturating_pow((attempts - 1) as u32);
            (self.opts.retry_delay * factor).min(Duration::from_millis(300_000))
        });
        let inner = self.clone();
        let id = id.to_owned();
        tokio::spawn(async move {
            tokio::time::sleep(delay).await;
            let still_failed = inner
                .snapshot(&id)
                .is_some_and(|job| job.status == JobStatus::Failed);
            if still_failed && !inner.node.closing() && !inner.closed.load(Ordering::SeqCst) {
                inner.with_job(&id, |job| job.status = JobStatus::Queued);
                inner.save();
                inner.next();
            }
        });
    }
}

/// A sink for streams that report nothing back (mirror ingestion).
struct NoopSink;

impl JobSink for NoopSink {
    fn on_bytes(&self, _bytes: u64) {}
    fn on_source_error(&self, _error: &AcquireError) {}
    fn on_hash_state(&self, _state: &[u8], _bytes: u64) {}
    fn set_response(
        &self,
        _total: Option<u64>,
        _etag: Option<String>,
        _last_modified: Option<String>,
    ) {
    }
}

/// `JobSink` for one job; holds the acquirer weakly so the stream never keeps it alive.
struct Sink<N: Node> {
    inner: Weak<Inner<N>>,
    job_id: String,
}

impl<N: Node + 'static> JobSink for Sink<N>
where
    N::Error: From<AcquireError>,
{
    fn on_bytes(&self, bytes: u64) {
        if let Some(inner) = self.inner.upgrade() {
            inner.with_job(&self.job_id, |job| job.bytes = Some(bytes));
        }
    }

    fn on_source_error(&self, error: &AcquireError) {
        if let Some(inner) = self.inner.upgrade() {
            inner
                .source_errors
                .lock()
                .expect("acquirer source errors")
                .insert(self.job_id.clone(), error.clone());
        }
    }

    fn on_hash_state(&self, state: &[u8], bytes: u64) {
        if let Some(inner) = self.inner.upgrade() {
            inner.with_job(&self.job_id, |job| {
                job.hash_state = Some(json!(base64_encode(state)));
                job.hash_bytes = Some(bytes);
            });
            inner.save();
        }
    }

    fn set_response(
        &self,
        total: Option<u64>,
        etag: Option<String>,
        last_modified: Option<String>,
    ) {
        if let Some(inner) = self.inner.upgrade() {
            inner.with_job(&self.job_id, |job| {
                job.total = Some(total);
                job.etag = Some(etag.map_or(Value::Null, Value::String));
                job.last_modified = Some(last_modified.map_or(Value::Null, Value::String));
            });
            // Durable at once: a crash before the first hash checkpoint resumes with the response it saw.
            inner.save();
        }
    }
}

/// `createAcquirer`'s returned handle.
pub struct Acquirer<N: Node> {
    inner: Arc<Inner<N>>,
}

impl<N: Node + 'static> Clone for Acquirer<N> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl<N: Node + 'static> Acquirer<N>
where
    N::Error: From<AcquireError>,
{
    pub fn new(
        node: Arc<N>,
        files: Arc<dyn JobFiles>,
        pages: Arc<dyn PageDownloader>,
        http: Arc<dyn HttpOpen>,
        factory: Arc<dyn PutStreamFactory<N>>,
        random: Arc<dyn RandomSource>,
        opts: AcquireOptions,
    ) -> Self {
        files.clean_temp();
        let mut jobs: Vec<Job> = files
            .read_jobs()
            .and_then(|text| serde_json::from_str::<Value>(&text).ok())
            .and_then(|value| value.as_array().cloned())
            .map(|list| list.iter().filter_map(Job::from_json).collect())
            .unwrap_or_default();
        for job in &mut jobs {
            if job.status == JobStatus::Running
                || (job.status == JobStatus::Failed && job.source.is_some())
            {
                job.status = if job.announce.is_some() {
                    JobStatus::Announcing
                } else {
                    JobStatus::Queued
                };
            }
        }
        let concurrency = opts.concurrency;
        let acquirer = Self {
            inner: Arc::new(Inner {
                node,
                files,
                pages,
                http,
                factory,
                random,
                opts,
                jobs: Mutex::new(jobs),
                source_errors: Mutex::new(HashMap::new()),
                concurrency: AtomicUsize::new(concurrency),
                running: Mutex::new(HashMap::new()),
                active: AtomicUsize::new(0),
                idle: Notify::new(),
                closed: Arc::new(AtomicBool::new(false)),
            }),
        };
        acquirer.inner.next();
        acquirer
    }

    /// `add`: validate the source URL, durably queue the job, and start it when a slot is free.
    pub fn add(&self, spec: AddJob) -> Result<Value, AcquireError> {
        if !(spec.source.url.starts_with("http://") || spec.source.url.starts_with("https://")) {
            return Err(AcquireError::Message("source.url must be http(s)".into()));
        }
        let inner = &self.inner;
        let private = spec.private || spec.source.private;
        let job_id = inner.random.uuid_v4();
        let job = Job::new(NewJob {
            job_id: job_id.clone(),
            id: spec.id,
            title: spec.title,
            plugin: spec.plugin.unwrap_or_else(|| "http-acquire".to_owned()),
            channel: spec.channel.unwrap_or(inner.opts.default_channel),
            private,
            created: inner.now(),
            store: inner.new_store(),
            source: spec.source,
            store_key: private.then(|| hex(&inner.random.bytes(32))),
        });
        inner.jobs.lock().expect("acquirer jobs").push(job);
        inner.save();
        inner.next();
        self.get(&job_id)
            .ok_or_else(|| AcquireError::Message("job vanished".into()))
    }

    /// `cancel`: stop a queued or running job; a finished/failed one is dismissed from the list.
    pub fn cancel(&self, job_id: &str) -> Option<Value> {
        let inner = &self.inner;
        // The status is read and acted on under one lock: a run finishing in between must not be clobbered
        // with `cancelled`, and a job that is no longer running has nobody else to drop its core.
        let (status, running_cancel, drop_store) = {
            let mut jobs = inner.jobs.lock().expect("acquirer jobs");
            let index = jobs.iter().position(|job| job.job_id == job_id)?;
            let status = jobs[index].status;
            if status == JobStatus::Queued || status == JobStatus::Running {
                jobs[index].status = JobStatus::Cancelled;
                jobs[index].source = None;
                let running = inner
                    .running
                    .lock()
                    .expect("acquirer running")
                    .get(job_id)
                    .cloned();
                let job = &jobs[index];
                // A running job drops its half-written core in `run`'s failure path; a queued one has nobody
                // else to do it.
                let drop_store = (running.is_none() && job.announce.is_none())
                    .then(|| job.store.clone())
                    .flatten();
                (status, running, drop_store)
            } else {
                (status, None, None)
            }
        };
        if status == JobStatus::Queued || status == JobStatus::Running {
            if let Some(cancel) = running_cancel {
                cancel.abort();
            }
            inner.save();
            if let Some(store) = drop_store {
                inner.drop_core(store);
            }
        } else if status != JobStatus::Announcing {
            let removed = {
                let mut jobs = inner.jobs.lock().expect("acquirer jobs");
                let index = jobs.iter().position(|job| job.job_id == job_id)?;
                let mut job = jobs.remove(index);
                job.status = JobStatus::Removed;
                job
            };
            inner.save();
            // A dismissed job that never announced leaves no core behind.
            if removed.announce.is_none()
                && let Some(store) = removed.store.clone()
            {
                inner.drop_core(store);
            }
            return Some(removed.public_json());
        }
        inner.snapshot(job_id).map(|job| job.public_json())
    }

    /// `repointBlob`: follow the blob-rechunk migration so a done job announces the new blob tag.
    pub fn repoint_blob(&self, old_tag: &str, target: &Value) -> usize {
        let inner = &self.inner;
        let Some(blobs) = target.get("blobs").and_then(Value::as_str) else {
            return 0;
        };
        let Some(blob) = target.get("blob") else {
            return 0;
        };
        let mut jobs = inner.jobs.lock().expect("acquirer jobs");
        let mut updated = 0;
        for job in jobs.iter_mut() {
            let Some(announce) = job.announce.as_mut() else {
                continue;
            };
            let tag = format!(
                "{}:{}",
                announce
                    .get("blobs")
                    .and_then(Value::as_str)
                    .unwrap_or_default(),
                announce
                    .get("blob")
                    .and_then(|blob| blob.get("blockOffset"))
                    .and_then(Value::as_u64)
                    .unwrap_or(0)
            );
            if tag == old_tag
                && let Some(map) = announce.as_object_mut()
            {
                map.insert("blobs".into(), json!(blobs));
                map.insert("blob".into(), blob.clone());
                updated += 1;
            }
        }
        drop(jobs);
        if updated > 0 {
            inner.save();
        }
        updated
    }

    pub fn get(&self, job_id: &str) -> Option<Value> {
        self.inner.snapshot(job_id).map(|job| job.public_json())
    }

    pub fn list(&self) -> Vec<Value> {
        self.inner
            .jobs
            .lock()
            .expect("acquirer jobs")
            .iter()
            .map(Job::public_json)
            .collect()
    }

    pub fn set_concurrency(&self, concurrency: usize) {
        self.inner.concurrency.store(concurrency, Ordering::SeqCst);
        self.inner.next();
    }

    /// `close`: leave `jobs.json` as it was at close so the next boot re-queues in-flight work.
    pub fn close(&self) {
        self.inner.save();
        self.inner.closed.store(true, Ordering::SeqCst);
        let cancels: Vec<Arc<Cancel>> = self
            .inner
            .running
            .lock()
            .expect("acquirer running")
            .values()
            .cloned()
            .collect();
        for cancel in cancels {
            cancel.abort();
        }
    }

    /// Wait until no job is being transferred.
    pub async fn wait_idle(&self) {
        loop {
            let idle = self.inner.idle.notified();
            if self.inner.active.load(Ordering::SeqCst) == 0 {
                return;
            }
            idle.await;
        }
    }
}
