//! Port of PearTube's `src/art.js`: Cinemeta artwork and the small bounded http(s) client that `art.js`,
//! `identify.js` and `mirror.js` share.
//!
//! Relay-only: the apps' worklet points at metahub directly and bundles no TLS, so this is the one place a link is
//! fetched. The request follows at most five redirects, carries a `PearTube` user agent, and caps what a remote
//! server can make us hold. Metadata answers are cached in memory, including failures, which the title retries after
//! five minutes rather than poisoning it forever.

use std::collections::HashMap;
use std::sync::{LazyLock, Mutex};
use std::time::{Duration, Instant};

use http_body_util::{BodyExt, Full};
use hyper::body::Bytes;
use hyper::header::{HeaderMap, HeaderName, HeaderValue, USER_AGENT};
use hyper::{Method, Request};
use hyper_rustls::HttpsConnectorBuilder;
use hyper_util::client::legacy::Client;
use hyper_util::client::legacy::connect::HttpConnector;
use hyper_util::rt::TokioExecutor;
use serde_json::Value;
use url::Url;

/// `CINEMETA_URL`: the default metadata host.
pub const CINEMETA_URL: &str = "https://v3-cinemeta.strem.io";

/// How long a failed metadata lookup is remembered before the title is asked for again.
const NEGATIVE_TTL: Duration = Duration::from_millis(300_000);
/// `metaCache.size > 2000` clears the cache.
const MAX_CACHE_ENTRIES: usize = 2000;

/// Why a link could not be fetched. The message is the one the worklet throws.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HttpError {
    message: String,
}

impl HttpError {
    fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
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

/// What `send` is asked for. The defaults are the app's: GET, a ten second timeout, no redirect budget left over
/// from a caller, and no byte cap.
#[derive(Debug, Clone)]
pub struct SendOptions {
    pub method: Method,
    pub headers: Vec<(String, String)>,
    pub timeout: Duration,
    pub max_bytes: Option<u64>,
    pub redirects: u8,
}

impl Default for SendOptions {
    fn default() -> Self {
        Self {
            method: Method::GET,
            headers: Vec::new(),
            timeout: Duration::from_millis(10_000),
            max_bytes: None,
            redirects: 0,
        }
    }
}

/// A buffered reply. `url` is the href after redirects; `headers` are lowercased, repeated names joined with ", "
/// the way Node hands them over.
#[derive(Debug, Clone)]
pub struct HttpResponse {
    pub status: u16,
    pub headers: Vec<(String, String)>,
    pub url: String,
    pub body: String,
}

impl HttpResponse {
    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(key, _)| key == name)
            .map(|(_, value)| value.as_str())
    }
}

/// `send`: fetch a link, following up to five redirects, and read the reply as UTF-8.
///
/// The timeout covers the whole exchange, while the app's `req.setTimeout` only covered socket inactivity; a slow
/// trickle of bytes therefore times out here where the app would keep waiting.
pub async fn send(url: &str, opts: SendOptions) -> Result<HttpResponse, HttpError> {
    let mut current = Url::parse(url).map_err(|_| HttpError::new("Not a valid link"))?;
    let mut redirects = opts.redirects;
    loop {
        if !matches!(current.scheme(), "http" | "https") {
            return Err(HttpError::new("Only http(s) links can be added"));
        }
        let response = tokio::time::timeout(opts.timeout, hop(&current, &opts))
            .await
            .map_err(|_| HttpError::new("Timed out"))??;
        let location = response.header("location").map(str::to_owned);
        if (300..400).contains(&response.status)
            && redirects < 5
            && let Some(location) = location
        {
            redirects += 1;
            current = current
                .join(&location)
                .map_err(|_| HttpError::new("Invalid URL"))?;
            continue;
        }
        return Ok(HttpResponse {
            url: current.as_str().to_owned(),
            ..response
        });
    }
}

type Https = hyper_rustls::HttpsConnector<HttpConnector>;
type HttpClient = Client<Https, Full<Bytes>>;

static CLIENT: LazyLock<HttpClient> = LazyLock::new(|| {
    let https = HttpsConnectorBuilder::new()
        .with_webpki_roots()
        .https_or_http()
        .enable_http1()
        .build();
    Client::builder(TokioExecutor::new()).build(https)
});

async fn hop(url: &Url, opts: &SendOptions) -> Result<HttpResponse, HttpError> {
    let mut headers = HeaderMap::new();
    headers.insert(USER_AGENT, HeaderValue::from_static("PearTube"));
    for (name, value) in &opts.headers {
        let (Ok(name), Ok(value)) = (
            HeaderName::from_bytes(name.as_bytes()),
            HeaderValue::from_str(value),
        ) else {
            continue;
        };
        headers.insert(name, value);
    }
    let mut request = Request::builder()
        .method(opts.method.clone())
        .uri(url.as_str())
        .body(Full::new(Bytes::new()))
        .map_err(|error| HttpError::new(error.to_string()))?;
    *request.headers_mut() = headers;

    let response = CLIENT
        .request(request)
        .await
        .map_err(|error| HttpError::new(error.to_string()))?;
    let status = response.status().as_u16();
    let mut collected: Vec<(String, String)> = Vec::new();
    for (name, value) in response.headers() {
        let name = name.as_str().to_ascii_lowercase();
        let value = value.to_str().unwrap_or_default().to_owned();
        match collected.iter_mut().find(|(key, _)| *key == name) {
            Some((_, seen)) => {
                seen.push_str(", ");
                seen.push_str(&value);
            }
            None => collected.push((name, value)),
        }
    }

    let mut body = Vec::new();
    if opts.method != Method::HEAD {
        let mut incoming = response.into_body();
        let mut total = 0u64;
        while let Some(frame) = incoming.frame().await {
            let frame = frame.map_err(|error| HttpError::new(error.to_string()))?;
            let Some(chunk) = frame.data_ref() else {
                continue;
            };
            total += chunk.len() as u64;
            if let Some(max) = opts.max_bytes
                && total > max
            {
                return Err(HttpError::new(format!("Reply exceeded {max} bytes")));
            }
            body.extend_from_slice(chunk);
        }
    }
    Ok(HttpResponse {
        status,
        headers: collected,
        url: url.as_str().to_owned(),
        body: String::from_utf8_lossy(&body).into_owned(),
    })
}

/// `IMDB`: `tt` and five to ten digits.
pub fn is_imdb_id(imdb: &str) -> bool {
    imdb.strip_prefix("tt").is_some_and(|digits| {
        (5..=10).contains(&digits.len()) && digits.bytes().all(|b| b.is_ascii_digit())
    })
}

struct CacheEntry {
    meta: Option<Value>,
    at: Instant,
}

static META_CACHE: LazyLock<Mutex<HashMap<String, CacheEntry>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

/// A poisoned cache still holds usable entries, so a panicking lookup never takes the relay down with it.
fn meta_cache() -> std::sync::MutexGuard<'static, HashMap<String, CacheEntry>> {
    META_CACHE
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

/// Drops the shared metadata cache. Tests use it to keep cases independent.
pub fn clear_meta_cache() {
    meta_cache().clear();
}

/// `metaFor`: the Cinemeta metadata for an imdb id, or nothing when neither search type answers with art.
///
/// Cinemeta answers the wrong `type` with a redirect and an empty meta sometimes, so this takes the first answer
/// that carries a poster rather than the first answer at all.
pub async fn meta_for(base: &str, imdb: &str) -> Option<Value> {
    if base.is_empty() || !is_imdb_id(imdb) {
        return None;
    }
    {
        let cache = meta_cache();
        if let Some(hit) = cache.get(imdb)
            && (hit.meta.is_some() || hit.at.elapsed() < NEGATIVE_TTL)
        {
            return hit.meta.clone();
        }
    }
    let mut meta: Option<Value> = None;
    for kind in ["movie", "series"] {
        let url = format!("{base}/meta/{kind}/{imdb}.json");
        let answer = send(
            &url,
            SendOptions {
                timeout: Duration::from_millis(8000),
                ..Default::default()
            },
        )
        .await
        .ok();
        let found = answer
            .filter(|response| response.status == 200)
            .and_then(|response| serde_json::from_str::<Value>(&response.body).ok())
            .and_then(|body| body.get("meta").cloned())
            .filter(crate::app_settings::js_truthy);
        let Some(found) = found else {
            continue;
        };
        let has_poster = found
            .get("poster")
            .and_then(Value::as_str)
            .is_some_and(|poster| !poster.is_empty());
        if meta.is_none() {
            meta = Some(found.clone());
        }
        if has_poster {
            meta = Some(found);
            break;
        }
    }
    let mut cache = meta_cache();
    if cache.len() > MAX_CACHE_ENTRIES {
        cache.clear();
    }
    cache.insert(
        imdb.to_owned(),
        CacheEntry {
            meta: meta.clone(),
            at: Instant::now(),
        },
    );
    meta
}

/// `artOf`: a non-empty string field, or nothing.
pub fn art_of(meta: Option<&Value>, field: &str) -> Option<String> {
    meta.and_then(|meta| meta.get(field))
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .map(str::to_owned)
}

/// The poster Cinemeta has for an imdb id.
pub async fn poster_url(base: &str, imdb: &str) -> Option<String> {
    art_of(meta_for(base, imdb).await.as_ref(), "poster")
}

/// A wide still for heroes and rails; the poster is the honest fallback when Cinemeta has none.
pub async fn backdrop_url(base: &str, imdb: &str) -> Option<String> {
    let meta = meta_for(base, imdb).await;
    art_of(meta.as_ref(), "background").or_else(|| art_of(meta.as_ref(), "poster"))
}
