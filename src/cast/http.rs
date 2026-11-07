//! HTTP plumbing shared by both roles: Range parsing, media MIME guessing, CORS, the server loop
//! and the JSON client, ported from `parseHttpRange`, `guessMediaMime`, `CORS`, `listenHttp`,
//! `attachSocketTracker` and `httpJson` in `src/cast.js`.

use std::convert::Infallible;
use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use futures_util::future::BoxFuture;
use http_body_util::{BodyExt, Empty, Full, StreamBody, combinators::UnsyncBoxBody};
use hyper::body::{Frame, Incoming};
use hyper::header::{self, HeaderMap, HeaderName, HeaderValue};
use hyper::{Method, Request, Response, StatusCode};
use hyper_util::client::legacy::Client;
use hyper_util::client::legacy::connect::HttpConnector;
use hyper_util::rt::{TokioExecutor, TokioIo};
use serde_json::{Map, Value, json};
use tokio::io::AsyncReadExt;
use tokio::net::TcpListener;
use tokio::sync::mpsc;

use super::state::{is_safe_integer, js_number, text_value};
use super::{CastError, MAX_JSON_BODY};

/// Every receiver response body.
pub type Body = UnsyncBoxBody<Bytes, io::Error>;
/// The shared request handler; the peer address feeds `web-info`'s local check.
pub type Handler =
    Arc<dyn Fn(Request<Incoming>, SocketAddr) -> BoxFuture<'static, Response<Body>> + Send + Sync>;

/// `CORS`: every route answers preflights and exposes the Range headers a browser player reads.
pub fn cors_headers(headers: &mut HeaderMap) {
    headers.insert(
        header::ACCESS_CONTROL_ALLOW_ORIGIN,
        HeaderValue::from_static("*"),
    );
    headers.insert(
        header::ACCESS_CONTROL_ALLOW_METHODS,
        HeaderValue::from_static("GET, HEAD, POST, DELETE, OPTIONS"),
    );
    headers.insert(
        header::ACCESS_CONTROL_ALLOW_HEADERS,
        HeaderValue::from_static("content-type, authorization, range"),
    );
    headers.insert(
        header::ACCESS_CONTROL_EXPOSE_HEADERS,
        HeaderValue::from_static("content-length, content-range, accept-ranges"),
    );
}

/// An empty, already-boxed body (for responses whose body follows later).
pub fn empty_body() -> Body {
    Empty::new().map_err(|e| match e {}).boxed_unsync()
}

/// An empty response with the CORS headers attached.
pub fn empty(status: StatusCode) -> Response<Body> {
    let mut res = Response::new(Empty::new().map_err(|e| match e {}).boxed_unsync());
    *res.status_mut() = status;
    cors_headers(res.headers_mut());
    res
}

/// `res.writeHead(status, {...CORS, 'content-type': 'application/json'})` + `JSON.stringify`.
pub fn json_response(status: u16, value: &Value) -> Response<Body> {
    let text = serde_json::to_string(value).unwrap_or_else(|_| "{}".to_owned());
    let mut res = Response::new(
        Full::new(Bytes::from(text))
            .map_err(|e| match e {})
            .boxed_unsync(),
    );
    *res.status_mut() = StatusCode::from_u16(status).unwrap_or(StatusCode::BAD_REQUEST);
    cors_headers(res.headers_mut());
    res.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/json"),
    );
    res
}

/// A UTF-8 HTML response with the CORS headers.
pub fn html_response(html: String) -> Response<Body> {
    let mut res = Response::new(
        Full::new(Bytes::from(html))
            .map_err(|e| match e {})
            .boxed_unsync(),
    );
    cors_headers(res.headers_mut());
    res.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("text/html; charset=utf-8"),
    );
    res
}

/// A 204 preflight answer.
pub fn preflight() -> Response<Body> {
    empty(StatusCode::NO_CONTENT)
}

/// A body fed from a channel, used for SSE streams and Range responses.
pub fn channel_body(rx: mpsc::Receiver<Result<Frame<Bytes>, io::Error>>) -> Body {
    let stream = futures_util::stream::unfold(rx, |mut rx| async move {
        rx.recv().await.map(|item| (item, rx))
    });
    StreamBody::new(stream).boxed_unsync()
}

/// What a `Range` header means for one media size.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RangeSpec {
    /// No (usable) header: the whole object.
    Full { start: u64, end: u64 },
    /// `bytes=a-b`, `bytes=a-` or `bytes=-n`, clamped to the object.
    Partial { start: u64, end: u64 },
    /// Syntactically invalid or unsatisfiable: answer 416.
    Invalid,
}

/// `parseHttpRange`. A missing, foreign or empty total answers the whole object, exactly like the
/// app's `{ partial: false, start: 0, end: max(0, totalSize - 1) }`.
pub fn parse_http_range(header: Option<&str>, total: u64) -> RangeSpec {
    let full = RangeSpec::Full {
        start: 0,
        end: total.saturating_sub(1),
    };
    let Some(header) = header else { return full };
    if !header.starts_with("bytes=") || total == 0 {
        return full;
    }
    let spec = header[6..].split(',').next().unwrap_or_default().trim();
    let Some(dash) = spec.find('-') else {
        return RangeSpec::Invalid;
    };
    let left = spec[..dash].trim();
    let right = spec[dash + 1..].trim();
    let (start, end);
    if left.is_empty() {
        let suffix = js_number(right);
        if !is_safe_integer(suffix) || suffix <= 0.0 {
            return RangeSpec::Invalid;
        }
        start = total.saturating_sub(suffix as u64);
        end = total - 1;
    } else {
        let raw_start = js_number(left);
        if !is_safe_integer(raw_start) || raw_start < 0.0 {
            return RangeSpec::Invalid;
        }
        let raw_end = if right.is_empty() {
            total as f64 - 1.0
        } else {
            js_number(right)
        };
        if !is_safe_integer(raw_end) {
            return RangeSpec::Invalid;
        }
        start = raw_start as u64;
        end = (raw_end as u64).min(total - 1);
    }
    if start >= total || start > end {
        return RangeSpec::Invalid;
    }
    RangeSpec::Partial { start, end }
}

/// `guessMediaMime`: Safari/WebKit demand `video/*` on 206 slices.
pub fn guess_media_mime(entry: &Value) -> String {
    let title = entry.get("title").map(text_value).unwrap_or_default();
    let id = entry.get("id").map(text_value).unwrap_or_default();
    let haystack = format!("{title} {id}").to_lowercase();
    if haystack.contains(".webm") {
        "video/webm".to_owned()
    } else if haystack.contains(".mkv") {
        "video/x-matroska".to_owned()
    } else if haystack.contains(".avi") {
        "video/x-msvideo".to_owned()
    } else {
        "video/mp4".to_owned()
    }
}

/// `readJsonBody` over an already-collected body: empty is `{}`, malformed is a 400.
pub fn read_json_body(bytes: &[u8]) -> Result<Value, CastError> {
    if bytes.is_empty() {
        return Ok(json!({}));
    }
    serde_json::from_slice(bytes).map_err(|_| CastError::bad_request("Body must be JSON"))
}

/// Frames an already-validated `data: …\n\n` SSE payload.
pub fn sse_frame(event: &Value) -> Bytes {
    Bytes::from(format!("data: {event}\n\n"))
}

/// `attachSocketTracker`: a server whose close aborts every open connection (SSE included).
pub struct HttpServer {
    addr: SocketAddr,
    accept: tokio::task::JoinHandle<()>,
}

impl HttpServer {
    /// `listenHttp(port, host, anyPort)`: a taken port falls back to an ephemeral one.
    pub async fn bind(port: u16, host: &str, any_port: bool, handler: Handler) -> io::Result<Self> {
        let mut chosen = TcpListener::bind((host, port)).await;
        if chosen.is_err() && any_port && port != 0 {
            chosen = TcpListener::bind((host, 0)).await;
        }
        let listener = chosen?;
        let addr = listener.local_addr()?;
        let accept = tokio::spawn(accept_loop(listener, handler));
        Ok(Self { addr, accept })
    }

    pub fn addr(&self) -> SocketAddr {
        self.addr
    }

    pub fn port(&self) -> u16 {
        self.addr.port()
    }

    /// `closeAllAndStop`: drop every connection, then stop accepting.
    pub async fn close(self) {
        self.accept.abort();
        let _ = self.accept.await;
    }
}

async fn accept_loop(listener: TcpListener, handler: Handler) {
    // Dropping the set with this task aborts every in-flight connection.
    let mut conns = tokio::task::JoinSet::new();
    loop {
        tokio::select! {
            accepted = listener.accept() => {
                let Ok((sock, peer)) = accepted else { continue };
                let _ = sock.set_nodelay(true);
                let handler = Arc::clone(&handler);
                conns.spawn(async move {
                    let service = hyper::service::service_fn(move |req: Request<Incoming>| {
                        let handler = Arc::clone(&handler);
                        async move { Ok::<_, Infallible>(handler(req, peer).await) }
                    });
                    let _ = hyper::server::conn::http1::Builder::new()
                        .serve_connection(TokioIo::new(sock), service)
                        .await;
                });
            }
            Some(_) = conns.join_next(), if !conns.is_empty() => {}
        }
    }
}

/// Reads a request body, refusing anything past [`MAX_JSON_BODY`].
pub async fn collect_body(req: Request<Incoming>) -> Result<Bytes, CastError> {
    let (_, body) = req.into_parts();
    let collected = body
        .collect()
        .await
        .map_err(|err| CastError::new(err.to_string(), 400))?
        .to_bytes();
    if collected.len() > MAX_JSON_BODY {
        return Err(CastError::new("Body too large", 413));
    }
    Ok(collected)
}

/// The app's HTTP client (`httpJson`), plus the streaming request the controller's SSE needs.
pub struct HttpClient {
    client: Client<HttpConnector, Full<Bytes>>,
}

impl Default for HttpClient {
    fn default() -> Self {
        Self::new()
    }
}

impl HttpClient {
    pub fn new() -> Self {
        Self {
            client: Client::builder(TokioExecutor::new()).build(HttpConnector::new()),
        }
    }

    /// A plain request; the caller streams `Incoming` frames itself.
    pub async fn request(
        &self,
        method: Method,
        url: &str,
        headers: Vec<(HeaderName, String)>,
        body: Option<&Value>,
        timeout: Duration,
    ) -> Result<Response<Incoming>, CastError> {
        let payload: Bytes = body
            .map(|value| {
                Bytes::from(serde_json::to_vec(value).unwrap_or_else(|_| b"null".to_vec()))
            })
            .unwrap_or_default();
        let mut builder = Request::builder().method(method).uri(url);
        for (name, value) in headers {
            builder = builder.header(name, value);
        }
        if body.is_some() {
            builder = builder
                .header(header::CONTENT_TYPE, "application/json")
                .header(header::CONTENT_LENGTH, payload.len().to_string());
        }
        let request = builder
            .body(Full::new(payload))
            .map_err(|err| CastError::new(err.to_string(), 400))?;
        match tokio::time::timeout(timeout, self.client.request(request)).await {
            Err(_) => Err(CastError::new("Receiver HTTP request timed out", 504)),
            Ok(Err(err)) => Err(CastError::new(err.to_string(), 502)),
            Ok(Ok(response)) => Ok(response),
        }
    }

    /// `httpJson`: JSON body, `{}` for an empty one, `error` on a non-2xx answer.
    pub async fn json(
        &self,
        method: Method,
        url: &str,
        headers: Vec<(HeaderName, String)>,
        body: Option<&Value>,
        timeout: Duration,
    ) -> Result<Value, CastError> {
        let response = self.request(method, url, headers, body, timeout).await?;
        let status = response.status();
        let bytes = response
            .into_body()
            .collect()
            .await
            .map_err(|err| CastError::new(err.to_string(), 502))?
            .to_bytes();
        let text = String::from_utf8_lossy(&bytes).to_string();
        let parsed: Value = if text.is_empty() {
            json!({})
        } else {
            match serde_json::from_str(&text) {
                Ok(value) => value,
                Err(_) => json!({
                    "error": if text.is_empty() {
                        format!("HTTP {}", status.as_u16())
                    } else {
                        text
                    }
                }),
            }
        };
        if status.is_success() {
            Ok(parsed)
        } else {
            let message = parsed
                .get("error")
                .and_then(Value::as_str)
                .map(str::to_owned)
                .unwrap_or_else(|| format!("Receiver answered {}", status.as_u16()));
            Err(CastError::new(message, status.as_u16()))
        }
    }

    /// One upstream Range slice, for the hybrid splice.
    pub async fn range_slice(
        &self,
        url: &str,
        start: u64,
        end: u64,
        timeout: Option<Duration>,
    ) -> Result<Response<Incoming>, CastError> {
        let request = Request::builder()
            .method(Method::GET)
            .uri(url)
            .header(header::RANGE, format!("bytes={start}-{end}"))
            .body(Full::new(Bytes::new()))
            .map_err(|err| CastError::new(err.to_string(), 400))?;
        let future = self.client.request(request);
        match timeout {
            Some(limit) => match tokio::time::timeout(limit, future).await {
                Err(_) => Err(CastError::new("Upstream slice stalled", 504)),
                Ok(Err(err)) => Err(CastError::new(err.to_string(), 502)),
                Ok(Ok(response)) => Ok(response),
            },
            None => future
                .await
                .map_err(|err| CastError::new(err.to_string(), 502)),
        }
    }
}

/// Key/value pairs as the receiver's `/cast/v1/info` orders them.
pub fn json_object(pairs: Vec<(&str, Value)>) -> Value {
    let mut out = Map::with_capacity(pairs.len());
    for (key, value) in pairs {
        out.insert(key.to_owned(), value);
    }
    Value::Object(out)
}

/// The bearer token (or query token) a request carries.
pub fn bearer_token(headers: &HeaderMap, query: Option<&str>) -> String {
    let header = headers
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default();
    let bearer = header
        .strip_prefix("Bearer ")
        .map(str::trim)
        .unwrap_or_default();
    if !bearer.is_empty() {
        bearer.to_owned()
    } else {
        query.unwrap_or_default().to_owned()
    }
}

/// Reads `token` out of a raw query string.
pub fn query_param(query: Option<&str>, name: &str) -> Option<String> {
    for part in query?.split('&') {
        if let Some((key, value)) = part.split_once('=')
            && key == name
        {
            return Some(percent_decode(value));
        }
    }
    None
}

/// `decodeURIComponent`, returning the raw text when it is not valid percent-encoding.
pub fn percent_decode(text: &str) -> String {
    let bytes = text.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' && i + 2 < bytes.len() {
            let hex = std::str::from_utf8(&bytes[i + 1..i + 3]).ok();
            if let Some(byte) = hex.and_then(|h| u8::from_str_radix(h, 16).ok()) {
                out.push(byte);
                i += 3;
                continue;
            }
        }
        out.push(bytes[i]);
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// Reads one media chunk's bytes without buffering the whole body.
pub async fn read_frame(frame: Frame<Bytes>) -> Option<Bytes> {
    frame.into_data().ok()
}

/// Reads a whole small body (used by tests and the hub's registration route).
pub async fn read_all<R>(reader: &mut R) -> io::Result<Vec<u8>>
where
    R: tokio::io::AsyncRead + Unpin,
{
    let mut buf = Vec::new();
    reader.read_to_end(&mut buf).await?;
    Ok(buf)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn range(header: Option<&str>, total: u64) -> RangeSpec {
        parse_http_range(header, total)
    }

    #[test]
    fn missing_or_foreign_headers_mean_the_whole_object() {
        assert_eq!(range(None, 10), RangeSpec::Full { start: 0, end: 9 });
        assert_eq!(
            range(Some("items=0-1"), 10),
            RangeSpec::Full { start: 0, end: 9 }
        );
        assert_eq!(range(None, 0), RangeSpec::Full { start: 0, end: 0 });
        assert_eq!(
            range(Some("bytes=0-1"), 0),
            RangeSpec::Full { start: 0, end: 0 }
        );
    }

    #[test]
    fn explicit_and_open_ranges_clamp_to_the_total() {
        assert_eq!(
            range(Some("bytes=2-5"), 10),
            RangeSpec::Partial { start: 2, end: 5 }
        );
        assert_eq!(
            range(Some("bytes=7-"), 10),
            RangeSpec::Partial { start: 7, end: 9 }
        );
        assert_eq!(
            range(Some("bytes=7-99"), 10),
            RangeSpec::Partial { start: 7, end: 9 }
        );
        assert_eq!(
            range(Some("bytes=-3"), 10),
            RangeSpec::Partial { start: 7, end: 9 }
        );
        assert_eq!(
            range(Some("bytes=1-2,5-6"), 10),
            RangeSpec::Partial { start: 1, end: 2 },
            "only the first range is served"
        );
    }

    #[test]
    fn unsatisfiable_and_malformed_ranges_are_invalid() {
        assert_eq!(range(Some("bytes=10-"), 10), RangeSpec::Invalid);
        assert_eq!(range(Some("bytes=5-4"), 10), RangeSpec::Invalid);
        assert_eq!(range(Some("bytes=-0"), 10), RangeSpec::Invalid);
        assert_eq!(range(Some("bytes=abc"), 10), RangeSpec::Invalid);
        assert_eq!(range(Some("bytes="), 10), RangeSpec::Invalid);
    }

    #[test]
    fn mime_follows_the_entry_name() {
        assert_eq!(
            guess_media_mime(&json!({"title": "Film.WEBM"})),
            "video/webm"
        );
        assert_eq!(
            guess_media_mime(&json!({"title": "Film", "id": "x.mkv"})),
            "video/x-matroska"
        );
        assert_eq!(
            guess_media_mime(&json!({"id": "show.avi"})),
            "video/x-msvideo"
        );
        assert_eq!(guess_media_mime(&json!({"title": "Film"})), "video/mp4");
    }
}
