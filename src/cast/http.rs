//! The receiver's loopback HTTP: Range parsing, media MIME guessing, CORS and the server loop,
//! ported from `parseHttpRange`, `guessMediaMime`, `CORS`, `listenHttp` and
//! `attachSocketTracker` in `src/cast.js`. The receiver's player reads its film here; nothing on
//! the LAN answers on this port.

use std::convert::Infallible;
use std::io;
use std::net::SocketAddr;
use std::sync::Arc;

use bytes::Bytes;
use http_body_util::{BodyExt, Empty, Full, StreamBody, combinators::UnsyncBoxBody};
use hyper::body::{Frame, Incoming};
use hyper::header::{self, HeaderMap, HeaderValue};
use hyper::{Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use serde_json::Value;
use tokio::net::TcpListener;
use tokio::sync::mpsc;

use super::state::{is_safe_integer, js_number, text_value};

/// Every receiver response body.
pub type Body = UnsyncBoxBody<Bytes, io::Error>;
/// The shared request handler; the peer address feeds `/cast/v1/info`'s local check.
pub type Handler =
    Arc<dyn Fn(Request<Incoming>, SocketAddr) -> BoxFuture<'static, Response<Body>> + Send + Sync>;

use futures_util::future::BoxFuture;

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
    let mut res = Response::new(empty_body());
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

/// A body fed from a channel, used for Range responses.
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

/// `attachSocketTracker`: a server whose close aborts every open connection.
pub struct HttpServer {
    addr: SocketAddr,
    accept: tokio::task::JoinHandle<()>,
}

impl HttpServer {
    /// `listenHttp(port, '127.0.0.1', anyPort)`: a taken port falls back to an ephemeral one.
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ranges_match_the_app() {
        assert_eq!(
            parse_http_range(None, 100),
            RangeSpec::Full { start: 0, end: 99 }
        );
        assert_eq!(
            parse_http_range(Some("bytes=10-19"), 100),
            RangeSpec::Partial { start: 10, end: 19 }
        );
        assert_eq!(
            parse_http_range(Some("bytes=-20"), 100),
            RangeSpec::Partial { start: 80, end: 99 }
        );
        assert_eq!(
            parse_http_range(Some("bytes=90-"), 100),
            RangeSpec::Partial { start: 90, end: 99 }
        );
        assert_eq!(
            parse_http_range(Some("bytes=200-"), 100),
            RangeSpec::Invalid
        );
        assert_eq!(parse_http_range(Some("bytes=x-2"), 100), RangeSpec::Invalid);
        assert_eq!(
            parse_http_range(Some("items=0-1"), 100),
            RangeSpec::Full { start: 0, end: 99 }
        );
    }
}
