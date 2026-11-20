//! Which hosts the relay may fetch a pasted or fed URL from (`src/ssrf.js`): direct acquisitions,
//! feeds, identification and Cinemeta.
//!
//! Link-local (`169.254.0.0/16`, `fe80::/10`: cloud metadata) is always refused. Loopback,
//! private, CGNAT, unspecified, multicast and reserved addresses are refused unless the host opts
//! in with `PEARTUBE_ALLOW_PRIVATE_URLS=1` (a NAS on the LAN, local test suites). Names are
//! checked after DNS resolution, before connecting, so a name that resolves to a refused address
//! never connects.

use std::fmt;
use std::io;
use std::net::{IpAddr, SocketAddr, ToSocketAddrs};
use std::sync::{Arc, LazyLock};

use bytes::Bytes;
use http_body_util::Full;
use hyper::client::conn::http1;
use hyper::{Request, Response, body::Incoming};
use hyper_rustls::{
    ConfigBuilderExt, DefaultServerNameResolver, MaybeHttpsStream, ResolveServerName,
};
use hyper_util::rt::TokioIo;
use tokio::net::TcpStream;
use tokio_rustls::TlsConnector;

use url::Url;

use crate::protocol::ipv6_groups;

/// `privateUrlsAllowed()`: `PEARTUBE_ALLOW_PRIVATE_URLS` set to `"1"` or `"true"`.
pub fn private_urls_allowed() -> bool {
    matches!(
        std::env::var("PEARTUBE_ALLOW_PRIVATE_URLS").as_deref(),
        Ok("1" | "true")
    )
}

/// An SSRF refusal (`ERR_SSRF`, HTTP 403).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SsrfError {
    pub message: String,
    pub code: &'static str,
    pub status: u16,
}

impl SsrfError {
    pub fn refusal(host: &str, reason: &'static str) -> Self {
        Self {
            message: format!("Refusing to fetch from {host}: {reason} address"),
            code: "ERR_SSRF",
            status: 403,
        }
    }
}

impl fmt::Display for SsrfError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for SsrfError {}

pub(crate) fn ipv4_parts(ip: &str) -> Option<[u8; 4]> {
    let parts: Vec<&str> = ip.split('.').collect();
    if parts.len() != 4 {
        return None;
    }
    let mut out = [0u8; 4];
    for (idx, part) in parts.iter().enumerate() {
        if part.is_empty() || part.len() > 3 || !part.bytes().all(|b| b.is_ascii_digit()) {
            return None;
        }
        let n: u16 = part.parse().ok()?;
        if n > 255 {
            return None;
        }
        out[idx] = n as u8;
    }
    Some(out)
}

fn v4_reason(a: u8, b: u8) -> Option<&'static str> {
    if a == 169 && b == 254 {
        return Some("link-local");
    }
    if a == 127 {
        return Some("loopback");
    }
    if a == 0 {
        return Some("unspecified");
    }
    if a == 10 || (a == 172 && (16..=31).contains(&b)) || (a == 192 && b == 168) {
        return Some("private");
    }
    if a == 100 && (64..=127).contains(&b) {
        return Some("private");
    }
    if a >= 224 {
        return Some("reserved");
    }
    None
}

/// Why an address is refused (`"link-local"`, `"loopback"`, `"private"`, …), or `None` when it
/// may be fetched.
pub fn address_reason(ip: &str) -> Option<&'static str> {
    if let Some(v4) = ipv4_parts(ip) {
        return v4_reason(v4[0], v4[1]);
    }
    let Some(g) = ipv6_groups(ip) else {
        return Some("invalid");
    };
    // NAT64 well-known prefix (64:ff9b::/96, RFC 6052), IPv4-mapped (::ffff:a.b.c.d) and
    // IPv4-compatible (::a.b.c.d) addresses are their IPv4 address.
    if g[0] == 0x0064 && g[1] == 0xff9b && g[2..6].iter().all(|x| *x == 0) {
        return v4_reason((g[6] >> 8) as u8, (g[6] & 0xff) as u8);
    }
    if g[..5].iter().all(|x| *x == 0) && (g[5] == 0xffff || (g[5] == 0 && (g[6] != 0 || g[7] > 1)))
    {
        return v4_reason((g[6] >> 8) as u8, (g[6] & 0xff) as u8);
    }
    if (g[0] & 0xffc0) == 0xfe80 {
        return Some("link-local");
    }
    if g[..7].iter().all(|x| *x == 0) {
        return Some(if g[7] == 1 { "loopback" } else { "unspecified" });
    }
    if (g[0] & 0xfe00) == 0xfc00 {
        return Some("private");
    }
    if (g[0] & 0xff00) == 0xff00 {
        return Some("reserved");
    }
    None
}

pub fn refused(ip: &str, allow_private: bool) -> Option<&'static str> {
    let reason = address_reason(ip)?;
    if reason == "link-local" || reason == "invalid" || !allow_private {
        Some(reason)
    } else {
        None
    }
}

pub fn refused_ip(ip: IpAddr, allow_private: bool) -> Option<&'static str> {
    refused(&ip.to_string(), allow_private)
}

/// Checks a URL whose host is an IP literal; a name is checked when it resolves.
pub fn check_url_host(url: &Url, allow_private: bool) -> Result<(), SsrfError> {
    let host = url.host_str().unwrap_or_default();
    let host = host
        .strip_prefix('[')
        .unwrap_or(host)
        .strip_suffix(']')
        .unwrap_or(host);
    if ipv4_parts(host).is_none() && ipv6_groups(host).is_none() {
        return Ok(());
    }
    if let Some(reason) = refused(host, allow_private) {
        return Err(SsrfError::refusal(host, reason));
    }
    Ok(())
}

/// Resolves `host:port` and checks every returned address against the SSRF rules before any
/// socket connects.
pub async fn resolve_guarded(
    host: &str,
    port: u16,
    allow_private: bool,
) -> Result<Vec<SocketAddr>, io::Error> {
    let clean = host
        .strip_prefix('[')
        .unwrap_or(host)
        .strip_suffix(']')
        .unwrap_or(host)
        .to_owned();
    if ipv4_parts(&clean).is_some() || ipv6_groups(&clean).is_some() {
        if let Some(reason) = refused(&clean, allow_private) {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                SsrfError::refusal(&clean, reason),
            ));
        }
        let ip: IpAddr = clean
            .parse()
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "Invalid IP literal"))?;
        return Ok(vec![SocketAddr::new(ip, port)]);
    }
    let lookup_host = clean.clone();
    let addrs: Vec<SocketAddr> = tokio::task::spawn_blocking(move || {
        (lookup_host.as_str(), port)
            .to_socket_addrs()
            .map(Iterator::collect)
    })
    .await
    .map_err(io::Error::other)??;
    if addrs.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::NotFound,
            format!("No address for {clean}"),
        ));
    }
    for addr in &addrs {
        if let Some(reason) = refused_ip(addr.ip(), allow_private) {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                SsrfError::refusal(&clean, reason),
            ));
        }
    }
    Ok(addrs)
}

static TLS_CONFIG: LazyLock<Arc<tokio_rustls::rustls::ClientConfig>> = LazyLock::new(|| {
    let _ = tokio_rustls::rustls::crypto::ring::default_provider().install_default();
    Arc::new(
        tokio_rustls::rustls::ClientConfig::builder()
            .with_webpki_roots()
            .with_no_client_auth(),
    )
});

/// Opens a fresh HTTP/1.1 connection to `url` (`agent: false`: never reusing a pooled socket)
/// after checking both IP literals and every resolved DNS address against the SSRF rules.
pub async fn send_guarded_request(
    url: &Url,
    mut request: Request<Full<Bytes>>,
    allow_private: bool,
) -> Result<Response<Incoming>, GuardedRequestError> {
    check_url_host(url, allow_private).map_err(GuardedRequestError::Ssrf)?;
    let https = match url.scheme() {
        "https" => true,
        "http" => false,
        other => {
            return Err(GuardedRequestError::Transport(format!(
                "Unsupported scheme: {other}"
            )));
        }
    };
    let host = url.host_str().unwrap_or_default();
    let port = url.port().unwrap_or(if https { 443 } else { 80 });
    let addrs = resolve_guarded(host, port, allow_private)
        .await
        .map_err(|err| {
            if let Some(ssrf) = err.get_ref().and_then(|e| e.downcast_ref::<SsrfError>()) {
                GuardedRequestError::Ssrf(ssrf.clone())
            } else {
                GuardedRequestError::Transport(err.to_string())
            }
        })?;
    let mut last_err = None;
    let mut stream = None;
    for addr in addrs {
        match TcpStream::connect(addr).await {
            Ok(sock) => {
                let _ = sock.set_nodelay(true);
                stream = Some(sock);
                break;
            }
            Err(err) => last_err = Some(err),
        }
    }
    let tcp = match stream {
        Some(sock) => TokioIo::new(sock),
        None => {
            return Err(GuardedRequestError::Transport(
                last_err
                    .map(|e| e.to_string())
                    .unwrap_or_else(|| format!("Failed to connect to {host}:{port}")),
            ));
        }
    };
    let io: MaybeHttpsStream<TokioIo<TcpStream>> = if https {
        let uri: hyper::Uri = url
            .as_str()
            .parse()
            .map_err(|e: hyper::http::uri::InvalidUri| {
                GuardedRequestError::Transport(e.to_string())
            })?;
        let server_name = DefaultServerNameResolver::default()
            .resolve(&uri)
            .map_err(|e| GuardedRequestError::Transport(e.to_string()))?;
        let tls = TlsConnector::from(TLS_CONFIG.clone())
            .connect(server_name, TokioIo::new(tcp))
            .await
            .map_err(|e| GuardedRequestError::Transport(e.to_string()))?;
        MaybeHttpsStream::Https(TokioIo::new(tls))
    } else {
        MaybeHttpsStream::Http(tcp)
    };
    if !request.headers().contains_key(hyper::header::HOST) {
        let host_header = match url.port() {
            Some(p) => format!("{host}:{p}"),
            None => host.to_owned(),
        };
        if let Ok(hv) = hyper::header::HeaderValue::from_str(&host_header) {
            request.headers_mut().insert(hyper::header::HOST, hv);
        }
    }
    // HTTP/1.1 origin-form request target.
    let target = match url.query() {
        Some(q) => format!("{}?{q}", url.path()),
        None => url.path().to_owned(),
    };
    *request.uri_mut() = target
        .parse()
        .unwrap_or_else(|_| hyper::Uri::from_static("/"));

    let (mut sender, conn) = http1::handshake(io)
        .await
        .map_err(|e| GuardedRequestError::Transport(e.to_string()))?;
    tokio::spawn(async move {
        let _ = conn.await;
    });
    sender
        .send_request(request)
        .await
        .map_err(|e| GuardedRequestError::Transport(e.to_string()))
}

#[derive(Debug)]
pub enum GuardedRequestError {
    Ssrf(SsrfError),
    Transport(String),
}

impl fmt::Display for GuardedRequestError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Ssrf(err) => f.write_str(&err.message),
            Self::Transport(msg) => f.write_str(msg),
        }
    }
}

impl std::error::Error for GuardedRequestError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn nat64_and_private_classification_matches_js() {
        assert_eq!(address_reason("64:ff9b::a9fe:a9fe"), Some("link-local"));
        assert_eq!(address_reason("64:ff9b::7f00:1"), Some("loopback"));
        assert_eq!(address_reason("64:ff9b::a00:1"), Some("private"));
        assert_eq!(address_reason("169.254.169.254"), Some("link-local"));
        assert_eq!(address_reason("fe80::1"), Some("link-local"));
        assert_eq!(address_reason("127.0.0.1"), Some("loopback"));
        assert_eq!(address_reason("::1"), Some("loopback"));
        assert_eq!(address_reason("::ffff:127.0.0.1"), Some("loopback"));
        assert_eq!(address_reason("10.0.0.1"), Some("private"));
        assert_eq!(address_reason("100.64.0.1"), Some("private"));
        assert_eq!(address_reason("93.184.216.34"), None);
        assert_eq!(refused("64:ff9b::a9fe:a9fe", true), Some("link-local"));
        assert_eq!(refused("127.0.0.1", true), None);
        assert_eq!(refused("127.0.0.1", false), Some("loopback"));
    }
}
