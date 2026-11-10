//! The receiver's media side: the blob/entry a load points at, cached-byte accounting, the
//! windowed P2P route and the byte-exact HTTP→P2P splice, ported from `prepareMediaLoad`'s media
//! object, `hasBlockCached`, `hasActiveReplicationPeer`, `countCachedBlobBytes`,
//! `serveWindowedStream` and `serveSpliceStream` in `src/cast.js`.
//!
//! The app steers one sliding download window per stream every second (`steerReadAhead`) on the
//! node's core; the port hands the same disk-aware window size to `byte-stream-rs` for each
//! response, and asks the node to trim watched blocks from [`evict_plan`]. Port notes record the
//! difference.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use bytes::Bytes;
use http_body_util::BodyExt;
use hyper::body::Frame;
use hyper::header::{self, HeaderValue};
use hyper::{Method, Response, StatusCode};
use hyperblobs::BlobId;
use hypercore::{Core, RequestBag};
use serde_json::{Map, Value, json};
use tokio::sync::mpsc;

use byte_stream::{ByteStream, StreamOptions};

use super::CastError;

use super::http::{Body, RangeSpec, channel_body, cors_headers, parse_http_range};
use super::traits::{CastPeer, EvictOptions};
use super::{
    BEHIND_SECONDS, COLD_HTTP_WINDOW, DEFAULT_STREAM_BYTES_PER_SEC, EVICT_EVERY_MS,
    MIN_READ_AHEAD_SECONDS, READ_AHEAD_SECONDS, STREAM_FLOOR_BYTES, STREAM_RESERVE_BYTES,
    TRIM_FREE_BYTES,
};

/// Bytes a reader must have been sent before it counts as the streaming player.
const LEAD_MIN_BYTES: u64 = 2 * 1024 * 1024;
/// Cached-byte recounts are reused for this long (`cachedCounts`).
const COUNT_TTL_MS: u64 = 3_000;
/// A reader opened in this window counts as still settling after a start or seek.
pub const SETTLING_MS: u64 = 20_000;
/// Behind-the-player bytes kept on a tight disk.
const TIGHT_KEEP_BYTES: u64 = 64 * 1024 * 1024;

/// One load's media, as `activeMedia` / `streams` hold it.
pub struct Media {
    pub token: String,
    pub entry: Value,
    pub mime: String,
    pub prefer_mode: String,
    pub p2p_mode: String,
    pub private_routing: bool,
    pub peer: Option<Arc<dyn CastPeer>>,
    pub http_stream_url: Option<String>,
    /// The loopback `/cast/v1/p2p/<token>` route, or the node's own blob link.
    pub local_p2p_url: Option<String>,
    pub core: Option<Core>,
    pub core_key_hex: String,
    pub blob: Option<BlobId>,
    sender_http_alive: AtomicBool,
    served_bytes: AtomicU64,
    read_rate: Mutex<f64>,
    rate_served: AtomicU64,
    rate_at: Mutex<Instant>,
    avg_rate: Mutex<f64>,
    readers: Mutex<Vec<Reader>>,
    last_reader_at: Mutex<Instant>,
    counts: Mutex<Option<(u64, Instant, u64)>>,
}

#[derive(Clone, Copy)]
struct Reader {
    id: u64,
    pos: u64,
    sent: u64,
    at: Instant,
}

impl Media {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        token: String,
        entry: Value,
        mime: String,
        prefer_mode: String,
        p2p_mode: String,
        private_routing: bool,
        peer: Option<Arc<dyn CastPeer>>,
        http_stream_url: Option<String>,
        local_p2p_url: Option<String>,
        core: Option<Core>,
        core_key_hex: String,
        blob: Option<BlobId>,
        sender_http_alive: bool,
    ) -> Self {
        Self {
            token,
            entry,
            mime,
            prefer_mode,
            p2p_mode,
            private_routing,
            peer,
            http_stream_url,
            local_p2p_url,
            core,
            core_key_hex,
            blob,
            sender_http_alive: AtomicBool::new(sender_http_alive),
            served_bytes: AtomicU64::new(0),
            read_rate: Mutex::new(0.0),
            rate_served: AtomicU64::new(0),
            rate_at: Mutex::new(Instant::now()),
            avg_rate: Mutex::new(0.0),
            readers: Mutex::new(Vec::new()),
            last_reader_at: Mutex::new(Instant::now()),
            counts: Mutex::new(None),
        }
    }

    /// `blob.byteLength || entry.size`, whichever the load knows.
    pub fn total_size(&self) -> u64 {
        if let Some(blob) = self.blob
            && blob.byte_length > 0
        {
            return blob.byte_length;
        }
        self.entry.get("size").and_then(Value::as_u64).unwrap_or(0)
    }

    pub fn sender_http_alive(&self) -> bool {
        self.sender_http_alive.load(Ordering::Relaxed)
    }

    /// `estimateBlobBlock`: a blob's mean block size locates the byte in a block.
    fn estimate_block(&self, byte_start: u64) -> Option<u64> {
        let blob = self.blob?;
        if blob.block_length == 0 {
            return None;
        }
        let total = if blob.byte_length > 0 {
            blob.byte_length
        } else {
            DEFAULT_STREAM_BYTES_PER_SEC
        };
        let avg = (total / blob.block_length.max(1)).max(4096);
        Some(blob.block_offset + (byte_start / avg).min(blob.block_length - 1))
    }

    /// `hasBlockCached`.
    pub fn has_block_cached(&self, byte_start: u64) -> bool {
        let (Some(core), Some(block)) = (self.core.as_ref(), self.estimate_block(byte_start))
        else {
            return false;
        };
        core.length() > block && core.has(block)
    }

    /// `hasActiveReplicationPeer`: a peer holds the whole video.
    pub fn has_active_peer(&self) -> bool {
        let (Some(core), Some(blob)) = (self.core.as_ref(), self.blob) else {
            return false;
        };
        core.remote_contiguous_length() >= blob.block_offset + blob.block_length
    }

    /// `countCachedBlobBytes`, cached for three seconds so the status loop is cheap.
    pub async fn cached_bytes(&self) -> u64 {
        let (Some(core), Some(blob)) = (self.core.as_ref(), self.blob) else {
            return 0;
        };
        if blob.block_length == 0 || core.length() <= blob.block_offset {
            return 0;
        }
        let total = self.total_size();
        let key = (blob.block_offset << 32) | core.length();
        if let Some((cached_key, cached_at, value)) =
            *self.counts.lock().expect("cast media counts")
            && cached_key == key
            && cached_at.elapsed() < Duration::from_millis(COUNT_TTL_MS)
        {
            return value;
        }
        let end = (blob.block_offset + blob.block_length).min(core.length());
        let mut blocks = 0u64;
        for index in blob.block_offset..end {
            if core.has(index) {
                blocks += 1;
            }
        }
        let value = if blocks == 0 {
            0
        } else if blocks >= blob.block_length {
            total
        } else {
            total.min((blocks as f64 / blob.block_length as f64 * total as f64).round() as u64)
        };
        *self.counts.lock().expect("cast media counts") = Some((key, Instant::now(), value));
        value
    }

    fn register_reader(&self, start: u64) -> u64 {
        static NEXT: AtomicU64 = AtomicU64::new(1);
        let id = NEXT.fetch_add(1, Ordering::Relaxed);
        self.readers.lock().expect("cast readers").push(Reader {
            id,
            pos: start,
            sent: 0,
            at: Instant::now(),
        });
        *self.last_reader_at.lock().expect("cast readers") = Instant::now();
        id
    }

    fn advance_reader(&self, id: u64, pos: u64, delta: u64) {
        if let Some(reader) = self
            .readers
            .lock()
            .expect("cast readers")
            .iter_mut()
            .find(|r| r.id == id)
        {
            reader.pos = pos;
            reader.sent += delta;
            reader.at = Instant::now();
        }
        self.served_bytes.fetch_add(delta, Ordering::Relaxed);
    }

    fn drop_reader(&self, id: u64) {
        self.readers
            .lock()
            .expect("cast readers")
            .retain(|r| r.id != id);
    }

    /// `steerStreams`' rate estimate for one stream.
    pub fn refresh_rate(&self) {
        let now = Instant::now();
        let mut at = self.rate_at.lock().expect("cast media rate");
        let seconds = (now.duration_since(*at).as_secs_f64()).max(0.25);
        *at = now;
        let served = self.served_bytes.load(Ordering::Relaxed);
        let previous = self.rate_served.swap(served, Ordering::Relaxed);
        let instant = served.saturating_sub(previous) as f64 / seconds;
        let mut rate = self.read_rate.lock().expect("cast media rate");
        *rate = 0.8 * *rate + 0.2 * instant;
    }

    pub fn rate(&self, duration_ms: Option<u64>) -> f64 {
        let avg = match duration_ms {
            Some(ms) if ms > 0 => self.total_size() as f64 / (ms as f64 / 1000.0),
            _ => {
                let known = *self.avg_rate.lock().expect("cast media rate");
                if known > 0.0 {
                    known
                } else {
                    DEFAULT_STREAM_BYTES_PER_SEC as f64
                }
            }
        };
        *self.avg_rate.lock().expect("cast media rate") = avg;
        let recent = *self.read_rate.lock().expect("cast media rate");
        avg.max(recent.min(2.0 * avg))
    }

    pub fn furthest_reader_byte(&self) -> u64 {
        self.readers
            .lock()
            .expect("cast readers")
            .iter()
            .map(|reader| reader.pos)
            .max()
            .unwrap_or(0)
    }

    /// The playhead in bytes: the lead reader's position, else the reported position.
    pub fn lead_byte(&self, state: &Map<String, Value>, duration_ms: Option<u64>) -> u64 {
        let readers = self.readers.lock().expect("cast readers");
        let lead = readers
            .iter()
            .filter(|r| r.sent >= LEAD_MIN_BYTES)
            .max_by_key(|r| r.at)
            .or_else(|| readers.last());
        let lead_pos = match lead {
            Some(reader) => reader.pos,
            None => {
                let total = self.total_size();
                let position = state
                    .get("positionMs")
                    .map(super::state::num)
                    .unwrap_or(0.0);
                match duration_ms {
                    Some(ms) if ms > 0 => ((position / ms as f64) * total as f64) as u64,
                    _ => 0,
                }
            }
        };
        readers
            .iter()
            .map(|reader| reader.pos)
            .min()
            .map_or(lead_pos, |pos| pos.min(lead_pos))
    }

    /// Whether a peer holds the whole video (or the cache covers it).
    pub fn has_peer(&self) -> bool {
        self.has_active_peer()
    }

    /// When a reader last started or advanced.
    pub fn last_reader_at(&self) -> Instant {
        *self.last_reader_at.lock().expect("cast readers")
    }

    /// The byte count a response may read ahead, from [`read_ahead_bytes`].
    pub fn window_bytes(&self, free: u64) -> u64 {
        let rate = self.rate(None);
        let cached = 0;
        let wanted = read_ahead_bytes(rate, free, cached);
        self.peer
            .as_ref()
            .and_then(|peer| peer.memory_cache_bytes())
            .map_or(wanted, |limit| wanted.min(limit * 3 / 4))
    }
}

fn reserve_window(
    budget: Option<Arc<tokio::sync::Semaphore>>,
    wanted: u64,
) -> (u64, Option<tokio::sync::OwnedSemaphorePermit>) {
    let Some(budget) = budget else {
        return (wanted, None);
    };
    let bytes = wanted
        .min(budget.available_permits() as u64)
        .min(u32::MAX as u64);
    match budget.try_acquire_many_owned(bytes as u32) {
        Ok(permit) => (bytes, Some(permit)),
        Err(_) => (0, None),
    }
}

struct ReaderLease<'a> {
    media: &'a Media,
    id: u64,
    bag: RequestBag,
    budget: Option<Arc<tokio::sync::Semaphore>>,
    cap: u64,
    window: u64,
    permit: Option<tokio::sync::OwnedSemaphorePermit>,
}

impl<'a> ReaderLease<'a> {
    fn new(media: &'a Media, start: u64, wanted: u64) -> Self {
        let limit = media
            .peer
            .as_ref()
            .and_then(|peer| peer.memory_cache_bytes());
        Self {
            media,
            id: media.register_reader(start),
            bag: RequestBag::new(),
            budget: media
                .peer
                .as_ref()
                .and_then(|peer| peer.memory_window_budget()),
            cap: limit.map_or(wanted, |limit| wanted.min(limit * 3 / 8)),
            window: 0,
            permit: None,
        }
    }

    fn reserve(&mut self) -> u64 {
        if self.window == 0 {
            (self.window, self.permit) = reserve_window(self.budget.clone(), self.cap);
        }
        self.window
    }

    async fn send(
        &self,
        tx: &mpsc::Sender<Result<Frame<Bytes>, std::io::Error>>,
        chunk: Bytes,
    ) -> bool {
        let send = tx.send(Ok(Frame::data(chunk)));
        if self.budget.is_some() {
            tokio::time::timeout(Duration::from_secs(15), send)
                .await
                .is_ok_and(|result| result.is_ok())
        } else {
            send.await.is_ok()
        }
    }
}

impl Drop for ReaderLease<'_> {
    fn drop(&mut self) {
        self.bag.destroy("reader closed");
        self.media.drop_reader(self.id);
    }
}

async fn while_connected<T>(
    tx: &mpsc::Sender<Result<Frame<Bytes>, std::io::Error>>,
    future: impl Future<Output = T>,
) -> Option<T> {
    tokio::select! {
        result = future => Some(result),
        _ = tx.closed() => None,
    }
}

/// A protocol blob pointer as the blob reader wants it.
pub fn blob_from_pointer(pointer: crate::protocol::BlobPointer) -> BlobId {
    BlobId {
        block_offset: pointer.block_offset,
        block_length: pointer.block_length,
        byte_offset: pointer.byte_offset,
        byte_length: pointer.byte_length,
    }
}

/// `hasBlockCached` without a [`Media`]: a load decides its first mode before the media exists.
pub fn blob_has_block(core: &Core, blob: BlobId, byte_start: u64) -> bool {
    if blob.block_length == 0 {
        return false;
    }
    let total = if blob.byte_length > 0 {
        blob.byte_length
    } else {
        DEFAULT_STREAM_BYTES_PER_SEC
    };
    let avg = (total / blob.block_length.max(1)).max(4096);
    let block = blob.block_offset + (byte_start / avg).min(blob.block_length - 1);
    core.length() > block && core.has(block)
}

/// `hasActiveReplicationPeer` without a [`Media`].
pub fn blob_has_peer(core: &Core, blob: BlobId) -> bool {
    core.remote_contiguous_length() >= blob.block_offset + blob.block_length
}

/// The optional form used by the receiver's status tick.
pub fn blob_has_peer_opt(core: Option<&Core>, blob: Option<BlobId>) -> bool {
    match (core, blob) {
        (Some(core), Some(blob)) => blob_has_peer(core, blob),
        _ => false,
    }
}

/// Disk-aware read-ahead in bytes, the arithmetic `steerReadAhead` uses for its window end.
pub fn read_ahead_bytes(rate: f64, free: u64, cached_ahead: u64) -> u64 {
    let wanted = (rate * MIN_READ_AHEAD_SECONDS as f64).max(
        (rate * READ_AHEAD_SECONDS as f64)
            .min((free as f64 - STREAM_RESERVE_BYTES as f64) * 0.75 + cached_ahead as f64),
    );
    // Blob GC waits for 25% garbage, so new cached bytes need up to 4/3 their size on disk.
    let ahead = wanted
        .min(((free as f64 - STREAM_FLOOR_BYTES as f64) * 0.75 + cached_ahead as f64).max(0.0));
    ahead.max(0.0) as u64
}

/// When to ask the node to trim watched blocks, and how much to keep, as `steerReadAhead` decides.
pub fn evict_plan(free: u64, rate: f64, playhead_byte: u64) -> Option<EvictOptions> {
    let tight = free < STREAM_RESERVE_BYTES;
    let needed =
        (free as f64) < (TRIM_FREE_BYTES as f64).max(4.0 * rate * READ_AHEAD_SECONDS as f64);
    if !needed {
        return None;
    }
    let keep_behind_bytes = if tight {
        TIGHT_KEEP_BYTES
    } else {
        TIGHT_KEEP_BYTES.max((rate * BEHIND_SECONDS as f64).ceil() as u64)
    };
    let max_bytes =
        (128 * 1024 * 1024).max((rate * (EVICT_EVERY_MS as f64 / 1000.0) * 2.0).ceil() as u64);
    Some(EvictOptions {
        playhead_byte,
        keep_behind_bytes,
        keep_ahead_bytes: None,
        max_bytes,
    })
}

/// Whether a reader that just started (or seeked) should wait for its cushion, and for how long.
pub fn hold_deadline(now: Instant) -> Instant {
    now + Duration::from_millis(super::HOLD_MAX_MS)
}

fn partial_headers(
    total: u64,
    spec: RangeSpec,
    mime: &str,
) -> (StatusCode, Vec<(header::HeaderName, String)>) {
    let mut headers: Vec<(header::HeaderName, String)> = vec![
        (header::ACCEPT_RANGES, "bytes".to_owned()),
        (header::CONTENT_TYPE, mime.to_owned()),
    ];
    match spec {
        RangeSpec::Partial { start, end } => {
            headers.push((header::CONTENT_LENGTH, (end - start + 1).to_string()));
            headers.push((
                header::CONTENT_RANGE,
                format!("bytes {start}-{end}/{total}"),
            ));
            (StatusCode::PARTIAL_CONTENT, headers)
        }
        _ => {
            headers.push((header::CONTENT_LENGTH, total.to_string()));
            (StatusCode::OK, headers)
        }
    }
}

fn write_headers<B>(res: &mut Response<B>, headers: &[(header::HeaderName, String)]) {
    for (name, value) in headers {
        if let Ok(value) = HeaderValue::from_str(value) {
            res.headers_mut().insert(name.clone(), value);
        }
    }
    cors_headers(res.headers_mut());
}

fn unsatisfiable(total: u64) -> Response<Body> {
    let mut res = super::http::empty(StatusCode::RANGE_NOT_SATISFIABLE);
    if let Ok(value) = HeaderValue::from_str(&format!("bytes */{total}")) {
        res.headers_mut().insert(header::CONTENT_RANGE, value);
    }
    res
}

#[allow(clippy::result_large_err)]
fn partial_range(range: Option<&str>, total: u64) -> Result<(bool, u64, u64), Response<Body>> {
    match parse_http_range(range, total) {
        RangeSpec::Invalid => Err(unsatisfiable(total)),
        RangeSpec::Full { start, end } => Ok((false, start, end)),
        RangeSpec::Partial { start, end } => Ok((true, start, end)),
    }
}

/// `serveWindowedStream`: the receiver's own player reads straight from the video core, one
/// bounded read-ahead window per response and no download to the end of the film.
pub fn windowed_response(
    media: Arc<Media>,
    range: Option<&str>,
    head: bool,
    free: u64,
) -> Response<Body> {
    let Some(blob) = media.blob else {
        return super::http::empty(StatusCode::NOT_FOUND);
    };
    let total = blob.byte_length;
    let (partial, start, end) = match partial_range(range, total) {
        Ok(values) => values,
        Err(res) => return res,
    };
    let spec = if partial {
        RangeSpec::Partial { start, end }
    } else {
        RangeSpec::Full { start, end }
    };
    if media.core.is_none() {
        return super::http::empty(StatusCode::NOT_FOUND);
    }
    let mime = if media.mime.is_empty() {
        "video/mp4"
    } else {
        media.mime.as_str()
    };
    let (status, headers) = partial_headers(total, spec, mime);
    let mut res = Response::new(super::http::empty_body());
    *res.status_mut() = status;
    write_headers(&mut res, &headers);
    if head {
        return res;
    }
    let (tx, rx) = mpsc::channel(8);
    tokio::spawn(async move {
        let _ = pipe_p2p(&media, &tx, start, end, free, true).await;
    });
    *res.body_mut() = channel_body(rx);
    res
}

/// The zero-size fallback: transparently proxy the sender's own Player link.
pub async fn proxy_response(
    media: &Media,
    client: &super::http::HttpClient,
    method: &Method,
    range: Option<&str>,
) -> Response<Body> {
    let Some(url) = media
        .local_p2p_url
        .clone()
        .or_else(|| media.http_stream_url.clone())
    else {
        return super::http::empty(StatusCode::NOT_FOUND);
    };
    let headers = match range {
        Some(value) => vec![(header::RANGE, value.to_owned())],
        None => Vec::new(),
    };
    let upstream = match client
        .request(method.clone(), &url, headers, None, Duration::from_secs(8))
        .await
    {
        Ok(response) => response,
        Err(_) => return super::http::empty(StatusCode::BAD_GATEWAY),
    };
    let status = upstream.status();
    let content_type = upstream
        .headers()
        .get(header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned);
    let (tx, rx) = mpsc::channel(8);
    tokio::spawn(async move {
        let mut body = upstream.into_body();
        while let Some(frame) = body.frame().await {
            let Ok(frame) = frame else { break };
            let Some(data) = frame.into_data().ok() else {
                continue;
            };
            if tx.send(Ok(Frame::data(data))).await.is_err() {
                break;
            }
        }
    });
    let mut res = Response::new(channel_body(rx));
    *res.status_mut() = status;
    cors_headers(res.headers_mut());
    if let Some(content_type) = content_type
        && let Ok(value) = HeaderValue::from_str(&content_type)
    {
        res.headers_mut().insert(header::CONTENT_TYPE, value);
    }
    res
}

/// `serveSpliceStream`: start over HTTP when cold, cut over to local P2P blocks as they arrive,
/// and fall back mid-response when the sender dies.
pub async fn splice_response(
    media: Arc<Media>,
    client: Arc<super::http::HttpClient>,
    method: Method,
    range: Option<&str>,
    head: bool,
    on_mode_upgrade: Arc<dyn Fn(&str) + Send + Sync>,
) -> Response<Body> {
    let total = media.total_size();
    if total == 0 {
        return proxy_response(&media, &client, &method, range).await;
    }
    let (partial, start, end) = match partial_range(range, total) {
        Ok(values) => values,
        Err(res) => return res,
    };
    let spec = if partial {
        RangeSpec::Partial { start, end }
    } else {
        RangeSpec::Full { start, end }
    };
    let mime = if media.mime.is_empty() {
        "video/mp4"
    } else {
        media.mime.as_str()
    };
    let (status, headers) = partial_headers(total, spec, mime);
    let mut res = Response::new(super::http::empty_body());
    *res.status_mut() = status;
    write_headers(&mut res, &headers);
    if head {
        return res;
    }
    let (tx, rx) = mpsc::channel(8);
    tokio::spawn(async move {
        splice_loop(media, client, tx, start, end, on_mode_upgrade).await;
    });
    *res.body_mut() = channel_body(rx);
    res
}

async fn splice_loop(
    media: Arc<Media>,
    client: Arc<super::http::HttpClient>,
    tx: mpsc::Sender<Result<Frame<Bytes>, std::io::Error>>,
    start: u64,
    end: u64,
    on_mode_upgrade: Arc<dyn Fn(&str) + Send + Sync>,
) {
    let mut cursor = start;
    let mut force_http = false;
    let mut last_error: Option<CastError> = None;
    while cursor <= end {
        let can_http = media.sender_http_alive() && media.http_stream_url.is_some();
        let span_end = end.min(cursor.saturating_add(4 * 1024 * 1024));
        let span_cached = media.has_block_cached(cursor) && media.has_block_cached(span_end);
        let use_p2p = media.local_p2p_url.is_some()
            && media.core.is_some()
            && !force_http
            && (!can_http || span_cached || media.has_active_peer());
        if use_p2p {
            on_mode_upgrade(&media.p2p_mode);
            match pipe_p2p(&media, &tx, cursor, end, u64::MAX, false).await {
                Ok(_written) => {
                    break;
                }
                Err((written, err)) => {
                    cursor += written;
                    last_error = Some(err);
                    if !can_http {
                        break;
                    }
                    force_http = true;
                }
            }
        }
        let slice_end = if media.core.is_some() && media.local_p2p_url.is_some() {
            end.min(cursor.saturating_add(COLD_HTTP_WINDOW - 1))
        } else {
            end
        };
        let Some(url) = media.http_stream_url.clone() else {
            break;
        };
        match pipe_http(&client, &tx, &url, cursor, slice_end).await {
            Ok(written) => cursor += written,
            Err((written, _err)) => {
                cursor += written;
                media.sender_http_alive.store(false, Ordering::Relaxed);
                if media.local_p2p_url.is_none() || media.core.is_none() {
                    break;
                }
                on_mode_upgrade(&media.p2p_mode);
            }
        }
    }
    let _ = last_error;
}

async fn pipe_http(
    client: &super::http::HttpClient,
    tx: &mpsc::Sender<Result<Frame<Bytes>, std::io::Error>>,
    url: &str,
    start: u64,
    end: u64,
) -> Result<u64, (u64, CastError)> {
    let expected = end - start + 1;
    let response = match client
        .range_slice(url, start, end, Some(Duration::from_secs(8)))
        .await
    {
        Ok(response) => response,
        Err(err) => return Err((0, err)),
    };
    let status = response.status().as_u16();
    if status != 200 && status != 206 {
        return Err((
            0,
            CastError::new(format!("Upstream answered {status}"), 502),
        ));
    }
    let mut body = response.into_body();
    let mut written = 0u64;
    loop {
        let frame = match tokio::time::timeout(Duration::from_secs(8), body.frame()).await {
            Err(_) => return Err((written, CastError::new("Upstream slice stalled", 504))),
            Ok(None) => break,
            Ok(Some(Err(err))) => return Err((written, CastError::new(err.to_string(), 502))),
            Ok(Some(Ok(frame))) => frame,
        };
        let Some(data) = super::http::read_frame(frame).await else {
            continue;
        };
        let remaining = expected - written;
        if remaining == 0 {
            break;
        }
        let slice = if data.len() as u64 > remaining {
            data.slice(0..remaining as usize)
        } else {
            data
        };
        written += slice.len() as u64;
        if tx.send(Ok(Frame::data(slice))).await.is_err() {
            return Ok(written);
        }
    }
    if written >= expected {
        Ok(written)
    } else {
        Err((
            written,
            CastError::new("Upstream closed before slice finished", 502),
        ))
    }
}

async fn pipe_p2p(
    media: &Media,
    tx: &mpsc::Sender<Result<Frame<Bytes>, std::io::Error>>,
    start: u64,
    end: u64,
    free: u64,
    active: bool,
) -> Result<u64, (u64, CastError)> {
    let (Some(core), Some(blob)) = (media.core.as_ref(), media.blob) else {
        return Err((0, CastError::new("No local P2P source", 502)));
    };
    let wanted = if active {
        media.window_bytes(free)
    } else {
        (DEFAULT_STREAM_BYTES_PER_SEC * 12).min(media.window_bytes(free))
    };
    let mut lease = ReaderLease::new(media, start, wanted);
    let mut stream: Option<ByteStream> = None;
    let mut written = 0u64;
    loop {
        let previous = lease.window;
        let window = lease.reserve();
        if stream.is_none() || (previous == 0 && window > 0) {
            let opts = StreamOptions {
                start: start + written,
                length: Some(end - start + 1 - written),
                window_bytes: window,
                active,
            };
            let Some(opened) =
                while_connected(tx, ByteStream::open(core, blob, opts, lease.bag.clone())).await
            else {
                return Ok(written);
            };
            stream = Some(opened.map_err(|err| (written, CastError::new(err.to_string(), 502)))?);
        }
        let next = async {
            let stream = stream.as_mut().expect("opened stream");
            let next = if !active && media.sender_http_alive() {
                tokio::time::timeout(Duration::from_millis(3_500), stream.next())
                    .await
                    .map_err(|_| CastError::new("Upstream slice stalled", 504))?
            } else {
                stream.next().await
            };
            next.map_err(|err| CastError::new(err.to_string(), 502))
        };
        let Some(next) = while_connected(tx, next).await else {
            return Ok(written);
        };
        let Some(chunk) = next.map_err(|err| (written, err))? else {
            return Ok(written);
        };
        let len = chunk.len() as u64;
        if !lease.send(tx, chunk).await {
            return Ok(written);
        }
        written += len;
        media.advance_reader(lease.id, start + written, len);
    }
}

/// The status loop's next mode: P2P once anything is cached or a peer holds the video.
pub fn upgraded_mode(cached_bytes: u64, has_peer: bool, p2p_mode: &str) -> Option<&str> {
    (cached_bytes > 0 || has_peer).then_some(p2p_mode)
}

/// `reportState`'s idle reset condition, shared with the receiver.
pub fn media_snapshot(media: &Media) -> Value {
    json!({
        "token": media.token,
        "preferMode": media.prefer_mode,
        "p2pMode": media.p2p_mode,
        "http": media.http_stream_url.is_some(),
        "p2p": media.core.is_some(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn lease_media() -> Media {
        Media::new(
            "lease".into(),
            json!({}),
            "video/mp4".into(),
            "p2p".into(),
            "p2p".into(),
            false,
            None,
            None,
            None,
            None,
            String::new(),
            None,
            false,
        )
    }

    #[tokio::test]
    async fn disconnect_cancels_slow_open_and_drops_reader_resources() {
        let media = lease_media();
        let budget = Arc::new(tokio::sync::Semaphore::new(100));
        let (tx, rx) = mpsc::channel(1);
        let mut lease = ReaderLease::new(&media, 500, 50);
        lease.budget = Some(budget.clone());
        assert_eq!(lease.reserve(), 50);
        drop(rx);
        assert!(
            while_connected(&tx, std::future::pending::<()>())
                .await
                .is_none()
        );
        drop(lease);
        assert!(media.readers.lock().unwrap().is_empty());
        assert_eq!(budget.available_permits(), 100);
    }

    #[test]
    fn demand_only_reader_retries_and_panics_release_its_lease() {
        let media = lease_media();
        let budget = Arc::new(tokio::sync::Semaphore::new(100));
        let (_, held) = reserve_window(Some(budget.clone()), 100);
        let mut lease = ReaderLease::new(&media, 500, 50);
        lease.budget = Some(budget.clone());
        assert_eq!(lease.reserve(), 0);
        drop(held);
        assert_eq!(lease.reserve(), 50);
        assert_eq!(
            budget.available_permits(),
            50,
            "leave room for an overlapping seek"
        );
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || {
            let _lease = lease;
            panic!("reader failure");
        }));
        assert!(result.is_err());
        assert!(media.readers.lock().unwrap().is_empty());
        assert_eq!(budget.available_permits(), 100);
    }

    #[tokio::test]
    async fn stalled_client_releases_its_reader_and_prefetch_window() {
        let media = lease_media();
        let budget = Arc::new(tokio::sync::Semaphore::new(100));
        let (tx, _rx) = mpsc::channel(1);
        let mut lease = ReaderLease::new(&media, 500, 50);
        lease.budget = Some(budget.clone());
        assert_eq!(lease.reserve(), 50);
        assert!(lease.send(&tx, Bytes::from_static(b"a")).await);
        assert!(!lease.send(&tx, Bytes::from_static(b"b")).await);
        drop(lease);
        assert!(media.readers.lock().unwrap().is_empty());
        assert_eq!(budget.available_permits(), 100);
    }

    #[test]
    fn concurrent_responses_share_one_prefetch_budget() {
        let budget = Arc::new(tokio::sync::Semaphore::new(100));
        let (a, first) = reserve_window(Some(budget.clone()), 75);
        let (b, second) = reserve_window(Some(budget.clone()), 75);
        let (c, _) = reserve_window(Some(budget.clone()), 75);
        assert_eq!((a, b, c), (75, 25, 0));
        drop(first);
        assert_eq!(reserve_window(Some(budget.clone()), 75).0, 75);
        drop(second);
        assert_eq!(budget.available_permits(), 100);
    }

    #[test]
    fn forward_seek_starts_at_its_range_and_trims_behind_the_slowest_reader() {
        let media = Media::new(
            "test".into(),
            json!({"size": 1_000_000_000u64}),
            "video/mp4".into(),
            "p2p".into(),
            "p2p".into(),
            false,
            None,
            None,
            None,
            None,
            String::new(),
            None,
            false,
        );
        let first = media.register_reader(500_000_000);
        assert_eq!(media.lead_byte(&Map::new(), None), 500_000_000);
        let second = media.register_reader(600_000_000);
        media.advance_reader(second, 610_000_000, LEAD_MIN_BYTES);
        assert_eq!(media.lead_byte(&Map::new(), None), 500_000_000);
        assert_eq!(media.furthest_reader_byte(), 610_000_000);
        media.drop_reader(first);
        assert_eq!(media.lead_byte(&Map::new(), None), 610_000_000);
    }

    #[test]
    fn read_ahead_shrinks_with_the_disk() {
        let rate = 4.0 * 1024.0 * 1024.0;
        let roomy = read_ahead_bytes(rate, 64 * 1024 * 1024 * 1024, 0);
        assert_eq!(roomy, (rate * READ_AHEAD_SECONDS as f64) as u64);
        // Reserve the future garbage overhead as well as the free-space cushion.
        let tight = read_ahead_bytes(rate, STREAM_RESERVE_BYTES + 300 * 1024 * 1024, 0);
        assert!(tight < roomy);
        assert_eq!(tight, 225 * 1024 * 1024);
        let at_floor = read_ahead_bytes(rate, STREAM_FLOOR_BYTES + 100 * 1024 * 1024, 0);
        assert_eq!(at_floor, 75 * 1024 * 1024);
        assert_eq!(
            read_ahead_bytes(rate, STREAM_FLOOR_BYTES, 64 * 1024 * 1024),
            64 * 1024 * 1024
        );
        assert_eq!(read_ahead_bytes(rate, 0, 0), 0);
    }

    #[test]
    fn eviction_only_when_space_is_short() {
        assert!(evict_plan(64 * 1024 * 1024 * 1024, 1.0e7, 0).is_none());
        let plan = evict_plan(512 * 1024 * 1024, 1.0e7, 4096).expect("tight plan");
        assert_eq!(plan.playhead_byte, 4096);
        assert_eq!(plan.keep_behind_bytes, TIGHT_KEEP_BYTES);
        assert!(plan.max_bytes >= 128 * 1024 * 1024);
        let high_rate = 16.0 * 1024.0 * 1024.0;
        let three_windows = (3.0 * high_rate * READ_AHEAD_SECONDS as f64) as u64;
        assert!(evict_plan(three_windows, high_rate, 0).is_some());
    }

    #[test]
    fn mode_upgrades_once_bytes_or_peers_exist() {
        assert_eq!(upgraded_mode(0, false, "p2p"), None);
        assert_eq!(upgraded_mode(1, false, "p2p"), Some("p2p"));
        assert_eq!(upgraded_mode(0, true, "private-p2p"), Some("private-p2p"));
    }
}
