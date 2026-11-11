//! The receiver's media side: the blob/entry a load points at, cached-byte accounting, the
//! read rate the disk-aware window is sized from, and the windowed loopback response the
//! receiver's own player reads (`serveWindowedStream` / `steerReadAhead` in `src/cast.js`).
//!
//! The app steers one sliding `core.download` window per stream every second, sized by the
//! stream's bitrate and the free disk; the port does the same on the node's core
//! ([`Media::steer_window`]), and each response reads through it on demand with no prefetch of
//! its own. A reader past its first 2 MiB waits while the cushion ahead of it is thin
//! (`hold_if_thin`). [`evict_plan`] trims watched blocks behind the player.

use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use bytes::Bytes;
use hyper::header::{self, HeaderValue};
use hyper::{Response, StatusCode};
use hyperblobs::BlobId;
use hypercore::{Core, RequestBag};
use serde_json::{Map, Value};
use tokio::sync::mpsc;

use byte_stream::{ByteStream, StreamOptions};

use super::CastError;

use super::http::{Body, RangeSpec, channel_body, cors_headers, parse_http_range};
use super::traits::{CastPeer, EvictOptions};
use super::{
    BEHIND_SECONDS, DEFAULT_STREAM_BYTES_PER_SEC, EVICT_EVERY_MS, HOLD_AFTER_BYTES,
    HOLD_LOW_SECONDS, HOLD_MAX_MS, HOLD_SECONDS, MIN_READ_AHEAD_SECONDS, READ_AHEAD_SECONDS,
    STREAM_FLOOR_BYTES, STREAM_RESERVE_BYTES, TRIM_FREE_BYTES,
};

/// Bytes a reader must have been sent before it counts as the streaming player.
const LEAD_MIN_BYTES: u64 = 2 * 1024 * 1024;
/// Cached-byte recounts are reused for this long (`cachedCounts`).
const COUNT_TTL_MS: u64 = 3_000;
/// A reader opened in this window counts as still settling after a start or seek.
pub const SETTLING_MS: u64 = 20_000;
/// Behind-the-player bytes kept on a tight disk.
const TIGHT_KEEP_BYTES: u64 = 64 * 1024 * 1024;
/// How often a held reader checks whether its cushion arrived.
const HOLD_POLL: Duration = Duration::from_millis(250);
/// Two hold checks at least this far apart (`reader.at - reader.checkedAt >= 500`).
const HOLD_CHECK: Duration = Duration::from_millis(500);

/// One stream's sliding download window: the block range `core.download` is holding open, the
/// permits its blocks take from the TV build's memory budget, and where the range sits.
struct DownloadWindow {
    start: u64,
    end: u64,
    /// Registered by `Core::download` when it is created and cancelled when it drops; it is never
    /// polled, so holding it keeps the range open.
    #[allow(dead_code)]
    range: Pin<Box<dyn Future<Output = ()> + Send>>,
    #[allow(dead_code)]
    permit: Option<tokio::sync::OwnedSemaphorePermit>,
}

/// One load's media, as `activeMedia` / `streams` hold it.
pub struct Media {
    pub token: String,
    pub entry: Value,
    pub mime: String,
    pub core: Option<Core>,
    pub core_key_hex: String,
    pub blob: Option<BlobId>,
    /// The node, for its memory cache and window budget.
    pub peer: Option<Arc<dyn CastPeer>>,
    served_bytes: AtomicU64,
    read_rate: Mutex<f64>,
    rate_served: AtomicU64,
    rate_at: Mutex<Instant>,
    avg_rate: Mutex<f64>,
    readers: Mutex<Vec<Reader>>,
    last_reader_at: Mutex<Instant>,
    counts: Mutex<Option<(u64, Instant, u64)>>,
    /// The stream's one sliding download window, as `media.window` holds it.
    window: Mutex<Option<DownloadWindow>>,
}

#[derive(Clone, Copy)]
struct Reader {
    id: u64,
    pos: u64,
    sent: u64,
    at: Instant,
}

impl Media {
    pub fn new(
        token: String,
        entry: Value,
        mime: String,
        core: Option<Core>,
        core_key_hex: String,
        blob: Option<BlobId>,
        peer: Option<Arc<dyn CastPeer>>,
    ) -> Self {
        Self {
            token,
            entry,
            mime,
            core,
            core_key_hex,
            blob,
            peer,
            served_bytes: AtomicU64::new(0),
            read_rate: Mutex::new(0.0),
            rate_served: AtomicU64::new(0),
            rate_at: Mutex::new(Instant::now()),
            avg_rate: Mutex::new(0.0),
            readers: Mutex::new(Vec::new()),
            last_reader_at: Mutex::new(Instant::now()),
            counts: Mutex::new(None),
            window: Mutex::new(None),
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

    fn advance_reader(&self, id: u64, pos: u64, delta: u64) -> u64 {
        let mut sent = 0;
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
            sent = reader.sent;
        }
        self.served_bytes.fetch_add(delta, Ordering::Relaxed);
        sent
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

    /// `media.avgRate || DEFAULT_STREAM_BYTES_PER_SEC`, as `holdIfThin` sizes its cushion.
    fn avg_rate_or_default(&self) -> f64 {
        let known = *self.avg_rate.lock().expect("cast media rate");
        if known > 0.0 {
            known
        } else {
            DEFAULT_STREAM_BYTES_PER_SEC as f64
        }
    }

    pub fn reader_count(&self) -> usize {
        self.readers.lock().expect("cast readers").len()
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

    /// When a reader last started or advanced.
    pub fn last_reader_at(&self) -> Instant {
        *self.last_reader_at.lock().expect("cast readers")
    }

    /// `steerReadAhead`'s window half: one sliding `core.download` window per stream, ahead of
    /// the playhead, sized in seconds of the stream and shrunk by the free disk. A registered
    /// range moves only once the playhead or the window end left its step, so a slow reader keeps
    /// one range instead of re-requesting every tick. Returns whether a range was (re)registered.
    ///
    /// The TV build's memory budget caps it: the window's bytes hold permits from
    /// `memory_window_budget`, and a range is only as long as the permits it could take.
    pub fn steer_window(&self, free: u64, rate: f64, playhead_byte: u64) -> bool {
        let (Some(core), Some(blob)) = (self.core.as_ref(), self.blob) else {
            self.clear_window();
            return false;
        };
        if blob.block_length == 0 {
            self.clear_window();
            return false;
        }
        // `bs = Math.ceil(blob.byteLength / blob.blockLength)`: the blob's mean block size.
        let bs = blob.byte_length.div_ceil(blob.block_length).max(1);
        let last = blob.block_offset + blob.block_length;
        let first = (blob.block_offset + playhead_byte / bs).min(last - 1);
        let ahead_blocks = ((rate * READ_AHEAD_SECONDS as f64) / bs as f64).ceil() as u64;
        let full_end = (first + ahead_blocks).min(last);
        let cached_ahead = core.count_local(first, full_end) * bs;
        let wanted = read_ahead_bytes(rate, free, cached_ahead);
        // The app's RAM-built TV caps a window at its memory cache; the permits bound it further.
        let cache_cap = self
            .peer
            .as_ref()
            .and_then(|peer| peer.memory_cache_bytes())
            .map_or(u64::MAX, |limit| limit * 3 / 4);
        let (bytes, permit) = reserve_window(
            self.peer
                .as_ref()
                .and_then(|peer| peer.memory_window_budget()),
            wanted.min(cache_cap),
        );
        let end = (first + bytes.div_ceil(bs).max(1)).min(last);
        let step = ((end - first) / 8).max(4);
        {
            let window = self.window.lock().expect("cast window");
            if let Some(window) = window.as_ref()
                && first.abs_diff(window.start) < step
                && end.abs_diff(window.end) < step
            {
                return false;
            }
        }
        // Register the new range before the old one is dropped, so no block request falls between.
        let registered = core.download(first, Some(end), true);
        let range: Pin<Box<dyn Future<Output = ()> + Send>> = Box::pin(async move {
            let _ = registered.await;
        });
        *self.window.lock().expect("cast window") = Some(DownloadWindow {
            start: first,
            end,
            range,
            permit,
        });
        true
    }

    /// The block range this stream's download window covers, while it has one.
    pub fn window_range(&self) -> Option<(u64, u64)> {
        self.window
            .lock()
            .expect("cast window")
            .as_ref()
            .map(|window| (window.start, window.end))
    }

    /// `media.window = null`: nothing on this stream is playing, so the range is cancelled.
    pub fn clear_window(&self) {
        let _ = self.window.lock().expect("cast window").take();
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

/// A response's reader: its place in [`Media::readers`], and the bag that cancels every block it
/// asked for when the player disconnects. The stream's prefetch window is not the reader's; one
/// window per stream belongs to [`Media`].
struct ReaderLease<'a> {
    media: &'a Media,
    id: u64,
    bag: RequestBag,
}

impl<'a> ReaderLease<'a> {
    fn new(media: &'a Media, start: u64) -> Self {
        Self {
            media,
            id: media.register_reader(start),
            bag: RequestBag::new(),
        }
    }

    async fn send(
        &self,
        tx: &mpsc::Sender<Result<hyper::body::Frame<Bytes>, std::io::Error>>,
        chunk: Bytes,
    ) -> bool {
        let Ok(slot) = tx.reserve().await else {
            return false;
        };
        slot.send(Ok(hyper::body::Frame::data(chunk)));
        true
    }
}

impl Drop for ReaderLease<'_> {
    fn drop(&mut self) {
        self.bag.destroy("reader closed");
        self.media.drop_reader(self.id);
    }
}

async fn while_connected<T>(
    tx: &mpsc::Sender<Result<hyper::body::Frame<Bytes>, std::io::Error>>,
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

/// A tight disk trims every three seconds instead of every fifteen.
pub fn trim_due(free: u64, last_evict_at: Instant) -> bool {
    let every = if free < STREAM_RESERVE_BYTES {
        Duration::from_millis(3_000)
    } else {
        Duration::from_millis(EVICT_EVERY_MS)
    };
    last_evict_at.elapsed() >= every
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

/// `serveWindowedStream`: the receiver's own player reads straight from the video core through
/// the stream's one sliding window, with no prefetch of its own and no download to the end of the
/// film. Each open response is a reader whose position the window follows.
pub fn windowed_response(media: Arc<Media>, range: Option<&str>, head: bool) -> Response<Body> {
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
        let _ = pipe_p2p(&media, &tx, start, end).await;
    });
    *res.body_mut() = channel_body(rx);
    res
}

/// `holdIfThin`: a reader past its first 2 MiB is the player streaming, not probing a header or
/// an index. It gets no more bytes while fewer than [`HOLD_LOW_SECONDS`] of the film lie
/// downloaded ahead of it ([`HOLD_SECONDS`] before its first cushion), until [`HOLD_SECONDS`] do
/// or [`HOLD_MAX_MS`] passes. The player then waits once, through one "Buffering…", instead of
/// resuming with a second in hand and stalling again moments later. False when the player left.
async fn hold_if_thin(
    tx: &mpsc::Sender<Result<hyper::body::Frame<Bytes>, std::io::Error>>,
    media: &Media,
    pos: u64,
    cushioned: bool,
) -> bool {
    hold_for(
        tx,
        media,
        pos,
        cushioned,
        Duration::from_millis(HOLD_MAX_MS),
    )
    .await
}

/// [`hold_if_thin`] with the wait the caller allows; a test does not need 20 s.
async fn hold_for(
    tx: &mpsc::Sender<Result<hyper::body::Frame<Bytes>, std::io::Error>>,
    media: &Media,
    pos: u64,
    cushioned: bool,
    max_wait: Duration,
) -> bool {
    let (Some(core), Some(blob)) = (media.core.as_ref(), media.blob) else {
        return true;
    };
    if blob.block_length == 0 {
        return true;
    }
    let bs = blob.byte_length.div_ceil(blob.block_length).max(1);
    let rate = media.avg_rate_or_default();
    let last = blob.block_offset + blob.block_length;
    let at = (blob.block_offset + pos / bs).min(last);
    let blocks_for = |seconds: u64| -> u64 {
        let wanted = ((rate * seconds as f64) / bs as f64).ceil() as u64;
        (last - at).min(wanted)
    };
    let buffered = |seconds: u64| {
        let blocks = blocks_for(seconds);
        blocks == 0 || core.count_local(at, at + blocks) == blocks
    };
    if buffered(if cushioned {
        HOLD_LOW_SECONDS
    } else {
        HOLD_SECONDS
    }) {
        return true;
    }
    let deadline = Instant::now() + max_wait;
    while Instant::now() < deadline {
        if buffered(HOLD_SECONDS) {
            break;
        }
        tokio::select! {
            _ = tx.closed() => return false,
            _ = tokio::time::sleep(HOLD_POLL) => {}
        }
    }
    true
}

async fn pipe_p2p(
    media: &Media,
    tx: &mpsc::Sender<Result<hyper::body::Frame<Bytes>, std::io::Error>>,
    start: u64,
    end: u64,
) -> Result<u64, (u64, CastError)> {
    let (Some(core), Some(blob)) = (media.core.as_ref(), media.blob) else {
        return Err((0, CastError::new("No local P2P source", 502)));
    };
    let lease = ReaderLease::new(media, start);
    // Prefetch belongs to the stream's one window, not to the response.
    let opts = StreamOptions {
        start,
        length: Some(end - start + 1),
        window_bytes: 0,
        active: true,
    };
    let Some(opened) =
        while_connected(tx, ByteStream::open(core, blob, opts, lease.bag.clone())).await
    else {
        return Ok(0);
    };
    let mut stream = opened.map_err(|err| (0, CastError::new(err.to_string(), 502)))?;
    let mut written = 0u64;
    let mut cushioned = false;
    let mut checked_at = Instant::now() - HOLD_CHECK;
    loop {
        let next = async {
            stream
                .next()
                .await
                .map_err(|err| CastError::new(err.to_string(), 502))
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
        let sent = media.advance_reader(lease.id, start + written, len);
        let now = Instant::now();
        if sent >= HOLD_AFTER_BYTES && now.duration_since(checked_at) >= HOLD_CHECK {
            checked_at = now;
            if !hold_if_thin(tx, media, start + written, cushioned).await {
                return Ok(written);
            }
            cushioned = true;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use http_body_util::BodyExt;
    use hypercore::CoreOptions;
    use hypercore::crypto::KeyPair;
    use hypercore::storage::Store;
    use serde_json::json;
    use tokio::time::timeout;

    /// Block size of the fixture film, and the blob's mean block size.
    const BS: usize = 64 * 1024;

    fn blank_media() -> Media {
        Media::new(
            "lease".into(),
            json!({}),
            "video/mp4".into(),
            None,
            String::new(),
            None,
            None,
        )
    }

    #[test]
    fn a_reader_counts_only_past_the_probe_size() {
        let media = blank_media();
        let id = media.register_reader(0);
        media.advance_reader(id, 112, 112);
        // A reader still inside its first 2 MiB is a probe, not the lead.
        assert_eq!(media.lead_byte(&Map::new(), None), 112);
        assert_eq!(media.reader_count(), 1);
        media.drop_reader(id);
        assert_eq!(media.reader_count(), 0);
        assert_eq!(media.lead_byte(&Map::new(), None), 0);
    }

    #[tokio::test]
    async fn disconnect_cancels_a_slow_open_and_drops_the_reader() {
        let media = blank_media();
        let (tx, rx) = mpsc::channel(1);
        drop(rx);
        assert!(
            while_connected(&tx, std::future::pending::<()>())
                .await
                .is_none()
        );
        let lease = ReaderLease::new(&media, 500);
        assert_eq!(media.reader_count(), 1);
        drop(lease);
        assert!(media.readers.lock().unwrap().is_empty());
    }

    /// Two stores and one in-memory replication link: a writer with `blocks` blocks, and a
    /// reader that knows how long the film is but has fetched nothing yet.
    struct Peers {
        _writer_dir: tempfile::TempDir,
        _reader_dir: tempfile::TempDir,
        _writer_store: Store,
        _reader_store: Store,
        _writer: Core,
        reader: Core,
        _session: hypercore::Session,
        data: Vec<u8>,
    }

    async fn peers(blocks: usize) -> Peers {
        let writer_dir = tempfile::tempdir().unwrap();
        let reader_dir = tempfile::tempdir().unwrap();
        let writer_store = Store::open(writer_dir.path()).unwrap();
        let reader_store = Store::open(reader_dir.path()).unwrap();
        let writer = Core::open(
            &writer_store,
            CoreOptions {
                key_pair: Some(KeyPair::from_seed(&[4u8; 32])),
                ..CoreOptions::default()
            },
        )
        .unwrap();
        let data: Vec<u8> = (0..blocks * BS).map(|index| (index % 251) as u8).collect();
        if blocks > 0 {
            let chunks: Vec<&[u8]> = data.chunks(BS).collect();
            writer.append(&chunks).await.unwrap();
        }
        let reader = Core::open(
            &reader_store,
            CoreOptions {
                key: Some(*writer.key()),
                ..CoreOptions::default()
            },
        )
        .unwrap();
        let session = reader.session(true);
        let (mw, mr) = link();
        writer.replicate(&mw);
        reader.replicate(&mr);
        if blocks > 0 {
            timeout(Duration::from_secs(10), reader.update(None))
                .await
                .expect("the reader learns the length")
                .expect("update");
        }
        Peers {
            _writer_dir: writer_dir,
            _reader_dir: reader_dir,
            _writer_store: writer_store,
            _reader_store: reader_store,
            _writer: writer,
            reader,
            _session: session,
            data,
        }
    }

    /// Two muxes over an in-memory link that look like one secret-stream session.
    fn link() -> (protomux::Mux, protomux::Mux) {
        let ((ar, aw), (br, bw)) = protomux::memory::duplex(64);
        let handshake = Some([7u8; 64]);
        (
            protomux::Mux::new(
                ar,
                aw,
                protomux::StreamInfo {
                    is_initiator: true,
                    handshake_hash: handshake,
                    ..protomux::StreamInfo::default()
                },
            ),
            protomux::Mux::new(
                br,
                bw,
                protomux::StreamInfo {
                    handshake_hash: handshake,
                    ..protomux::StreamInfo::default()
                },
            ),
        )
    }

    /// A load of `blocks` blocks of the fixture film, read from `reader`.
    fn media(reader: Core, blocks: u64) -> Media {
        Media::new(
            "stream".into(),
            json!({ "size": blocks * BS as u64 }),
            "video/mp4".into(),
            Some(reader),
            String::new(),
            Some(BlobId {
                block_offset: 0,
                block_length: blocks,
                byte_offset: 0,
                byte_length: blocks * BS as u64,
            }),
            None,
        )
    }

    async fn fetch(reader: &Core, index: u64) {
        timeout(
            Duration::from_secs(10),
            reader.get(index, Default::default()),
        )
        .await
        .unwrap_or_else(|_| panic!("block {index} arrives"))
        .expect("get")
        .expect("a block");
    }

    #[tokio::test]
    async fn one_window_follows_the_reader_and_shrinks_with_the_disk() {
        let none = blank_media();
        assert!(!none.steer_window(64 << 30, 8.0e6, 0));
        assert_eq!(none.window_range(), None);

        let stream = peers(0).await;
        let media = media(stream.reader.clone(), 400);
        // A roomy disk asks for 120 s of film: the whole blob at this bitrate.
        assert!(media.steer_window(64 << 30, 8.0e6, 0));
        assert_eq!(media.window_range(), Some((0, 400)));
        // A small move stays inside the window's step: the one range is not re-registered.
        assert!(!media.steer_window(64 << 30, 8.0e6, 5 * BS as u64));
        assert_eq!(media.window_range(), Some((0, 400)));
        // The player jumped far enough: the window follows it.
        assert!(media.steer_window(64 << 30, 8.0e6, 100 * BS as u64));
        assert_eq!(media.window_range(), Some((100, 400)));
        // A disk at the floor leaves only the block being read.
        assert!(media.steer_window(STREAM_FLOOR_BYTES, 8.0e6, 100 * BS as u64));
        assert_eq!(media.window_range(), Some((100, 101)));
        // Nothing is playing on the stream: the range is cancelled.
        media.clear_window();
        assert_eq!(media.window_range(), None);
    }

    #[tokio::test]
    async fn a_thin_reader_past_the_probe_size_holds_until_the_cushion_arrives() {
        let stream = peers(200).await;
        let media = media(stream.reader.clone(), 200);
        // 512 KiB/s: 10 s of film is 80 blocks, 2 s is 16.
        assert_eq!(media.rate(Some(25_000)), 512.0 * 1024.0);
        let mut body = windowed_response(Arc::new(media), None, false).into_body();
        let mut received: Vec<u8> = Vec::new();
        // The player streams until its first 2 MiB; the cushion ahead (blocks 32..112) is not
        // downloaded, so it waits instead of racing on with one second in hand.
        while let Ok(Some(Ok(frame))) = timeout(Duration::from_secs(2), body.frame()).await {
            if let Ok(data) = frame.into_data() {
                received.extend_from_slice(&data);
            }
        }
        assert_eq!(
            received.len(),
            HOLD_AFTER_BYTES as usize,
            "a thin reader stops at its first 2 MiB"
        );
        // The stream's window fills the cushion; the reader resumes at once. The rest of the film is
        // fetched too, so the 2 s rule after the cushion never holds the tail on a slow runner: the
        // test is about the release, not about how fast an unsteered tail arrives.
        for index in 32..200 {
            fetch(&stream.reader, index).await;
        }
        let rest = timeout(Duration::from_secs(30), async {
            while let Some(frame) = body.frame().await {
                if let Ok(frame) = frame
                    && let Ok(data) = frame.into_data()
                {
                    received.extend_from_slice(&data);
                }
            }
        })
        .await;
        assert!(rest.is_ok(), "the held reader resumes");
        assert_eq!(received, stream.data, "every byte of the film arrives");
    }

    #[tokio::test]
    async fn a_cushioned_reader_needs_only_two_seconds_ahead() {
        let stream = peers(200).await;
        let media = media(stream.reader.clone(), 200);
        media.rate(Some(25_000));
        for index in 0..48 {
            fetch(&stream.reader, index).await;
        }
        let (tx, _rx) = mpsc::channel(8);
        // Blocks 32..48 are 2 s of film: enough once the reader has taken a cushion.
        let started = Instant::now();
        assert!(
            hold_for(
                &tx,
                &media,
                32 * BS as u64,
                true,
                Duration::from_millis(400)
            )
            .await
        );
        assert!(started.elapsed() < Duration::from_millis(200));
        // Before its first cushion the same reader needs 10 s, so it waits out the caller's limit.
        let started = Instant::now();
        assert!(
            hold_for(
                &tx,
                &media,
                32 * BS as u64,
                false,
                Duration::from_millis(400)
            )
            .await
        );
        assert!(started.elapsed() >= Duration::from_millis(400));
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
        let long_ago = Instant::now() - Duration::from_secs(EVICT_EVERY_MS / 1000 + 1);
        assert!(
            trim_due(STREAM_FLOOR_BYTES, long_ago),
            "a tight disk waits 3 s"
        );
        assert!(trim_due(64 * 1024 * 1024 * 1024, long_ago));
        assert!(!trim_due(64 * 1024 * 1024 * 1024, Instant::now()));
    }
}
