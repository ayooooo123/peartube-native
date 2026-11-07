//! PearTube Cast & TV Receiver stack, ported from the app's `src/cast.js`.
//!
//! The split mirrors the two roles the worker drives:
//!
//! - [`controller`] — the sender/hub: mDNS discovery, pairing (LAN HTTP or Noise P2P), the
//!   token-gated LAN blob server, zero-install Smart TV web receivers, and the cast session.
//! - [`receiver`] — the TV: pairing identity on disk, the LAN HTTP server (with `/tv`, the JSON
//!   command API and the token-gated Range routes), the Noise RPC server, and mDNS advertising.
//!
//! Shared pieces: [`state`] (the cast state document and its transitions), [`pairing`] (the
//! receiver identity file and pair codes), [`records`] (saved receivers and the merged listing),
//! [`http`] (Range parsing, CORS, the server and the JSON client), [`media`] (blob windowing and
//! the byte-exact HTTP→P2P splice), [`rpc`] (the newline JSON protocol on the Noise pipe),
//! [`disk`] (free space, the low-disk reclaim and the peer-storage probe), [`qr`] and [`ui`].
//!
//! `docs/port-notes-cast.md` records the deliberate differences and what is deferred.

pub mod controller;
pub mod disk;
pub mod http;
pub mod mdns;
pub mod media;
pub mod pairing;
pub mod qr;
pub mod receiver;
pub mod records;
pub mod rpc;
pub mod state;
pub mod traits;
pub mod ui;

use std::fmt;

pub use controller::{CastController, CastSession, ControllerOptions};
pub use disk::{free_disk_bytes, reclaim_low_disk_receiver_storage, resolve_peer_storage};
pub use pairing::{
    PairLimiter, ReceiverIdentity, code_from_bytes, generate_pair_code, normalize_pair_code,
    random_hex, safe_equal,
};
pub use receiver::{Receiver, ReceiverOptions};
pub use records::{
    DiscoveredReceiver, ListedReceiver, SavedReceiver, SavedReceivers, WebListing, list_receivers,
    receiver_address,
};
pub use state::{CastState, Plan};
pub use ui::receiver_page;

/// Pair code alphabet, minus `0`, `1`, `I` and `O`.
pub const CODE_ALPHABET: &[u8] = b"23456789ABCDEFGHJKLMNPQRSTUVWXYZ";
/// mDNS service type advertised by a receiver.
pub const CAST_SERVICE_TYPE: &str = "peartube-cast";
/// Largest JSON body the command API reads.
pub const MAX_JSON_BODY: usize = 64 * 1024;
/// Below this free space a receiver purges its stale Corestores on boot.
pub const LOW_DISK_BOOT_BYTES: u64 = 2 * 1024 * 1024 * 1024;
/// Below this free space a load clears blocks for other videos.
pub const LOW_DISK_STREAM_BYTES: u64 = 1536 * 1024 * 1024;
/// The receiver's read-ahead, in seconds of the stream it plays.
pub const READ_AHEAD_SECONDS: u64 = 120;
/// Floor of the read-ahead window.
pub const MIN_READ_AHEAD_SECONDS: u64 = 20;
/// Watched blocks kept behind the player so a skip back stays local.
pub const BEHIND_SECONDS: u64 = 30;
/// Watched blocks are trimmed once free space falls below this.
pub const TRIM_FREE_BYTES: u64 = 4 * 1024 * 1024 * 1024;
/// Free space the read-ahead never plans to use.
pub const STREAM_RESERVE_BYTES: u64 = 1024 * 1024 * 1024;
/// Below this the window shrinks to the block being read.
pub const STREAM_FLOOR_BYTES: u64 = 256 * 1024 * 1024;
/// A stream's assumed bitrate (~64 Mbps) until the player reports a duration.
pub const DEFAULT_STREAM_BYTES_PER_SEC: u64 = 8 * 1024 * 1024;
/// Cadence of the eviction pass.
pub const EVICT_EVERY_MS: u64 = 15_000;
/// Bytes after which a reader is treated as the streaming player, not a probe.
pub const HOLD_AFTER_BYTES: u64 = 2 * 1024 * 1024;
/// Buffered seconds the player waits for when starting (or just after a seek).
pub const HOLD_SECONDS: u64 = 10;
/// Buffered seconds a cushioned reader waits for.
pub const HOLD_LOW_SECONDS: u64 = 2;
/// Longest a thin reader waits for its cushion.
pub const HOLD_MAX_MS: u64 = 20_000;
/// HTTP→P2P splice: bytes served over HTTP from a cold start before re-checking P2P.
pub const COLD_HTTP_WINDOW: u64 = 1024 * 1024;
/// A load's stream token is remembered for this many recent loads.
pub const REMEMBERED_STREAMS: usize = 4;

/// A failure carrying the HTTP status the app's handlers answer with.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CastError {
    pub message: String,
    pub status: u16,
}

impl CastError {
    pub fn new(message: impl Into<String>, status: u16) -> Self {
        Self {
            message: message.into(),
            status,
        }
    }

    pub fn bad_request(message: impl Into<String>) -> Self {
        Self::new(message, 400)
    }

    pub fn unauthorized(message: impl Into<String>) -> Self {
        Self::new(message, 401)
    }

    pub fn not_found(message: impl Into<String>) -> Self {
        Self::new(message, 404)
    }

    pub fn forbidden(message: impl Into<String>) -> Self {
        Self::new(message, 403)
    }

    pub fn too_many(message: impl Into<String>) -> Self {
        Self::new(message, 429)
    }
}

impl fmt::Display for CastError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for CastError {}
