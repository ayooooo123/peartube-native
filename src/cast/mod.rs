//! PearTube Cast & TV Receiver stack, ported from the app's `src/cast.js`.
//!
//! A receiver is a node with a player: senders find it by mDNS or by its key, connect to it as to
//! any peer, and pair with and control it on a Protomux channel of that one connection
//! ([`control`], the app's `src/cast-control.js`). The film reaches it over the same connection
//! and the swarm. Pairing secrets stay outside Corestore, so they never reach replicating
//! strangers.
//!
//! - [`controller`] — the sender: mDNS discovery, pairing (6-char code or a receiver link), the
//!   cast session over the peer connection, and `castPlay`/`castAttach`/`castControl`/`castStop`.
//! - [`receiver`] — the TV: pairing identity on disk, the control channel on every connection, the
//!   loopback HTTP server its own player reads, and mDNS advertising (`v=2`, LAN DHT port).
//!
//! Shared pieces: [`control`] (the cast channel and its JSON frames), [`state`] (the cast state
//! document and its transitions), [`pairing`] (the receiver identity file, pair codes and the
//! receiver's node key), [`records`] (saved receivers and the merged listing), [`http`] (Range
//! parsing, CORS, the loopback server), [`media`] (the windowed P2P stream and its read-ahead),
//! [`mdns`] (advertising and discovery), [`disk`] (free space, the low-disk reclaim and the
//! peer-storage probe), [`qr`] and [`traits`] (the seams the host fills).
//!
//! `docs/port-notes-cast.md` records the deliberate differences and what is deferred.

pub mod adapters;
pub mod control;
pub mod controller;
pub mod disk;
pub mod http;
pub mod mdns;
pub mod media;
pub mod pairing;
pub mod qr;
pub mod receiver;
pub mod records;
pub mod state;
pub mod traits;

use std::fmt;

pub use controller::{CastController, ControllerOptions};
pub use disk::{free_disk_bytes, reclaim_low_disk_receiver_storage, resolve_peer_storage};
pub use pairing::{
    PairLimiter, ReceiverIdentity, code_from_bytes, generate_pair_code, normalize_pair_code,
    random_hex, receiver_key_pair, safe_equal,
};
pub use receiver::{Receiver, ReceiverOptions, cast_lan_topic};
pub use records::{
    DiscoveredReceiver, ListedReceiver, SavedReceiver, SavedReceivers, list_receivers,
    receiver_target,
};
pub use state::{CastState, Plan};

/// Pair code alphabet, minus `0`, `1`, `I` and `O`.
pub const CODE_ALPHABET: &[u8] = b"23456789ABCDEFGHJKLMNPQRSTUVWXYZ";
/// mDNS service type advertised by a receiver.
pub const CAST_SERVICE_TYPE: &str = "peartube-cast";
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
/// A load's stream token is remembered for this many recent loads.
pub const REMEMBERED_STREAMS: usize = 4;

/// A failure carrying the HTTP status the app's handlers answer with; `0` for a cast-channel
/// error, which has no HTTP status.
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
