//! The Soulseek client the provider drives.
//!
//! The app loads `peartube-soulseek-bare`, an addon around the Rust crate `peartube-soulseek`. A
//! build with the `soulseek` feature links that crate directly: what [`network()`] gives stands
//! where the addon stands, with the calls and the error texts the addon gives the app. A test
//! puts a scripted [`Backend`] there instead.

#[cfg(feature = "soulseek")]
mod network;

use std::path::Path;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use super::rank::PeerFile;

/// What a client call answers; an error is the client's own text.
pub type ClientFuture<'a, T> = Pin<Box<dyn Future<Output = Result<T, String>> + Send + 'a>>;

/// Where a client is, as the addon's `state()` reports it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct ClientState {
    pub connected: bool,
    pub logged_in: bool,
    /// The TCP port peers connect back to; 0 when it could not be opened.
    pub listen_port: u16,
}

/// Where one download is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Phase {
    /// Waiting on the peer.
    Queued,
    /// The peer is ready and opens the file connection.
    Connecting,
    Transferring,
    Completed,
    Failed,
    Cancelled,
}

impl Phase {
    /// The addon's name for it, which a job's `phase` shows.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Queued => "queued",
            Self::Connecting => "connecting",
            Self::Transferring => "transferring",
            Self::Completed => "completed",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
        }
    }
}

/// One report of a download.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Progress {
    pub phase: Phase,
    /// Bytes received so far.
    pub bytes: u64,
    /// The file's size as the peer states it when it starts to send; 0 before.
    pub total: u64,
    /// The place in the peer's queue, once the peer said one.
    pub queue: Option<u32>,
}

/// Takes each report of a download, from whichever thread the client works on.
pub type ProgressFn = Arc<dyn Fn(Progress) + Send + Sync>;

/// One signed-in session with the Soulseek network (the addon's `Soulseek` object).
pub trait Client: Send + Sync {
    fn state(&self) -> ClientState;
    /// Signs in; `Ok(false)` when the server did not log the user in.
    fn login<'a>(
        &'a self,
        host: &'a str,
        port: u16,
        user: &'a str,
        pass: &'a str,
    ) -> ClientFuture<'a, bool>;
    /// Every hit that arrives within `wait_ms`.
    fn search<'a>(&'a self, query: &'a str, wait_ms: u64) -> ClientFuture<'a, Vec<PeerFile>>;
    /// Everything `user` shares.
    fn browse<'a>(&'a self, user: &'a str, timeout_ms: u64) -> ClientFuture<'a, Vec<PeerFile>>;
    /// Downloads `filename` from `user` into `dest` and answers the size written. It waits on the
    /// peer for as long as it takes; `cancel` ends it.
    fn download<'a>(
        &'a self,
        user: &'a str,
        filename: &'a str,
        dest: &'a Path,
        cancel: Arc<AtomicBool>,
        progress: ProgressFn,
    ) -> ClientFuture<'a, u64>;
    fn close(&self);
}

/// What a new client is made with.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ClientSetup {
    /// 0 lets the system pick a port each start, which cannot be published.
    pub listen_port: u16,
    /// Advertise and serve the album folders to peers.
    pub share: bool,
    /// The most files the share index holds.
    pub share_max_files: u32,
}

/// Makes clients (the addon module itself).
pub trait Backend: Send + Sync {
    fn client(&self, setup: ClientSetup) -> Arc<dyn Client>;
}

/// The Soulseek network, in a build that links its client (the `soulseek` feature). A build
/// without it has none, and the provider reports `unavailable`, as the app does on a platform with
/// no prebuilt addon.
pub fn network() -> Option<Arc<dyn Backend>> {
    #[cfg(feature = "soulseek")]
    return Some(Arc::new(network::Network));
    #[cfg(not(feature = "soulseek"))]
    None
}
