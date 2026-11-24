//! The client that reaches the Soulseek network: `peartube-soulseek`, the crate the app's
//! `peartube-soulseek-bare` addon wraps, linked directly.
//!
//! Only a build with the `soulseek` feature has it. The crate exports its own C ABI for that addon,
//! and a library that links the crate exports it too, so the apps' engine is built without.

use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::time::Duration;

use peartube_soulseek::{
    ClientError, ClientOptions, SharedFileRecord, SoulseekClient, TransferPhase, TransferProgress,
};

use super::{Backend, Client, ClientFuture, ClientSetup, ClientState, Phase, Progress, ProgressFn};
use crate::providers::soulseek::rank::PeerFile;

/// The Soulseek network.
pub(super) struct Network;

impl Backend for Network {
    fn client(&self, setup: ClientSetup) -> Arc<dyn Client> {
        // The folder to share stays the host's choice, as the import folder is: the defaults read
        // PEARTUBE_SLSK_SHARE_DIR, else PEARTUBE_WATCH_DIR, else `<PEARTUBE_STORAGE>/staging`.
        let options = ClientOptions {
            listen_port: setup.listen_port,
            share_enabled: setup.share,
            share_max_files: setup.share_max_files as usize,
            ..ClientOptions::default()
        };
        Arc::new(NetworkClient(Arc::new(SoulseekClient::with_options(
            options,
        ))))
    }
}

struct NetworkClient(Arc<SoulseekClient>);

/// Runs one of the client's calls, which block until the network answers, off the async threads.
async fn blocking<T: Send + 'static>(
    call: impl FnOnce() -> Result<T, ClientError> + Send + 'static,
) -> Result<T, String> {
    match tokio::task::spawn_blocking(call).await {
        Ok(answer) => answer.map_err(|error| error.to_string()),
        Err(error) => Err(error.to_string()),
    }
}

fn peer_file(record: SharedFileRecord) -> PeerFile {
    PeerFile {
        username: record.username,
        slots: record.slots,
        queue: record.queue,
        filename: record.filename,
        size: record.size,
        bitrate: record.bitrate,
        sample_rate: record.sample_rate,
        bit_depth: record.bit_depth,
        length: record.length_secs,
    }
}

impl Client for NetworkClient {
    fn state(&self) -> ClientState {
        let state = self.0.state();
        ClientState {
            connected: state.connected,
            logged_in: state.logged_in,
            listen_port: state.listen_port,
        }
    }

    fn login<'a>(
        &'a self,
        host: &'a str,
        port: u16,
        user: &'a str,
        pass: &'a str,
    ) -> ClientFuture<'a, bool> {
        let client = self.0.clone();
        let (host, user, pass) = (host.to_owned(), user.to_owned(), pass.to_owned());
        Box::pin(blocking(move || {
            client
                .login(&host, port, &user, &pass)
                .map(|state| state.logged_in)
        }))
    }

    fn search<'a>(&'a self, query: &'a str, wait_ms: u64) -> ClientFuture<'a, Vec<PeerFile>> {
        let client = self.0.clone();
        let query = query.to_owned();
        Box::pin(blocking(move || {
            let hits = client.search(&query, wait_ms)?;
            Ok(hits.into_iter().map(peer_file).collect())
        }))
    }

    fn browse<'a>(&'a self, user: &'a str, timeout_ms: u64) -> ClientFuture<'a, Vec<PeerFile>> {
        let client = self.0.clone();
        let user = user.to_owned();
        Box::pin(blocking(move || {
            let shared = client.browse_with_timeout(&user, Duration::from_millis(timeout_ms))?;
            Ok(shared.into_iter().map(peer_file).collect())
        }))
    }

    fn download<'a>(
        &'a self,
        user: &'a str,
        filename: &'a str,
        dest: &'a Path,
        cancel: Arc<AtomicBool>,
        progress: ProgressFn,
    ) -> ClientFuture<'a, u64> {
        let client = self.0.clone();
        let (user, filename, dest) = (user.to_owned(), filename.to_owned(), dest.to_path_buf());
        let report = Arc::new(move |progress_now: TransferProgress| {
            progress(Progress {
                phase: match progress_now.phase {
                    TransferPhase::Queued { .. } => Phase::Queued,
                    TransferPhase::Connecting => Phase::Connecting,
                    TransferPhase::Transferring => Phase::Transferring,
                    TransferPhase::Completed => Phase::Completed,
                    TransferPhase::Failed(_) => Phase::Failed,
                    TransferPhase::Cancelled => Phase::Cancelled,
                },
                bytes: progress_now.bytes_downloaded,
                total: progress_now.total_bytes,
                queue: progress_now.queue_place,
            });
        });
        Box::pin(blocking(move || {
            client
                .download_resumable(&user, &filename, dest, None, cancel, Some(report))
                .map(|done| done.size)
        }))
    }

    fn close(&self) {
        self.0.close();
    }
}
