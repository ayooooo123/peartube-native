//! What the mirror host does for a provider (`placeAndScan`), for the oracles that run one provider on its own;
//! `tests/js/provider-host.mjs` is the same host for the app. The other import is scripted: `hold` lets it hold the
//! folder for a time from now, and `limit` is how long the host waits before it gives up. Nothing is placed while it
//! waits. Then the provider's `place` runs, and the scan answers one item for each media file it has not reported
//! before; a file named by `refuse` is answered as a failed import. The host's own wait is compared in the library
//! oracle; this one only gives a provider every answer a host can give.

use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use peartube_native::providers::staging::{Cancelled, Place, PlaceAndScan, PlaceError};
use serde_json::{Value, json};

use super::files::{sha256_hex, sorted_names};

pub struct FakeHost {
    /// Takes `{ "scan": <folder> }` for each scan.
    log: Arc<dyn Fn(Value) + Send + Sync>,
    /// The folder providers place into.
    root: PathBuf,
    /// The step of the scripted clock.
    tick: Duration,
    now_ms: Arc<dyn Fn() -> u64 + Send + Sync>,
    seen: Mutex<HashSet<String>>,
    /// File names the scan answers as failed imports, each with its error.
    refused: Mutex<Vec<(String, String)>>,
    held_until: AtomicU64,
    /// `u64::MAX` when the host never gives up.
    limit: AtomicU64,
}

impl FakeHost {
    pub fn new(
        log: Arc<dyn Fn(Value) + Send + Sync>,
        root: PathBuf,
        tick: Duration,
        now_ms: Arc<dyn Fn() -> u64 + Send + Sync>,
    ) -> Arc<Self> {
        Arc::new(Self {
            log,
            root,
            tick,
            now_ms,
            seen: Mutex::new(HashSet::new()),
            refused: Mutex::new(Vec::new()),
            held_until: AtomicU64::new(0),
            limit: AtomicU64::new(u64::MAX),
        })
    }

    pub fn hold(&self, ms: u64) {
        self.held_until
            .store((self.now_ms)() + ms, Ordering::SeqCst);
    }

    pub fn limit(&self, ms: u64) {
        self.limit.store(ms, Ordering::SeqCst);
    }

    // The Soulseek provider does not read failed imports, so only the YouTube oracle has such a step.
    #[allow(dead_code)]
    pub fn refuse(&self, name: &str, error: &str) {
        let mut refused = self.refused.lock().unwrap();
        refused.retain(|(file, _)| file != name);
        refused.push((name.to_owned(), error.to_owned()));
    }

    /// The host as a provider is given it.
    pub fn place_and_scan(self: &Arc<Self>) -> PlaceAndScan {
        let host = self.clone();
        Arc::new(move |place, cancelled| Box::pin(host.clone().run(place, cancelled)))
    }

    async fn run(self: Arc<Self>, place: Place, cancelled: Cancelled) -> Result<Value, PlaceError> {
        let limit = self.limit.load(Ordering::SeqCst);
        let end = (self.now_ms)().saturating_add(limit);
        while (self.now_ms)() < self.held_until.load(Ordering::SeqCst) {
            if cancelled() {
                return Err(PlaceError::cancelled());
            }
            if (self.now_ms)() >= end {
                return Err(PlaceError {
                    message: format!(
                        "Another import has held the folder for {} s. Nothing was saved; try again when it finishes.",
                        limit.div_ceil(1000)
                    ),
                    status: Some(503),
                });
            }
            tokio::time::sleep(self.tick).await;
        }
        if cancelled() {
            return Err(PlaceError::cancelled());
        }
        place()?;
        Ok(self.scan().await)
    }

    async fn scan(&self) -> Value {
        let folder = self
            .root
            .file_name()
            .unwrap()
            .to_string_lossy()
            .into_owned();
        (self.log)(json!({ "scan": folder }));
        tokio::time::sleep(self.tick).await;
        let mut items = Vec::new();
        let mut failed = Vec::new();
        let mut scanned = 0;
        for folder in sorted_names(&self.root) {
            let dir = self.root.join(&folder);
            if folder.starts_with('.') || !dir.is_dir() {
                continue;
            }
            for name in sorted_names(&dir) {
                if name.starts_with('.') || name.ends_with(".json") {
                    continue;
                }
                scanned += 1;
                let rel = format!("{folder}/{name}");
                if self.seen.lock().unwrap().contains(&rel) {
                    continue;
                }
                let refused = self.refused.lock().unwrap();
                if let Some((_, error)) = refused.iter().find(|(file, _)| *file == name) {
                    failed.push(json!({ "file": name, "rel": rel, "error": error }));
                    continue;
                }
                drop(refused);
                self.seen.lock().unwrap().insert(rel.clone());
                let bytes = std::fs::read(dir.join(&name)).unwrap();
                items.push(json!({
                    "file": name,
                    "rel": rel,
                    "size": bytes.len(),
                    "sha256": sha256_hex(&bytes),
                }));
            }
        }
        json!({ "scanned": scanned, "ingested": items.len(), "failed": failed, "items": items })
    }
}
