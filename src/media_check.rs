//! `src/mirror.js`'s `media-check` plugin. Global search offers only files whose first bytes are
//! media (`search`). Each held public file is read once, its first block from local disk, and the
//! verdict kept in `media-checked.json`, keyed by `<blobs>:<blockOffset>` (the bytes at a pointer
//! never change). It reads 2 files a second at most, and one every 5 s while viewers are served.

use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant};

use serde_json::{Value, json};

use crate::mirror::{Ctx, MirrorError, MirrorPlugin};
use crate::node::core::CoreNode;
use crate::protocol::media_format;
use crate::traffic::TrafficGate;

/// The file beside `mirror-watch.json`.
pub const FILE: &str = "media-checked.json";
const IDLE_PACE: Duration = Duration::from_millis(500);
const VIEWER_PACE: Duration = Duration::from_secs(5);
const SAVE_EVERY: Duration = Duration::from_secs(10);

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(|error| error.into_inner())
}

pub struct MediaCheck {
    node: Arc<CoreNode>,
    traffic: TrafficGate,
    file: PathBuf,
    verdicts: Mutex<HashMap<String, bool>>,
    enabled: AtomicBool,
    checking: AtomicBool,
    closed: AtomicBool,
}

impl MediaCheck {
    pub fn new(
        node: Arc<CoreNode>,
        traffic: TrafficGate,
        storage: PathBuf,
        enabled: bool,
    ) -> Arc<Self> {
        let file = storage.join(FILE);
        let mut verdicts = HashMap::new();
        if let Some(saved) = std::fs::read(&file)
            .ok()
            .and_then(|bytes| serde_json::from_slice::<Value>(&bytes).ok())
            .filter(|saved| saved.get("version").and_then(Value::as_u64) == Some(1))
            && let Some(checked) = saved.get("checked").and_then(Value::as_object)
        {
            for (key, value) in checked {
                if let Some(flag) = value.as_bool() {
                    verdicts.insert(key.clone(), flag);
                }
            }
        }
        Arc::new(Self {
            node,
            traffic,
            file,
            verdicts: Mutex::new(verdicts),
            enabled: AtomicBool::new(enabled),
            checking: AtomicBool::new(false),
            closed: AtomicBool::new(false),
        })
    }

    /// Whether the file at `<blobs>:<blockOffset>` has been checked and is media.
    pub fn is_media(&self, key: &str) -> bool {
        lock(&self.verdicts).get(key).copied() == Some(true)
    }

    pub fn enabled(&self) -> bool {
        self.enabled.load(Ordering::Acquire)
    }

    pub fn set_enabled(&self, on: bool) -> bool {
        let was = self.enabled.swap(on, Ordering::AcqRel);
        on && !was
    }

    fn save(&self) {
        let checked: serde_json::Map<String, Value> = lock(&self.verdicts)
            .iter()
            .map(|(key, value)| (key.clone(), Value::Bool(*value)))
            .collect();
        let text = json!({ "version": 1, "checked": checked }).to_string();
        let tmp = self.file.with_extension("json.tmp");
        if write_private(&tmp, text.as_bytes()).is_ok() {
            let _ = std::fs::rename(&tmp, &self.file);
        }
    }

    async fn pass(&self) -> Value {
        let mut checked = 0u64;
        let mut whole = true;
        let mut held = HashSet::new();
        let mut saved_at = Instant::now();
        for entry in self.node.public_entries().await {
            let key = format!("{}:{}", entry.blobs, entry.blob.block_offset);
            held.insert(key.clone());
            if lock(&self.verdicts).contains_key(&key) {
                continue;
            }
            let pace = if self.traffic.info().serving_viewers {
                VIEWER_PACE
            } else {
                IDLE_PACE
            };
            tokio::time::sleep(pace).await;
            if self.closed.load(Ordering::Acquire) || !self.enabled() {
                whole = false;
                break;
            }
            // Not on this disk after all: the next run tries again.
            let Some(head) = self
                .node
                .first_block(&entry.blobs, entry.blob.block_offset)
                .await
            else {
                continue;
            };
            lock(&self.verdicts).insert(key, media_format(&head).is_some());
            checked += 1;
            if saved_at.elapsed() > SAVE_EVERY {
                self.save();
                saved_at = Instant::now();
            }
        }
        let mut dropped = 0u64;
        if whole {
            lock(&self.verdicts).retain(|key, _| {
                let keep = held.contains(key);
                if !keep {
                    dropped += 1;
                }
                keep
            });
        }
        if checked > 0 || dropped > 0 {
            self.save();
        }
        json!({ "checked": checked, "dropped": dropped })
    }
}

#[cfg(unix)]
fn write_private(path: &std::path::Path, bytes: &[u8]) -> std::io::Result<()> {
    use std::io::Write;
    use std::os::unix::fs::OpenOptionsExt;
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .mode(0o600)
        .open(path)?;
    file.write_all(bytes)
}

#[cfg(not(unix))]
fn write_private(path: &std::path::Path, bytes: &[u8]) -> std::io::Result<()> {
    std::fs::write(path, bytes)
}

impl MirrorPlugin<CoreNode> for MediaCheck {
    fn name(&self) -> &str {
        "media-check"
    }

    fn description(&self) -> &str {
        "Checks once that each held public file is media, so global search can offer it"
    }

    fn run<'a>(
        &'a self,
        _payload: Value,
        _ctx: &'a Ctx<CoreNode>,
    ) -> Pin<Box<dyn Future<Output = Result<Value, MirrorError>> + Send + 'a>> {
        Box::pin(async move {
            if !self.enabled() || self.checking.swap(true, Ordering::AcqRel) {
                return Ok(
                    json!({ "busy": self.checking.load(Ordering::Acquire) && self.enabled() }),
                );
            }
            let result = self.pass().await;
            self.checking.store(false, Ordering::Release);
            Ok(result)
        })
    }

    fn poll<'a>(
        &'a self,
        ctx: &'a Ctx<CoreNode>,
    ) -> Pin<Box<dyn Future<Output = Result<Option<Value>, MirrorError>> + Send + 'a>> {
        Box::pin(async move {
            if !self.enabled() || self.checking.load(Ordering::Acquire) {
                return Ok(None);
            }
            self.run(Value::Null, ctx).await.map(Some)
        })
    }

    fn stop(&self) -> crate::mirror::close::Stop<'_> {
        Box::pin(async move {
            self.closed.store(true, Ordering::Release);
            Ok(())
        })
    }

    fn info(&self) -> Value {
        let verdicts = lock(&self.verdicts);
        json!({
            "enabled": self.enabled(),
            "checking": self.checking.load(Ordering::Acquire),
            "checkedFiles": verdicts.len(),
            "mediaFiles": verdicts.values().filter(|value| **value).count(),
        })
    }
}
