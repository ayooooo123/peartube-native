//! `src/node/storage.js`: the Corestore the whole node shares.
//!
//! The JS owns a `hypercore-storage` instance so compaction and core lookups go through it; the
//! Rust Corestore is the same engine (RocksDB in the hypercore-storage format), so only the folder
//! and the stored-core index are mirrored here. The worklet RocksDB tuning options map onto the
//! engine's own defaults.

use std::collections::HashSet;
use std::sync::Mutex;
use std::path::{Path, PathBuf};

use corestore::Corestore;

use super::blobs::BlobsError;

/// Where the Corestore lives and which cores it already holds.
pub struct Storage {
    path: PathBuf,
    store: Corestore,
    stored_disc: Mutex<HashSet<[u8; 32]>>,
    worklet: bool,
    mobile_worklet: bool,
}

impl Storage {
    /// `createStorage(ctx)`: `corestore/` under the node folder, with the cores it already holds.
    pub fn open(path: impl AsRef<Path>, worklet: bool) -> Result<Self, BlobsError> {
        let path = path.as_ref().to_path_buf();
        let dir = path.join("corestore");
        std::fs::create_dir_all(&dir)?;
        let store = Corestore::open(&dir)?;
        let stored_disc = store.list()?.into_iter().collect();
        let mobile_worklet = worklet && !cfg!(target_os = "macos");
        Ok(Self {
            path,
            store,
            stored_disc: Mutex::new(stored_disc),
            worklet,
            mobile_worklet,
        })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn store(&self) -> &Corestore {
        &self.store
    }

    pub fn stored_disc(&self) -> HashSet<[u8; 32]> {
        self.stored_disc
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .clone()
    }

    pub fn has_stored(&self, discovery_key: &[u8; 32]) -> bool {
        self.stored_disc
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .contains(discovery_key)
    }

    pub fn mark_stored(&self, discovery_key: [u8; 32]) {
        self.stored_disc
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .insert(discovery_key);
    }

    pub fn forget_stored(&self, discovery_key: &[u8; 32]) {
        self.stored_disc
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .remove(discovery_key);
    }

    pub fn worklet(&self) -> bool {
        self.worklet
    }

    /// `isMobileWorklet`: the phone worklet keeps small RocksDB caches.
    pub fn is_mobile_worklet(&self) -> bool {
        self.mobile_worklet
    }

    pub async fn close(&self) {
        self.store.close().await;
    }
}
