//! `src/node/storage.js`: the Corestore the whole node shares.
//!
//! The JS owns a `hypercore-storage` instance so compaction and core lookups go through it; the
//! Rust Corestore is the same engine (RocksDB in the hypercore-storage format), so only the folder
//! and the engine's maintenance calls are mirrored here. The worklet RocksDB tuning options map
//! onto the engine's own defaults.

use std::path::{Path, PathBuf};

use corestore::Corestore;

use super::api::StoreSuspension;

use super::blobs::BlobsError;

/// Where the Corestore lives.
pub struct Storage {
    path: PathBuf,
    store: Corestore,
    worklet: bool,
    mobile_worklet: bool,
}

impl Storage {
    /// `createStorage(ctx)`: `corestore/` under the node folder, with the cores it already holds.
    pub fn open(path: impl AsRef<Path>, worklet: bool, wal: bool) -> Result<Self, BlobsError> {
        let path = path.as_ref().to_path_buf();
        let store = Corestore::open_with_options(
            &path.join("corestore"),
            hypercore::storage::StoreOptions {
                wal,
                ..Default::default()
            },
        )?;
        let mobile_worklet = worklet && !cfg!(target_os = "macos");
        Ok(Self {
            path,
            store,
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

    /// `coreStorage.flush()`: memtable to disk, so the size `clearCache` measures is real.
    pub fn flush(&self) {
        let _ = self.store.store().flush();
    }

    /// `coreStorage.compact(options)`: rewrite the SSTs and `.blob` files now, so bytes
    /// tombstoned by a cache clear leave the disk. Blocks; `relocate_blobs` is JS
    /// `blobGarbageCollectionPolicy: 1` (a full wipe with no live blobs left), else policy 2.
    pub fn compact(&self, relocate_blobs: bool) {
        let _ = self.store.store().compact(relocate_blobs);
    }
}

/// `store.suspend()`/`store.resume()`: RocksDB is flushed and closed, and its accesses wait,
/// until the resume reopens it. A closed store has nothing to park or wake.
impl StoreSuspension for Corestore {
    fn suspend(&self) -> std::io::Result<()> {
        match self.store().suspend() {
            Err(_) if self.store().is_closed() => Ok(()),
            result => result.map_err(|error| std::io::Error::other(error.to_string())),
        }
    }

    fn resume(&self) -> std::io::Result<()> {
        match self.resume_blocking() {
            Err(_) if self.store().is_closed() => Ok(()),
            result => result.map_err(|error| std::io::Error::other(error.to_string())),
        }
    }

    fn close(&self) {
        self.store().close();
    }
}
