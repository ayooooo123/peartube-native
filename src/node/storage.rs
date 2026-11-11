//! `src/node/storage.js`: the Corestore the whole node shares.
//!
//! The JS owns a `hypercore-storage` instance so compaction and core lookups go through it; the
//! Rust Corestore is the same engine (RocksDB in the hypercore-storage format), so only the folder
//! and the engine's maintenance calls are mirrored here. The worklet RocksDB tuning options map
//! onto the engine's own defaults.

use std::path::{Path, PathBuf};

use corestore::Corestore;

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
    pub fn open(
        path: impl AsRef<Path>,
        worklet: bool,
        wal: bool,
        memory: bool,
    ) -> Result<Self, BlobsError> {
        let path = path.as_ref().to_path_buf();
        let dir = path.join("corestore");
        if memory {
            match std::fs::remove_dir_all(&dir) {
                Ok(()) => {}
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(error.into()),
            }
            for cache in ["cache-lru.json", "catalog-cache.json"] {
                match std::fs::remove_file(path.join(cache)) {
                    Ok(()) => {}
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                    Err(error) => return Err(error.into()),
                }
            }
        }
        let store =
            Corestore::open_with_options(&dir, hypercore::storage::StoreOptions { wal, memory })?;
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

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn memory_restart_discards_cache_indexes_but_keeps_identity() {
        for memory in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            for file in [
                "cache-lru.json",
                "catalog-cache.json",
                "identity.json",
                "receiver.json",
            ] {
                std::fs::write(dir.path().join(file), b"preserve").unwrap();
            }
            let storage = Storage::open(dir.path(), true, true, memory).unwrap();
            for file in ["cache-lru.json", "catalog-cache.json"] {
                assert_eq!(dir.path().join(file).exists(), !memory);
            }
            for file in ["identity.json", "receiver.json"] {
                assert_eq!(std::fs::read(dir.path().join(file)).unwrap(), b"preserve");
            }
            storage.close().await;
        }
    }
}
