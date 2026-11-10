//! Free-space helpers, the low-disk receiver reclaim and the peer-storage probe, ported from
//! `freeDiskBytes`, `reclaimLowDiskReceiverStorage` and `resolvePeerStorage` in `src/cast.js`.

use std::collections::HashMap;
use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::sync::{LazyLock, Mutex};

use hypercore::crypto::KeyPair;
use hypercore::storage::Store;
use hypercore::{Core, CoreOptions};

use super::LOW_DISK_BOOT_BYTES;

/// Free bytes for non-root users on the filesystem holding `dir`, as `statfsSync` reports
/// `bavail * bsize`. A syscall, not `df`: the relay image has no shell tools.
pub fn free_disk_bytes(dir: &Path) -> Option<u64> {
    let stat = rustix::fs::statvfs(dir).ok()?;
    Some(stat.f_bavail.saturating_mul(stat.f_frsize))
}

/// `freeDiskBytes` with the app's `Infinity` fallback.
pub fn free_disk_bytes_or_unbounded(dir: &Path) -> u64 {
    free_disk_bytes(dir).unwrap_or(u64::MAX)
}

/// `reclaimLowDiskReceiverStorage`: under 2 GiB free, drop stale Corestores but keep the pairing
/// identity. Answers whether anything was removed.
pub fn reclaim_low_disk_receiver_storage(storage: &Path) -> bool {
    if storage.as_os_str().is_empty() {
        return false;
    }
    if fs::create_dir_all(storage).is_err() {
        return false;
    }
    let _ = fs::set_permissions(storage, fs::Permissions::from_mode(0o700));
    let free = match free_disk_bytes(storage) {
        Some(bytes) => bytes,
        None => return false,
    };
    if free >= LOW_DISK_BOOT_BYTES {
        return false;
    }
    let Ok(entries) = fs::read_dir(storage) else {
        return false;
    };
    let mut removed = false;
    for entry in entries.flatten() {
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if name == "corestore" || name.starts_with("peer-") {
            let path = entry.path();
            let ok = if path.is_dir() {
                fs::remove_dir_all(&path).is_ok()
            } else {
                fs::remove_file(&path).is_ok()
            };
            removed |= ok;
        }
    }
    removed
}

static PROBE_CACHE: LazyLock<Mutex<HashMap<PathBuf, bool>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

fn probe_cache() -> &'static Mutex<HashMap<PathBuf, bool>> {
    &PROBE_CACHE
}

/// `resolvePeerStorage`: trust an external path only once a real core has been written there.
///
/// The app probes with a RocksDB Corestore; the Rust store is hypercore-rs's own, so the probe
/// creates a throwaway core, appends one block and deletes the directory. The verdict is cached
/// for the process, like `probedPeerDirs`.
pub async fn resolve_peer_storage(candidate: Option<&Path>, fallback: &Path) -> PathBuf {
    let Some(candidate) = candidate else {
        return fallback.to_path_buf();
    };
    if candidate == fallback {
        return fallback.to_path_buf();
    }
    if let Some(known) = probe_cache()
        .lock()
        .expect("cast probe cache")
        .get(candidate)
        .copied()
    {
        return if known {
            candidate.to_path_buf()
        } else {
            fallback.to_path_buf()
        };
    }
    let probe_dir = candidate.join(".rocksdb-probe");
    let _ = fs::remove_dir_all(&probe_dir);
    let ok = probe_storage(&probe_dir).await;
    let _ = fs::remove_dir_all(&probe_dir);
    probe_cache()
        .lock()
        .expect("cast probe cache")
        .insert(candidate.to_path_buf(), ok);
    if ok {
        candidate.to_path_buf()
    } else {
        fallback.to_path_buf()
    }
}

async fn probe_storage(dir: &Path) -> bool {
    if fs::create_dir_all(dir).is_err() {
        return false;
    }
    let Ok(store) = Store::open(dir) else {
        return false;
    };
    let key_pair = KeyPair::generate();
    let Ok(core) = Core::open(
        &store,
        CoreOptions {
            key_pair: Some(key_pair),
            manifest_version: 3,
            ..Default::default()
        },
    ) else {
        store.close();
        return false;
    };
    let ok = core.append(&[b"ok".as_slice()]).await.is_ok();
    core.close();
    store.close();
    ok
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn probe_accepts_a_writable_directory_and_rejects_a_file() {
        let root = tempfile::tempdir().expect("tempdir");
        let good = root.path().join("peer");
        assert_eq!(resolve_peer_storage(Some(&good), root.path()).await, good);
        assert!(!good.join(".rocksdb-probe").exists(), "probe cleans up");

        let blocked = root.path().join("blocked");
        fs::write(&blocked, b"not a directory").expect("write");
        assert_eq!(
            resolve_peer_storage(Some(&blocked), root.path()).await,
            root.path(),
            "an unwritable candidate falls back"
        );
    }

    #[test]
    fn free_disk_bytes_reads_the_filesystem() {
        let bytes = free_disk_bytes(Path::new(".")).expect("df");
        assert!(bytes > 0);
    }
}
