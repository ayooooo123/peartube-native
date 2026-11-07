//! The receiver identity file (`receiver.json`), pair codes and the pairing rate limit, ported
//! from `createReceiver`'s identity block plus `normalizePairCode`, `generatePairCode`,
//! `randomHex`, `safeEqual` and `checkPairRateLimit` in `src/cast.js`.

use std::fs;
use std::io;
use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
use std::path::{Path, PathBuf};

use rand::RngCore;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};

use crate::protocol::{clean_name, is_hex64};

use super::{CODE_ALPHABET, CastError};

/// Failures the pairing store can report.
#[derive(Debug)]
pub enum PairingError {
    Io(io::Error),
    Json(serde_json::Error),
}

impl std::fmt::Display for PairingError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PairingError::Io(e) => write!(f, "io: {e}"),
            PairingError::Json(e) => write!(f, "json: {e}"),
        }
    }
}

impl std::error::Error for PairingError {}

impl From<io::Error> for PairingError {
    fn from(e: io::Error) -> Self {
        PairingError::Io(e)
    }
}

impl From<serde_json::Error> for PairingError {
    fn from(e: serde_json::Error) -> Self {
        PairingError::Json(e)
    }
}

/// `storage/receiver.json`; the field order is the file's own.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ReceiverIdentity {
    pub device_id: String,
    pub name: String,
    pub seed: String,
    pub pair_token: String,
    pub pair_code: String,
}

impl ReceiverIdentity {
    fn generate(name: &str) -> Self {
        Self {
            device_id: random_hex(16),
            name: clean_name(Some(name), "PearTube TV"),
            seed: random_hex(32),
            pair_token: random_hex(16),
            pair_code: generate_pair_code(),
        }
    }

    fn is_valid(&self) -> bool {
        is_hex64(&self.seed)
    }

    /// The seed as a 32-byte key, when the file holds a valid one.
    pub fn seed_bytes(&self) -> Option<[u8; 32]> {
        if !is_hex64(&self.seed) {
            return None;
        }
        let mut out = [0u8; 32];
        for (i, byte) in out.iter_mut().enumerate() {
            *byte = u8::from_str_radix(&self.seed[i * 2..i * 2 + 2], 16).ok()?;
        }
        Some(out)
    }

    pub fn file(dir: &Path) -> PathBuf {
        dir.join("receiver.json")
    }

    /// Reads `receiver.json`, creating it when absent or invalid, and syncing a changed name —
    /// exactly what `createReceiver` does before it serves anything.
    pub fn load_or_create(dir: &Path, name: &str) -> Result<Self, PairingError> {
        fs::create_dir_all(dir)?;
        fs::set_permissions(dir, fs::Permissions::from_mode(0o700))?;
        let file = Self::file(dir);
        let existing = fs::read(&file)
            .ok()
            .and_then(|bytes| serde_json::from_slice::<Self>(&bytes).ok())
            .filter(ReceiverIdentity::is_valid);
        let identity = match existing {
            Some(mut identity) => {
                let clean = clean_name(Some(name), "PearTube TV");
                if !name.is_empty() && identity.name != clean {
                    identity.name = clean;
                    write_json_atomic(&file, &serde_json::to_value(&identity)?)?;
                }
                identity
            }
            None => {
                let identity = Self::generate(name);
                write_json_atomic(&file, &serde_json::to_value(&identity)?)?;
                identity
            }
        };
        Ok(identity)
    }
}

/// `writeJsonAtomic`: pretty JSON through `<file>.tmp`, mode 0600, then rename.
pub fn write_json_atomic(file: &Path, value: &Value) -> Result<(), PairingError> {
    let tmp = PathBuf::from(format!("{}.tmp", file.display()));
    let text = serde_json::to_string_pretty(value)?;
    {
        use std::io::Write;
        let mut handle = fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .mode(0o600)
            .open(&tmp)?;
        handle.write_all(text.as_bytes())?;
        handle.sync_all()?;
    }
    fs::rename(&tmp, file)?;
    Ok(())
}

/// `normalizePairCode`: strip everything but ASCII alphanumerics, then upper-case.
pub fn normalize_pair_code(raw: Option<&str>) -> String {
    raw.unwrap_or_default()
        .chars()
        .filter(char::is_ascii_alphanumeric)
        .flat_map(char::to_uppercase)
        .collect()
}

/// `generatePairCode` over an already-drawn 6-byte buffer.
pub fn code_from_bytes(bytes: &[u8; 6]) -> String {
    bytes
        .iter()
        .map(|b| CODE_ALPHABET[usize::from(*b) % CODE_ALPHABET.len()] as char)
        .collect()
}

/// `generatePairCode`.
pub fn generate_pair_code() -> String {
    let mut bytes = [0u8; 6];
    rand::thread_rng().fill_bytes(&mut bytes);
    code_from_bytes(&bytes)
}

/// `randomHex(bytes)`.
pub fn random_hex(bytes: usize) -> String {
    let mut buf = vec![0u8; bytes];
    rand::thread_rng().fill_bytes(&mut buf);
    let mut out = String::with_capacity(bytes * 2);
    for byte in buf {
        use std::fmt::Write;
        let _ = write!(out, "{byte:02x}");
    }
    out
}

/// `safeEqual`: equal length, non-empty, and compared without an early exit.
pub fn safe_equal(a: Option<&str>, b: Option<&str>) -> bool {
    let (a, b) = (
        a.unwrap_or_default().as_bytes(),
        b.unwrap_or_default().as_bytes(),
    );
    if a.is_empty() || a.len() != b.len() {
        return false;
    }
    let mut diff = 0u8;
    for (x, y) in a.iter().zip(b) {
        diff |= x ^ y;
    }
    diff == 0
}

/// `checkPairRateLimit`: six failures inside 15 s lock pairing out.
#[derive(Debug, Default)]
pub struct PairLimiter {
    failures: Vec<u64>,
}

impl PairLimiter {
    const WINDOW_MS: u64 = 15_000;
    const MAX_FAILURES: usize = 6;

    /// `checkPairRateLimit()`, answering the app's 429.
    pub fn check(&mut self, now_ms: u64) -> Result<(), CastError> {
        self.failures
            .retain(|at| now_ms.saturating_sub(*at) < Self::WINDOW_MS);
        if self.failures.len() >= Self::MAX_FAILURES {
            return Err(CastError::too_many(
                "Too many pairing attempts; wait 15 seconds",
            ));
        }
        Ok(())
    }

    /// Records a failed attempt (callers push after a rejected code).
    pub fn record_failure(&mut self, now_ms: u64) {
        self.failures.push(now_ms);
    }

    /// Pair-code check plus the failure bookkeeping the app does inline.
    pub fn verify_code(
        &mut self,
        supplied: Option<&str>,
        expected: &str,
        open_auth: bool,
        now_ms: u64,
    ) -> Result<(), CastError> {
        self.check(now_ms)?;
        if !open_auth && !safe_equal(Some(&normalize_pair_code(supplied)), Some(expected)) {
            self.record_failure(now_ms);
            return Err(CastError::forbidden("Invalid TV pairing code"));
        }
        Ok(())
    }
}

/// The capabilities object a receiver advertises.
pub fn capabilities(p2p: bool) -> Map<String, Value> {
    let mut out = Map::new();
    out.insert("http".into(), json!(true));
    out.insert("p2p".into(), json!(p2p));
    out.insert("privateP2P".into(), json!(p2p));
    out.insert("tracks".into(), json!(true));
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pair_codes_keep_the_alphabet_order_and_normalize() {
        assert_eq!(code_from_bytes(&[0, 1, 2, 3, 4, 5]), "234567");
        assert_eq!(normalize_pair_code(Some(" ab-c d1_2! ")), "ABCD12");
        assert_eq!(normalize_pair_code(None), "");
    }

    #[test]
    fn safe_equal_rejects_empty_and_differing_lengths() {
        assert!(!safe_equal(None, Some("")), "empty codes never match");
        assert!(!safe_equal(Some("abc"), Some("ab")));
        assert!(safe_equal(Some("ab"), Some("ab")));
        assert!(!safe_equal(Some("ab"), Some("ac")));
    }

    #[test]
    fn limiter_locks_out_after_six_failures_inside_the_window() {
        let mut limiter = PairLimiter::default();
        for at in 0..6 {
            assert!(
                limiter
                    .verify_code(Some("WRONG"), "ABC123", false, at)
                    .is_err()
            );
        }
        let err = limiter.check(5_000).unwrap_err();
        assert_eq!(err.status, 429);
        // A later window forgets the failures.
        assert!(limiter.check(16_000).is_ok());
    }

    #[test]
    fn identity_round_trips_through_the_identity_file() {
        let dir = std::env::temp_dir().join(format!("cast-pairing-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        let identity = ReceiverIdentity::load_or_create(&dir, "Kitchen TV").unwrap();
        assert_eq!(identity.name, "Kitchen TV");
        assert!(identity.is_valid());
        let again = ReceiverIdentity::load_or_create(&dir, "Kitchen TV").unwrap();
        assert_eq!(again, identity, "second open reuses the stored identity");
        let renamed = ReceiverIdentity::load_or_create(&dir, "Den TV").unwrap();
        assert_eq!(renamed.name, "Den TV");
        assert_eq!(renamed.pair_token, identity.pair_token);
        let mode = fs::metadata(ReceiverIdentity::file(&dir))
            .unwrap()
            .permissions()
            .mode()
            & 0o777;
        assert_eq!(mode, 0o600);
        let _ = fs::remove_dir_all(&dir);
    }
}
