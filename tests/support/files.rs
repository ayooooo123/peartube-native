//! What the oracle tests share about the files a run leaves and about the names of random ids: the listing and
//! the labels the JS harnesses print (`tree` in `tests/js/soulseek-network.mjs`, `job-<n>` for each job id).

use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};

use peartube_native::acquire::RandomSource;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

pub fn sha256_hex(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

/// The names in a folder, in the order JS sorts them (by UTF-16 code unit).
pub fn sorted_names(dir: &Path) -> Vec<String> {
    let mut names: Vec<String> = std::fs::read_dir(dir)
        .unwrap()
        .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
        .collect();
    names.sort_by(|a, b| a.encode_utf16().cmp(b.encode_utf16()));
    names
}

/// Every folder and file under `root`: a `.json` file with its text, any other file with its size and hash.
pub fn tree(root: &Path) -> Value {
    fn walk(dir: &Path, rel: &str, out: &mut Vec<Value>) {
        for name in sorted_names(dir) {
            let full = dir.join(&name);
            let path = if rel.is_empty() {
                name.clone()
            } else {
                format!("{rel}/{name}")
            };
            if full.is_dir() {
                out.push(json!({ "path": format!("{path}/") }));
                walk(&full, &path, out);
            } else if name.ends_with(".json") {
                out.push(json!({ "path": path, "text": std::fs::read_to_string(&full).unwrap() }));
            } else {
                let bytes = std::fs::read(&full).unwrap();
                out.push(
                    json!({ "path": path, "size": bytes.len(), "sha256": sha256_hex(&bytes) }),
                );
            }
        }
    }
    let mut out = Vec::new();
    walk(root, "", &mut out);
    Value::Array(out)
}

/// Job ids by the order they were made in, as the oracles name the app's random ones.
#[derive(Default)]
pub struct Labels(AtomicU64);

impl RandomSource for Labels {
    fn uuid_v4(&self) -> String {
        format!("job-{}", self.0.fetch_add(1, Ordering::SeqCst) + 1)
    }

    fn bytes(&self, len: usize) -> Vec<u8> {
        vec![0; len]
    }
}
