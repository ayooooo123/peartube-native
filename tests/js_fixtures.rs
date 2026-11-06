//! Every case in `fixtures/js-reference.json` must match the app's own output.

use std::process::Command;

use peartube_native::mnemonic::{
    derive_identity, format_access_uri, unwrap_video_key, verify_claim, wrap_video_key,
};
use peartube_native::protocol::{
    BlobSize, ReceiverUriInput, WatchEntry, clean_name, clean_text, decode_op, describe_entry,
    format_receiver_uri, format_watch_uri, is_media_file, is_system_view_key, is_uniform_512,
    parse_uri,
};
use serde_json::Value;

const FIXTURES: &str = include_str!("../fixtures/js-reference.json");

fn fixtures() -> Value {
    serde_json::from_str(FIXTURES).expect("fixtures/js-reference.json")
}

fn hex(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        out.push_str(&format!("{b:02x}"));
    }
    out
}

fn unhex(s: &str) -> Vec<u8> {
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16).unwrap())
        .collect()
}

fn key32(s: &str) -> [u8; 32] {
    unhex(s).try_into().unwrap()
}

fn arr(cases: &Value, key: &str) -> Vec<Value> {
    let list = cases[key].as_array().unwrap_or_else(|| panic!("{key}"));
    assert!(!list.is_empty(), "{key} is empty");
    list.clone()
}

#[test]
fn identities_match() {
    for case in arr(&fixtures(), "identities") {
        let phrase = case["mnemonic"].as_str().unwrap();
        if case["words"] == 12 {
            assert_eq!(phrase.split(' ').count(), 12);
        } else {
            assert_eq!(phrase.split(' ').count(), 24);
        }
        let id = derive_identity(phrase).unwrap();
        assert_eq!(id.mnemonic, phrase);
        assert_eq!(
            hex(&id.genesis_key_pair.public_key),
            case["genesisPublicKey"]
        );
        assert_eq!(
            hex(&id.genesis_key_pair.secret_key),
            case["genesisSecretKey"]
        );
        assert_eq!(id.channel_key, case["channelKey"]);
        assert_eq!(id.owner_public_key, case["ownerPublicKey"]);
        assert_eq!(hex(&id.vault_key), case["vaultKey"]);
    }
}

#[test]
fn claims_match() {
    let cases = fixtures();
    let identities = arr(&cases, "identities");
    for case in arr(&cases, "claims") {
        let index = case["identity"].as_u64().unwrap() as usize;
        let id = derive_identity(identities[index]["mnemonic"].as_str().unwrap()).unwrap();
        let channel = case["channel"].as_str().unwrap();
        let writer = case["writer"].as_str().unwrap();
        let owner = case["owner"].as_str().unwrap();
        let sig = case["sig"].as_str().unwrap();
        let valid = case["valid"].as_bool().unwrap();
        assert_eq!(
            verify_claim(channel, writer, owner, sig),
            valid,
            "verify {} {}",
            case["identity"],
            channel
        );
        if valid {
            assert_eq!(id.sign_claim(channel, writer), sig);
        }
    }
}

#[test]
fn epoch_keys_match() {
    let cases = fixtures();
    let identities = arr(&cases, "identities");
    for case in arr(&cases, "epochs") {
        let index = case["identity"].as_u64().unwrap() as usize;
        let id = derive_identity(identities[index]["mnemonic"].as_str().unwrap()).unwrap();
        let channel = case["channel"].as_str().unwrap();
        let epoch = case["epoch"].as_u64().unwrap();
        assert_eq!(
            hex(&id.epoch_key(channel, epoch)),
            case["epochKey"],
            "{channel} {epoch}"
        );
    }
}

#[test]
fn wrapped_keys_match() {
    for case in arr(&fixtures(), "wrapped") {
        let wrapped = case["wrapped"].as_str().unwrap();
        let epoch_key = key32(case["epochKey"].as_str().unwrap());
        let opened = unwrap_video_key(wrapped, &epoch_key);
        match case["unwrap"].as_str() {
            Some(expected) => assert_eq!(opened.as_deref(), Some(unhex(expected).as_slice())),
            None => assert_eq!(opened, None),
        }
    }
}

#[test]
fn rust_wrapping_roundtrips() {
    for case in arr(&fixtures(), "wrapped") {
        let Some(video_key) = case["unwrap"].as_str() else {
            continue;
        };
        let epoch_key = key32(case["epochKey"].as_str().unwrap());
        let wrapped = wrap_video_key(&unhex(video_key), &epoch_key);
        assert_eq!(wrapped.len(), 144);
        assert_eq!(
            unwrap_video_key(&wrapped, &epoch_key).as_deref(),
            Some(unhex(video_key).as_slice())
        );
    }
}

/// The generator cannot know Rust's random wrappings, so this cross-check runs the JS reference.
#[test]
#[ignore = "needs node and `npm install` in fixtures/gen"]
fn rust_wrapping_opens_in_js() {
    let deps = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("fixtures/gen/node_modules/hypercore");
    if !deps.exists() {
        eprintln!("skipped: run `npm install` in fixtures/gen to enable the JS cross-check");
        return;
    }
    let cases = fixtures();
    let mut rust_wraps = Vec::new();
    for case in arr(&cases, "wrapped") {
        let Some(video_key) = case["unwrap"].as_str() else {
            continue;
        };
        let epoch_key = case["epochKey"].as_str().unwrap();
        rust_wraps.push(serde_json::json!({
            "videoKey": video_key,
            "epochKey": epoch_key,
            "wrapped": wrap_video_key(&unhex(video_key), &key32(epoch_key)),
        }));
    }
    let path =
        std::env::temp_dir().join(format!("peartube-native-wraps-{}.json", std::process::id()));
    std::fs::write(&path, serde_json::to_vec(&rust_wraps).unwrap()).unwrap();
    let script = concat!(env!("CARGO_MANIFEST_DIR"), "/fixtures/gen/verify-rust.mjs");
    let status = Command::new("node")
        .arg(script)
        .arg(&path)
        .status()
        .expect("node must be installed");
    let _ = std::fs::remove_file(&path);
    assert!(status.success(), "JS could not open Rust wrappings");
}

#[test]
fn mnemonics_match() {
    for case in arr(&fixtures(), "mnemonicCheck") {
        let phrase = case["phrase"].as_str().unwrap();
        assert_eq!(
            peartube_native::mnemonic::normalize_mnemonic(phrase),
            case["normalized"],
            "{phrase}"
        );
        assert_eq!(
            peartube_native::mnemonic::validate_mnemonic(phrase),
            case["valid"].as_bool().unwrap(),
            "{phrase}"
        );
    }
}

#[test]
fn parsed_uris_match() {
    for case in arr(&fixtures(), "parseUri") {
        let input = case["input"].as_str().unwrap();
        match parse_uri(input) {
            Ok(value) => {
                assert!(case["ok"].as_bool().unwrap(), "expected error for {input}");
                assert_eq!(
                    serde_json::to_value(value).unwrap(),
                    case["value"],
                    "{input}"
                );
            }
            Err(error) => {
                assert!(!case["ok"].as_bool().unwrap(), "expected ok for {input}");
                assert_eq!(
                    error.to_string(),
                    case["error"].as_str().unwrap(),
                    "{input}"
                );
            }
        }
    }
}

#[test]
fn watch_uris_match() {
    for case in arr(&fixtures(), "formatWatchUri") {
        let entry: WatchEntry = serde_json::from_value(case["entry"].clone()).unwrap();
        assert_eq!(
            format_watch_uri(
                &entry,
                case["source"].as_str().unwrap(),
                case["sourceKind"].as_str().unwrap()
            ),
            case["value"].as_str().unwrap()
        );
    }
}

#[test]
fn receiver_uris_match() {
    for case in arr(&fixtures(), "formatReceiverUri") {
        let input: ReceiverUriInput = serde_json::from_value(case["input"].clone()).unwrap();
        assert_eq!(
            format_receiver_uri(&input),
            case["value"].as_str().unwrap(),
            "{}",
            case["input"]
        );
    }
}

#[test]
fn ops_match() {
    for case in arr(&fixtures(), "decodeOp") {
        let input = case["input"].as_str().unwrap();
        let actual = serde_json::to_value(decode_op(input)).unwrap();
        assert_eq!(actual, case["value"], "{input}");
    }
}

#[test]
fn text_matches() {
    for case in arr(&fixtures(), "cleanText") {
        let input = case["input"].as_str();
        let fallback = case["fallback"].as_str().unwrap_or("");
        assert_eq!(clean_text(input, fallback), case["value"].as_str().unwrap());
    }
    for case in arr(&fixtures(), "cleanName") {
        let input = case["input"].as_str();
        let fallback = case["fallback"].as_str().unwrap_or("");
        assert_eq!(clean_name(input, fallback), case["value"].as_str().unwrap());
    }
}

#[test]
fn misc_matches() {
    let cases = fixtures();
    for case in arr(&cases, "formatAccessUri") {
        assert_eq!(
            format_access_uri(
                case["channel"].as_str().unwrap(),
                case["epoch"].as_u64().unwrap(),
                case["epochKey"].as_str().unwrap()
            ),
            case["value"].as_str().unwrap()
        );
    }
    for case in arr(&cases, "describeEntry") {
        assert_eq!(
            serde_json::to_value(describe_entry(
                case["id"].as_str().unwrap(),
                case["title"].as_str().unwrap()
            ))
            .unwrap(),
            case["value"],
            "{}",
            case["title"]
        );
    }
    for case in arr(&cases, "isUniform512") {
        let blob = case["input"].as_object().map(|o| BlobSize {
            byte_length: o["byteLength"].as_u64().unwrap(),
            block_length: o["blockLength"].as_u64().unwrap(),
        });
        assert_eq!(
            is_uniform_512(blob.as_ref()),
            case["value"].as_bool().unwrap(),
            "{}",
            case["input"]
        );
    }
    for case in arr(&cases, "isSystemViewKey") {
        assert_eq!(
            is_system_view_key(case["key"].as_str().unwrap()),
            case["value"].as_bool().unwrap()
        );
    }
    for case in arr(&cases, "mediaFile") {
        assert_eq!(
            is_media_file(case["name"].as_str().unwrap()),
            case["value"].as_bool().unwrap(),
            "{}",
            case["name"]
        );
    }
}
