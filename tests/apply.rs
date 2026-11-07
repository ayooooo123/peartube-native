//! Replays `fixtures/apply-reference.json`: `fixtures/gen/apply.mjs` drives the app's own apply.js
//! through a stub view/host that records put/del/flush, addWriter/removeWriter and onWriterChange.
//! Every scenario runs here against the Rust handler on a real hyperbee, and both the recorded host
//! calls and the bytes that landed in the view must match.

use std::sync::{Arc, Mutex};

use autobee::apply_calls::{ApplyCalls, Auto, BoxFuture, SystemAddWriter};
use autobee::apply_view::Handlers;
use autobee::types::{BatchCoords, Head, Key, Link, Node};
use autobee::updates::{Change, Changes};
use bytes::Bytes;
use corestore::Corestore;
use hyperbee::{Bee, RangeOptions};
use peartube_native::node::apply::{Apply, Mirror};
use serde_json::{Value, json};

const FIXTURE: &str = include_str!("../fixtures/apply-reference.json");

fn fixture() -> Value {
    serde_json::from_str(FIXTURE).expect("apply reference json")
}

fn hex_to_bytes(text: &str) -> Vec<u8> {
    assert!(text.len().is_multiple_of(2), "odd hex length");
    (0..text.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&text[i..i + 2], 16).expect("hex"))
        .collect()
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn key_of(value: &Value) -> Key {
    hex_to_bytes(value.as_str().expect("hex string"))
        .try_into()
        .expect("32-byte key")
}

/// A node as `apply` hands it over: the writer's key plus the op bytes.
fn node_of(value: &Value) -> Node {
    Node {
        core: None,
        key: key_of(&value["key"]),
        length: 0,
        weight: 0,
        version: 0,
        timestamp: 0,
        links: Vec::new(),
        batch: BatchCoords { start: 0, end: 0 },
        views: None,
        trusted: Vec::new(),
        witness: None,
        approvals: None,
        optimistic: false,
        value: Some(Bytes::from(hex_to_bytes(value["value"].as_str().unwrap()))),
        hash: None,
        weight_hint: 0,
        drift: 0,
    }
}

/// The host calls and writer changes, in the order apply made them.
#[derive(Clone, Default)]
struct Recorder {
    calls: Arc<Mutex<Vec<Value>>>,
}

impl Recorder {
    fn push(&self, call: Value) {
        self.calls.lock().unwrap().push(call);
    }
}

impl Auto for Recorder {
    fn id(&self) -> Option<&str> {
        None
    }

    fn key(&self) -> Key {
        [0; 32]
    }

    fn discovery_key(&self) -> Option<Key> {
        None
    }

    fn name(&self) -> Option<&str> {
        None
    }

    fn flushes(&self) -> u64 {
        0
    }

    fn is_genesis(&self) -> bool {
        true
    }

    fn add_writer<'a>(
        &'a self,
        key: Key,
        opts: SystemAddWriter,
    ) -> BoxFuture<'a, std::result::Result<(), Box<dyn std::error::Error + Send + Sync>>> {
        // `AddWriterOptions::writer()` weighs 1; the indexer weight is 2.
        self.push(json!({ "t": "addWriter", "key": hex(&key), "isIndexer": opts.weight != 1 }));
        Box::pin(async { Ok(()) })
    }

    fn remove_writer<'a>(
        &'a self,
        key: Key,
    ) -> BoxFuture<'a, std::result::Result<(), Box<dyn std::error::Error + Send + Sync>>> {
        self.push(json!({ "t": "removeWriter", "key": hex(&key) }));
        Box::pin(async { Ok(()) })
    }

    fn interrupt(&self, _reason: Option<&str>) {}

    fn create_anchor<'a>(
        &'a self,
        key: Key,
        length: u64,
    ) -> BoxFuture<'a, std::result::Result<Link, Box<dyn std::error::Error + Send + Sync>>> {
        Box::pin(async move { Ok(Link { key, length }) })
    }
}

async fn open(dir: &tempfile::TempDir) -> (Corestore, Bee) {
    let store = Corestore::open(dir.path()).expect("corestore");
    let bee = Bee::open(&store, None).await.expect("bee open");
    (store, bee)
}

async fn seed(bee: &Bee, view: &Value) {
    let mut batch = bee.batch();
    for entry in view.as_array().expect("view array") {
        batch.put(
            hex_to_bytes(entry["key"].as_str().unwrap()),
            hex_to_bytes(entry["value"].as_str().unwrap()),
        );
    }
    batch.flush().await.expect("seed view");
}

async fn dump(bee: &Bee) -> Vec<Value> {
    let mut stream = bee.range(RangeOptions::default());
    let mut entries = Vec::new();
    while let Some(entry) = stream.next().await.expect("range") {
        entries.push(json!({ "key": hex(&entry.key), "value": hex(&entry.value) }));
    }
    entries
}

/// Replays the reference's own op rows into a fresh bee, so flush boundaries and bytes can be checked.
async fn replay(dir: &tempfile::TempDir, scenario: &Value) -> Bee {
    let (_store, bee) = open(dir).await;
    seed(&bee, &scenario["view"]).await;
    let mut batch = bee.batch();
    for call in scenario["calls"].as_array().unwrap() {
        match call["t"].as_str().unwrap() {
            "put" => batch.put(
                hex_to_bytes(call["key"].as_str().unwrap()),
                hex_to_bytes(call["value"].as_str().unwrap()),
            ),
            "del" => batch.del(hex_to_bytes(call["key"].as_str().unwrap())),
            "flush" => {
                std::mem::replace(&mut batch, bee.batch())
                    .flush()
                    .await
                    .unwrap();
            }
            _ => {}
        }
    }
    batch.flush().await.unwrap();
    bee
}

#[tokio::test]
async fn apply_fixtures() {
    let fixtures = fixture();
    let channel = fixtures["channel"].as_str().unwrap().to_string();
    for scenario in fixtures["apply"].as_array().unwrap() {
        let name = scenario["name"].as_str().unwrap();
        let dir = tempfile::tempdir().unwrap();
        let (_store, bee) = open(&dir).await;
        seed(&bee, &scenario["view"]).await;

        let recorder = Recorder::default();
        let nodes: Vec<Node> = scenario["nodes"]
            .as_array()
            .unwrap()
            .iter()
            .map(node_of)
            .collect();
        let calls = recorder.calls.clone();
        let mut handler = Apply::new(&channel);
        {
            let calls = calls.clone();
            handler = handler.on_writer_change(Arc::new(move |writer: &str, added: bool| {
                calls
                    .lock()
                    .unwrap()
                    .push(json!({ "t": "writerChange", "writer": writer, "added": added }));
            }));
        }
        let host = ApplyCalls::new(&recorder, Some(&nodes));
        handler
            .apply(&nodes, &bee, &host)
            .await
            .unwrap_or_else(|error| panic!("{name}: apply failed: {error}"));

        let expected_calls: Vec<Value> = scenario["calls"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|call| call["t"].as_str().unwrap() != "put")
            .filter(|call| call["t"].as_str().unwrap() != "del")
            .filter(|call| call["t"].as_str().unwrap() != "flush")
            .cloned()
            .collect();
        assert_eq!(
            *recorder.calls.lock().unwrap(),
            expected_calls,
            "{name}: host call sequence"
        );

        assert_eq!(
            dump(&bee).await,
            scenario["result"].as_array().unwrap().clone(),
            "{name}: view bytes"
        );

        let replay_dir = tempfile::tempdir().unwrap();
        let replayed = replay(&replay_dir, scenario).await;
        assert_eq!(bee.length(), replayed.length(), "{name}: flush boundaries");
        assert_eq!(
            dump(&bee).await,
            dump(&replayed).await,
            "{name}: replayed view"
        );
    }
}

fn change_of(value: &Value) -> Option<Change> {
    if !value.is_object() {
        return None;
    }
    let head = |head: &Value| {
        head.as_object().map(|head| Head {
            key: Some(key_of(&head["key"])),
            length: head["length"].as_u64().unwrap(),
        })
    };
    Some(Change {
        flushes: value["flushes"].as_u64().unwrap_or(0),
        from: head(&value["from"]),
        to: head(&value["to"]),
        incremental: value["incremental"].as_bool().unwrap_or(true),
    })
}

#[tokio::test]
async fn is_trusted_fixtures() {
    let fixtures = fixture();
    let channel = fixtures["channel"].as_str().unwrap().to_string();
    for scenario in fixtures["isTrusted"].as_array().unwrap() {
        let name = scenario["name"].as_str().unwrap();
        let dir = tempfile::tempdir().unwrap();
        let (_store, bee) = open(&dir).await;
        seed(&bee, &scenario["view"]).await;
        let key = key_of(&scenario["key"]);
        let reference = scenario["reference"].as_bool().unwrap().then_some(&bee);
        let trusted = Apply::new(&channel).is_trusted(&key, reference).await;
        assert_eq!(trusted, scenario["trusted"].as_bool().unwrap(), "{name}");
    }
}

#[tokio::test]
async fn update_fixtures() {
    let fixtures = fixture();
    let channel = fixtures["channel"].as_str().unwrap().to_string();
    let dir = tempfile::tempdir().unwrap();
    let (_store, bee) = open(&dir).await;
    for scenario in fixtures["update"].as_array().unwrap() {
        let name = scenario["name"].as_str().unwrap();
        let calls = Arc::new(Mutex::new(Vec::new()));
        let landed = Arc::new(Mutex::new(Vec::new()));
        let mirror = Mirror::new(
            {
                let calls = calls.clone();
                let landed = landed.clone();
                Arc::new(move |key: Key| {
                    landed.lock().unwrap().push(hex(&key));
                    calls
                        .lock()
                        .unwrap()
                        .push(json!({ "t": "mirror", "key": hex(&key) }));
                })
            },
            {
                let calls = calls.clone();
                Arc::new(move || calls.lock().unwrap().push(json!({ "t": "update" })))
            },
        );
        let handler = Apply::new(&channel).with_mirror(mirror);
        for step in scenario["steps"].as_array().unwrap() {
            let changes = Changes {
                view: change_of(&step["view"]),
                system: change_of(&step["system"]),
            };
            handler.update(&bee, &changes).await;
        }
        assert_eq!(
            calls.lock().unwrap().clone(),
            scenario["calls"].as_array().unwrap().clone(),
            "{name}"
        );
    }
}
