//! Slice B of the mixed-node scenarios: channel ACL (2), a malformed announce (4), relays after the
//! founder goes offline (5) and publishing twice before the first sync (6), each run in both role
//! directions against a real JS PearTube node on one local HyperDHT testnet.
//!
//! The scenarios and their assertions are `peartube/test/network.test.js`'s. This node takes the
//! role the JS test puts under test (the owner, the attacker, the founder, the fresh writer) and the
//! JS node takes the other, then they swap.
//!
//! Needs `node`, the app checkout (`PEARTUBE_APP`, else the sibling `../peartube`) and its installed
//! modules; `#[ignore]` so `cargo test` stays offline.

use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::process::{Child, ChildStdin, ChildStdout, Command, Stdio};
use std::sync::Arc;
use std::time::{Duration, Instant};

use serde_json::{Map, Value, json};

use peartube_native::node::Node;
use peartube_native::node::api::{Entry, PutMeta, SearchOptions};
use peartube_native::node::core::{CoreNode, NodeSource, create_node};

const HARNESS: &str = "tests/js/network-mixed.mjs";

fn app_dir() -> String {
    std::env::var("PEARTUBE_APP").unwrap_or_else(|_| {
        std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../peartube")
            .to_string_lossy()
            .into_owned()
    })
}

fn has_app() -> bool {
    std::path::Path::new(&app_dir())
        .join("src/index.js")
        .exists()
}

// ---------------------------------------------------------------- JS process

/// One harness process: every JS node of a scenario, on its own testnet, over a JSON line protocol.
struct Js {
    child: Child,
    stdin: Option<ChildStdin>,
    lines: std::io::Lines<BufReader<ChildStdout>>,
    next_id: u64,
    bootstrap: Value,
}

impl Drop for Js {
    fn drop(&mut self) {
        drop(self.stdin.take());
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

impl Js {
    fn spawn(size: u32) -> Self {
        let mut child = Command::new("node")
            .arg(HARNESS)
            .arg("serve")
            .arg(size.to_string())
            .current_dir(env!("CARGO_MANIFEST_DIR"))
            .env("PEARTUBE_APP", app_dir())
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()
            .expect("node");
        let stdin = child.stdin.take();
        let stdout = child.stdout.take().expect("stdout");
        let mut lines = BufReader::new(stdout).lines();
        let first = lines
            .next()
            .expect("harness printed nothing")
            .expect("line");
        let startup: Value = serde_json::from_str(&first).expect("harness startup json");
        Self {
            child,
            stdin,
            lines,
            next_id: 0,
            bootstrap: startup["bootstrap"].clone(),
        }
    }

    fn bootstrap(&self) -> Value {
        self.bootstrap.clone()
    }

    fn call(&mut self, op: &str, args: Value) -> Value {
        self.next_id += 1;
        let id = self.next_id;
        let mut command = Map::new();
        // The envelope is `msgId`: publish/search carry an entry `id` of their own.
        command.insert("msgId".into(), json!(id));
        command.insert("op".into(), json!(op));
        if let Value::Object(fields) = args {
            for (key, value) in fields {
                command.insert(key, value);
            }
        }
        let line = Value::Object(command).to_string();
        self.stdin
            .as_mut()
            .expect("harness stdin")
            .write_all(format!("{line}\n").as_bytes())
            .expect("harness stdin write");
        loop {
            let line = self
                .lines
                .next()
                .unwrap_or_else(|| panic!("harness died during {op}"))
                .expect("harness line");
            let reply: Value = serde_json::from_str(&line)
                .unwrap_or_else(|_| panic!("harness printed a non-JSON line during {op}: {line}"));
            if reply["msgId"].as_u64() != Some(id) {
                continue;
            }
            assert!(
                reply["ok"].as_bool().unwrap_or(false),
                "{op} failed: {}",
                reply["error"]
            );
            return reply["result"].clone();
        }
    }
}

// ---------------------------------------------------------------- small helpers

fn tempdir(label: &str) -> PathBuf {
    tempfile::Builder::new()
        .prefix(&format!("peartube-mixed-b-{label}-"))
        .tempdir()
        .expect("tempdir")
        .keep()
}

/// Progress for a mixed run too slow to read from a bare pass/fail: `MIXED_TRACE=1` prints each step.
fn step(label: &str) {
    if std::env::var_os("MIXED_TRACE").is_some() {
        eprintln!("[mixed-b] {label}");
    }
}

fn until<T>(mut check: impl FnMut() -> Option<T>, ms: u64) -> Option<T> {
    let end = Instant::now() + Duration::from_millis(ms);
    loop {
        if let Some(value) = check() {
            return Some(value);
        }
        if Instant::now() > end {
            return None;
        }
        std::thread::sleep(Duration::from_millis(250));
    }
}

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(8)
        .enable_all()
        .build()
        .expect("runtime")
}

fn rust_node(rt: &tokio::runtime::Runtime, options: Value) -> Arc<CoreNode> {
    rt.block_on(create_node(options)).expect("rust node")
}

fn search(rt: &tokio::runtime::Runtime, node: &CoreNode, id: Option<&str>) -> Vec<Entry> {
    rt.block_on(async { node.search(id, SearchOptions::default()).await })
        .unwrap_or_default()
}

fn search_until(rt: &tokio::runtime::Runtime, node: &CoreNode, id: &str, ms: u64) -> Option<Entry> {
    until(
        || {
            search(rt, node, Some(id))
                .into_iter()
                .find(|entry| entry.id == id)
        },
        ms,
    )
}

fn no_entry(rt: &tokio::runtime::Runtime, node: &CoreNode, id: &str, ms: u64) -> bool {
    until(
        || {
            if search(rt, node, Some(id)).is_empty() {
                Some(())
            } else {
                std::thread::sleep(Duration::from_millis(500));
                None
            }
        },
        ms,
    )
    .is_some()
}

/// The JS node's own listing for one id, as the app's `search` answers it.
fn js_search(js: &mut Js, name: &str, id: &str) -> Vec<Value> {
    let found = js.call("search", json!({"name": name, "id": id}));
    found["entries"].as_array().cloned().unwrap_or_default()
}

fn js_search_until(js: &mut Js, name: &str, id: &str, ms: u64) -> Option<Value> {
    until(|| js_search(js, name, id).into_iter().next(), ms)
}

fn js_entries(js: &mut Js, name: &str) -> Vec<Value> {
    let listing = js.call("search", json!({"name": name, "id": null}));
    listing["entries"].as_array().cloned().unwrap_or_default()
}

fn js_no_entry(js: &mut Js, name: &str, id: &str, ms: u64) -> bool {
    until(
        || {
            if js_search(js, name, id).is_empty() {
                Some(())
            } else {
                std::thread::sleep(Duration::from_millis(500));
                None
            }
        },
        ms,
    )
    .is_some()
}

fn put_meta(id: &str, title: &str) -> PutMeta {
    PutMeta {
        id: id.into(),
        title: title.into(),
        private_entry: None,
        epoch: None,
        store: None,
        expected_size: None,
        extra: Map::new(),
    }
}

fn publish_bytes(
    rt: &tokio::runtime::Runtime,
    node: &CoreNode,
    id: &str,
    title: &str,
    bytes: Vec<u8>,
) -> String {
    rt.block_on(node.publish(put_meta(id, title), NodeSource::bytes(bytes)))
        .unwrap_or_else(|error| panic!("publish {id}: {error}"))
        .key
}

fn close(rt: &tokio::runtime::Runtime, node: &CoreNode) {
    rt.block_on(node.close()).expect("close");
}

fn cleanup(dir: &Path) {
    let _ = std::fs::remove_dir_all(dir);
}

// ---------------------------------------------------------------- 2. channel ACL

/// A Rust owner, JS writers: strangers are swallowed, co-writers cannot delete, the owner can.
#[test]
#[ignore = "needs node and the PearTube app checkout"]
fn rust_owner_acl_blocks_strangers_and_limits_cowriters() {
    if !has_app() {
        return;
    }
    let rt = runtime();
    let _enter = rt.enter();
    let mut js = Js::spawn(3);
    let bootstrap = js.bootstrap();
    let storage = tempdir("acl-owner");
    let owner = rust_node(&rt, json!({"storage": storage, "bootstrap": bootstrap}));
    let channel = rt.block_on(owner.status()).channel;

    for name in ["b", "c", "stranger"] {
        js.call(
            "node",
            json!({"name": name, "bootstrap": bootstrap, "tracker": channel}),
        );
    }
    let b = js.call("status", json!({"name": "b"}));
    let c = js.call("status", json!({"name": "c"}));
    rt.block_on(owner.grant_writer(b["writer"].as_str().unwrap(), "writer", ""))
        .expect("grant b");
    rt.block_on(owner.grant_writer(c["writer"].as_str().unwrap(), "writer", ""))
        .expect("grant c");
    step("2A: writers granted");

    // An ungranted stranger's optimistic write is ignored by every peer.
    let zero = "0".repeat(64);
    let spam = json!({
        "type": "announce", "id": "imdb:tt9999991", "title": "Spam", "size": 1,
        "sha256": zero, "blobs": zero,
        "blob": {"blockOffset": 0, "blockLength": 1, "byteOffset": 0, "byteLength": 1}
    });
    js.call(
        "rawAppend",
        json!({"name": "stranger", "text": spam.to_string()}),
    );

    let published = js.call(
        "publish",
        json!({"name": "b", "id": "imdb:tt0068646", "title": "Kept", "size": 1000}),
    );
    let entry = search_until(&rt, &owner, "imdb:tt0068646", 180_000).expect("kept entry");
    assert_eq!(entry.title, "Kept");
    assert!(
        no_entry(&rt, &owner, "imdb:tt9999991", 5_000),
        "ungranted optimistic write rejected"
    );
    let stranger = js.call("channel", json!({"name": "stranger"}));
    assert_eq!(
        stranger["writable"],
        json!(false),
        "a stranger never becomes writable"
    );
    step("2A: stranger swallowed");

    // A co-writer cannot delete another writer's entry.
    js.call("remove", json!({"name": "c", "key": published["key"]}));
    std::thread::sleep(Duration::from_millis(1000));
    assert_eq!(search(&rt, &owner, Some("imdb:tt0068646")).len(), 1);
    let b_view = js_search_until(&mut js, "b", "imdb:tt0068646", 30_000)
        .expect("the publisher still lists its own entry");
    assert_eq!(b_view["id"], json!("imdb:tt0068646"));
    step("2A: co-writer delete rejected");

    // The publisher can delete its own entry, and the owner can revoke with a purge.
    js.call("remove", json!({"name": "b", "key": published["key"]}));
    assert!(
        until(
            || search(&rt, &owner, Some("imdb:tt0068646"))
                .is_empty()
                .then_some(()),
            30_000
        )
        .is_some(),
        "the publisher's delete propagates"
    );

    let kept2 = js.call(
        "publish",
        json!({"name": "b", "id": "imdb:tt0068647", "title": "Owner Deletable", "size": 800}),
    );
    js.call(
        "publish",
        json!({"name": "b", "id": "imdb:tt0068648", "title": "Purged On Revoke", "size": 800}),
    );
    until(
        || {
            (search(&rt, &owner, Some("imdb:tt0068647")).len() == 1
                && search(&rt, &owner, Some("imdb:tt0068648")).len() == 1)
                .then_some(())
        },
        60_000,
    )
    .expect("both entries replicate");
    rt.block_on(owner.remove(kept2["key"].as_str().unwrap()))
        .expect("owner delete");
    assert!(
        until(
            || search(&rt, &owner, Some("imdb:tt0068647"))
                .is_empty()
                .then_some(()),
            30_000
        )
        .is_some(),
        "the owner deletes a writer's entry"
    );
    rt.block_on(owner.revoke_writer(b["writer"].as_str().unwrap(), true))
        .expect("revoke with purge");
    assert!(
        until(
            || search(&rt, &owner, Some("imdb:tt0068648"))
                .is_empty()
                .then_some(()),
            30_000
        )
        .is_some(),
        "revoking with purgeEntries removes the writer's entries"
    );
    step("2A: owner delete and purge");

    close(&rt, &owner);
    cleanup(&storage);
}

/// A JS owner, a Rust co-writer and a Rust stranger: the same ACL outcomes from this side.
#[test]
#[ignore = "needs node and the PearTube app checkout"]
fn js_owner_limits_a_rust_cowriter_and_purges_it() {
    if !has_app() {
        return;
    }
    let rt = runtime();
    let _enter = rt.enter();
    let mut js = Js::spawn(3);
    let bootstrap = js.bootstrap();
    let a = js.call("node", json!({"name": "a", "bootstrap": bootstrap}));
    let c = js.call(
        "node",
        json!({"name": "c", "bootstrap": bootstrap, "tracker": a["channel"]}),
    );
    js.call("grant", json!({"name": "a", "writer": c["writer"]}));

    let storage = tempdir("acl-writer");
    let writer = rust_node(
        &rt,
        json!({"storage": storage, "bootstrap": bootstrap, "tracker": a["channel"]}),
    );
    let writer_hex = rt.block_on(writer.status()).writer;
    js.call(
        "grant",
        json!({"name": "a", "writer": writer_hex, "role": "writer"}),
    );
    step("2B: rust writer granted");

    let key = publish_bytes(&rt, &writer, "imdb:tt0068646", "Kept", vec![7u8; 4096]);
    let entry = search_until(&rt, &writer, "imdb:tt0068646", 180_000).expect("own entry");
    assert!(entry.local, "the publisher owns the blob");

    // A co-writer cannot delete another writer's entry; this node cannot either.
    js.call("remove", json!({"name": "c", "key": key}));
    std::thread::sleep(Duration::from_millis(1000));
    assert_eq!(search(&rt, &writer, Some("imdb:tt0068646")).len(), 1);
    let a_view = js_search_until(&mut js, "a", "imdb:tt0068646", 30_000)
        .expect("the owner still lists the writer's entry");
    assert_eq!(a_view["id"], json!("imdb:tt0068646"));
    step("2B: co-writer delete rejected");

    // The owner deletes this node's entry, then revokes it with a purge.
    let kept = publish_bytes(
        &rt,
        &writer,
        "imdb:tt0068647",
        "Owner Deletable",
        vec![9u8; 800],
    );
    publish_bytes(
        &rt,
        &writer,
        "imdb:tt0068648",
        "Purged On Revoke",
        vec![11u8; 800],
    );
    until(
        || {
            let list = js_entries(&mut js, "a");
            (list
                .iter()
                .any(|entry| entry["id"] == json!("imdb:tt0068647"))
                && list
                    .iter()
                    .any(|entry| entry["id"] == json!("imdb:tt0068648")))
            .then_some(())
        },
        60_000,
    )
    .expect("both entries replicate to the owner");
    js.call("remove", json!({"name": "a", "key": kept}));
    assert!(
        js_no_entry(&mut js, "a", "imdb:tt0068647", 30_000),
        "the owner deletes a writer's entry"
    );
    js.call(
        "revoke",
        json!({"name": "a", "writer": writer_hex, "purgeEntries": true}),
    );
    assert!(
        js_no_entry(&mut js, "a", "imdb:tt0068648", 30_000),
        "revoking with purgeEntries removes the writer's entries"
    );
    step("2B: owner delete and purge");

    // A stranger this side: an ungranted Rust node cannot write to the channel.
    let stranger_storage = tempdir("acl-stranger");
    let stranger = rust_node(
        &rt,
        json!({"storage": stranger_storage, "bootstrap": bootstrap, "tracker": a["channel"]}),
    );
    let spam = json!({
        "type": "announce", "id": "imdb:tt9999991", "title": "Spam", "size": 1,
        "sha256": "0".repeat(64), "blobs": "0".repeat(64),
        "blob": {"blockOffset": 0, "blockLength": 1, "byteOffset": 0, "byteLength": 1}
    });
    {
        let channel = stranger.channel();
        rt.block_on(
            channel
                .auto()
                .append(vec![bytes::Bytes::from(spam.to_string())], true),
        )
        .expect("an ungranted writer still appends optimistically");
    }
    std::thread::sleep(Duration::from_millis(1500));
    assert!(
        js_search(&mut js, "a", "imdb:tt9999991").is_empty(),
        "an ungranted Rust writer is rejected"
    );
    assert!(
        !rt.block_on(stranger.status()).writable,
        "a stranger never becomes writable"
    );
    step("2B: stranger swallowed");

    close(&rt, &stranger);
    close(&rt, &writer);
    cleanup(&storage);
    cleanup(&stranger_storage);
}

// ---------------------------------------------------------------- 4. malformed announce

const HOSTILE_ANNOUNCE: &str = r#"{"type":"announce","id":{"toString":null,"valueOf":null},"blobs":{"toString":null},"sha256":"x","title":"t","size":1,"blob":{}}"#;

/// A hostile JS append never becomes an entry and never stops the Rust tracker.
#[test]
#[ignore = "needs node and the PearTube app checkout"]
fn malformed_js_announce_leaves_the_rust_tracker_intact() {
    if !has_app() {
        return;
    }
    let rt = runtime();
    let _enter = rt.enter();
    let mut js = Js::spawn(3);
    let bootstrap = js.bootstrap();
    let storage = tempdir("hostile-js");
    let owner = rust_node(&rt, json!({"storage": storage, "bootstrap": bootstrap}));
    let channel = rt.block_on(owner.status()).channel;
    let b = js.call(
        "node",
        json!({"name": "b", "bootstrap": bootstrap, "tracker": channel}),
    );
    js.call("grant", json!({"name": "b", "writer": b["writer"]}));

    js.call("rawAppend", json!({"name": "b", "text": HOSTILE_ANNOUNCE}));
    publish_bytes(&rt, &owner, "imdb:tt0000003", "After", vec![3u8; 500]);
    let hit = search_until(&rt, &owner, "imdb:tt0000003", 60_000).expect("the tracker still works");
    assert_eq!(hit.title, "After");
    js_search_until(&mut js, "b", "imdb:tt0000003", 120_000)
        .expect("the malformed announce does not stop replication");
    let entries = js_entries(&mut js, "b");
    assert!(
        entries
            .iter()
            .all(|entry| entry["id"] == json!("imdb:tt0000003")),
        "the malformed announce never becomes an entry: {entries:?}"
    );
    assert_eq!(
        js.call("channel", json!({"name": "b"}))["closed"],
        json!(false)
    );
    assert!(
        rt.block_on(owner.status()).writable,
        "the founder keeps writing"
    );

    close(&rt, &owner);
    cleanup(&storage);
}

/// A hostile append from this node never breaks the JS tracker.
#[test]
#[ignore = "needs node and the PearTube app checkout"]
fn malformed_rust_announce_leaves_the_js_tracker_intact() {
    if !has_app() {
        return;
    }
    let rt = runtime();
    let _enter = rt.enter();
    let mut js = Js::spawn(3);
    let bootstrap = js.bootstrap();
    let a = js.call("node", json!({"name": "a", "bootstrap": bootstrap}));
    let storage = tempdir("hostile-rust");
    let node = rust_node(
        &rt,
        json!({"storage": storage, "bootstrap": bootstrap, "tracker": a["channel"]}),
    );
    let writer = rt.block_on(node.status()).writer;
    js.call("grant", json!({"name": "a", "writer": writer}));
    {
        let channel = node.channel();
        rt.block_on(
            channel
                .auto()
                .append(vec![bytes::Bytes::from(HOSTILE_ANNOUNCE)], true),
        )
        .expect("the hostile announce is appended");
    }
    let published = js.call(
        "publish",
        json!({"name": "a", "id": "imdb:tt0000003", "title": "After", "size": 500}),
    );
    let hit = js_search_until(&mut js, "a", "imdb:tt0000003", 60_000)
        .expect("the JS tracker still works");
    assert_eq!(hit["title"], json!("After"));
    assert_eq!(hit["sha256"], published["sha256"]);
    let listing = search(&rt, &node, None);
    assert!(
        listing.iter().all(|entry| entry.id == "imdb:tt0000003"),
        "the malformed announce never becomes an entry: {listing:?}"
    );
    assert_eq!(
        js.call("channel", json!({"name": "a"}))["closed"],
        json!(false)
    );

    close(&rt, &node);
    cleanup(&storage);
}

// ---------------------------------------------------------------- 5. founder offline

/// A JS founder leaves; the Rust relay and the JS relay keep publishing and converge.
#[test]
#[ignore = "needs node and the PearTube app checkout"]
fn relays_converge_after_a_js_founder_goes_offline() {
    if !has_app() {
        return;
    }
    let rt = runtime();
    let _enter = rt.enter();
    let mut js = Js::spawn(3);
    let bootstrap = js.bootstrap();
    let a = js.call("node", json!({"name": "a", "bootstrap": bootstrap}));
    let c = js.call(
        "node",
        json!({"name": "c", "bootstrap": bootstrap, "tracker": a["channel"]}),
    );
    let storage = tempdir("founder-js");
    let b = rust_node(
        &rt,
        json!({"storage": storage, "bootstrap": bootstrap, "tracker": a["channel"]}),
    );
    let writer = rt.block_on(b.status()).writer;
    js.call("grant", json!({"name": "a", "writer": writer}));
    js.call("grant", json!({"name": "a", "writer": c["writer"]}));

    js.call(
        "publish",
        json!({"name": "a", "id": "imdb:tt0000010", "title": "Founder", "size": 100}),
    );
    until(
        || {
            let rust_sees = !search(&rt, &b, Some("imdb:tt0000010")).is_empty();
            let js_sees = !js_search(&mut js, "c", "imdb:tt0000010").is_empty();
            (rust_sees && js_sees).then_some(())
        },
        180_000,
    )
    .expect("both relays sync the founder's entry");
    js.call("closeNode", json!({"name": "a"}));
    step("5A: founder offline");

    publish_bytes(&rt, &b, "imdb:tt0000011", "B one", vec![1u8; 100]);
    publish_bytes(&rt, &b, "imdb:tt0000012", "B two", vec![2u8; 100]);
    js.call(
        "publish",
        json!({"name": "c", "id": "imdb:tt0000013", "title": "C one", "size": 100}),
    );

    let ids = [
        "imdb:tt0000010",
        "imdb:tt0000011",
        "imdb:tt0000012",
        "imdb:tt0000013",
    ];
    until(
        || {
            let rust = ids.iter().all(|id| search(&rt, &b, Some(id)).len() == 1);
            let list = js_entries(&mut js, "c");
            let other = ids
                .iter()
                .all(|id| list.iter().filter(|entry| entry["id"] == *id).count() == 1);
            (rust && other).then_some(())
        },
        180_000,
    )
    .unwrap_or_else(|| {
        panic!(
            "both relays never converged; rust={:?} js={:?}",
            ids.map(|id| search(&rt, &b, Some(id)).len()),
            js_entries(&mut js, "c")
                .iter()
                .map(|entry| entry["id"].clone())
                .collect::<Vec<_>>()
        )
    });
    step("5A: converged");

    close(&rt, &b);
    cleanup(&storage);
}

/// A Rust founder leaves; the two JS relays keep publishing and converge.
#[test]
#[ignore = "needs node and the PearTube app checkout"]
fn js_relays_converge_after_a_rust_founder_goes_offline() {
    if !has_app() {
        return;
    }
    let rt = runtime();
    let _enter = rt.enter();
    let mut js = Js::spawn(3);
    let bootstrap = js.bootstrap();
    let storage = tempdir("founder-rust");
    let founder = rust_node(&rt, json!({"storage": storage, "bootstrap": bootstrap}));
    let channel = rt.block_on(founder.status()).channel;
    let b = js.call(
        "node",
        json!({"name": "b", "bootstrap": bootstrap, "tracker": channel}),
    );
    let c = js.call(
        "node",
        json!({"name": "c", "bootstrap": bootstrap, "tracker": channel}),
    );
    rt.block_on(founder.grant_writer(b["writer"].as_str().unwrap(), "writer", ""))
        .expect("grant b");
    rt.block_on(founder.grant_writer(c["writer"].as_str().unwrap(), "writer", ""))
        .expect("grant c");

    publish_bytes(&rt, &founder, "imdb:tt0000010", "Founder", vec![10u8; 100]);
    js_search_until(&mut js, "b", "imdb:tt0000010", 120_000)
        .expect("the relay syncs the founder's entry");
    js.call(
        "publish",
        json!({"name": "b", "id": "imdb:tt0000011", "title": "B one", "size": 100}),
    );
    close(&rt, &founder);
    step("5B: founder offline");

    js.call(
        "publish",
        json!({"name": "b", "id": "imdb:tt0000012", "title": "B two", "size": 100}),
    );
    js.call(
        "publish",
        json!({"name": "c", "id": "imdb:tt0000013", "title": "C one", "size": 100}),
    );
    let ids = [
        "imdb:tt0000010",
        "imdb:tt0000011",
        "imdb:tt0000012",
        "imdb:tt0000013",
    ];
    for name in ["b", "c"] {
        until(
            || {
                let list = js_entries(&mut js, name);
                ids.iter()
                    .all(|id| list.iter().filter(|entry| entry["id"] == *id).count() == 1)
                    .then_some(())
            },
            180_000,
        )
        .unwrap_or_else(|| {
            panic!(
                "{name} never converged: {:?}",
                js_entries(&mut js, name)
                    .iter()
                    .map(|entry| entry["id"].clone())
                    .collect::<Vec<_>>()
            )
        });
    }
    step("5B: converged");

    cleanup(&storage);
}

// ---------------------------------------------------------------- 6. publish before sync

/// A Rust writer publishes twice before its first sync; both reach the JS founder.
#[test]
#[ignore = "needs node and the PearTube app checkout"]
fn rust_writer_publishes_twice_before_its_first_sync() {
    if !has_app() {
        return;
    }
    let rt = runtime();
    let _enter = rt.enter();
    let mut js = Js::spawn(3);
    let bootstrap = js.bootstrap();
    let a = js.call("node", json!({"name": "a", "bootstrap": bootstrap}));
    let storage = tempdir("presync-rust");
    let node = rust_node(
        &rt,
        json!({"storage": storage, "bootstrap": bootstrap, "tracker": a["channel"]}),
    );
    let writer = rt.block_on(node.status()).writer;
    js.call("grant", json!({"name": "a", "writer": writer}));

    publish_bytes(&rt, &node, "imdb:tt0000021", "First", vec![21u8; 100]);
    publish_bytes(&rt, &node, "imdb:tt0000022", "Second", vec![22u8; 100]);

    until(
        || {
            (!js_search(&mut js, "a", "imdb:tt0000021").is_empty()
                && !js_search(&mut js, "a", "imdb:tt0000022").is_empty())
            .then_some(())
        },
        180_000,
    )
    .unwrap_or_else(|| {
        panic!(
            "both publishes never reached the founder: first={:?} second={:?}",
            js_search(&mut js, "a", "imdb:tt0000021"),
            js_search(&mut js, "a", "imdb:tt0000022")
        )
    });
    step("6A: both publishes reached the founder");

    close(&rt, &node);
    cleanup(&storage);
}

/// A JS writer publishes twice before its first sync; both reach this follower.
#[test]
#[ignore = "needs node and the PearTube app checkout"]
fn js_writer_publishes_twice_before_its_first_sync() {
    if !has_app() {
        return;
    }
    let rt = runtime();
    let _enter = rt.enter();
    let mut js = Js::spawn(3);
    let bootstrap = js.bootstrap();
    let a = js.call("node", json!({"name": "a", "bootstrap": bootstrap}));
    let b = js.call(
        "node",
        json!({"name": "b", "bootstrap": bootstrap, "tracker": a["channel"]}),
    );
    js.call("grant", json!({"name": "a", "writer": b["writer"]}));
    let storage = tempdir("presync-js");
    let follower = rust_node(
        &rt,
        json!({"storage": storage, "bootstrap": bootstrap, "tracker": a["channel"]}),
    );
    js.call(
        "publish",
        json!({"name": "b", "id": "imdb:tt0000021", "title": "First", "size": 100}),
    );
    js.call(
        "publish",
        json!({"name": "b", "id": "imdb:tt0000022", "title": "Second", "size": 100}),
    );
    until(
        || {
            (!search(&rt, &follower, Some("imdb:tt0000021")).is_empty()
                && !search(&rt, &follower, Some("imdb:tt0000022")).is_empty())
            .then_some(())
        },
        180_000,
    )
    .unwrap_or_else(|| {
        panic!(
            "both publishes never replicated: first={:?} second={:?}",
            search(&rt, &follower, Some("imdb:tt0000021")),
            search(&rt, &follower, Some("imdb:tt0000022"))
        )
    });
    step("6B: both publishes replicated");

    close(&rt, &follower);
    cleanup(&storage);
}
