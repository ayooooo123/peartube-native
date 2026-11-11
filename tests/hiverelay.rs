use hypercore::crypto::KeyPair;
use peartube_native::hiverelay::*;
use serde_json::Value;
use std::process::Command;
fn hex_decode(s: &str) -> Option<Vec<u8>> {
    s.as_bytes()
        .as_chunks::<2>()
        .0
        .iter()
        .map(|p: &[u8; 2]| u8::from_str_radix(std::str::from_utf8(p).ok()?, 16).ok())
        .collect()
}
fn hex_encode(b: &[u8]) -> String {
    b.iter().map(|n| format!("{n:02x}")).collect()
}

fn fixtures() -> Value {
    serde_json::from_str(include_str!("../fixtures/hiverelay-reference.json")).unwrap()
}
fn bytes(s: &Value) -> Vec<u8> {
    hex_decode(s.as_str().unwrap()).unwrap()
}

/// The app checkout the generator reads: `PEARTUBE_APP`, else this repository's sibling `../peartube`.
fn app_checkout() -> Option<std::path::PathBuf> {
    std::env::var_os("PEARTUBE_APP")
        .map(std::path::PathBuf::from)
        .or_else(|| {
            let sibling = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../peartube");
            sibling.is_dir().then_some(sibling)
        })
}
#[test]
fn js_vectors() {
    let f = fixtures();
    assert_eq!(HANDSHAKE, bytes(&f["handshake"]));
    assert_eq!(discovery_topic().as_slice(), bytes(&f["topic"]));
    assert_eq!(
        epoch_topics(123 * DISCOVERY_EPOCH_MS)[0].as_slice(),
        bytes(&f["epoch"])
    );
    for b in f["beacons"].as_array().unwrap() {
        let feed: [u8; 32] = bytes(&b["feed"]).try_into().unwrap();
        let x = &b["blob"];
        let blob = BlobId {
            block_offset: x["blockOffset"].as_u64().unwrap(),
            block_length: x["blockLength"].as_u64().unwrap(),
            byte_offset: x["byteOffset"].as_u64().unwrap(),
            byte_length: x["byteLength"].as_u64().unwrap(),
        };
        assert_eq!(hyperdrive_header(&feed), bytes(&b["header"]));
        assert_eq!(
            hyperdrive_root(
                b["seq"].as_u64().unwrap(),
                b["path"].as_str().unwrap(),
                &blob
            ),
            bytes(&b["root"])
        );
    }
    for v in f["bounds"].as_array().unwrap() {
        assert_eq!(bound_bytes(v[0].as_u64().unwrap()), v[1].as_u64().unwrap());
    }
    let encoded = bytes(&f["request"]);
    let mut request = SeedRequest::decode(&encoded).unwrap();
    assert!(request.verify());
    assert_eq!(request.encode(), encoded);
    let mut empty = SeedRequest::decode(&bytes(&f["emptyRequest"])).unwrap();
    assert!(empty.verify());
    let expected = empty.publisher_signature;
    empty.sign(&KeyPair::from_seed(&[7; 32]));
    assert_eq!(empty.publisher_signature, expected);
    let messages = &f["messages"];
    let accept = SeedAccept::decode(&bytes(&messages["accept"])).unwrap();
    assert_eq!(accept.encode(), bytes(&messages["accept"]));
    assert_eq!(accept.region, "eu-west");
    let unseed = UnseedRequest::decode(&bytes(&messages["unseed"])).unwrap();
    assert_eq!(unseed.encode(), bytes(&messages["unseed"]));
    assert_eq!(unseed.timestamp, 123456);
    let deny = SeedDeny::decode(&bytes(&messages["deny"])).unwrap();
    assert_eq!(deny.encode(), bytes(&messages["deny"]));
    assert_eq!(deny.reason_code, "capacity");
    let relay = KeyPair::from_seed(&[8; 32]);
    let mut signed_accept = accept.clone();
    signed_accept.relay_pubkey = relay.public_key;
    signed_accept.relay_signature = relay.sign(
        &[
            signed_accept.app_key.as_slice(),
            relay.public_key.as_slice(),
            signed_accept.region.as_bytes(),
        ]
        .concat(),
    );
    assert!(signed_accept.valid_from(&relay.public_key));
    assert!(!signed_accept.valid_from(&[0; 32]));
    signed_accept.region.push('x');
    assert!(!signed_accept.valid_from(&relay.public_key));
    signed_accept.relay_signature = [0; 64];
    assert!(signed_accept.valid_from(&relay.public_key));
    let mut signed_deny = deny.clone();
    signed_deny.relay_pubkey = relay.public_key;
    signed_deny.relay_signature = relay.sign(
        &[
            signed_deny.app_key.as_slice(),
            relay.public_key.as_slice(),
            signed_deny.reason_code.as_bytes(),
        ]
        .concat(),
    );
    assert!(signed_deny.valid_from(&relay.public_key));
    signed_deny.reason_code.push('x');
    assert!(!signed_deny.valid_from(&relay.public_key));
    let mut legacy = encoded.clone();
    legacy.truncate(legacy.len() - 5);
    let old = SeedRequest::decode(&legacy).unwrap();
    assert_eq!(
        (old.revocable, old.unseed_freeze_ms, old.durability),
        (1, 0, 0)
    );
    for expected in f["http"].as_array().unwrap() {
        let actual = seed_core_request(
            "https://relay.example",
            expected["apiKey"].as_str(),
            "abcd",
            1234,
        );
        assert_eq!(actual.method, expected["method"].as_str().unwrap());
        assert_eq!(actual.url, expected["url"].as_str().unwrap());
        assert_eq!(actual.body, expected["body"].as_str().unwrap());
        assert_eq!(
            serde_json::to_value(actual.headers).unwrap(),
            expected["headers"]
        );
    }
    request.sign(&KeyPair::from_seed(&[7; 32]));
    assert_eq!(
        request.publisher_signature.as_slice(),
        bytes(&f["signature"])
    );
    let status = match app_checkout() {
        Some(app) => Command::new("node")
            .arg("hiverelay.mjs")
            .arg("--verify")
            .arg(hex_encode(&request.encode()))
            .current_dir(concat!(env!("CARGO_MANIFEST_DIR"), "/fixtures/gen"))
            .env("PEARTUBE_APP", app)
            .status()
            .unwrap(),
        None => {
            eprintln!(
                "skipped: set PEARTUBE_APP to the app checkout to verify the signature in JS"
            );
            return;
        }
    };
    assert!(status.success());
    request.app_key[0] ^= 1;
    assert!(!request.verify());
}
#[test]
fn paging_and_http() {
    let cores: Vec<_> = (0..20)
        .map(|i| Core {
            key: i.to_string(),
            kind: if i == 0 { "channel" } else { "blob" }.into(),
            byte_length: i,
            accepted: 0,
        })
        .collect();
    let mut cursor = 0;
    let a = select_page(&cores, 3, false, &mut cursor);
    assert_eq!(a.len(), 16);
    assert_eq!(a[0].key, "0");
    assert_eq!(cursor, 15);
    let b = select_page(&cores, 3, false, &mut cursor);
    assert_eq!(b[1].key, "16");
    assert_eq!(page_delay_ms(1000, 1200), 550);
    assert_eq!(page_delay_ms(1000, 1750), 0);
    assert_eq!(page_delay_ms(0, 100), 0);
    let req = seed_core_request("https://relay.example/", Some("token"), "ab", 0);
    assert_eq!(req.method, "POST");
    assert_eq!(req.url, "https://relay.example/seed-core");
    assert_eq!(
        req.body,
        "{\"coreKey\":\"ab\",\"maxStorageBytes\":16777216}"
    );
    assert_eq!(req.headers["authorization"], "Bearer token");
    assert_eq!(req.headers["content-length"], req.body.len().to_string());
}

#[tokio::test]
async fn node_secret_stream_interop() {
    use peeroxide_dht::{noise, secret_stream::SecretStream};
    use protomux::{Event, Mux};
    use std::io::{BufRead, BufReader};
    use std::process::Stdio;
    use std::time::Duration;
    let Some(app) = app_checkout() else {
        eprintln!("skipped: set PEARTUBE_APP to the app checkout");
        return;
    };
    let mut child = Command::new("node")
        .arg("hiverelay.mjs")
        .arg("--peer")
        .current_dir(concat!(env!("CARGO_MANIFEST_DIR"), "/fixtures/gen"))
        .env("PEARTUBE_APP", app)
        .stdout(Stdio::piped())
        .spawn()
        .unwrap();
    let mut stdout = BufReader::new(child.stdout.take().unwrap());
    let mut line = String::new();
    stdout.read_line(&mut line).unwrap();
    let port: u16 = line.strip_prefix("port ").unwrap().trim().parse().unwrap();
    let scenario = async {
        let tcp = tokio::net::TcpStream::connect(("127.0.0.1", port))
            .await
            .unwrap();
        let ss = SecretStream::new(true, tcp, noise::generate_keypair())
            .await
            .unwrap();
        let mux = Mux::from_secret_stream(ss);
        let mut channel = open_channel(&mux).unwrap();
        assert_eq!(channel.opened().await.unwrap(), HANDSHAKE);
        let request = SeedRequest::decode(&bytes(&fixtures()["request"])).unwrap();
        assert!(send_seed(&channel, &request));
        match channel.recv().await.unwrap() {
            Event::Message(msg) => {
                assert_eq!(msg.kind, 0);
                let other = SeedRequest::decode(&msg.data).unwrap();
                assert_eq!(other, request);
                assert!(other.verify());
            }
            event => panic!("unexpected event: {event:?}"),
        }
        let received = tokio::task::spawn_blocking(move || {
            let mut line = String::new();
            stdout.read_line(&mut line).unwrap();
            line
        })
        .await
        .unwrap();
        assert_eq!(received, "received\n");
        mux.destroy();
    };
    let result = tokio::time::timeout(Duration::from_secs(20), scenario).await;
    child.kill().ok();
    child.wait().ok();
    result.expect("relay exchange timed out");
}

// ---------------------------------------------------------------------------------------------
// Live interop: the app's own seeder (`src/hiverelay.js`) and this crate's seeder, against one
// HiveRelay stand-in, from copies of the same storage, must send the same requests.

mod live {
    use peartube_native::hiverelay::seeder::{HiverelayConfig, HiverelaySeeder};
    use serde_json::{Value, json};
    use std::collections::BTreeMap;
    use std::io::{BufRead, BufReader, Write};
    use std::path::Path;
    use std::process::{Child, ChildStdin, Command, Stdio};
    use std::sync::mpsc::{Receiver, channel};
    use std::time::Duration;

    const HARNESS: &str = "tests/js/hiverelay-seed.mjs";

    fn app_dir() -> String {
        std::env::var("PEARTUBE_APP").unwrap_or_default()
    }

    fn has_app() -> bool {
        Path::new(&app_dir()).join("src/hiverelay.js").exists()
    }

    struct Js {
        child: Child,
        stdin: Option<ChildStdin>,
        lines: Receiver<String>,
        next: u64,
    }

    impl Drop for Js {
        fn drop(&mut self) {
            drop(self.stdin.take());
            let _ = self.child.kill();
            let _ = self.child.wait();
        }
    }

    impl Js {
        fn spawn() -> Self {
            let mut child = Command::new("node")
                .arg(HARNESS)
                .arg("serve")
                .current_dir(env!("CARGO_MANIFEST_DIR"))
                .env("PEARTUBE_APP", app_dir())
                .stdin(Stdio::piped())
                .stdout(Stdio::piped())
                .stderr(Stdio::inherit())
                .spawn()
                .expect("node");
            let stdin = child.stdin.take();
            let stdout = child.stdout.take().expect("stdout");
            let (sender, lines) = channel();
            std::thread::spawn(move || {
                for line in BufReader::new(stdout).lines().map_while(Result::ok) {
                    let _ = sender.send(line);
                }
            });
            let mut js = Self {
                child,
                stdin,
                lines,
                next: 1,
            };
            let ready = js.line();
            assert_eq!(ready["ready"], true, "harness ready: {ready}");
            js
        }

        fn line(&mut self) -> Value {
            let line = self
                .lines
                .recv_timeout(Duration::from_secs(180))
                .expect("harness line");
            serde_json::from_str(&line).unwrap_or_else(|_| panic!("not JSON: {line}"))
        }

        fn call(&mut self, op: &str, args: Value) -> Value {
            let id = self.next;
            self.next += 1;
            let mut message = args;
            message["id"] = json!(id);
            message["op"] = json!(op);
            let stdin = self.stdin.as_mut().expect("harness stdin");
            writeln!(stdin, "{message}").expect("write");
            stdin.flush().expect("flush");
            loop {
                let line = self.line();
                if line["id"] != json!(id) {
                    continue;
                }
                assert_eq!(line["ok"], true, "{op}: {}", line["error"]);
                return line["result"].clone();
            }
        }
    }

    fn copy_tree(from: &Path, to: &Path) {
        std::fs::create_dir_all(to).expect("destination");
        for entry in std::fs::read_dir(from).expect("source") {
            let entry = entry.expect("entry");
            let target = to.join(entry.file_name());
            if entry.file_type().expect("type").is_dir() {
                copy_tree(&entry.path(), &target);
            } else {
                std::fs::copy(entry.path(), &target).expect("copy");
            }
        }
    }

    fn by_key(list: &[Value]) -> BTreeMap<String, Value> {
        list.iter()
            .map(|item| {
                (
                    item["appKey"].as_str().unwrap_or_default().to_owned(),
                    item.clone(),
                )
            })
            .collect()
    }

    /// Which of the two runs' lists got more entries.
    fn tail(list: &[Value], from: usize) -> Vec<Value> {
        list.iter().skip(from).cloned().collect()
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[ignore = "needs node and the PearTube app checkout (PEARTUBE_APP)"]
    async fn the_js_seeder_and_this_seeder_send_the_same_requests() {
        if !has_app() {
            eprintln!("skipped: set PEARTUBE_APP to the app checkout");
            return;
        }
        let mut js = Js::spawn();
        let bootstrap = js.call("testnet", json!({ "size": 2 }))["bootstrap"].clone();
        let standin = js.call("standin", json!({ "apiKey": "hr-secret" }));
        let url = standin["url"].as_str().expect("stand-in url").to_owned();
        let pubkey = standin["pubkey"].as_str().expect("stand-in key").to_owned();

        let base = tempfile::tempdir().expect("temp");
        let shared = base.path().join("shared");
        let opened = js.call(
            "node",
            json!({ "name": "base", "storage": shared, "tracker": Value::Null }),
        );
        let channel = opened["channel"].as_str().expect("channel").to_owned();
        js.call("publish", json!({ "name": "base" }));
        let seedable = js.call("seedable", json!({ "name": "base" }));
        assert!(
            seedable.as_array().is_some_and(|list| list.len() >= 2),
            "the channel and its video are both seedable: {seedable}"
        );
        js.call("close", json!({ "name": "base" }));
        // Both runs start from the same storage state, so both derive the same beacons.
        let reference = base.path().join("reference");
        copy_tree(&shared, &reference);

        // The app's seeder, the reference.
        js.call(
            "node",
            json!({ "name": "js", "storage": shared, "tracker": channel }),
        );
        let mark = js.call("mark", json!({ "pubkey": pubkey }));
        js.call(
            "seeder",
            json!({
                "name": "js",
                "relays": format!("{url}=hr-secret"),
                "discover": true,
                "replicas": 2
            }),
        );
        let recorded = js.call("requests", json!({ "pubkey": pubkey }));
        let js_http = tail(
            recorded["http"].as_array().expect("http"),
            mark["http"].as_u64().unwrap_or(0) as usize,
        );
        let js_p2p = tail(
            recorded["p2p"].as_array().expect("p2p"),
            mark["p2p"].as_u64().unwrap_or(0) as usize,
        );
        js.call("stop", json!({ "name": "js" }));
        js.call("close", json!({ "name": "js" }));
        assert_eq!(
            js_http.len(),
            seedable.as_array().unwrap().len(),
            "one /seed-core per seedable core: {js_http:?}"
        );
        assert!(!js_p2p.is_empty(), "the reference broadcast seed requests");

        // The same node state, read by this crate's seeder, on its own copy.
        let node = peartube_native::node::core::create_node(json!({
            "storage": reference,
            "bootstrap": bootstrap,
            "tracker": channel,
            "streamHost": "127.0.0.1",
            "streamPort": 0,
        }))
        .await
        .expect("rust node");
        let (traffic, detach) = node.traffic_gate(50.0);
        let seeder = HiverelaySeeder::new(
            node.clone(),
            traffic,
            HiverelayConfig {
                relays: vec![format!("{url}=hr-secret")],
                api_key: None,
                discover: true,
                replicas: 2,
                ttl_days: 30,
                include_cached: true,
            },
            None,
        );
        let result = seeder
            .sync_once(&json!({ "once": true }))
            .await
            .expect("rust sync");
        let after = js.call("requests", json!({ "pubkey": pubkey }));
        let rust_http = tail(
            after["http"].as_array().expect("http"),
            (mark["http"].as_u64().unwrap_or(0) as usize) + js_http.len(),
        );
        let rust_p2p = tail(
            after["p2p"].as_array().expect("p2p"),
            (mark["p2p"].as_u64().unwrap_or(0) as usize) + js_p2p.len(),
        );
        assert_eq!(
            rust_http.len(),
            js_http.len(),
            "one /seed-core per core, as the reference sent: {result} {rust_http:?}"
        );

        let mut js_bodies: Vec<String> = js_http
            .iter()
            .map(|item| item["body"].as_str().unwrap_or_default().to_owned())
            .collect();
        let mut rust_bodies: Vec<String> = rust_http
            .iter()
            .map(|item| item["body"].as_str().unwrap_or_default().to_owned())
            .collect();
        js_bodies.sort();
        rust_bodies.sort();
        assert_eq!(rust_bodies, js_bodies, "identical /seed-core bodies");
        for item in &rust_http {
            assert_eq!(item["authorization"], "Bearer hr-secret");
            assert_eq!(item["contentType"], "application/json");
        }

        let js_requests = by_key(&js_p2p);
        let rust_requests = by_key(&rust_p2p);
        assert_eq!(
            rust_requests.keys().collect::<Vec<_>>(),
            js_requests.keys().collect::<Vec<_>>(),
            "the same beacon keys, request for request: rust {rust_requests:?} js {js_requests:?}"
        );
        for (key, expected) in &js_requests {
            let actual = &rust_requests[key];
            assert_eq!(
                actual["raw"], expected["raw"],
                "hiverelay-seed request bytes for beacon {key}"
            );
            assert_eq!(actual["discoveryKeys"], expected["discoveryKeys"]);
            assert_eq!(actual["publisherPubkey"], expected["publisherPubkey"]);
        }
        assert_eq!(
            result["p2pBroadcasts"].as_u64().unwrap_or(0),
            rust_p2p.len() as u64,
            "every request the run reports went out: {result}"
        );

        detach();
        seeder.stop().await;
        node.close().await.expect("close");
        js.call("destroy", json!({}));
    }
}
