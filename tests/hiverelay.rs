use hypercore::crypto::KeyPair;
use peartube_native::hiverelay::*;
use serde_json::Value;
use std::process::Command;
fn hex_decode(s: &str) -> Option<Vec<u8>> {
    s.as_bytes()
        .chunks_exact(2)
        .map(|p| u8::from_str_radix(std::str::from_utf8(p).ok()?, 16).ok())
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
    let status = Command::new("node")
        .arg("hiverelay.mjs")
        .arg("--verify")
        .arg(hex_encode(&request.encode()))
        .current_dir(concat!(env!("CARGO_MANIFEST_DIR"), "/fixtures/gen"))
        .status()
        .unwrap();
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
    let mut child = Command::new("node")
        .arg("hiverelay.mjs")
        .arg("--peer")
        .current_dir(concat!(env!("CARGO_MANIFEST_DIR"), "/fixtures/gen"))
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
