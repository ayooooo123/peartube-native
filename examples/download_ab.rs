//! Uncached, byte-checked HTTP/P2P consumer paired with tests/js/download-ab.mjs.
//! Run in release mode; the JSON config supplies entry, host, peers, start and bytes.
use http_body_util::BodyExt;
use peartube_native::node::{
    Node,
    api::{Announce, LanEndpoint},
    core::create_node,
};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::time::{Duration, Instant};

fn cpu_seconds() -> f64 {
    #[repr(C)]
    struct Timespec {
        sec: std::ffi::c_long,
        nsec: std::ffi::c_long,
    }
    unsafe extern "C" {
        fn clock_gettime(clock: i32, time: *mut Timespec) -> i32;
    }
    #[cfg(target_os = "macos")]
    const PROCESS_CPU: i32 = 12;
    #[cfg(not(target_os = "macos"))]
    const PROCESS_CPU: i32 = 2;
    let mut t = Timespec { sec: 0, nsec: 0 };
    assert_eq!(unsafe { clock_gettime(PROCESS_CPU, &mut t) }, 0);
    t.sec as f64 + t.nsec as f64 / 1e9
}

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() {
    let path = std::env::args().nth(1).expect("JSON config path");
    let config: Value = serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap();
    let storage = tempfile::Builder::new()
        .prefix("pt-download-ab-")
        .tempdir_in("/tmp")
        .unwrap();
    let node = create_node(json!({
        "storage": storage.path(), "bootstrap": ["127.0.0.1:9"],
        "worklet": config["worklet"].as_bool().unwrap_or(true),
        "streamHost": "127.0.0.1", "streamPort": 0,
    }))
    .await
    .unwrap();
    let host = config["host"].as_str().unwrap().parse().unwrap();
    // Explicit dials only; ambient mDNS must not add seeders to an A/B.
    let adapter = peartube_native::lan::SelfAddressAdapter::with_adapter(
        peartube_native::lan::Adapter::Host(std::sync::Arc::new(
            peartube_native::lan::HostAdapter::new(|_| {}),
        )),
        host,
    );
    let lan = peartube_native::lan::LanDiscovery::open_with_adapter(
        adapter,
        node.net().key_pair().clone(),
    )
    .await
    .unwrap();
    node.net().set_lan(Some(lan)).await.unwrap();
    let peers: Vec<LanEndpoint> = serde_json::from_value(config["peers"].clone()).unwrap();
    node.set_allowed_peers(Some(peers.iter().map(|p| p.public_key.clone()).collect()))
        .await
        .unwrap();
    for peer in &peers {
        node.dial_lan_peer(peer.clone()).await;
    }
    tokio::time::timeout(Duration::from_secs(30), async {
        while node.net().lan_connection_count() < peers.len() as u64 {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("LAN peer connect timeout");
    let entry: Announce = serde_json::from_value(config["entry"].clone()).unwrap();
    let url = node.stream_url(&entry).await;
    let before = node.status().await;
    assert_eq!(before.cached_bytes, 0, "consumer must start uncached");
    println!(
        "{}",
        json!({"event":"ready","pid":std::process::id(),"url":url,"status":before})
    );
    let start = config["start"].as_u64().unwrap();
    let expected = config["bytes"].as_u64().unwrap();
    let request = hyper::Request::builder()
        .uri(&url)
        .header("range", format!("bytes={start}-{}", start + expected - 1))
        .body(http_body_util::Empty::<bytes::Bytes>::new())
        .unwrap();
    let client = hyper_util::client::legacy::Client::builder(hyper_util::rt::TokioExecutor::new())
        .build_http();
    let sampler = config["trace"].as_bool().unwrap_or(false).then(|| {
        let node = node.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_millis(500));
            loop {
                interval.tick().await;
                println!("{}", json!({"event":"sample","status":node.status().await}));
            }
        })
    });
    let cpu = cpu_seconds();
    let wall = Instant::now();
    let (count, hash) = tokio::time::timeout(Duration::from_secs(180), async {
        let response = client.request(request).await.unwrap();
        assert_eq!(response.status(), 206);
        let mut body = response.into_body();
        let mut count = 0u64;
        let mut digest = Sha256::new();
        while let Some(frame) = body.frame().await {
            if let Ok(data) = frame.unwrap().into_data() {
                count += data.len() as u64;
                digest.update(&data);
            }
        }
        (count, format!("{:x}", digest.finalize()))
    })
    .await
    .expect("range timed out");
    if let Some(sampler) = sampler {
        sampler.abort();
    }
    let seconds = wall.elapsed().as_secs_f64();
    let cpu = cpu_seconds() - cpu;
    assert_eq!(count, expected);
    if let Some(expected_hash) = config["sha256"].as_str() {
        assert_eq!(hash, expected_hash);
    }
    let status = node.status().await;
    println!(
        "{}",
        json!({"event":"result","backend":"rust","bytes":count,"seconds":seconds,
        "MBps":count as f64 / seconds / 1e6,"cpu_seconds":cpu,"cpu_s_per_GiB":cpu * 1073741824.0 / count as f64,
        "sha256":hash,"status":status,"peers":node.net().connections().iter().map(|p|p.remote_public_key()).collect::<Vec<_>>()})
    );
    node.close().await.unwrap();
}
