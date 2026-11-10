//! Seeder paired with download_ab and tests/js/seed-ab.js: `seed_ab <file> [workers]` publishes
//! the file through the node's own `put` (as a relay's acquirer does), prints one JSON line with
//! its loopback LAN peer and announce, then serves until stdin closes. `workers` 0 runs Tokio's
//! current-thread scheduler; the default is 4, as on the desktop.
use peartube_native::node::{
    Node,
    api::PutMeta,
    core::{NodeSource, create_node},
};
use serde_json::json;
use tokio::io::AsyncReadExt;

fn main() {
    let mut args = std::env::args().skip(1);
    let file = args.next().expect("file to publish");
    let workers: usize = args.next().map_or(4, |w| w.parse().expect("workers"));
    let runtime = match workers {
        0 => tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build(),
        n => tokio::runtime::Builder::new_multi_thread()
            .worker_threads(n)
            .enable_all()
            .build(),
    };
    runtime.unwrap().block_on(seed(file));
}

async fn seed(file: String) {
    let storage = tempfile::Builder::new()
        .prefix("pt-seed-ab-")
        .tempdir_in("/tmp")
        .unwrap();
    let node = create_node(json!({
        "storage": storage.path(), "bootstrap": ["127.0.0.1:9"], "worklet": false,
        "streamHost": "127.0.0.1", "streamPort": 0,
    }))
    .await
    .unwrap();
    // Explicit dials only, as in download_ab.
    let adapter = peartube_native::lan::SelfAddressAdapter::with_adapter(
        peartube_native::lan::Adapter::Host(std::sync::Arc::new(
            peartube_native::lan::HostAdapter::new(|_| {}),
        )),
        "127.0.0.1".parse().unwrap(),
    );
    let lan = peartube_native::lan::LanDiscovery::open_with_adapter(
        adapter,
        node.net().key_pair().clone(),
    )
    .await
    .unwrap();
    node.net().set_lan(Some(lan)).await.unwrap();
    let meta: PutMeta =
        serde_json::from_value(json!({"id": "imdb:tt0000001", "title": "Throughput A/B"})).unwrap();
    let entry = node.put(meta, NodeSource::file(file)).await.unwrap();
    let peer = node.net().lan_endpoint().expect("LAN endpoint");
    println!(
        "{}",
        json!({"event": "seed", "pid": std::process::id(), "peer": peer, "entry": entry})
    );
    let mut sink = Vec::new();
    let _ = tokio::io::stdin().read_to_end(&mut sink).await;
    node.close().await.unwrap();
}
