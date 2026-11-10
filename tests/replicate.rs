//! `p2p-replicate` on a local testnet: a relay following a channel pulls every block of a
//! title it does not hold, and a free-disk reserve it cannot meet skips the title instead.

use std::sync::Arc;
use std::time::{Duration, Instant};

use peartube_native::node::Node;
use peartube_native::node::api::{PutMeta, SearchOptions};
use peartube_native::node::core::{CoreNode, NodeSource, create_node};
use peartube_native::replicate::{ReplicateConfig, Replicator};
use serde_json::{Value, json};

async fn testnet(udx: &libudx::UdxRuntime) -> Value {
    let mut config = peeroxide_dht::hyperdht::HyperDhtConfig::default();
    config.dht.bootstrap = Vec::new();
    config.dht.host = "127.0.0.1".to_owned();
    config.dht.port = 0;
    config.dht.firewalled = false;
    let (task, handle, _events) = peeroxide_dht::hyperdht::spawn(udx, config)
        .await
        .expect("testnet node");
    let port = handle.local_port().await.expect("testnet port");
    // The handle and task live for the test.
    std::mem::forget((task, handle));
    json!([format!("127.0.0.1:{port}")])
}

async fn node(dir: &std::path::Path, bootstrap: &Value, tracker: Option<&str>) -> Arc<CoreNode> {
    create_node(json!({
        "storage": dir.display().to_string(),
        "bootstrap": bootstrap,
        "tracker": tracker,
    }))
    .await
    .expect("node opens")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_follower_replicates_a_title_and_respects_the_disk_reserve() {
    let udx = libudx::UdxRuntime::new().expect("udx");
    let bootstrap = testnet(&udx).await;
    let (seed_dir, replica_dir) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());

    let seeder = node(seed_dir.path(), &bootstrap, None).await;
    let bytes: Vec<u8> = (0..3_000_001u64)
        .map(|i| (i.wrapping_mul(2_654_435_761) >> 7) as u8)
        .collect();
    let published = seeder
        .publish(
            PutMeta {
                id: "imdb:tt0000003".into(),
                title: "Replica Fixture".into(),
                private_entry: None,
                epoch: None,
                store: None,
                expected_size: Some(Some(bytes.len() as u64)),
                extra: serde_json::Map::new(),
            },
            NodeSource::bytes(bytes.clone()),
        )
        .await
        .expect("publish");

    let replica = node(replica_dir.path(), &bootstrap, Some(&published.channel)).await;
    let deadline = Instant::now() + Duration::from_secs(90);
    loop {
        let listed = replica
            .search(None, SearchOptions::default())
            .await
            .unwrap_or_default();
        if listed.iter().any(|entry| entry.id == "imdb:tt0000003") {
            break;
        }
        assert!(Instant::now() < deadline, "the follower lists the title");
        tokio::time::sleep(Duration::from_millis(250)).await;
    }

    let (traffic, detach) = replica.traffic_gate(0.0);
    let replicator = Replicator::new(
        replica.clone(),
        replica_dir.path().to_owned(),
        traffic.clone(),
        ReplicateConfig::new(true, &[], 0.0, 0.0),
        None,
    );

    let full = replicator
        .replicate(json!({ "once": true, "minFreeBytes": u64::MAX as f64 }))
        .await;
    assert_eq!(full["skippedDiskFull"], 1, "{full}");
    assert_eq!(full["replicatedEntries"], 0, "{full}");

    let done = replicator.replicate(json!({ "once": true })).await;
    assert_eq!(done["totalEntries"], 1, "{done}");
    assert_eq!(done["replicatedEntries"], 1, "{done}");
    assert_eq!(done["replicatedBytes"], bytes.len() as u64, "{done}");

    // The blocks are local: a second pass finds nothing to fetch.
    let again = replicator.replicate(json!({ "once": true })).await;
    assert_eq!(again["replicatedEntries"], 1, "{again}");
    assert_eq!(again["skippedUnavailable"], 0, "{again}");

    detach();
    traffic.stop();
    replica.close().await.expect("replica closes");
    seeder.close().await.expect("seeder closes");
}
