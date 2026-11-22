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

/// The follower copies a title from the seeder of its channel, which is connected and serves it,
/// so the pass never looks up the title's topic: as in the app's `src/mirror.js`, only a slice
/// that gets no block for 5 s is looked up. The title is two blocks, so that the seeder serves it
/// well inside those 5 s on a slow test machine too. A debug build on an Intel i7-10710U took 5.2
/// to 5.4 s to copy a title of 3 MB between two nodes in one process, on native main as well.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_follower_replicates_a_title_and_respects_the_disk_reserve() {
    let udx = libudx::UdxRuntime::new().expect("udx");
    let bootstrap = testnet(&udx).await;
    let (seed_dir, replica_dir) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());

    let seeder = node(seed_dir.path(), &bootstrap, None).await;
    let bytes: Vec<u8> = (0..(700 * 1024 + 13u64))
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
    assert_eq!(
        published.announce.blob.block_length, 2,
        "premise: the title spans two blocks"
    );

    let replica = node(replica_dir.path(), &bootstrap, Some(&published.channel)).await;
    let topics = record_topics(&replica).await;
    let key = *seeder
        .blob_core(&published.announce.blobs)
        .await
        .unwrap()
        .key();
    let topic = hypercore::crypto::discovery_key(&key);
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

    let copying = Instant::now();
    let done = replicator.replicate(json!({ "once": true })).await;
    let copy_time = copying.elapsed();
    assert_eq!(done["totalEntries"], 1, "{done}");
    assert_eq!(done["replicatedEntries"], 1, "{done}");
    assert_eq!(done["replicatedBytes"], bytes.len() as u64, "{done}");

    // The blocks are local: a second pass finds nothing to fetch.
    let again = replicator.replicate(json!({ "once": true })).await;
    assert_eq!(again["replicatedEntries"], 1, "{again}");
    assert_eq!(again["skippedUnavailable"], 0, "{again}");

    // The premise of the next check: the whole pass ended before a slice with no block is looked
    // up.
    assert!(
        copy_time < Duration::from_secs(5),
        "premise: the connected seeder served the title in {copy_time:?}"
    );
    assert!(
        topics.changes(&topic).is_empty(),
        "a serving peer needs no content lookup"
    );
    assert_eq!(
        local_bytes(&replica, &published.announce).await.as_deref(),
        Some(bytes.as_slice()),
        "the follower holds the title's bytes"
    );
    eprintln!(
        "replicate-connected: {} bytes copied in {copy_time:?}; 0 content-topic joins",
        done["replicatedBytes"]
    );

    detach();
    traffic.stop();
    replica.close().await.expect("replica closes");
    seeder.close().await.expect("seeder closes");
}

async fn publish_bytes(
    node: &CoreNode,
    id: &str,
    bytes: &[u8],
) -> peartube_native::node::api::Published {
    node.publish(
        PutMeta {
            id: id.to_owned(),
            title: format!("Film {id}"),
            private_entry: None,
            epoch: None,
            store: None,
            expected_size: Some(Some(bytes.len() as u64)),
            extra: serde_json::Map::new(),
        },
        NodeSource::bytes(bytes.to_vec()),
    )
    .await
    .expect("publish film")
}

async fn wait_listed(node: &CoreNode, ids: &[String]) {
    tokio::time::timeout(Duration::from_secs(120), async {
        loop {
            let listed = node
                .search(None, SearchOptions::default())
                .await
                .expect("search");
            if ids
                .iter()
                .all(|id| listed.iter().any(|entry| &entry.id == id))
            {
                return;
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
    })
    .await
    .expect("follower lists all titles within 120 seconds");
}

async fn local_bytes(
    node: &CoreNode,
    op: &peartube_native::node::api::Announce,
) -> Option<Vec<u8>> {
    let session = node.blob_core(&op.blobs).await.unwrap();
    let mut bytes = Vec::with_capacity(op.size as usize);
    for index in op.blob.block_offset..op.blob.block_offset + op.blob.block_length {
        bytes.extend_from_slice(&session.get_local(index).unwrap()?);
    }
    Some(bytes)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn many_films_replicate_across_more_than_twenty_four_cores() {
    let udx = libudx::UdxRuntime::new().expect("udx");
    let bootstrap = testnet(&udx).await;
    let (seed_dir, follower_dir) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let seeder = node(seed_dir.path(), &bootstrap, None).await;

    let mut films = Vec::new();
    for i in 1..=25 {
        let bytes = vec![i as u8; 2048 + i];
        let published = publish_bytes(&seeder, &format!("imdb:tt00010{i:02}"), &bytes).await;
        assert_eq!(published.announce.blob.block_offset, 0);
        assert_eq!(published.announce.blob.block_length, 1);
        assert_eq!(published.announce.blob.byte_offset, 0);
        assert_eq!(published.announce.blob.byte_length, bytes.len() as u64);
        assert_eq!(published.announce.size, bytes.len() as u64);
        films.push((published, bytes));
    }
    let keys: std::collections::HashSet<_> = films
        .iter()
        .map(|(published, _)| published.announce.blobs.as_str())
        .collect();
    assert_eq!(keys.len(), 25, "each film has a distinct core");
    let channel = films[0].0.channel.as_str();
    assert!(
        films
            .iter()
            .all(|(published, _)| published.channel == channel)
    );
    assert_eq!(
        local_bytes(&seeder, &films[0].0.announce).await.as_deref(),
        Some(films[0].1.as_slice()),
        "the local reader returns the published bytes"
    );

    let dead_key = "00".repeat(32);
    assert!(
        films
            .iter()
            .all(|(published, _)| dead_key.as_str() < published.announce.blobs.as_str())
    );
    let mut dead = serde_json::to_value(&films[0].0.announce).unwrap();
    dead["id"] = json!("imdb:tt0001099");
    dead["title"] = json!("Listed, held by nobody");
    dead["blobs"] = json!(dead_key);
    dead["size"] = json!(4096);
    dead["blob"] = json!({"blockOffset": 0, "blockLength": 1, "byteOffset": 0, "byteLength": 4096});
    let missing = seeder
        .publish_channel(dead, None)
        .await
        .expect("publish unheld pointer");
    assert_eq!(missing.channel, channel);

    let follower = node(follower_dir.path(), &bootstrap, Some(channel)).await;
    let mut ids: Vec<String> = films
        .iter()
        .map(|(published, _)| published.announce.id.clone())
        .collect();
    ids.push(missing.announce.id);
    wait_listed(&follower, &ids).await;

    let (traffic, detach) = follower.traffic_gate(0.0);
    let replicator = Replicator::new(
        follower.clone(),
        follower_dir.path().to_owned(),
        traffic.clone(),
        ReplicateConfig::new(true, &[], 0.0, 0.0),
        None,
    );
    let started = Instant::now();
    let report = tokio::time::timeout(
        Duration::from_secs(240),
        replicator.replicate(json!({"once": true, "stallMs": 45_000, "minFreeBytes": 0})),
    )
    .await
    .expect("replication pass ends within 240 seconds");
    let elapsed = started.elapsed();
    let expected_bytes: u64 = films.iter().map(|(_, bytes)| bytes.len() as u64).sum();
    assert_eq!(report["totalEntries"].as_u64(), Some(26), "{report}");
    assert_eq!(report["replicatedEntries"].as_u64(), Some(25), "{report}");
    assert_eq!(
        report["replicatedBytes"].as_u64(),
        Some(expected_bytes),
        "{report}"
    );
    assert_eq!(
        report["targetBytes"].as_u64(),
        Some(expected_bytes + 4096),
        "{report}"
    );
    assert_eq!(report["skippedDiskFull"].as_u64(), Some(0), "{report}");
    assert_eq!(report["skippedUnavailable"].as_u64(), Some(1), "{report}");
    assert!(
        elapsed >= Duration::from_millis(45_000),
        "pass took {elapsed:?}: {report}"
    );

    seeder.close().await.expect("publisher closes");
    for (published, expected) in &films {
        assert_eq!(
            local_bytes(&follower, &published.announce).await.as_deref(),
            Some(expected.as_slice()),
            "local bytes for {}",
            published.announce.id
        );
    }
    eprintln!(
        "replicate-many: {} of {} films copied; {} unavailable; {} bytes verified locally after publisher closed",
        report["replicatedEntries"],
        report["totalEntries"],
        report["skippedUnavailable"],
        expected_bytes
    );
    replicator.close();
    detach();
    traffic.stop();
    follower.close().await.expect("follower closes");
}

#[derive(Default)]
struct TopicEvents {
    events: std::sync::Mutex<Vec<([u8; 32], bool, Instant)>>,
}

impl TopicEvents {
    fn changes(&self, topic: &[u8; 32]) -> Vec<(bool, Instant)> {
        self.events
            .lock()
            .unwrap()
            .iter()
            .filter(|(key, _, _)| key == topic)
            .map(|(_, joined, at)| (*joined, *at))
            .collect()
    }
}

impl peartube_native::node::network::LanBinding for TopicEvents {
    fn join(&self, topic: [u8; 32]) {
        self.events
            .lock()
            .unwrap()
            .push((topic, true, Instant::now()));
    }

    fn leave(&self, topic: [u8; 32]) {
        self.events
            .lock()
            .unwrap()
            .push((topic, false, Instant::now()));
    }

    fn destroy(&self) -> futures_util::future::BoxFuture<'_, ()> {
        Box::pin(async {})
    }

    fn host(&self) -> Option<String> {
        None
    }

    fn port(&self) -> Option<u16> {
        None
    }

    fn stats(&self) -> Option<Value> {
        None
    }

    fn dial_direct(&self, _: &str, _: &str, _: u16) {}

    fn on_connection(
        &self,
        _: Arc<dyn Fn(peartube_native::node::network::LanConnection) + Send + Sync>,
    ) -> Box<dyn Fn() + Send + Sync> {
        Box::new(|| {})
    }
}

async fn record_topics(node: &CoreNode) -> Arc<TopicEvents> {
    let events = Arc::new(TopicEvents::default());
    node.net().set_lan(Some(events.clone())).await.unwrap();
    events
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_follower_discovers_a_far_holder_after_the_content_topic_goes_quiet() {
    let udx = libudx::UdxRuntime::new().expect("udx");
    let bootstrap = testnet(&udx).await;
    let (holder_dir, pointer_dir, follower_dir) = (
        tempfile::tempdir().unwrap(),
        tempfile::tempdir().unwrap(),
        tempfile::tempdir().unwrap(),
    );
    let bytes: Vec<u8> = (0..(700 * 1024 + 13u64))
        .map(|i| (i.wrapping_mul(2_654_435_761) >> 7) as u8)
        .collect();

    let holder = node(holder_dir.path(), &bootstrap, None).await;
    let published = publish_bytes(&holder, "imdb:tt0000004", &bytes).await;
    assert_eq!(published.announce.blob.block_offset, 0);
    assert_eq!(published.announce.blob.block_length, 2);
    assert_eq!(published.announce.blob.byte_length, bytes.len() as u64);
    assert_eq!(
        local_bytes(&holder, &published.announce).await.as_deref(),
        Some(bytes.as_slice())
    );
    let key = *holder
        .blob_core(&published.announce.blobs)
        .await
        .unwrap()
        .key();
    let target = hypercore::crypto::discovery_key(&key);
    assert!(holder.net().content_topics().contains(&key));

    let pointer = node(pointer_dir.path(), &bootstrap, None).await;
    let listed = pointer
        .publish_channel(serde_json::to_value(&published.announce).unwrap(), None)
        .await
        .unwrap();
    assert_ne!(listed.channel, published.channel);
    assert_eq!(listed.announce.blobs, published.announce.blobs);

    let follower = node(follower_dir.path(), &bootstrap, Some(&listed.channel)).await;
    let events = record_topics(&follower).await;
    wait_listed(&follower, std::slice::from_ref(&published.announce.id)).await;
    let sources = follower.list_sources().await;
    assert_eq!(follower.status_value().await.tracker, listed.channel);
    assert!(!sources.iter().any(|source| source.key == published.channel));
    assert_eq!(local_bytes(&pointer, &published.announce).await, None);
    assert_eq!(
        pointer
            .blob_core(&published.announce.blobs)
            .await
            .unwrap()
            .count_local(0, 2),
        0
    );
    assert_eq!(
        follower
            .blob_core(&published.announce.blobs)
            .await
            .unwrap()
            .count_local(0, 2),
        0
    );
    assert!(events.changes(&target).is_empty());
    assert!(!follower.net().lookup_topics().contains(&key));
    assert!(!follower.net().content_topics().contains(&key));

    let holder_key = holder.peer_key();
    let pointer_key = pointer.peer_key();
    let follower_key = follower.peer_key();
    let follower_peers = follower.peer_connections();
    assert!(
        follower_peers
            .iter()
            .any(|peer| !peer.destroyed() && peer.remote_public_key() == pointer_key)
    );
    assert!(
        !follower_peers
            .iter()
            .any(|peer| !peer.destroyed() && peer.remote_public_key() == holder_key)
    );
    assert!(
        !pointer
            .peer_connections()
            .iter()
            .any(|peer| !peer.destroyed() && peer.remote_public_key() == holder_key)
    );
    assert!(!holder.peer_connections().iter().any(|peer| {
        let key = peer.remote_public_key();
        !peer.destroyed() && (key == follower_key || key == pointer_key)
    }));

    let (traffic, detach) = follower.traffic_gate(0.0);
    let replicator = Replicator::new(
        follower.clone(),
        follower_dir.path().to_owned(),
        traffic.clone(),
        ReplicateConfig::new(true, &[], 0.0, 0.0),
        None,
    );
    let started = Instant::now();
    let done = tokio::time::timeout(
        Duration::from_secs(240),
        replicator.replicate(json!({"once": true, "stallMs": 120000})),
    )
    .await
    .expect("replication pass timed out");
    assert_eq!(done["totalEntries"], 1, "{done}");
    assert_eq!(done["replicatedEntries"], 1, "{done}");
    assert_eq!(done["replicatedBytes"], bytes.len() as u64, "{done}");
    assert_eq!(done["skippedUnavailable"], 0, "{done}");

    let changes = events.changes(&target);
    assert_eq!(
        changes
            .iter()
            .map(|(joined, _)| *joined)
            .collect::<Vec<_>>(),
        [true, false],
        "{changes:?}"
    );
    assert!(
        changes[0].1.duration_since(started) >= Duration::from_secs(5),
        "{changes:?}"
    );
    assert!(!follower.net().lookup_topics().contains(&key));
    assert!(!follower.net().content_topics().contains(&key));

    holder.close().await.expect("holder closes");
    assert_eq!(local_bytes(&pointer, &published.announce).await, None);
    assert_eq!(
        local_bytes(&follower, &published.announce).await.as_deref(),
        Some(bytes.as_slice())
    );
    eprintln!(
        "replicate-far: lookup after {} ms; 1 join, 1 leave; {} bytes verified locally after holder closed",
        changes[0].1.duration_since(started).as_millis(),
        bytes.len()
    );
    detach();
    traffic.stop();
    follower.close().await.expect("follower closes");
    pointer.close().await.expect("pointer closes");
}
