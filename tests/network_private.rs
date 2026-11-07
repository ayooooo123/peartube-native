//! `privateRouting` must never reach the public DHT or the LAN: the private overlay owns that
//! discovery. A node whose overlay cannot start announces nothing at all.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use serde_json::Value;

use peartube_native::node::network::{LanBinding, LanConnection, Network};

/// Records every topic a LAN binding was asked to join.
#[derive(Default)]
struct SpyLan {
    joins: AtomicUsize,
    left: AtomicUsize,
    destroyed: AtomicBool,
}

impl LanBinding for SpyLan {
    fn join(&self, _topic: [u8; 32]) {
        self.joins.fetch_add(1, Ordering::Relaxed);
    }
    fn leave(&self, _topic: [u8; 32]) {
        self.left.fetch_add(1, Ordering::Relaxed);
    }
    fn destroy(&self) -> futures_util::future::BoxFuture<'_, ()> {
        Box::pin(async { self.destroyed.store(true, Ordering::Release) })
    }
    fn host(&self) -> Option<String> {
        Some("127.0.0.1".to_owned())
    }
    fn port(&self) -> Option<u16> {
        Some(1234)
    }
    fn stats(&self) -> Option<Value> {
        None
    }
    fn dial_direct(&self, _public_key: &str, _host: &str, _port: u16) {}
    fn on_connection(
        &self,
        _callback: Arc<dyn Fn(LanConnection) + Send + Sync>,
    ) -> Box<dyn Fn() + Send + Sync> {
        Box::new(|| {})
    }
}

async fn network(storage: &std::path::Path, private_routing: bool) -> Arc<Network> {
    let store = corestore::Corestore::open(storage).expect("corestore");
    Network::open(
        store,
        Vec::new(),
        Some("127.0.0.1".to_owned()),
        0,
        None,
        None,
        private_routing,
        Arc::new(AtomicBool::new(false)),
    )
    .await
    .expect("network")
}

#[tokio::test(flavor = "multi_thread")]
async fn private_routing_announces_nothing_public_or_lan() {
    let tmp = tempfile::tempdir().unwrap();
    let net = network(&tmp.path().join("private"), true).await;
    let channel = [7u8; 32];
    let blob = [9u8; 32];

    net.join_overlay_topic("channel:test", channel);
    net.join_content(blob);
    let lan = Arc::new(SpyLan::default());
    net.set_lan(Some(lan.clone())).await.expect("set lan");

    // The overlay owns the discovery: the topic is joined there, not on the public swarm.
    assert_eq!(
        net.overlay_topic_count(),
        1,
        "the overlay joins the channel"
    );
    assert!(net.content_topics().is_empty(), "no content topic join");
    assert_eq!(lan.joins.load(Ordering::Relaxed), 0, "no LAN topic join");
    assert!(
        lan.destroyed.load(Ordering::Acquire),
        "a binding a private-routed node may not use is destroyed, not hosted"
    );
    assert!(!net.has_lan());

    // `joinPeer` dials through the overlay's guards, never the public DHT.
    assert!(
        net.join_peer(&"ab".repeat(32), false),
        "the private overlay takes the dial"
    );
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    assert_eq!(net.connection_count(), 0, "no public connect happened");

    // No peer answered, so nothing carries private bytes.
    tokio::time::sleep(std::time::Duration::from_millis(600)).await;
    let sample = net.sample();
    assert_eq!(sample.routes.private.rx_bytes, 0);
    assert_eq!(sample.routes.private.tx_bytes, 0);
    assert_eq!(sample.routes.private.peers, 0);
    assert!(
        net.public_dht().is_none(),
        "cast's Noise RPC gets no public DHT to dial or serve on"
    );
    net.close().await;
}

#[tokio::test]
async fn public_routing_still_joins_its_topics() {
    let tmp = tempfile::tempdir().unwrap();
    let net = network(&tmp.path().join("public"), false).await;
    let lan = Arc::new(SpyLan::default());
    net.set_lan(Some(lan.clone())).await.expect("set lan");
    net.join_overlay_topic("channel:test", [7u8; 32]);
    net.join_content([9u8; 32]);

    assert_eq!(net.overlay_topic_count(), 1);
    assert_eq!(net.content_topics().len(), 1);
    assert_eq!(
        lan.joins.load(Ordering::Relaxed),
        2,
        "LAN joins are synchronous, without waiting for public discovery"
    );
    let replacement = Arc::new(SpyLan::default());
    net.set_lan(Some(replacement.clone()))
        .await
        .expect("replace lan");
    assert_eq!(replacement.joins.load(Ordering::Relaxed), 2);
    assert!(net.has_lan());
    assert!(
        net.join_peer(&"ab".repeat(32), false),
        "a public-routed node dials the peer"
    );
    assert!(net.public_dht().is_some());
    net.close().await;
}
