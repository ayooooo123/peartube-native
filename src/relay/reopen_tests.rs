use super::Relay;
use crate::mnemonic::{derive_identity, generate_mnemonic};
use crate::node::Node;
use hypercore::storage::CreateCore;
use serde_json::{Value, json};
use std::sync::{Arc, mpsc};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

async fn wait_for(what: &str, condition: impl Fn() -> bool) {
    tokio::time::timeout(Duration::from_secs(10), async {
        while !condition() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect(what);
}

async fn check_disconnect(
    method: &str,
    path: &str,
    body: String,
    channel: Option<String>,
    before_save: bool,
) {
    tokio::time::timeout(
        Duration::from_secs(30),
        disconnect(method, path, body, channel, before_save),
    )
    .await
    .expect("the relay did not finish the disconnect test");
}

async fn disconnect(
    method: &str,
    path: &str,
    body: String,
    channel: Option<String>,
    before_save: bool,
) {
    let dir = tempfile::tempdir().unwrap();
    let env = [
        ("PEARTUBE_STORAGE", dir.path().to_str().unwrap()),
        ("PEARTUBE_API_HOST", "127.0.0.1"),
        ("PEARTUBE_API_PORT", "0"),
        ("PEARTUBE_HTTPS_PORT", "0"),
        ("PEARTUBE_BOOTSTRAP", "127.0.0.1:9"),
        ("PEARTUBE_LAN", "0"),
    ]
    .map(|(k, v)| (k.to_owned(), v.to_owned()))
    .into_iter()
    .collect();
    let relay = Relay::listen(env).await.unwrap();
    let initial = relay.start().await.unwrap();
    let expected = channel
        .as_deref()
        .unwrap_or_else(|| initial["tracker"].as_str().unwrap());
    let addr = relay.addresses()[0];

    // Hold a real read so that close cannot finish before the client disconnects.
    let node = relay.0.api.node().unwrap();
    let core = node
        .store()
        .store()
        .create_core(CreateCore {
            key: [1; 32],
            discovery_key: [2; 32],
            ..Default::default()
        })
        .unwrap();
    let (entered, started) = tokio::sync::oneshot::channel();
    let (release, resume) = mpsc::channel();
    let reader = std::thread::spawn(move || {
        core.missing_tree_nodes(std::iter::once_with(move || {
            entered.send(()).unwrap();
            let _ = resume.recv();
            1
        }))
        .unwrap();
    });
    started.await.unwrap();
    let idle = Arc::strong_count(&relay.0.api);
    let save_gate = if before_save {
        Some(relay.0.settings.lock().await)
    } else {
        None
    };
    let mut client = TcpStream::connect(addr).await.unwrap();
    let request = format!(
        "{method} {path} HTTP/1.1\r\nHost: {addr}\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{body}",
        body.len()
    );
    client.write_all(request.as_bytes()).await.unwrap();
    let settings = if let Some(gate) = save_gate {
        // The mnemonic is durable. Disconnect before the relay can save the corresponding channel.
        let phrase = serde_json::from_str::<Value>(&body).unwrap()["mnemonic"].clone();
        let file = dir.path().join("identity.json");
        wait_for("recovery did not save the mnemonic", || {
            std::fs::read(&file)
                .ok()
                .and_then(|data| serde_json::from_slice::<Value>(&data).ok())
                .is_some_and(|saved| saved["mnemonic"] == phrase)
        })
        .await;
        gate
    } else {
        wait_for("request did not reach node close", || {
            relay.0.api.node().is_none()
        })
        .await;
        // Resume can wait synchronously for this read. Let close finish, but pause the next
        // open at its settings snapshot until HTTP has observed the disconnect.
        relay.0.settings.lock().await
    };
    drop(client);
    release.send(()).unwrap();
    reader.join().unwrap();
    // This is the only connection. Wait until the server has dropped its request, not a fixed delay.
    wait_for("server retained the disconnected request", || {
        Arc::strong_count(&relay.0.api) == idle
    })
    .await;
    drop(settings);

    wait_for("client disconnect cancelled the reopen", || {
        relay
            .0
            .api
            .node()
            .is_some_and(|new| !Arc::ptr_eq(&node, &new))
    })
    .await;
    drop(node);
    // Wait for startup to finish before asking the new node for its identity.
    let open = tokio::time::timeout(Duration::from_secs(10), relay.0.open.lock())
        .await
        .expect("node startup did not finish");
    assert!(open.is_some());
    drop(open);
    let mut client = TcpStream::connect(addr).await.unwrap();
    let get = format!("GET /v1/identity HTTP/1.1\r\nHost: {addr}\r\nConnection: close\r\n\r\n");
    client.write_all(get.as_bytes()).await.unwrap();
    let mut response = String::new();
    tokio::time::timeout(
        Duration::from_secs(10),
        client.read_to_string(&mut response),
    )
    .await
    .expect("new client could not read the identity after disconnect")
    .unwrap();
    let (head, body) = response.split_once("\r\n\r\n").unwrap();
    assert!(head.starts_with("HTTP/1.1 200"), "{response}");
    assert_eq!(
        serde_json::from_str::<Value>(body).unwrap()["channel"],
        expected
    );
    assert_eq!(
        relay.0.settings.lock().await.view()["restartNeeded"],
        json!([])
    );
    relay.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn restart_finishes_after_client_disconnect() {
    check_disconnect("POST", "/v1/restart", String::new(), None, false).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn settings_apply_after_client_disconnect() {
    check_disconnect(
        "PUT",
        "/v1/settings",
        json!({"hiverelayReplicas":4}).to_string(),
        None,
        false,
    )
    .await;
}

async fn identity_disconnect(before_save: bool) {
    let mnemonic = generate_mnemonic(12);
    let channel = derive_identity(&mnemonic).unwrap().channel_key;
    check_disconnect(
        "POST",
        "/v1/identity/recover",
        json!({"mnemonic":mnemonic}).to_string(),
        Some(channel),
        before_save,
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn identity_reopens_after_client_disconnect() {
    identity_disconnect(false).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn identity_saves_the_channel_after_client_disconnect() {
    identity_disconnect(true).await;
}
