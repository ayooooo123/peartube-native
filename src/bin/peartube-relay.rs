//! `peartube-relay`: the headless relay (the JS image's `bin/relay.js`), configured by the
//! `PEARTUBE_*` environment and `<storage>/settings.json`.

use std::collections::HashMap;
use std::time::Duration;

use peartube_native::relay::{Relay, guard_mode, guard_only, log};
use serde_json::json;

#[tokio::main]
async fn main() {
    let env: HashMap<String, String> = std::env::vars().collect();
    // `PEARTUBE_PRIVATE_RELAY`: a standalone Corestore-free guard forwarder, never a content relay.
    if guard_mode(&env) {
        if let Err(error) = guard_only(env).await {
            log("private guard failed to start", json!({ "error": error }));
            std::process::exit(1);
        }
        return;
    }
    let relay = match Relay::listen(env).await {
        Ok(relay) => relay,
        Err(error) => {
            log("relay failed to start", json!({ "error": error }));
            std::process::exit(1);
        }
    };
    let mut status = match relay.start().await {
        Ok(status) => status,
        Err(error) => {
            log("relay failed to open its node", json!({ "error": error }));
            relay.shutdown().await;
            std::process::exit(1);
        }
    };
    if let Some(status) = status.as_object_mut() {
        status.insert(
            "api".into(),
            json!(port_env(
                "PEARTUBE_API_PORT",
                peartube_native::protocol::API_PORT
            )),
        );
    }
    log("relay ready", status);

    let heartbeat = {
        let relay = relay.clone();
        tokio::spawn(async move {
            let mut every = tokio::time::interval(Duration::from_secs(60));
            every.tick().await;
            loop {
                every.tick().await;
                if let Some(status) = relay.status().await {
                    log("heartbeat", status);
                }
            }
        })
    };

    wait_for_signal().await;
    heartbeat.abort();
    // A stuck close must not keep the container from stopping (JS: a 3 s kill timer).
    if tokio::time::timeout(Duration::from_secs(3), relay.shutdown())
        .await
        .is_err()
    {
        log("relay shutdown timed out", json!({}));
    }
    std::process::exit(0);
}

fn port_env(var: &str, default: u16) -> u16 {
    std::env::var(var)
        .ok()
        .and_then(|v| v.trim().parse().ok())
        .unwrap_or(default)
}

async fn wait_for_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};
        let (Ok(mut term), Ok(mut int)) = (
            signal(SignalKind::terminate()),
            signal(SignalKind::interrupt()),
        ) else {
            let _ = tokio::signal::ctrl_c().await;
            return;
        };
        tokio::select! {
            _ = term.recv() => {}
            _ = int.recv() => {}
        }
    }
    #[cfg(not(unix))]
    let _ = tokio::signal::ctrl_c().await;
}
