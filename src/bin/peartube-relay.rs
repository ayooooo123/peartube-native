//! `peartube-relay`: the headless relay (the JS image's `bin/relay.js`), configured by the
//! `PEARTUBE_*` environment and `<storage>/settings.json`.

use std::collections::HashMap;

use peartube_native::relay::{guard_mode, guard_only, log, run, signals};
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
    // `signals()` installs its handlers here, before the relay listens or opens its node.
    std::process::exit(run(env, signals()).await);
}
