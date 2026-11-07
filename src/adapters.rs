//! The adapters a bare `Backend::spawn` wires: the loopback API over the real node, LAN
//! discovery and the cast controller (`worker.js`'s `createApi`, `moveLan`, `ensureCast`).

use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

use serde_json::{Value, json};

use crate::http::{Api, ApiServer, ArtLookup, BlobAddr, Options};
use crate::node::Node;
use crate::node::api::{ApiServer as ApiServerTrait, Source};
use crate::node::core::CoreNode;
use crate::protocol::API_PORT;

/// `state.posterUrl` / `state.backdropUrl`: Metahub's artwork host, addressed by IMDb id.
fn metahub(kind: &str, size: &str) -> ArtLookup {
    let base = format!("https://images.metahub.space/{kind}/{size}/");
    Arc::new(move |imdb: String| {
        let base = base.clone();
        Box::pin(async move { Some(format!("{base}{imdb}/img")) })
    })
}

/// `state.extraStatus`: the fields the API adds over the node's own status.
fn extra_status(url: Arc<Mutex<Option<String>>>) -> Arc<dyn Fn() -> Value + Send + Sync> {
    Arc::new(move || {
        let url = url
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .clone();
        json!({ "apiUrl": url })
    })
}

/// `createApi` + `listenLoopback`: the loopback HTTP service over the node the worker opened.
///
/// The app prefers [`API_PORT`] and falls back to an ephemeral port when it is taken.
pub struct LoopbackApi {
    /// The server this adapter started; `start` replaces it.
    server: Mutex<Option<ApiServer>>,
}

impl Default for LoopbackApi {
    fn default() -> Self {
        Self {
            server: Mutex::new(None),
        }
    }
}

impl ApiServerTrait<CoreNode> for LoopbackApi {
    type Error = std::io::Error;

    async fn start(
        &self,
        node: Arc<CoreNode>,
        on_sources: Arc<dyn Fn(Vec<Source>) + Send + Sync>,
    ) -> Result<Option<String>, Self::Error> {
        let api_url = Arc::new(Mutex::new(None));
        let blob = node.blob_server();
        let options = || Options {
            blob: Some(BlobAddr::new(blob.ip().to_string(), blob.port())),
            poster: Some(metahub("poster", "small")),
            backdrop: Some(metahub("background", "medium")),
            extra_status: Some(extra_status(api_url.clone())),
            on_sources_changed: Some(on_sources.clone()),
            ..Options::default()
        };
        let preferred = SocketAddr::from(([127, 0, 0, 1], API_PORT));
        let server = match Api::with_node(options(), node.clone())
            .bind(preferred)
            .await
        {
            Ok(server) => server,
            Err(_) => {
                let ephemeral = SocketAddr::from(([127, 0, 0, 1], 0));
                Api::with_node(options(), node).bind(ephemeral).await?
            }
        };
        let url = server.url();
        *api_url.lock().unwrap_or_else(|error| error.into_inner()) = Some(url.clone());
        *self
            .server
            .lock()
            .unwrap_or_else(|error| error.into_inner()) = Some(server);
        Ok(Some(url))
    }

    fn notify(&self, event: &str) {
        if let Some(server) = self
            .server
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .as_ref()
        {
            server.notify(event);
        }
    }

    async fn close(&self) {
        let server = self
            .server
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .take();
        if let Some(server) = server {
            server.close().await;
        }
    }
}
