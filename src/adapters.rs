//! The adapters a bare `Backend::spawn` wires: the loopback API over the real node, LAN
//! discovery and the cast controller (`worker.js`'s `createApi`, `moveLan`, `ensureCast`).

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

use futures_util::future::BoxFuture;
use serde_json::{Value, json};

use crate::acquire::{
    AcquireOptions, Acquirer, AddJob, HyperHttp, LocalJobFiles, OsRandom, SourceSpec, UbitPages,
};
use crate::app_settings::{js_string, js_truthy};
use crate::http::{self, Api, ApiServer, ArtLookup, BlobAddr, HttpError, Options};
use crate::identify::{self, IdentifyOptions};
use crate::mirror::{
    ArtFeed, ArtPictures, LocalMirrorFiles, MirrorConfig, MirrorHost, MirrorOptions,
    SoulseekSettings, YoutubeSettings,
};
use crate::node::Node;
use crate::node::api::{ApiServer as ApiServerTrait, Source};
use crate::node::core::{CoreNode, NodeStreams};
use crate::protocol::API_PORT;
use crate::providers::{soulseek, youtube};
use crate::settings::{CINEMETA_URL, Settings};

/// `state.posterUrl` / `state.backdropUrl`: Metahub's artwork host, addressed by IMDb id.
fn metahub(kind: &str, size: &str) -> ArtLookup {
    let base = format!("https://images.metahub.space/{kind}/{size}/");
    Arc::new(move |imdb: String| {
        let base = base.clone();
        Box::pin(async move { Some(format!("{base}{imdb}/img")) })
    })
}

/// `state.extraStatus`: the fields the API adds over the node's own status.
fn extra_status(
    url: Arc<Mutex<Option<String>>>,
    network: Arc<crate::node::network::Network>,
) -> Arc<dyn Fn() -> Value + Send + Sync> {
    Arc::new(move || {
        let url = url
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .clone();
        json!({ "apiUrl": url, "connectedPeers": network.peer_connections() })
    })
}

fn text_setting(value: &Value) -> String {
    value.as_str().map(str::to_owned).unwrap_or_default()
}

struct PeerAcquirer(Acquirer<CoreNode>);

impl http::Acquirer for PeerAcquirer {
    fn add<'a>(&'a self, request: Value) -> BoxFuture<'a, Result<Value, HttpError>> {
        Box::pin(async move {
            let source = &request["source"];
            let job = AddJob {
                id: js_string(&request["id"]),
                title: js_string(&request["title"]),
                source: SourceSpec {
                    url: source["url"].as_str().unwrap_or_default().to_owned(),
                    headers: source["headers"]
                        .as_object()
                        .map(|h| h.iter().map(|(k, v)| (k.clone(), js_string(v))).collect())
                        .unwrap_or_default(),
                    page: js_truthy(&source["page"]),
                    audio: js_truthy(&source["audio"]),
                    private: js_truthy(&source["private"]),
                },
                channel: (!request["channel"].is_null()).then(|| js_truthy(&request["channel"])),
                private: js_truthy(&request["private"]),
                plugin: None,
                existing: None,
                music: request.get("music").filter(|v| v.is_object()).cloned(),
            };
            self.0
                .add(job)
                .map_err(|error| HttpError::new(400, error.to_string()))
        })
    }

    fn list(&self) -> Value {
        Value::Array(self.0.list())
    }

    fn get(&self, id: &str) -> Value {
        self.0.get(id).unwrap_or(Value::Null)
    }

    fn cancel(&self, id: &str) -> Value {
        self.0.cancel(id).unwrap_or(Value::Null)
    }
}

struct PeerMirror(Arc<MirrorHost<CoreNode>>);

fn mirror_err(error: crate::mirror::MirrorError) -> HttpError {
    HttpError::new(error.status.unwrap_or(500), error.message)
}

impl http::Mirror for PeerMirror {
    fn list(&self) -> Value {
        Value::Array(self.0.list())
    }

    fn run<'a>(
        &'a self,
        plugin: &'a str,
        payload: Value,
    ) -> BoxFuture<'a, Result<Value, HttpError>> {
        Box::pin(async move { self.0.run(plugin, payload).await.map_err(mirror_err) })
    }

    fn source_search(self: Arc<Self>) -> Result<Arc<dyn http::SourceSearch>, HttpError> {
        Ok(self.0.clone())
    }

    fn provider<'a>(
        &'a self,
        call: http::ProviderCall,
        name: &'a str,
        payload: Value,
    ) -> BoxFuture<'a, Result<Value, HttpError>> {
        Box::pin(async move {
            match call {
                http::ProviderCall::Search => self.0.search_provider(name, payload).await,
                http::ProviderCall::Browse => self.0.browse_provider(name, payload).await,
                http::ProviderCall::Mirror => self.0.mirror_provider(name, payload).await,
            }
            .map_err(mirror_err)
        })
    }

    fn provider_jobs(&self) -> Vec<Value> {
        self.0.list_provider_jobs()
    }

    fn provider_job(&self, id: &str) -> Option<Value> {
        self.0.get_provider_job(id)
    }

    fn cancel_provider_job(&self, id: &str) -> Option<Value> {
        self.0.cancel_provider_job(id)
    }

    fn retry_provider_job(&self, id: &str) -> Option<Value> {
        self.0.retry_provider_job(id)
    }

    fn wake_provider_jobs(&self) -> usize {
        self.0.wake_provider_jobs()
    }

    fn forget_imported<'a>(
        &'a self,
        gone: Vec<(String, String)>,
    ) -> BoxFuture<'a, Vec<(String, &'static str)>> {
        Box::pin(async move { self.0.forget_imported(&gone).await })
    }

    fn provider_thumb<'a>(
        &'a self,
        name: &'a str,
        id: &'a str,
    ) -> BoxFuture<'a, Result<Option<youtube::Picture>, HttpError>> {
        Box::pin(async move { Ok(self.0.provider_thumb(name, id).await) })
    }
}

struct RunningApi {
    server: Arc<Mutex<Option<ApiServer>>>,
    acquirer: Acquirer<CoreNode>,
    mirror: Arc<MirrorHost<CoreNode>>,
}

/// `createApi` + `listenLoopback`: the loopback HTTP service over the node the worker opened.
///
/// Every device runs the full peer HTTP API locally — `ubit` link identification and acquisition,
/// direct Soulseek, YouTube, SoundCloud, and Rumble source providers, staging import, and private
/// peer settings (`<storage>/settings.json`).
///
/// The app prefers [`API_PORT`] and falls back to an ephemeral port when it is taken; `0` asks for
/// an ephemeral port up front.
pub struct LoopbackApi {
    /// The first port `start` tries; `0` skips straight to an ephemeral one.
    preferred: u16,
    /// The server, acquirer, and mirror host this adapter started; `start` replaces them.
    running: Mutex<Option<RunningApi>>,
}

impl Default for LoopbackApi {
    fn default() -> Self {
        Self::new(API_PORT)
    }
}

impl LoopbackApi {
    pub fn new(preferred: u16) -> Self {
        Self {
            preferred,
            running: Mutex::new(None),
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
        let storage = node.storage_dir().to_path_buf();
        let ubit_config = storage.join("ubit").display().to_string();
        let settings = Arc::new(tokio::sync::Mutex::new(Settings::new(
            &storage,
            HashMap::new(),
        )));
        let (slsk_user, slsk_pass, slsk_server, slsk_share, slsk_share_max) = {
            let guard = settings.lock().await;
            (
                text_setting(&guard.get("slskUser")),
                text_setting(&guard.get("slskPass")),
                text_setting(&guard.get("slskServer")),
                js_truthy(&guard.get("slskShare")),
                guard
                    .get("slskShareMaxFiles")
                    .as_u64()
                    .and_then(|n| u32::try_from(n).ok())
                    .unwrap_or(soulseek::DEFAULT_SHARE_MAX_FILES),
            )
        };

        let server_slot: Arc<Mutex<Option<ApiServer>>> = Arc::new(Mutex::new(None));
        let notify: Arc<dyn Fn() + Send + Sync> = {
            let slot = server_slot.clone();
            Arc::new(move || {
                if let Some(server) = slot.lock().unwrap_or_else(|e| e.into_inner()).as_ref() {
                    server.notify("update");
                }
            })
        };

        let acquirer = Acquirer::new(
            node.clone(),
            Arc::new(LocalJobFiles::new(storage.join("jobs.json"))),
            Arc::new(UbitPages {
                ubit: String::new(),
            }),
            Arc::new(HyperHttp::default()),
            Arc::new(NodeStreams),
            Arc::new(OsRandom),
            AcquireOptions {
                concurrency: 2,
                default_channel: true,
                ubit_config_dir: Some(ubit_config.clone()),
                on_done: Some({
                    let notify = notify.clone();
                    Arc::new(move |_, _| notify())
                }),
                ..AcquireOptions::default()
            },
        );

        let mirror = Arc::new(MirrorHost::open(
            node.clone(),
            Some(acquirer.clone()),
            Arc::new(LocalMirrorFiles),
            Arc::new(NodeStreams),
            Arc::new(NodeStreams),
            Arc::new(ArtFeed::default()),
            Vec::new(),
            MirrorOptions {
                storage: storage.display().to_string(),
                mirror_channel: false,
                rechunk: false,
                watch_dir: None,
                feeds: Vec::new(),
                soulseek: SoulseekSettings {
                    user: slsk_user,
                    pass: slsk_pass,
                    server: slsk_server,
                    listen_port: 0,
                    share: slsk_share,
                    share_max_files: slsk_share_max,
                    durable: true,
                    backend: soulseek::client::network(),
                    ..SoulseekSettings::default()
                },
                youtube: YoutubeSettings {
                    backend: Some(Arc::new(youtube::Ubit {
                        config_dir: ubit_config.clone(),
                        ubit: String::new(),
                    })),
                    fetch: Arc::new(ArtPictures::default()),
                    ..YoutubeSettings::default()
                },
                poll_interval_ms: 0,
                onchange: Some(notify.clone()),
                ..MirrorOptions::default()
            },
        ));
        tokio::spawn({
            let mirror = mirror.clone();
            async move {
                mirror.startup().await;
            }
        });

        let api_url = Arc::new(Mutex::new(None));
        let blob = node.blob_server();
        let network = node.net();
        let identify_opts = IdentifyOptions {
            cinemeta_url: CINEMETA_URL.to_owned(),
            config_dir: ubit_config,
            ubit: String::new(),
            allow_private: false,
        };
        let discover = crate::discover::DiscoverHost::open(
            storage,
            false,
            {
                let node = node.clone();
                Arc::new(move || Some(node.clone()))
            },
            {
                let mirror = mirror.clone();
                Arc::new(move || Some(mirror.clone()))
            },
        );
        let options = Options {
            blob: Some(BlobAddr::new(blob.ip().to_string(), blob.port())),
            poster: Some(metahub("poster", "small")),
            backdrop: Some(metahub("background", "medium")),
            extra_status: Some(extra_status(api_url.clone(), network)),
            on_sources_changed: Some(on_sources),
            settings: Some(settings.clone()),
            apply_settings: Some({
                let (settings, mirror) = (settings.clone(), mirror.clone());
                Arc::new(move |changed: Vec<String>| {
                    let (settings, mirror) = (settings.clone(), mirror.clone());
                    Box::pin(async move {
                        let has_slsk = changed.iter().any(|key| {
                            matches!(key.as_str(), "slskUser" | "slskPass" | "slskServer")
                        });
                        if has_slsk {
                            let guard = settings.lock().await;
                            mirror.configure(MirrorConfig {
                                slsk_user: Some(text_setting(&guard.get("slskUser"))),
                                slsk_pass: Some(text_setting(&guard.get("slskPass"))),
                                slsk_server: Some(text_setting(&guard.get("slskServer"))),
                                ..MirrorConfig::default()
                            });
                        }
                        Ok(())
                    }) as BoxFuture<'static, Result<(), String>>
                })
            }),
            identify: Some(Arc::new(move |link: String| {
                let opts = identify_opts.clone();
                Box::pin(async move {
                    identify::identify(&link, &opts)
                        .await
                        .map(|found| serde_json::to_value(found).unwrap_or(Value::Null))
                        .map_err(|error| {
                            HttpError::new(error.status().unwrap_or(500), error.to_string())
                        })
                }) as BoxFuture<'static, Result<Value, HttpError>>
            })),
            acquirer: Some(Arc::new(PeerAcquirer(acquirer.clone()))),
            mirror: Some(Arc::new(PeerMirror(mirror.clone()))),
            discover: Some(discover),
            ..Options::default()
        };

        let api = Arc::new(Api::with_node(options, node));
        let ephemeral = SocketAddr::from(([127, 0, 0, 1], 0));
        let server = if self.preferred == 0 {
            api.serve(ephemeral).await?
        } else {
            let preferred = SocketAddr::from(([127, 0, 0, 1], self.preferred));
            match api.clone().serve(preferred).await {
                Ok(server) => server,
                Err(_) => api.serve(ephemeral).await?,
            }
        };
        let url = server.url();
        *api_url.lock().unwrap_or_else(|error| error.into_inner()) = Some(url.clone());
        *server_slot
            .lock()
            .unwrap_or_else(|error| error.into_inner()) = Some(server);
        *self
            .running
            .lock()
            .unwrap_or_else(|error| error.into_inner()) = Some(RunningApi {
            server: server_slot,
            acquirer,
            mirror,
        });
        Ok(Some(url))
    }

    fn notify(&self, event: &str) {
        if let Some(running) = self
            .running
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .as_ref()
            && let Some(server) = running
                .server
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .as_ref()
        {
            server.notify(event);
        }
    }

    fn resume(&self) {
        if let Some(running) = self
            .running
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .as_ref()
        {
            running.mirror.wake_provider_jobs();
        }
    }

    async fn close(&self) {
        let running = self
            .running
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .take();
        if let Some(running) = running {
            let server = running
                .server
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .take();
            if let Some(server) = server {
                server.close().await;
            }
            running.mirror.close().await;
            running.acquirer.close();
        }
    }
}
