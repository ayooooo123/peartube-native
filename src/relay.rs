//! Port of the relay process, `bin/relay.js`: the node, its acquirer and mirror plugins and the HTTP
//! API (with the relay UI) on `0.0.0.0`, opened from `settings.json` and the environment.
//!
//! The API stays up for the whole process. The node, acquirer and plugins open from the current
//! settings and reopen in this process when a setting that shapes the node changes; live settings
//! apply in place. Opens, reopens and live changes run one at a time.

use std::collections::HashMap;
use std::net::{Ipv4Addr, SocketAddr};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use futures_util::future::BoxFuture;
use serde_json::{Value, json};

use crate::acquire::{
    AcquireOptions, Acquirer, AddJob, HyperHttp, LocalJobFiles, OsRandom, SourceSpec, UbitPages,
};
use crate::app_settings::{js_string, js_truthy};
use crate::cast::adapters::NodePeer;
use crate::cast::traits::CastPeer;
use crate::cast::{CastController, ControllerOptions};
use crate::http::{self, Api, ApiServer, BlobAddr, HttpError, Options};
use crate::mirror::{ArtFeed, LocalMirrorFiles, MirrorHost, MirrorOptions};
use crate::node::Node;
use crate::node::core::{CoreNode, NodeStreams, create_node};
use crate::private_overlay::dht::DhtOptions;
use crate::private_overlay::peeroxide::open_dht;
use crate::private_overlay::{GuardOptions, PrivateGuard, create_private_guard, to_hex};
use crate::protocol::API_PORT;
use crate::replicate::{ReplicateConfig, Replicator};
use crate::settings::Settings;
use crate::traffic::TrafficGate;

type Shared<T> = Arc<Mutex<Option<T>>>;

/// What one open of the node owns; a reopen closes it and opens a new one.
struct Open {
    node: Arc<CoreNode>,
    acquirer: Acquirer<CoreNode>,
    mirror: Arc<MirrorHost<CoreNode>>,
    changes: tokio::task::JoinHandle<()>,
    replication: Replication,
}

/// `p2p-replicate` and the traffic gate whose `peersBusy` it waits on.
struct Replication {
    replicator: Replicator,
    traffic: TrafficGate,
    detach_traffic: Box<dyn Fn() + Send + Sync>,
    /// `scheduleReplicate`: catalog changes ask for a pass, 2 s later and one at a time.
    kick: Arc<tokio::sync::Notify>,
    kicker: tokio::task::JoinHandle<()>,
}

struct Inner {
    env: HashMap<String, String>,
    storage: PathBuf,
    settings: Arc<tokio::sync::Mutex<Settings>>,
    api: Arc<Api<CoreNode>>,
    servers: Mutex<Vec<ApiServer>>,
    /// The serial lifecycle: `Some` while a node is open.
    open: tokio::sync::Mutex<Option<Open>>,
    acquirer: Shared<Acquirer<CoreNode>>,
    mirror: Shared<Arc<MirrorHost<CoreNode>>>,
    /// `state.cast`: the cast stack, open with the node (`null` between opens).
    cast: Shared<Arc<CastController>>,
    /// `lanHost()` as the cast stack reads it: the setting, else the detected LAN address.
    cast_host: Arc<Mutex<Option<String>>>,
    /// `state.extraStatus`'s LAN endpoint and private-routing flag, refreshed on each open.
    extra: Arc<Mutex<Value>>,
    /// `privateGuard`: the forwarder for other peers' private routing, with the settings it runs.
    guard: Arc<Mutex<Option<(String, PrivateGuard)>>>,
    shutting_down: AtomicBool,
}

/// The relay process. Cloning shares it.
#[derive(Clone)]
pub struct Relay(Arc<Inner>);

impl Relay {
    /// Create the storage folder, read the settings and start the API; [`Relay::start`] opens the node.
    pub async fn listen(env: HashMap<String, String>) -> Result<Self, String> {
        if guard_mode(&env) {
            return Err(
                "PEARTUBE_PRIVATE_RELAY runs a standalone guard forwarder, not a content relay; call relay::guard_only"
                    .into(),
            );
        }
        let storage = PathBuf::from(
            env.get("PEARTUBE_STORAGE")
                .map_or("./peartube-data", String::as_str),
        );
        create_private_dir(&storage).map_err(|e| format!("storage {}: {e}", storage.display()))?;
        let settings = Settings::new(&storage, env.clone());
        let cinemeta = text(&settings.get("cinemetaUrl"));
        let channel = text(&settings.get("channel"));
        let settings = Arc::new(tokio::sync::Mutex::new(settings));
        let ubit_config = env
            .get("UBIT_CONFIG_DIR")
            .cloned()
            .unwrap_or_else(|| storage.join("ubit").display().to_string());
        let tls_ca = env
            .get("PEARTUBE_TLS_CA")
            .map(PathBuf::from)
            .unwrap_or_else(|| storage.join("ca.crt"));
        let tls_ca = std::fs::read(tls_ca).ok();
        let mut fallback_status =
            serde_json::to_value(crate::app::idle_status()).unwrap_or_else(|_| json!({}));
        fallback_status["tracker"] = json!(channel);
        let tls = read_tls(&env, &storage);
        let extra = Arc::new(Mutex::new(json!({ "lan": null, "privateRouting": false })));
        let guard: Arc<Mutex<Option<(String, PrivateGuard)>>> = Arc::new(Mutex::new(None));
        let cast_host: Arc<Mutex<Option<String>>> = Arc::new(Mutex::new(None));

        let relay = Arc::new_cyclic(|weak: &std::sync::Weak<Inner>| {
            let weak = weak.clone();
            let jobs_file = storage.join("jobs.json");
            let options = Options {
                api_token: env
                    .get("PEARTUBE_API_TOKEN")
                    .cloned()
                    .filter(|t| !t.is_empty()),
                fallback_status: Some(fallback_status),
                extra_status: Some({
                    let (extra, guard) = (extra.clone(), guard.clone());
                    Arc::new(move || {
                        let mut status = extra.lock().unwrap_or_else(|e| e.into_inner()).clone();
                        status["privateGuard"] = guard
                            .lock()
                            .unwrap_or_else(|e| e.into_inner())
                            .as_ref()
                            .map_or(Value::Null, |(_, guard)| guard_status(guard));
                        status
                    })
                }),
                fallback_jobs: Some(Arc::new(move || {
                    let jobs: Vec<Value> = std::fs::read_to_string(&jobs_file)
                        .ok()
                        .and_then(|text| serde_json::from_str::<Vec<Value>>(&text).ok())
                        .unwrap_or_default();
                    Value::Array(jobs.iter().filter_map(crate::acquire::public_job).collect())
                })),
                settings: Some(settings.clone()),
                apply_settings: Some({
                    let weak = weak.clone();
                    Arc::new(move |changed: Vec<String>| {
                        let weak = weak.clone();
                        Box::pin(async move {
                            match weak.upgrade() {
                                Some(inner) => Relay(inner).apply_settings(changed).await,
                                None => Ok(()),
                            }
                        }) as BoxFuture<'static, _>
                    })
                }),
                restart: Some({
                    let weak = weak.clone();
                    Arc::new(move || {
                        let weak = weak.clone();
                        Box::pin(async move {
                            if let Some(inner) = weak.upgrade() {
                                let relay = Relay(inner);
                                if let Err(error) = relay.reopen().await {
                                    log("restart failed", json!({ "error": error }));
                                }
                            }
                        }) as BoxFuture<'static, ()>
                    })
                }),
                recover_mnemonic: Some({
                    let weak = weak.clone();
                    Arc::new(move |mnemonic: String, alias: String| {
                        let weak = weak.clone();
                        Box::pin(async move {
                            let inner = weak
                                .upgrade()
                                .ok_or_else(|| HttpError::new(503, "The relay is shutting down"))?;
                            Relay(inner).recover_mnemonic(mnemonic, alias).await
                        }) as BoxFuture<'static, _>
                    })
                }),
                identify: Some({
                    let options = crate::identify::IdentifyOptions {
                        cinemeta_url: cinemeta.clone(),
                        config_dir: ubit_config.clone(),
                        ubit: ubit_bin(&env),
                    };
                    Arc::new(move |link: String| {
                        let options = options.clone();
                        Box::pin(async move {
                            crate::identify::identify(&link, &options)
                                .await
                                .map(|found| serde_json::to_value(found).unwrap_or(Value::Null))
                                .map_err(|e| {
                                    HttpError::new(e.status().unwrap_or(500), e.to_string())
                                })
                        }) as BoxFuture<'static, _>
                    })
                }),
                on_sources_changed: Some({
                    let weak = weak.clone();
                    Arc::new(move |list| {
                        if let Some(inner) = weak.upgrade() {
                            Relay(inner).save_sources(list);
                        }
                    })
                }),
                poster: Some(http::cinemeta_lookup(&cinemeta, http::Art::Poster)),
                backdrop: Some(http::cinemeta_lookup(&cinemeta, http::Art::Backdrop)),
                // Set when the node opens: its blob server binds loopback on a fresh port.
                blob: None,
                tls_ca: tls_ca.map(Into::into),
                web_dir: Some(web_dir(&env)),
                acquirer: Some(Arc::new(AcquirerBridge(weak.clone()))),
                mirror: Some(Arc::new(MirrorBridge(weak.clone()))),
                cast: None,
            };
            Inner {
                env: env.clone(),
                storage: storage.clone(),
                settings: settings.clone(),
                api: Arc::new(Api::new(options)),
                servers: Mutex::new(Vec::new()),
                open: tokio::sync::Mutex::new(None),
                acquirer: Arc::new(Mutex::new(None)),
                mirror: Arc::new(Mutex::new(None)),
                cast: Arc::new(Mutex::new(None)),
                cast_host: cast_host.clone(),
                extra: extra.clone(),
                guard: guard.clone(),
                shutting_down: AtomicBool::new(false),
            }
        });
        let relay = Relay(relay);

        let host: Ipv4Addr = env
            .get("PEARTUBE_API_HOST")
            .map_or("0.0.0.0", String::as_str)
            .parse()
            .map_err(|_| "PEARTUBE_API_HOST must be an IPv4 address".to_owned())?;
        let api_port = port(&env, "PEARTUBE_API_PORT", API_PORT);
        let server = relay
            .0
            .api
            .clone()
            .serve(SocketAddr::from((host, api_port)), None)
            .await
            .map_err(|e| format!("API port {api_port}: {e}"))?;
        relay.0.servers.lock().unwrap().push(server);
        let https_port = port(&env, "PEARTUBE_HTTPS_PORT", 8443);
        if let (Some(acceptor), true) = (tls, https_port > 0) {
            let server = relay
                .0
                .api
                .clone()
                .serve(SocketAddr::from((host, https_port)), Some(acceptor))
                .await
                .map_err(|e| format!("HTTPS port {https_port}: {e}"))?;
            relay.0.servers.lock().unwrap().push(server);
        }
        Ok(relay)
    }

    /// The first open; the process logs `relay ready` once it returns.
    pub async fn start(&self) -> Result<Value, String> {
        let mut open = self.0.open.lock().await;
        self.open_locked(&mut open).await?;
        if let Err(error) = self.apply_guard().await {
            log("private guard failed to start", json!({ "error": error }));
        }
        let status = match open.as_ref() {
            Some(open) => serde_json::to_value(open.node.status().await).unwrap_or(Value::Null),
            None => Value::Null,
        };
        Ok(status)
    }

    /// Where the API listens: HTTP first, then HTTPS when a certificate is configured.
    pub fn addresses(&self) -> Vec<SocketAddr> {
        self.0
            .servers
            .lock()
            .unwrap()
            .iter()
            .map(ApiServer::addr)
            .collect()
    }

    /// The status the heartbeat logs, `None` between opens.
    pub async fn status(&self) -> Option<Value> {
        let node = self.0.api.node()?;
        Some(serde_json::to_value(node.status().await).unwrap_or(Value::Null))
    }

    /// SIGTERM: stop the API, close what is open and return.
    pub async fn shutdown(&self) {
        if self.0.shutting_down.swap(true, Ordering::SeqCst) {
            return;
        }
        let servers: Vec<ApiServer> = std::mem::take(&mut *self.0.servers.lock().unwrap());
        for server in servers {
            server.close().await;
        }
        let mut open = self.0.open.lock().await;
        self.close_locked(&mut open).await;
        self.stop_guard().await;
    }

    /// `applyGuard()`: the guard runs on its own HyperDHT and port with a fresh key each
    /// start, and opens no Corestore. JS runs it as a child process; here it is in-process.
    async fn apply_guard(&self) -> Result<(), String> {
        let port = self.cfg("privateGuardPort").await.as_u64().unwrap_or(0) as u16;
        let bootstrap = strings(&self.cfg("bootstrap").await);
        let want = js_truthy(&self.cfg("privateGuard").await)
            .then(|| json!([port, bootstrap]).to_string());
        let running = self
            .0
            .guard
            .lock()
            .unwrap()
            .as_ref()
            .map(|(config, _)| config.clone());
        if want == running {
            return Ok(());
        }
        self.stop_guard().await;
        let Some(want) = want else { return Ok(()) };
        if port != 0 && self.cfg("dhtPort").await.as_u64() == Some(u64::from(port)) {
            return Err("privateGuardPort: use a different port from the peer port".into());
        }
        let host = self.0.env.get("PEARTUBE_PRIVATE_HOST").cloned();
        // No `bootstrap` setting means the public DHT, as for the node.
        let bootstrap = crate::node::network::resolve_bootstrap(bootstrap);
        let dht = open_dht(DhtOptions {
            host: host.clone(),
            port,
            bootstrap: bootstrap.clone(),
        })
        .await
        .map_err(|e| format!("privateGuard: {e}"))?;
        let guard = create_private_guard(
            dht,
            GuardOptions {
                host,
                port,
                bootstrap,
                ..GuardOptions::default()
            },
        )
        .await
        .map_err(|e| format!("privateGuard: {e}"))?;
        log("private-guard ready", guard_status(&guard));
        *self.0.guard.lock().unwrap() = Some((want, guard));
        self.notify("update");
        Ok(())
    }

    async fn stop_guard(&self) {
        let running = self.0.guard.lock().unwrap().take();
        if let Some((_, guard)) = running {
            let _ = guard.close().await;
        }
    }

    fn notify(&self, event: &str) {
        for server in self.0.servers.lock().unwrap().iter() {
            server.notify(event);
        }
    }

    async fn cfg(&self, key: &str) -> Value {
        self.0.settings.lock().await.get(key)
    }

    async fn open_locked(&self, open: &mut Option<Open>) -> Result<(), String> {
        if self.0.shutting_down.load(Ordering::SeqCst) {
            return Err("The relay is shutting down".into());
        }
        let snapshot = self.0.settings.lock().await.snapshot();
        // The snapshot holds each value JSON-encoded, the form `markApplied` compares.
        let cfg = |key: &str| {
            snapshot
                .get(key)
                .and_then(Value::as_str)
                .and_then(|encoded| serde_json::from_str(encoded).ok())
                .unwrap_or(Value::Null)
        };
        let list = |key: &str| match cfg(key) {
            Value::Array(items) if !items.is_empty() => Value::Array(items),
            _ => Value::Null,
        };
        let mut options = json!({
            "storage": self.0.storage.display().to_string(),
            "tracker": non_empty(text(&cfg("channel"))),
            "sources": self.saved_sources(),
            "allowedPeers": list("allowedPeers"),
            // The blob server stays on loopback; clients stream through /v1/blob on the API port.
            "privateRouting": js_truthy(&cfg("privateRouting")),
            "relayThrough": list("relayThrough"),
            // A relay's job: announce the complete copies it caches, not only what it published.
            "announceCache": true,
        });
        if let Value::Array(bootstrap) = list("bootstrap") {
            options["bootstrap"] = Value::Array(bootstrap);
        }
        if let Some(port) = cfg("dhtPort").as_u64().filter(|port| *port > 0) {
            options["dhtPort"] = json!(port);
        }
        let node = create_node(options).await.map_err(|e| e.to_string())?;
        self.0.settings.lock().await.mark_applied(&snapshot);
        let blob = *node.blob_server();
        self.0
            .api
            .set_blob(Some(BlobAddr::new(blob.ip().to_string(), blob.port())));

        if js_truthy(&cfg("lan"))
            && let Some(host) = self.lan_host(&cfg("lanHost"))
        {
            match crate::lan::LanDiscovery::open(host, node.net().key_pair().clone()).await {
                Ok(lan) => {
                    if let Err(error) = node.net().set_lan(Some(lan)).await {
                        log(
                            "LAN discovery failed",
                            json!({ "error": error.to_string() }),
                        );
                    }
                }
                Err(error) => log(
                    "LAN discovery failed",
                    json!({ "error": error.to_string() }),
                ),
            }
        }

        // A relay started without a channel founds its own, keyed by its writer; saved sources
        // would otherwise replace it as the primary catalog on the next start.
        let status = node.status().await;
        if text(&cfg("channel")).is_empty() {
            let mut settings = self.0.settings.lock().await;
            if status.tracker == status.writer {
                let _ = settings.remember("channel", &json!(status.writer));
            } else {
                let _ = settings.update(&json!({ "channel": status.writer }));
            }
        }
        *self.0.extra.lock().unwrap() = json!({
            "lan": lan_status(&node).await,
            "privateRouting": node.private_routing(),
        });
        self.0.api.set_node(node.clone());
        let sources = node.list_sources().await;
        if let Err(error) = self.sync_directory(&node, &sources).await {
            log("directory sync failed", json!({ "error": error }));
        }

        // `createCastController({ storage, getNode, getLanHost, onchange })`: the sender side of a
        // cast. It opens with the node and browses the LAN for receivers.
        let lan_host = self.lan_host(&cfg("lanHost")).map(|host| host.to_string());
        *self.0.cast_host.lock().unwrap() = lan_host.clone();
        let cast = {
            let cast = Arc::downgrade(&self.0);
            let host = Arc::downgrade(&self.0);
            let change = Arc::downgrade(&self.0);
            CastController::start(ControllerOptions {
                storage: self.0.storage.clone(),
                get_node: Arc::new(move || {
                    let node = cast.upgrade()?.api.node()?;
                    Some(Arc::new(NodePeer::new(node)) as Arc<dyn CastPeer>)
                }),
                get_lan_host: Arc::new(move || host.upgrade()?.cast_host.lock().unwrap().clone()),
                mdns: true,
                on_change: Some(Arc::new(move || {
                    if let Some(inner) = change.upgrade() {
                        Relay(inner).notify("cast");
                    }
                })),
            })
            .await
            .map_err(|error| error.message)?
        };
        cast.set_lan_host(lan_host).await;
        let cast = Arc::new(cast);
        *self.0.cast.lock().unwrap() = Some(cast.clone());
        self.0.api.set_cast(Some(Arc::new(CastRoutes(cast))));

        let relay = self.clone();
        let acquirer = Acquirer::new(
            node.clone(),
            Arc::new(LocalJobFiles::new(self.0.storage.join("jobs.json"))),
            Arc::new(UbitPages {
                ubit: ubit_bin(&self.0.env),
            }),
            Arc::new(HyperHttp),
            Arc::new(NodeStreams),
            Arc::new(OsRandom),
            AcquireOptions {
                concurrency: cfg("acquireConcurrency").as_u64().unwrap_or(4).max(1) as usize,
                default_channel: js_truthy(&cfg("publishChannel")),
                ubit_config_dir: Some(self.ubit_config()),
                on_done: Some(Arc::new(move |_, _| relay.notify("update"))),
                ..AcquireOptions::default()
            },
        );
        *self.0.acquirer.lock().unwrap() = Some(acquirer.clone());

        let notify = {
            let relay = self.clone();
            Arc::new(move || relay.notify("update")) as Arc<dyn Fn() + Send + Sync>
        };
        // `hiverelayOpt()`: the HiveRelay seeder's relays, key and discovery, and whether it runs.
        let hiverelay = crate::hiverelay::seeder::HiverelayConfig {
            relays: strings(&cfg("hiverelays")),
            api_key: non_empty(text(&cfg("hiverelayApiKey"))),
            discover: js_truthy(&cfg("hiverelayDiscover"))
                || matches!(
                    self.0.env.get("PEARTUBE_HIVERELAY").map(String::as_str),
                    Some("1" | "true")
                ),
            replicas: cfg("hiverelayReplicas").as_u64().unwrap_or(3).clamp(1, 32),
            ttl_days: 30,
            include_cached: js_truthy(&cfg("replicate")),
        };
        let hiverelay_enabled = !hiverelay.relays.is_empty() || hiverelay.discover;
        // One gate for the relay's bulk work: `p2p-replicate` and the HiveRelay seeder.
        let (traffic, detach_traffic) =
            node.traffic_gate(cfg("hiverelayUploadMbps").as_f64().unwrap_or(0.0).max(0.0));
        let seeder = crate::hiverelay::seeder::plugin(
            node.clone(),
            traffic.clone(),
            hiverelay,
            Some(notify.clone()),
        );
        let mirror = Arc::new(
            MirrorHost::start(
                node.clone(),
                Some(acquirer.clone()),
                Arc::new(LocalMirrorFiles),
                Arc::new(NodeStreams),
                Arc::new(NodeStreams),
                Arc::new(ArtFeed),
                vec![seeder],
                MirrorOptions {
                    storage: self.0.storage.display().to_string(),
                    mirror_channel: js_truthy(&cfg("publishChannel")),
                    rechunk: js_truthy(&cfg("rechunk")),
                    watch_dir: non_empty(text(&cfg("watchDir"))),
                    feeds: strings(&cfg("feeds")),
                    plugins_dir: None,
                    start_runs: if hiverelay_enabled {
                        vec!["hiverelay-seed".to_owned()]
                    } else {
                        Vec::new()
                    },
                    onchange: Some(notify.clone()),
                    ..MirrorOptions::default()
                },
            )
            .await,
        );
        *self.0.mirror.lock().unwrap() = Some(mirror.clone());
        let replication = self
            .start_replication(
                &node,
                &mirror,
                notify.clone(),
                traffic,
                detach_traffic,
                hiverelay_enabled,
            )
            .await;

        let mut changes = node.onchange();
        let kick = replication.kick.clone();
        let changes = tokio::spawn(async move {
            while changes.changed().await.is_ok() {
                notify();
                kick.notify_one();
            }
        });
        *open = Some(Open {
            node,
            acquirer,
            mirror,
            changes,
            replication,
        });
        self.notify("update");
        Ok(())
    }

    #[allow(clippy::too_many_arguments)] // the node, its plugins, and the one gate they share
    async fn start_replication(
        &self,
        node: &Arc<CoreNode>,
        mirror: &Arc<MirrorHost<CoreNode>>,
        onchange: Arc<dyn Fn() + Send + Sync>,
        traffic: TrafficGate,
        detach_traffic: Box<dyn Fn() + Send + Sync>,
        hiverelay: bool,
    ) -> Replication {
        let replicator = Replicator::new(
            node.clone(),
            self.0.storage.clone(),
            traffic.clone(),
            self.replicate_config().await,
            Some(onchange),
        );
        mirror.register(Arc::new(replicator.clone()));
        let kick = Arc::new(tokio::sync::Notify::new());
        let kicker = {
            let (mirror, kick, replicator) = (mirror.clone(), kick.clone(), replicator.clone());
            tokio::spawn(async move {
                loop {
                    kick.notified().await;
                    tokio::time::sleep(std::time::Duration::from_secs(2)).await;
                    if replicator.enabled() {
                        let _ = mirror.run("p2p-replicate", json!({})).await;
                    }
                    // `scheduleReplicate`: catalog changes re-run the seeder too.
                    if hiverelay {
                        let _ = mirror.run("hiverelay-seed", json!({})).await;
                    }
                }
            })
        };
        kick.notify_one();
        Replication {
            replicator,
            traffic,
            detach_traffic,
            kick,
            kicker,
        }
    }

    /// `mirrorConfig()`'s `replicate*` part.
    async fn replicate_config(&self) -> ReplicateConfig {
        ReplicateConfig::new(
            js_truthy(&self.cfg("replicate").await),
            &strings(&self.cfg("replicateSources").await),
            self.cfg("replicateFreeGb").await.as_f64().unwrap_or(0.0),
            self.cfg("replicateMbps").await.as_f64().unwrap_or(0.0),
        )
    }

    /// The node keeps serving until the plugins have stopped.
    async fn close_locked(&self, open: &mut Option<Open>) {
        let Some(Open {
            node,
            acquirer,
            mirror,
            changes,
            replication,
        }) = open.take()
        else {
            return;
        };
        mirror.close().await;
        // The cast stack closes before the node it drives, as `stop()` does.
        self.0.api.set_cast(None);
        let cast = self.0.cast.lock().unwrap().take();
        if let Some(cast) = cast {
            cast.close().await;
        }
        replication.kicker.abort();
        (replication.detach_traffic)();
        replication.traffic.stop();
        *self.0.mirror.lock().unwrap() = None;
        *self.0.acquirer.lock().unwrap() = None;
        acquirer.close();
        changes.abort();
        self.0.api.clear_node();
        self.0.api.set_blob(None);
        if let Err(error) = node.close().await {
            log("node close failed", json!({ "error": error.to_string() }));
        }
        self.notify("update");
    }

    async fn reopen(&self) -> Result<(), String> {
        let mut open = self.0.open.lock().await;
        self.close_locked(&mut open).await;
        let result = self.open_locked(&mut open).await;
        self.notify("update");
        result
    }

    /// `state.applySettings(changed)`: live settings apply in place; any other change reopens.
    async fn apply_settings(&self, changed: Vec<String>) -> Result<(), String> {
        let mut open = self.0.open.lock().await;
        let live = {
            let settings = self.0.settings.lock().await;
            changed.iter().all(|key| settings.is_live(key))
        };
        let Some(current) = open.as_ref().filter(|_| live) else {
            self.close_locked(&mut open).await;
            self.open_locked(&mut open).await?;
            return self.apply_guard().await;
        };
        let (node, acquirer, mirror) = (
            current.node.clone(),
            current.acquirer.clone(),
            current.mirror.clone(),
        );
        let (replicator, kick) = (
            current.replication.replicator.clone(),
            current.replication.kick.clone(),
        );
        let has = |keys: &[&str]| changed.iter().any(|key| keys.contains(&key.as_str()));
        if has(&["allowedPeers"]) {
            let peers = strings(&self.cfg("allowedPeers").await);
            node.set_allowed_peers((!peers.is_empty()).then_some(peers))
                .await
                .map_err(|e| e.to_string())?;
        }
        if has(&["acquireConcurrency"]) {
            acquirer.set_concurrency(
                self.cfg("acquireConcurrency")
                    .await
                    .as_u64()
                    .unwrap_or(4)
                    .max(1) as usize,
            );
        }
        if has(&["cacheMaxGb"]) {
            node.set_cache_max_bytes(self.cfg("cacheMaxGb").await.as_f64().unwrap_or(0.0) * 1e9)
                .await;
        }
        if has(&["directory"]) {
            let sources = node.list_sources().await;
            self.sync_directory(&node, &sources).await?;
        }
        if has(&["rechunk"]) && js_truthy(&self.cfg("rechunk").await) {
            let mirror = mirror.clone();
            tokio::spawn(async move {
                let _ = mirror.run("blob-rechunk", Value::Null).await;
            });
        }
        if has(&["watchDir", "feeds"]) {
            mirror.configure(
                Some(non_empty(text(&self.cfg("watchDir").await))),
                Some(strings(&self.cfg("feeds").await)),
            );
        }
        if has(&[
            "replicate",
            "replicateSources",
            "replicateFreeGb",
            "replicateMbps",
        ]) {
            // Turning it off stops the running pass at its next window check.
            replicator.configure(self.replicate_config().await);
            kick.notify_one();
        }
        if has(&["lan", "lanHost"]) {
            self.refresh_cast_lan().await;
            let lan = match js_truthy(&self.cfg("lan").await) {
                true => match self.lan_host(&self.cfg("lanHost").await) {
                    Some(host) => Some(
                        crate::lan::LanDiscovery::open(host, node.net().key_pair().clone())
                            .await
                            .map_err(|e| e.to_string())?
                            as Arc<dyn crate::node::network::LanBinding>,
                    ),
                    None => None,
                },
                false => None,
            };
            node.net().set_lan(lan).await.map_err(|e| e.to_string())?;
            self.0.extra.lock().unwrap()["lan"] = lan_status(&node).await;
        }
        self.apply_guard().await?;
        self.notify("update");
        Ok(())
    }

    async fn recover_mnemonic(&self, mnemonic: String, alias: String) -> Result<Value, HttpError> {
        let node = self
            .0
            .api
            .node()
            .ok_or_else(|| HttpError::new(503, "Node still starting"))?;
        let claim = node
            .claim_with_mnemonic(&mnemonic, &alias)
            .await
            .map_err(|e| HttpError::new(400, e.to_string()))?;
        if claim.reopen_needed {
            let _ = self
                .0
                .settings
                .lock()
                .await
                .update(&json!({ "channel": claim.channel }));
            self.reopen().await.map_err(|e| HttpError::new(500, e))?;
            if let Some(node) = self.0.api.node() {
                let _ = node.claim_with_mnemonic(&mnemonic, &alias).await;
            }
        }
        self.notify("update");
        match self.0.api.node() {
            Some(node) => node
                .identity(false)
                .await
                .map(|id| serde_json::to_value(id).unwrap_or(Value::Null))
                .map_err(|e| HttpError::new(500, e.to_string())),
            None => Ok(serde_json::to_value(claim).unwrap_or(Value::Null)),
        }
    }

    fn saved_sources(&self) -> Value {
        let mut list: Vec<Value> = std::fs::read_to_string(self.0.storage.join("sources.json"))
            .ok()
            .and_then(|text| serde_json::from_str(&text).ok())
            .unwrap_or_default();
        if let Some(extra) = self.0.env.get("PEARTUBE_SOURCES") {
            list.extend(
                extra
                    .split(',')
                    .map(str::trim)
                    .filter(|s| !s.is_empty())
                    .map(|s| json!(s)),
            );
        }
        Value::Array(list)
    }

    /// `state.onSourcesChanged`: keep followed sources across restarts, then reconcile the directory.
    fn save_sources(&self, list: Vec<crate::node::api::Source>) {
        let keep = crate::app::followed_sources(&list);
        if let Err(error) = write_private(
            &self.0.storage.join("sources.json"),
            &serde_json::to_string(&keep).unwrap_or_else(|_| "[]".into()),
        ) {
            log("sources save failed", json!({ "error": error.to_string() }));
        }
        let relay = self.clone();
        tokio::spawn(async move {
            if let Some(node) = relay.0.api.node()
                && let Err(error) = relay.sync_directory(&node, &list).await
            {
                log("directory sync failed", json!({ "error": error }));
            }
        });
    }

    /// The `directory` setting lists followed sources on this relay's channel; turning it off
    /// unlists exactly what it listed (`directory.json`), so hand-listed sources stay.
    async fn sync_directory(
        &self,
        node: &Arc<CoreNode>,
        list: &[crate::node::api::Source],
    ) -> Result<(), String> {
        let file = self.0.storage.join("directory.json");
        let mut auto: Option<Vec<String>> = std::fs::read_to_string(&file)
            .ok()
            .and_then(|text| serde_json::from_str(&text).ok());
        let save = |auto: &Vec<String>| {
            write_private(
                &file,
                &serde_json::to_string(auto).unwrap_or_else(|_| "[]".into()),
            )
        };
        let published: Vec<String> = node
            .published_sources()
            .await
            .map_err(|e| e.to_string())?
            .into_iter()
            .map(|p| p.key)
            .collect();
        if !js_truthy(&self.cfg("directory").await) {
            let Some(mut listed) = auto else {
                save(&Vec::new()).map_err(|e| e.to_string())?;
                return Ok(());
            };
            while let Some(key) = listed.first().cloned() {
                if published.contains(&key) {
                    node.unpublish_source(&key)
                        .await
                        .map_err(|e| e.to_string())?;
                }
                listed.remove(0);
                save(&listed).map_err(|e| e.to_string())?;
            }
            return Ok(());
        }
        // Before this file existed, a relay with the setting on listed every followed source itself.
        let listed = auto.get_or_insert_with(|| {
            list.iter()
                .filter(|s| !s.primary && published.contains(&s.key))
                .map(|s| s.key.clone())
                .collect()
        });
        save(listed).map_err(|e| e.to_string())?;
        for source in list {
            if source.primary || published.contains(&source.key) {
                continue;
            }
            let title = if source.alias.is_empty() {
                source.key.chars().take(12).collect()
            } else {
                source.alias.clone()
            };
            node.publish_source(&source.key, &title)
                .await
                .map_err(|e| e.to_string())?;
            listed.push(source.key.clone());
            save(listed).map_err(|e| e.to_string())?;
        }
        Ok(())
    }

    /// `state.cast.setLanHost(lanHost())`: the controller browses from the address LAN
    /// receivers answer on, and a local receiver advertises it.
    async fn refresh_cast_lan(&self) {
        let host = self
            .lan_host(&self.cfg("lanHost").await)
            .map(|host| host.to_string());
        *self.0.cast_host.lock().unwrap() = host.clone();
        let cast = self.0.cast.lock().unwrap().clone();
        if let Some(cast) = cast {
            cast.set_lan_host(host).await;
        }
    }

    fn lan_host(&self, setting: &Value) -> Option<Ipv4Addr> {
        if let Some(host) = non_empty(text(setting)) {
            return host.parse().ok();
        }
        detect_lan_host()
    }

    fn ubit_config(&self) -> String {
        self.0
            .env
            .get("UBIT_CONFIG_DIR")
            .cloned()
            .unwrap_or_else(|| self.0.storage.join("ubit").display().to_string())
    }
}

/// `lanAddress(os.networkInterfaces())`: Wi-Fi or Ethernet, never loopback.
fn detect_lan_host() -> Option<Ipv4Addr> {
    let interfaces = if_addrs::get_if_addrs().ok()?;
    let addresses: Vec<crate::lan::IfaceAddress<'_>> = interfaces
        .iter()
        .filter_map(|iface| match iface.ip() {
            std::net::IpAddr::V4(address) => Some(crate::lan::IfaceAddress {
                name: iface.name.as_str(),
                address: std::net::IpAddr::V4(address),
                internal: iface.is_loopback(),
            }),
            std::net::IpAddr::V6(_) => None,
        })
        .collect();
    crate::lan::lan_address(&addresses)
}

/// `status.privateGuard`: `{ publicKey, port, ...guard.stats() }`, as the JS relay reports it.
fn guard_status(guard: &PrivateGuard) -> Value {
    let stats = guard.stats();
    json!({
        "publicKey": to_hex(&guard.public_key()),
        "port": guard.address().map_or(0, |addr| addr.port()),
        "registeredRoutes": stats.registered_routes,
        "activeCircuits": stats.active_circuits,
        "rendezvousTopics": stats.rendezvous_topics,
        "putMutableCount": stats.put_mutable_count,
        "rvRegisterCount": stats.rv_register_count,
    })
}

/// `status.lan`: where LAN peers reach this relay, `{host, port}` as the JS relay reports it.
async fn lan_status(node: &CoreNode) -> Value {
    match node.lan_endpoint().await {
        Some(lan) => json!({ "host": lan.host, "port": lan.port }),
        None => Value::Null,
    }
}

/// `state.cast` on the API: the relay's cast controller answers every `/v1/cast/*` route while
/// the node (and so the controller) is open.
struct CastRoutes(Arc<CastController>);

impl http::CastBridge for CastRoutes {
    fn status(&self) -> Value {
        self.0.status()
    }

    fn pair_receiver<'a>(&'a self, params: &'a Value) -> BoxFuture<'a, Result<Value, HttpError>> {
        Box::pin(async move { self.0.pair_receiver(params).await.map_err(cast_http) })
    }

    fn remove_receiver(&self, id: &str) -> Value {
        self.0.remove_receiver(id)
    }

    fn cast_play<'a>(&'a self, params: &'a Value) -> BoxFuture<'a, Result<Value, HttpError>> {
        Box::pin(async move { self.0.cast_play(params).await.map_err(cast_http) })
    }

    fn cast_control<'a>(&'a self, params: &'a Value) -> BoxFuture<'a, Result<Value, HttpError>> {
        Box::pin(async move { self.0.cast_control(params).await.map_err(cast_http) })
    }

    fn cast_stop<'a>(&'a self, params: &'a Value) -> BoxFuture<'a, Result<Value, HttpError>> {
        Box::pin(async move {
            // `castStop({ stopPlayback = true } = {})`
            let stop = params.get("stopPlayback").map(js_truthy).unwrap_or(true);
            self.0.cast_stop(stop).await.map_err(cast_http)
        })
    }
}

/// A cast failure with the status the app's handler answers with; a channel error is a 400.
fn cast_http(error: crate::cast::CastError) -> HttpError {
    HttpError::new(
        if error.status == 0 { 400 } else { error.status },
        error.message,
    )
}

/// `state.acquirer` behind the API; 404 while the node reopens, as on a read-only peer.
struct AcquirerBridge(std::sync::Weak<Inner>);

impl AcquirerBridge {
    fn current(&self) -> Option<Acquirer<CoreNode>> {
        self.0.upgrade()?.acquirer.lock().unwrap().clone()
    }
}

impl http::Acquirer for AcquirerBridge {
    fn add<'a>(&'a self, request: Value) -> BoxFuture<'a, Result<Value, HttpError>> {
        Box::pin(async move {
            let acquirer = self
                .current()
                .ok_or_else(|| HttpError::new(404, "Acquire unavailable on read-only peer"))?;
            acquirer
                .add(add_job(&request))
                .map_err(|e| HttpError::new(500, e.to_string()))
        })
    }

    fn list(&self) -> Value {
        Value::Array(self.current().map(|a| a.list()).unwrap_or_default())
    }

    fn get(&self, id: &str) -> Value {
        self.current()
            .and_then(|a| a.get(id))
            .unwrap_or(Value::Null)
    }

    fn cancel(&self, id: &str) -> Value {
        self.current()
            .and_then(|a| a.cancel(id))
            .unwrap_or(Value::Null)
    }
}

/// `acquirer.add({ id, title, source, channel, private })` from the API's JSON.
fn add_job(request: &Value) -> AddJob {
    let source = &request["source"];
    AddJob {
        id: js_string(&request["id"]),
        title: js_string(&request["title"]),
        source: SourceSpec {
            url: source["url"].as_str().unwrap_or_default().to_owned(),
            headers: source["headers"]
                .as_object()
                .map(|h| h.iter().map(|(k, v)| (k.clone(), js_string(v))).collect())
                .unwrap_or_default(),
            page: js_truthy(&source["page"]),
            private: js_truthy(&source["private"]),
        },
        channel: (!request["channel"].is_null()).then(|| js_truthy(&request["channel"])),
        private: js_truthy(&request["private"]),
        plugin: None,
    }
}

/// `state.mirror` behind the API.
struct MirrorBridge(std::sync::Weak<Inner>);

impl MirrorBridge {
    fn current(&self) -> Option<Arc<MirrorHost<CoreNode>>> {
        self.0.upgrade()?.mirror.lock().unwrap().clone()
    }
}

impl http::Mirror for MirrorBridge {
    fn list(&self) -> Value {
        Value::Array(self.current().map(|m| m.list()).unwrap_or_default())
    }

    fn run<'a>(
        &'a self,
        plugin: &'a str,
        payload: Value,
    ) -> BoxFuture<'a, Result<Value, HttpError>> {
        Box::pin(async move {
            let mirror = self
                .current()
                .ok_or_else(|| HttpError::new(404, "Mirror host unavailable on read-only peer"))?;
            mirror
                .run(plugin, payload)
                .await
                .map_err(|e| HttpError::new(e.status.unwrap_or(500), e.message))
        })
    }
}

/// `bin/private-relay.js`'s mode: the process is a Corestore-free 2-hop guard forwarder, never a
/// content relay. An empty variable is unset, as JS truthiness has it.
pub fn guard_mode(env: &HashMap<String, String>) -> bool {
    present(env, "PEARTUBE_PRIVATE_RELAY") || present(env, "PEARTUBE_PRIVATE_RELAY_PORT")
}

fn present(env: &HashMap<String, String>, key: &str) -> bool {
    env.get(key).is_some_and(|value| !value.is_empty())
}

/// The flags `bin/relay.js` refuses beside guard mode: a guard serves no channel and no content.
const CONTENT_FLAGS: [&str; 6] = [
    "PEARTUBE_CHANNEL",
    "PEARTUBE_SOURCES",
    "PEARTUBE_PRIVATE_ROUTING",
    "PEARTUBE_REPLICATE",
    "PEARTUBE_WATCH_DIR",
    "PEARTUBE_MIRROR_FEEDS",
];

/// A standalone guard, listening and announcing. No Corestore, no storage folder, no files.
pub struct StandaloneGuard {
    guard: PrivateGuard,
    host: String,
    port: u16,
    follow_parent: bool,
}

impl StandaloneGuard {
    /// `{"publicKey", "host", "port"}`, as `private-relay.js` prints its ready line.
    pub fn ready(&self) -> Value {
        json!({
            "publicKey": to_hex(&self.guard.public_key()),
            "host": self.host,
            "port": self.port,
        })
    }

    /// `{"publicKey", ...guard.stats()}`, the heartbeat line.
    pub fn heartbeat(&self) -> Value {
        heartbeat_line(&self.guard)
    }

    pub fn public_key(&self) -> [u8; 32] {
        self.guard.public_key()
    }

    pub fn address(&self) -> (String, u16) {
        (self.host.clone(), self.port)
    }

    /// How often `private-relay.js` reports: 10 s while following a parent, 60 s alone.
    pub fn heartbeat_interval(&self) -> std::time::Duration {
        std::time::Duration::from_secs(if self.follow_parent { 10 } else { 60 })
    }

    pub async fn close(self) {
        let _ = self.guard.close().await;
    }
}

/// `createPrivateGuard` over the environment `private-relay.js` reads: no storage is touched.
pub async fn start_standalone_guard(
    env: &HashMap<String, String>,
) -> Result<StandaloneGuard, String> {
    if CONTENT_FLAGS.iter().any(|flag| present(env, flag)) {
        return Err(
            "PEARTUBE_PRIVATE_RELAY runs a standalone Corestore-free guard forwarder and cannot be combined with channel or content flags; run peartube-private-relay separately"
                .to_owned(),
        );
    }
    let bootstrap = env
        .get("PEARTUBE_BOOTSTRAP")
        .map(|raw| {
            raw.split(',')
                .map(str::trim)
                .filter(|part| !part.is_empty())
                .map(str::to_owned)
                .collect()
        })
        .unwrap_or_default();
    let bootstrap = crate::node::network::resolve_bootstrap(bootstrap);
    let port = ["PEARTUBE_PRIVATE_RELAY_PORT", "PEARTUBE_DHT_PORT"]
        .iter()
        .find_map(|var| {
            env.get(*var)
                .filter(|value| !value.is_empty())
                .and_then(|value| value.trim().parse().ok())
        })
        .unwrap_or(0);
    let host = env
        .get("PEARTUBE_PRIVATE_HOST")
        .filter(|host| !host.trim().is_empty())
        .cloned();
    let seed: Option<[u8; 32]> = env
        .get("PEARTUBE_GUARD_SEED")
        .map(|value| value.trim().to_owned())
        .and_then(|value| parse_hex32(&value));
    // `createPrivateGuard` binds loopback for a local bootstrap, as the guard's own swarm does.
    let bind = host.clone().or_else(|| {
        crate::private_overlay::is_local_bootstrap(host.as_deref(), &bootstrap)
            .then(|| "127.0.0.1".to_owned())
    });
    let dht = open_dht(DhtOptions {
        host: bind.clone(),
        port,
        bootstrap: bootstrap.clone(),
    })
    .await
    .map_err(|e| format!("private guard dht: {e}"))?;
    let guard = create_private_guard(
        dht,
        GuardOptions {
            host: bind,
            port,
            bootstrap,
            seed,
            ..GuardOptions::default()
        },
    )
    .await
    .map_err(|e| format!("private guard: {e}"))?;
    let address = guard.address();
    Ok(StandaloneGuard {
        host: address
            .map(|addr| addr.ip().to_string())
            .or(host)
            .unwrap_or_else(|| "0.0.0.0".to_owned()),
        port: address.map_or(port, |addr| addr.port()),
        follow_parent: present(env, "PEARTUBE_GUARD_FOLLOW_PARENT"),
        guard,
    })
}

/// `bin/relay.js`'s guard-only branch: run the forwarder until a signal or the parent leaves.
pub async fn guard_only(env: HashMap<String, String>) -> Result<(), String> {
    // Read before the first await: a relay that dies while the guard is still opening would
    // otherwise be recorded as the pid the guard was reparented to, and the watch never fires.
    let follow = present(&env, "PEARTUBE_GUARD_FOLLOW_PARENT");
    let pid = if follow { parent_pid() } else { None };
    let guard = start_standalone_guard(&env).await?;
    log("private-guard ready", guard.ready());
    let every = guard.heartbeat_interval();
    let heartbeat = {
        let guard = guard.guard.clone();
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(every);
            ticker.tick().await;
            loop {
                ticker.tick().await;
                log("private-guard heartbeat", heartbeat_line(&guard));
            }
        })
    };
    // `private-relay.js`'s parent watch: leave with the relay that started us, even if it died
    // without saying so. Its `shutdown()` closes the guard, so the wait ends here, not only the
    // heartbeat above.
    tokio::select! {
        _ = wait_for_signal() => {}
        _ = parent_gone(pid), if follow => {}
    }
    heartbeat.abort();
    // JS clears the heartbeat and starts a 2 s kill timer before awaiting `guard.close()`: a close
    // that waits on the network must not keep a guard whose relay is gone.
    if tokio::time::timeout(std::time::Duration::from_secs(2), guard.close())
        .await
        .is_err()
    {
        log("private-guard close timed out", json!({}));
    }
    Ok(())
}

/// `{"publicKey", ...guard.stats()}`, the whole heartbeat line `private-relay.js` prints.
fn heartbeat_line(guard: &PrivateGuard) -> Value {
    let stats = guard.stats();
    json!({
        "publicKey": to_hex(&guard.public_key()),
        "registeredRoutes": stats.registered_routes,
        "activeCircuits": stats.active_circuits,
        "rendezvousTopics": stats.rendezvous_topics,
        "putMutableCount": stats.put_mutable_count,
        "rvRegisterCount": stats.rv_register_count,
    })
}

/// `process.ppid`, as `private-relay.js` captures it at start.
fn parent_pid() -> Option<u32> {
    #[cfg(unix)]
    {
        rustix::process::getppid().map(|pid| pid.as_raw_nonzero().get() as u32)
    }
    #[cfg(not(unix))]
    {
        None
    }
}

/// Resolves once the parent process is gone (the reparented pid changes).
async fn parent_gone(pid: Option<u32>) {
    let Some(pid) = pid else {
        std::future::pending::<()>().await;
        return;
    };
    loop {
        tokio::time::sleep(std::time::Duration::from_millis(2_000)).await;
        if parent_pid() != Some(pid) {
            return;
        }
    }
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

fn read_tls(env: &HashMap<String, String>, storage: &Path) -> Option<tokio_rustls::TlsAcceptor> {
    use tokio_rustls::rustls::pki_types::pem::PemObject;
    use tokio_rustls::rustls::pki_types::{CertificateDer, PrivateKeyDer};
    let path = |var: &str, file: &str| {
        env.get(var)
            .map(PathBuf::from)
            .unwrap_or_else(|| storage.join(file))
    };
    let certs: Vec<CertificateDer<'static>> =
        CertificateDer::pem_file_iter(path("PEARTUBE_TLS_CERT", "tls.crt"))
            .ok()?
            .collect::<Result<_, _>>()
            .ok()?;
    let key = PrivateKeyDer::from_pem_file(path("PEARTUBE_TLS_KEY", "tls.key")).ok()?;
    let config = tokio_rustls::rustls::ServerConfig::builder_with_provider(Arc::new(
        tokio_rustls::rustls::crypto::ring::default_provider(),
    ))
    .with_safe_default_protocol_versions()
    .ok()?
    .with_no_client_auth()
    .with_single_cert(certs, key)
    .map_err(|error| {
        log(
            "TLS certificate rejected",
            json!({ "error": error.to_string() }),
        )
    })
    .ok()?;
    Some(tokio_rustls::TlsAcceptor::from(Arc::new(config)))
}

/// The relay UI and the web player: `PEARTUBE_WEB_DIR`, else `web` beside the binary.
fn web_dir(env: &HashMap<String, String>) -> PathBuf {
    env.get("PEARTUBE_WEB_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            std::env::current_exe()
                .ok()
                .and_then(|exe| exe.parent().map(|dir| dir.join("web")))
                .unwrap_or_else(|| PathBuf::from("web"))
        })
}

fn ubit_bin(env: &HashMap<String, String>) -> String {
    env.get("UBIT_BIN").cloned().unwrap_or_default()
}

fn port(env: &HashMap<String, String>, var: &str, default: u16) -> u16 {
    env.get(var)
        .and_then(|v| v.trim().parse().ok())
        .unwrap_or(default)
}

/// 64 hex characters as 32 bytes.
fn parse_hex32(text: &str) -> Option<[u8; 32]> {
    if text.len() != 64 || !text.is_ascii() {
        return None;
    }
    let mut out = [0u8; 32];
    for (i, chunk) in text.as_bytes().chunks(2).enumerate() {
        out[i] = u8::from_str_radix(std::str::from_utf8(chunk).ok()?, 16).ok()?;
    }
    Some(out)
}

fn non_empty(text: String) -> Option<String> {
    (!text.trim().is_empty()).then_some(text)
}

/// A string setting; `null` (unset) reads as empty.
fn text(value: &Value) -> String {
    value.as_str().map(str::to_owned).unwrap_or_default()
}

fn strings(value: &Value) -> Vec<String> {
    value
        .as_array()
        .map(|items| {
            items
                .iter()
                .map(js_string)
                .filter(|s| !s.is_empty())
                .collect()
        })
        .unwrap_or_default()
}

/// `mkdirSync(storage, { recursive: true, mode: 0o700 })`: the mode applies to what it creates.
fn create_private_dir(dir: &Path) -> std::io::Result<()> {
    let mut builder = std::fs::DirBuilder::new();
    builder.recursive(true);
    #[cfg(unix)]
    std::os::unix::fs::DirBuilderExt::mode(&mut builder, 0o700);
    builder.create(dir)
}

/// A 0600 temp file renamed over `path`, as the JS relay writes its state files.
fn write_private(path: &Path, text: &str) -> std::io::Result<()> {
    let tmp = path.with_extension("json.tmp");
    {
        let mut options = std::fs::OpenOptions::new();
        options.write(true).create(true).truncate(true);
        #[cfg(unix)]
        std::os::unix::fs::OpenOptionsExt::mode(&mut options, 0o600);
        std::io::Write::write_all(&mut options.open(&tmp)?, text.as_bytes())?;
    }
    std::fs::rename(tmp, path)
}

/// One JSON log line, as the JS relay prints (`{ msg, ... }`).
pub fn log(msg: &str, fields: Value) {
    let mut line = json!({ "msg": msg });
    if let (Some(line), Some(fields)) = (line.as_object_mut(), fields.as_object()) {
        line.extend(fields.clone());
    }
    println!("{line}");
}
