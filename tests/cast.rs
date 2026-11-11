//! Cast parity with the app's `src/cast.js`, plus JS↔Rust interop over the HTTP API and over a
//! TCP stand-in for a secret stream.
//!
//! The fixture in `fixtures/cast-reference.json` is produced by `tests/js/cast-fixtures.mjs`,
//! which runs the app's own receiver (commands and player reports over the real HTTP API) and
//! pulls the module-private helpers out of `src/cast.js`.

use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::time::Duration;

use futures_util::future::BoxFuture;
use hyper::Method;
use serde_json::{Map, Value, json};

use peartube_native::cast::http::HttpClient;
use peartube_native::cast::pairing::ReceiverIdentity;
use peartube_native::cast::records::SavedReceivers;
use peartube_native::cast::traits::{DhtConnect, DhtError, DhtServer, NoiseHandler};
use peartube_native::cast::{
    CastController, ControllerOptions, Receiver, ReceiverOptions, qr, state,
};

const FIXTURES: &str = include_str!("../fixtures/cast-reference.json");
const DEVICE_ID: &str = "0123456789abcdef0123456789abcdef";
const SEED: &str = "abababababababababababababababababababababababababababababababab";
const PAIR_TOKEN: &str = "cdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcd";
const PAIR_CODE: &str = "ABC123";
const NAME: &str = "Kitchen TV";

fn fixtures() -> Value {
    serde_json::from_str(FIXTURES).expect("fixtures/cast-reference.json")
}

fn cases<'a>(fixtures: &'a Value, key: &str) -> &'a [Value] {
    fixtures[key]
        .as_array()
        .unwrap_or_else(|| panic!("{key} cases"))
}

/// Recursively zeroes the volatile fields, like the generator's `mask`.
fn mask(value: &Value) -> Value {
    match value {
        Value::Array(items) => Value::Array(items.iter().map(mask).collect()),
        Value::Object(fields) => {
            let mut out = Map::new();
            for (key, item) in fields {
                if key == "updatedAt" || key == "storageFreeBytes" {
                    out.insert(key.clone(), json!(0));
                } else {
                    out.insert(key.clone(), mask(item));
                }
            }
            Value::Object(out)
        }
        other => other.clone(),
    }
}

/// Replaces the run's ephemeral port with 0 wherever it appears in a string.
fn mask_ports(value: &Value, port: u16) -> Value {
    let text = serde_json::to_string(value).expect("json");
    let text = text
        .replace(&format!(":{port}"), ":0")
        .replace(&format!("port={port}"), "port=0");
    serde_json::from_str(&text).expect("json")
}

fn same_json(actual: &Value, expected: &Value, context: &str) {
    assert_eq!(
        serde_json::to_string(actual).expect("json"),
        serde_json::to_string(expected).expect("json"),
        "{context}"
    );
}

fn temp_dir(label: &str) -> PathBuf {
    let path = std::env::temp_dir().join(format!(
        "peartube-cast-{label}-{}-{:?}",
        std::process::id(),
        std::thread::current().id()
    ));
    let _ = std::fs::remove_dir_all(&path);
    std::fs::create_dir_all(&path).expect("temp dir");
    path
}

fn seed_identity(dir: &Path) {
    let identity = json!({
        "deviceId": DEVICE_ID,
        "name": NAME,
        "seed": SEED,
        "pairToken": PAIR_TOKEN,
        "pairCode": PAIR_CODE,
    });
    std::fs::write(
        ReceiverIdentity::file(dir),
        serde_json::to_string_pretty(&identity).expect("json"),
    )
    .expect("identity");
}

async fn start_receiver(dir: &Path, open_auth: bool) -> Receiver {
    seed_identity(dir);
    Receiver::start(ReceiverOptions {
        storage: dir.to_path_buf(),
        peer_storage: None,
        name: NAME.to_owned(),
        port: 0,
        bind_host: "127.0.0.1".to_owned(),
        lan_host: None,
        peer: None,
        allow_peer_node: false,
        peer_factory: None,
        dht_server: None,
        mdns: false,
        open_auth,
        any_port: true,
        on_command: None,
        on_change: None,
    })
    .await
    .expect("receiver")
}

#[test]
fn pair_codes_match() {
    for case in cases(&fixtures(), "pairCodes") {
        let input = case["input"].as_str().unwrap();
        assert_eq!(
            peartube_native::cast::normalize_pair_code(Some(input)),
            case["output"].as_str().unwrap(),
            "{input}"
        );
    }
}

#[test]
fn idle_state_matches() {
    let fixture = &fixtures()["idle"];
    let idle = state::idle(0);
    let keys: Vec<Value> = idle.keys().map(|k| json!(k)).collect();
    same_json(&Value::Array(keys), &fixture["keys"], "idle keys");
    same_json(&state::to_json(&idle), &fixture["state"], "idle state");
}

#[test]
fn ranges_match() {
    for case in cases(&fixtures(), "ranges") {
        let header = case["header"].as_str();
        let total = case["total"].as_u64().unwrap();
        let actual = match peartube_native::cast::http::parse_http_range(header, total) {
            peartube_native::cast::http::RangeSpec::Full { start, end } => {
                json!({ "partial": false, "start": start, "end": end })
            }
            peartube_native::cast::http::RangeSpec::Partial { start, end } => {
                json!({ "partial": true, "start": start, "end": end })
            }
            peartube_native::cast::http::RangeSpec::Invalid => json!({ "invalid": true }),
        };
        same_json(&actual, &case["result"], &format!("range {header:?}"));
    }
}

#[test]
fn media_mime_matches() {
    for case in cases(&fixtures(), "mime") {
        assert_eq!(
            peartube_native::cast::http::guess_media_mime(&case["entry"]),
            case["mime"].as_str().unwrap(),
            "{}",
            case["entry"]
        );
    }
}

#[test]
fn receiver_addresses_match() {
    for case in cases(&fixtures(), "addresses") {
        let input = case["input"].as_str().unwrap();
        match peartube_native::cast::receiver_address(input) {
            Ok(found) => {
                assert!(
                    case["ok"].as_bool().unwrap(),
                    "expected an error for {input:?}"
                );
                let actual = if !found.public_key.is_empty() {
                    json!({ "publicKey": found.public_key })
                } else {
                    json!({ "host": found.host, "port": found.port })
                };
                same_json(&actual, &case["value"], input);
            }
            Err(err) => {
                assert!(
                    !case["ok"].as_bool().unwrap(),
                    "expected success for {input:?}"
                );
                assert_eq!(err, case["error"].as_str().unwrap());
            }
        }
    }
}

#[test]
fn pairing_qr_matches() {
    for case in cases(&fixtures(), "qr") {
        assert_eq!(
            qr::qr_svg(case["text"].as_str().unwrap()),
            case["svg"].as_str().unwrap(),
            "{}",
            case["text"]
        );
    }
}

/// The full receiver flow the fixture recorded: transport commands, player reports, then `info`.
#[tokio::test]
async fn receiver_fixture_flow_matches() {
    let dir = temp_dir("flow");
    let receiver = start_receiver(&dir, true).await;
    let client = HttpClient::new();
    let base = format!("http://127.0.0.1:{}", receiver.port());
    let fixture = fixtures();

    for script in cases(&fixture, "commandScripts") {
        for (command, expected) in script["commands"]
            .as_array()
            .unwrap()
            .iter()
            .zip(script["states"].as_array().unwrap())
        {
            let response = client
                .json(
                    Method::POST,
                    &format!("{base}/cast/v1/command"),
                    Vec::new(),
                    Some(command),
                    Duration::from_secs(5),
                )
                .await;
            match response {
                Ok(body) => {
                    assert_eq!(expected["status"].as_u64().unwrap(), 200);
                    same_json(&mask(&body["state"]), &expected["state"], "command state");
                }
                Err(err) => {
                    assert_eq!(
                        u64::from(err.status),
                        expected["status"].as_u64().unwrap(),
                        "{}",
                        script["name"]
                    );
                    assert_eq!(err.message, expected["error"].as_str().unwrap());
                }
            }
        }
    }

    for script in cases(&fixture, "reportScripts") {
        for (patch, expected) in script["patches"]
            .as_array()
            .unwrap()
            .iter()
            .zip(script["states"].as_array().unwrap())
        {
            same_json(
                &mask(&receiver.report_state(patch)),
                expected,
                "report state",
            );
        }
    }

    let mut info = client
        .json(
            Method::GET,
            &format!("{base}/cast/v1/info"),
            Vec::new(),
            None,
            Duration::from_secs(5),
        )
        .await
        .expect("info");
    // Native diagnostics extend the JS contract; assert them before comparing its fields.
    assert_eq!(
        info.as_object_mut().unwrap().shift_remove("connectedPeers"),
        Some(json!([]))
    );
    same_json(
        &mask_ports(&mask(&info), receiver.port()),
        &fixture["info"],
        "info",
    );

    let mut web_info = client
        .json(
            Method::GET,
            &format!("{base}/cast/v1/web-info"),
            Vec::new(),
            None,
            Duration::from_secs(5),
        )
        .await
        .expect("web info");
    if let Value::Object(fields) = &mut web_info {
        assert_eq!(fields.shift_remove("connectedPeers"), Some(json!([])));
        fields.insert("qrSvg".into(), json!(""));
        fields.insert("webToken".into(), json!(""));
    }
    same_json(
        &mask_ports(&mask(&web_info), receiver.port()),
        &fixture["webInfo"],
        "web info",
    );

    assert_eq!(receiver.info()["pairCode"], json!("OPEN"));
    same_json(
        &mask_ports(receiver.info().get("pairingUri").unwrap(), receiver.port()),
        &fixture["pairingUri"],
        "pairing uri",
    );
    receiver.close().await;
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn unpairing_and_bad_codes_behave_like_the_app() {
    let dir = temp_dir("badpair");
    let receiver = start_receiver(&dir, false).await;
    let controller = CastController::start(ControllerOptions {
        storage: temp_dir("badpair-sender"),
        mdns: false,
        ..Default::default()
    })
    .await
    .expect("controller");
    let err = controller
        .pair_receiver(&json!({
            "host": "127.0.0.1",
            "port": receiver.port(),
            "code": "WRONG",
        }))
        .await
        .expect_err("bad code");
    assert_eq!(err.message, "Invalid TV pairing code");
    assert_eq!(err.status, 403);

    let record = controller
        .pair_receiver(&json!({
            "host": "127.0.0.1",
            "port": receiver.port(),
            "code": PAIR_CODE,
        }))
        .await
        .expect("pair");
    assert_eq!(record["token"], json!(PAIR_TOKEN));
    assert_eq!(record["name"], json!(NAME));

    // An unauthenticated command is refused until the pairing token is presented.
    let client = HttpClient::new();
    let base = format!("http://127.0.0.1:{}", receiver.port());
    let denied = client
        .json(
            Method::POST,
            &format!("{base}/cast/v1/command"),
            Vec::new(),
            Some(&json!({ "action": "status" })),
            Duration::from_secs(5),
        )
        .await
        .expect_err("denied");
    assert_eq!(denied.status, 401);

    let allowed = client
        .json(
            Method::POST,
            &format!("{base}/cast/v1/command"),
            vec![(hyper::header::AUTHORIZATION, format!("Bearer {PAIR_TOKEN}"))],
            Some(&json!({ "action": "status" })),
            Duration::from_secs(5),
        )
        .await
        .expect("allowed");
    assert_eq!(mask(&allowed["state"])["status"], json!("idle"));

    controller.close().await;
    receiver.close().await;
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn pairing_records_match() {
    let dir = temp_dir("pairrecord");
    let receiver = start_receiver(&dir, true).await;
    let controller_storage = temp_dir("pairrecord-sender");
    let controller = CastController::start(ControllerOptions {
        storage: controller_storage.clone(),
        mdns: false,
        ..Default::default()
    })
    .await
    .expect("controller");

    let record = controller
        .pair_receiver(&json!({
            "host": "127.0.0.1",
            "port": receiver.port(),
            "code": PAIR_CODE,
        }))
        .await
        .expect("pair");
    let fixture = &fixtures()["pairing"];
    same_json(
        &mask_ports(&record, receiver.port()),
        &fixture["record"],
        "pair record",
    );

    let saved = SavedReceivers::load(&controller_storage);
    same_json(
        &mask_ports(
            &serde_json::to_value(saved.iter().collect::<Vec<_>>()).unwrap(),
            receiver.port(),
        ),
        &fixture["saved"],
        "saved receivers",
    );

    same_json(
        &mask_ports(
            &serde_json::to_value(controller.list_receivers()).unwrap(),
            receiver.port(),
        ),
        &fixture["listing"],
        "receiver listing",
    );

    let removed = controller.remove_receiver(DEVICE_ID);
    assert_eq!(removed["removed"], json!(true));
    assert!(SavedReceivers::load(&controller_storage).is_empty());

    controller.close().await;
    receiver.close().await;
    let _ = std::fs::remove_dir_all(&dir);
    let _ = std::fs::remove_dir_all(&controller_storage);
}

// ---------------------------------------------------------------------------------------------
// Interop: the real `cast.js` controller drives this receiver, and this controller drives the
// real `cast.js` receiver. The P2P half speaks the same newline-JSON protocol over a TCP socket
// standing in for the DHT's secret stream.

fn js_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/js")
}

fn node_script() -> PathBuf {
    js_dir().join("cast-interop.mjs")
}

fn has_app() -> bool {
    if std::env::var("PEARTUBE_APP").is_ok() {
        return true;
    }
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../peartube/src/cast.js")
        .exists()
}

struct JsProcess {
    child: Child,
}

impl Drop for JsProcess {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn spawn_js(args: &[&str]) -> std::io::Result<JsProcess> {
    Ok(JsProcess {
        child: Command::new("node")
            .arg(node_script())
            .args(args)
            .current_dir(js_dir())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()?,
    })
}

/// The TCP stand-in for `dht.createServer`: every accepted socket is handed to the receiver's
/// Noise handler, and the socket itself is the secret-stream-like duplex.
struct TestDhtServer {
    handlers: std::sync::Arc<std::sync::Mutex<Option<NoiseHandler>>>,
    task: tokio::task::JoinHandle<()>,
}

impl DhtServer for TestDhtServer {
    fn serve(
        &self,
        _public_key: [u8; 32],
        handler: NoiseHandler,
    ) -> BoxFuture<'_, Result<(), DhtError>> {
        *self.handlers.lock().expect("handlers") = Some(handler);
        Box::pin(async { Ok(()) })
    }

    fn close(&self) -> BoxFuture<'_, ()> {
        self.task.abort();
        Box::pin(async {})
    }
}

/// Starts the TCP stand-in and answers its address.
async fn start_dht_server() -> std::io::Result<(TestDhtServer, std::net::SocketAddr)> {
    let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0)).await?;
    let addr = listener.local_addr()?;
    let handlers: std::sync::Arc<std::sync::Mutex<Option<NoiseHandler>>> =
        std::sync::Arc::new(std::sync::Mutex::new(None));
    let shared = std::sync::Arc::clone(&handlers);
    let task = tokio::spawn(async move {
        loop {
            let Ok((socket, _)) = listener.accept().await else {
                continue;
            };
            let handler = shared.lock().expect("handlers").clone();
            if let Some(handler) = handler {
                handler(Box::new(socket));
            }
        }
    });
    Ok((TestDhtServer { handlers, task }, addr))
}

struct TestDhtConnect {
    addr: std::net::SocketAddr,
}

impl DhtConnect for TestDhtConnect {
    fn connect(
        &self,
        _public_key: [u8; 32],
        _relay_through: Option<Vec<String>>,
    ) -> BoxFuture<'_, Result<Box<dyn peartube_native::cast::traits::Duplex>, DhtError>> {
        Box::pin(async move {
            let socket = tokio::net::TcpStream::connect(self.addr)
                .await
                .map_err(|err| DhtError::other(err.to_string()))?;
            Ok(Box::new(socket) as Box<dyn peartube_native::cast::traits::Duplex>)
        })
    }
}

async fn start_receiver_with_dht(dir: &Path) -> (Receiver, std::net::SocketAddr) {
    seed_identity(dir);
    let (server, addr) = start_dht_server().await.expect("dht stand-in");
    let receiver = Receiver::start(ReceiverOptions {
        storage: dir.to_path_buf(),
        peer_storage: None,
        name: NAME.to_owned(),
        port: 0,
        bind_host: "127.0.0.1".to_owned(),
        lan_host: None,
        peer: None,
        allow_peer_node: true,
        peer_factory: None,
        dht_server: Some(Arc::new(server)),
        mdns: false,
        open_auth: true,
        any_port: true,
        on_command: None,
        on_change: None,
    })
    .await
    .expect("receiver");
    (receiver, addr)
}

fn run_js(args: &[&str]) -> Value {
    let output = Command::new("node")
        .arg(node_script())
        .args(args)
        .current_dir(js_dir())
        .output()
        .expect("node must be installed");
    assert!(
        output.status.success(),
        "node failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    let last = stdout
        .lines()
        .rfind(|line| line.trim_start().starts_with('{'))
        .unwrap_or_else(|| panic!("no json on stdout: {stdout}"));
    serde_json::from_str(last).expect("controller result")
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs node and the app checkout (PEARTUBE_APP)"]
async fn js_controller_drives_the_receiver_over_http() {
    if !has_app() {
        eprintln!("skipped: set PEARTUBE_APP to the app checkout");
        return;
    }
    let dir = temp_dir("interop-http");
    let receiver = start_receiver(&dir, false).await;
    let port = receiver.port();
    let result = tokio::task::spawn_blocking(move || {
        run_js(&["controller-http", "127.0.0.1", &port.to_string(), PAIR_CODE])
    })
    .await
    .expect("join");

    assert_eq!(result["record"]["id"], json!(DEVICE_ID));
    assert_eq!(result["record"]["token"], json!(PAIR_TOKEN));
    assert_eq!(result["attach"]["transport"], json!("lan"));
    assert_eq!(result["play"]["status"], json!("playing"));
    assert_eq!(result["play"]["playing"], json!(true));
    assert_eq!(result["seek"]["positionMs"], json!(3000));
    assert_eq!(result["paused"]["status"], json!("paused"));
    assert_eq!(result["stopped"], Value::Null);
    // The JavaScript controller's last command reached this receiver.
    assert_eq!(receiver.state()["status"], json!("idle"));
    receiver.close().await;
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs node and the app checkout (PEARTUBE_APP)"]
async fn js_controller_drives_the_receiver_over_the_noise_stream() {
    if !has_app() {
        eprintln!("skipped: set PEARTUBE_APP to the app checkout");
        return;
    }
    let dir = temp_dir("interop-noise");
    let (receiver, addr) = start_receiver_with_dht(&dir).await;
    let public_key = receiver.public_key().to_owned();
    assert_eq!(public_key.len(), 64, "the identity seed yields a Noise key");
    let result = tokio::task::spawn_blocking(move || {
        run_js(&[
            "controller-noise",
            "127.0.0.1",
            &addr.port().to_string(),
            &public_key,
        ])
    })
    .await
    .expect("join");

    assert_eq!(result["record"]["id"], json!(DEVICE_ID));
    assert_eq!(result["record"]["p2p"], json!(true));
    assert_eq!(result["attach"]["transport"], json!("p2p"));
    assert_eq!(result["play"]["status"], json!("playing"));
    assert_eq!(result["paused"]["playing"], json!(false));
    receiver.close().await;
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs node and the app checkout (PEARTUBE_APP)"]
async fn rust_controller_drives_the_js_receiver_over_http() {
    if !has_app() {
        eprintln!("skipped: set PEARTUBE_APP to the app checkout");
        return;
    }
    let mut process = spawn_js(&["receiver-http"]).expect("node");
    let ready = {
        let child = &mut process.child;
        let stdout = child.stdout.take().expect("stdout");
        let mut lines = std::io::BufRead::lines(std::io::BufReader::new(stdout));
        let line = lines.next().expect("ready").expect("read");
        serde_json::from_str::<Value>(line.trim_start_matches("READY ")).expect("ready json")
    };
    // Keep the pipe alive without reading further.
    let port = ready["httpPort"].as_u64().expect("port") as u16;
    let code = ready["pairCode"].as_str().expect("code").to_owned();

    let controller = CastController::start(ControllerOptions {
        storage: temp_dir("interop-rust-http"),
        mdns: false,
        ..Default::default()
    })
    .await
    .expect("controller");
    let record = controller
        .pair_receiver(&json!({
            "host": "127.0.0.1",
            "port": port,
            "code": code,
        }))
        .await
        .expect("pair");
    assert_eq!(record["token"], json!(PAIR_TOKEN));
    let attached = controller
        .cast_attach(&json!({ "receiverId": record["id"] }))
        .await
        .expect("attach");
    assert_eq!(attached["session"]["transport"], json!("lan"));
    let played = controller
        .cast_control(&json!({ "action": "play" }))
        .await
        .expect("play");
    assert_eq!(played["session"]["state"]["playing"], json!(true));
    let sought = controller
        .cast_control(&json!({ "action": "seek", "positionMs": 2500 }))
        .await
        .expect("seek");
    assert_eq!(sought["session"]["state"]["positionMs"], json!(2500));

    // The JavaScript receiver itself agrees.
    let client = HttpClient::new();
    let info = client
        .json(
            Method::GET,
            &format!("http://127.0.0.1:{port}/cast/v1/info"),
            Vec::new(),
            None,
            Duration::from_secs(5),
        )
        .await
        .expect("info");
    assert_eq!(info["state"]["positionMs"], json!(2500));
    assert_eq!(info["state"]["playing"], json!(true));

    controller.close().await;
    drop(process);
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs node and the app checkout (PEARTUBE_APP)"]
async fn rust_controller_drives_the_js_receiver_over_the_noise_stream() {
    if !has_app() {
        eprintln!("skipped: set PEARTUBE_APP to the app checkout");
        return;
    }
    let mut process = spawn_js(&["receiver-noise"]).expect("node");
    let ready = {
        let child = &mut process.child;
        let stdout = child.stdout.take().expect("stdout");
        let mut lines = std::io::BufRead::lines(std::io::BufReader::new(stdout));
        let line = lines.next().expect("ready").expect("read");
        serde_json::from_str::<Value>(line.trim_start_matches("READY ")).expect("ready json")
    };
    let noise_port = ready["noisePort"].as_u64().expect("noise port") as u16;
    let public_key = ready["publicKey"].as_str().expect("public key").to_owned();
    assert_eq!(public_key.len(), 64);

    let addr = format!("127.0.0.1:{noise_port}").parse().expect("address");
    let controller = CastController::start(ControllerOptions {
        storage: temp_dir("interop-rust-noise"),
        mdns: false,
        dht: Some(Arc::new(TestDhtConnect { addr })),
        ..Default::default()
    })
    .await
    .expect("controller");
    let record = controller
        .pair_receiver(&json!({ "publicKey": public_key }))
        .await
        .expect("pair over the stand-in stream");
    assert_eq!(record["p2p"], json!(true));
    let attached = controller
        .cast_attach(&json!({ "receiverId": record["id"] }))
        .await
        .expect("attach");
    assert_eq!(attached["session"]["transport"], json!("p2p"));
    let played = controller
        .cast_control(&json!({ "action": "play" }))
        .await
        .expect("play");
    assert_eq!(played["session"]["state"]["status"], json!("playing"));

    controller.close().await;
    drop(process);
}

/// Serves one fixed byte range per request, like the sender's LAN blob server.
async fn start_origin(body: Vec<u8>) -> (String, tokio::task::JoinHandle<()>) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
        .await
        .expect("origin bind");
    let addr = listener.local_addr().expect("origin addr");
    let task = tokio::spawn(async move {
        loop {
            let Ok((mut socket, _)) = listener.accept().await else {
                continue;
            };
            let body = body.clone();
            tokio::spawn(async move {
                let mut head = Vec::new();
                let mut chunk = [0u8; 1024];
                loop {
                    let Ok(read) = socket.read(&mut chunk).await else {
                        return;
                    };
                    if read == 0 {
                        return;
                    }
                    head.extend_from_slice(&chunk[..read]);
                    if head.windows(4).any(|window| window == b"\r\n\r\n") {
                        break;
                    }
                }
                let head = String::from_utf8_lossy(&head).to_lowercase();
                let range = head
                    .lines()
                    .find_map(|line| line.strip_prefix("range: bytes="))
                    .and_then(|spec| spec.split_once('-'))
                    .map(|(start, end)| {
                        let start: usize = start.trim().parse().unwrap_or(0);
                        let end: usize = end.trim().parse().unwrap_or(body.len() - 1);
                        (start, end.min(body.len() - 1))
                    });
                let (status, start, end) = match range {
                    Some((start, end)) => ("206 Partial Content", start, end),
                    None => ("200 OK", 0, body.len() - 1),
                };
                let slice = &body[start..=end];
                let response = format!(
                    "HTTP/1.1 {status}\r\ncontent-type: video/mp4\r\ncontent-length: {}\r\ncontent-range: bytes {start}-{end}/{}\r\naccept-ranges: bytes\r\nconnection: close\r\n\r\n",
                    slice.len(),
                    body.len()
                );
                let _ = socket.write_all(response.as_bytes()).await;
                let _ = socket.write_all(slice).await;
            });
        }
    });
    (format!("http://127.0.0.1:{}", addr.port()), task)
}

#[tokio::test]
async fn the_splice_proxies_http_ranges() {
    use http_body_util::BodyExt;
    use peartube_native::cast::media::{Media, splice_response};
    use std::sync::Arc;

    let body: Vec<u8> = (0..1024u32).map(|i| (i % 251) as u8).collect();
    let (url, origin) = start_origin(body.clone()).await;
    let media = Arc::new(Media::new(
        "0123456789abcdef0123456789abcdef".to_owned(),
        json!({ "id": "stream", "title": "Clip", "size": 1024 }),
        "video/mp4".to_owned(),
        "auto".to_owned(),
        "p2p".to_owned(),
        false,
        None,
        Some(url),
        None,
        None,
        String::new(),
        None,
        true,
    ));
    let client = Arc::new(HttpClient::new());
    let upgrade: Arc<dyn Fn(&str) + Send + Sync> = Arc::new(|_mode| {});

    let partial = splice_response(
        Arc::clone(&media),
        Arc::clone(&client),
        Method::GET,
        Some("bytes=10-19"),
        false,
        Arc::clone(&upgrade),
    )
    .await;
    assert_eq!(partial.status(), 206);
    assert_eq!(
        partial.headers()[&hyper::header::CONTENT_RANGE]
            .to_str()
            .unwrap(),
        "bytes 10-19/1024"
    );
    let bytes = partial
        .into_body()
        .collect()
        .await
        .expect("body")
        .to_bytes();
    assert_eq!(&bytes[..], &body[10..20]);

    let head = splice_response(
        Arc::clone(&media),
        Arc::clone(&client),
        Method::HEAD,
        Some("bytes=0-99"),
        true,
        Arc::clone(&upgrade),
    )
    .await;
    assert_eq!(
        head.headers()[&hyper::header::CONTENT_LENGTH]
            .to_str()
            .unwrap(),
        "100"
    );
    assert!(
        head.into_body()
            .collect()
            .await
            .expect("body")
            .to_bytes()
            .is_empty()
    );

    let unsatisfiable = splice_response(
        media,
        client,
        Method::GET,
        Some("bytes=2000-"),
        false,
        upgrade,
    )
    .await;
    assert_eq!(unsatisfiable.status(), 416);
    assert_eq!(
        unsatisfiable.headers()[&hyper::header::CONTENT_RANGE]
            .to_str()
            .unwrap(),
        "bytes */1024"
    );
    origin.abort();
}

#[tokio::test]
async fn receiver_routes_match_the_app_contract() {
    let dir = temp_dir("routes");
    let receiver = start_receiver(&dir, false).await;
    let base = format!("http://127.0.0.1:{}", receiver.port());
    let client = HttpClient::new();
    assert_eq!(receiver.info()["connectedPeers"], json!([]));

    let page = client
        .request(Method::GET, &base, Vec::new(), None, Duration::from_secs(5))
        .await
        .expect("page");
    assert_eq!(page.status(), 200);
    let page = http_body_util::BodyExt::collect(page.into_body())
        .await
        .expect("page body")
        .to_bytes();
    let page = String::from_utf8_lossy(&page);
    assert!(page.starts_with("<!doctype html>"));
    assert!(page.contains(r#"const BOOT = {"mode":"standalone""#));

    let preflight = client
        .request(
            Method::OPTIONS,
            &format!("{base}/cast/v1/command"),
            Vec::new(),
            None,
            Duration::from_secs(5),
        )
        .await
        .expect("preflight");
    assert_eq!(preflight.status(), 204);
    assert_eq!(
        preflight.headers()[&hyper::header::ACCESS_CONTROL_ALLOW_ORIGIN]
            .to_str()
            .unwrap(),
        "*"
    );

    let events = client
        .request(
            Method::GET,
            &format!("{base}/cast/v1/events"),
            Vec::new(),
            None,
            Duration::from_secs(5),
        )
        .await
        .expect("events");
    assert_eq!(events.status(), 401);

    let authorised = client
        .request(
            Method::GET,
            &format!("{base}/cast/v1/events?token={PAIR_TOKEN}"),
            Vec::new(),
            None,
            Duration::from_secs(5),
        )
        .await
        .expect("authorised events");
    assert_eq!(authorised.status(), 200);
    assert_eq!(
        authorised.headers()[&hyper::header::CONTENT_TYPE]
            .to_str()
            .unwrap(),
        "text/event-stream"
    );

    let web_events = client
        .request(
            Method::GET,
            &format!("{base}/cast/v1/web-events?token=wrong"),
            Vec::new(),
            None,
            Duration::from_secs(5),
        )
        .await
        .expect("web events");
    assert_eq!(web_events.status(), 401);

    let missing_stream = client
        .request(
            Method::GET,
            &format!("{base}/cast/v1/p2p/{}", "0".repeat(32)),
            Vec::new(),
            None,
            Duration::from_secs(5),
        )
        .await
        .expect("stream");
    assert_eq!(missing_stream.status(), 404);

    let missing_hybrid = client
        .request(
            Method::GET,
            &format!("{base}/cast/v1/hybrid/{}", "0".repeat(32)),
            Vec::new(),
            None,
            Duration::from_secs(5),
        )
        .await
        .expect("hybrid");
    assert_eq!(missing_hybrid.status(), 404);

    let unknown = client
        .json(
            Method::POST,
            &format!("{base}/cast/v1/nope"),
            Vec::new(),
            Some(&json!({})),
            Duration::from_secs(5),
        )
        .await
        .expect_err("unknown route");
    assert_eq!(unknown.status, 404);
    assert_eq!(unknown.message, "Not found");

    receiver.close().await;
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
#[ignore = "needs node and the app checkout (PEARTUBE_APP)"]
fn fixtures_are_current() {
    if !has_app() {
        eprintln!("skipped: set PEARTUBE_APP to the app checkout");
        return;
    }
    let output = Command::new("node")
        .arg(js_dir().join("cast-fixtures.mjs"))
        .current_dir(js_dir())
        .output()
        .expect("node must be installed");
    assert!(
        output.status.success(),
        "generator failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let generated: Value = serde_json::from_slice(&output.stdout).expect("fixture json");
    same_json(
        &generated,
        &fixtures(),
        "fixtures/cast-reference.json is stale; regenerate it",
    );
}
