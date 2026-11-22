//! The relay process (`bin/relay.js`'s port): a relay started without a tracker founds its own
//! channel and keeps it across restarts, with its API up on the configured address.

// Only the FLAC builder is used here.
#[allow(dead_code)]
#[path = "support/soulseek_network.rs"]
mod network;

use std::collections::HashMap;
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::sync::Arc;
use std::time::Duration;

use peartube_native::cast::adapters::NodePeer;
use peartube_native::cast::traits::CastPeer;
use peartube_native::cast::{Receiver, ReceiverOptions};
use peartube_native::node::core::{CoreNode, create_node};
use peartube_native::relay::Relay;
use serde_json::{Value, json};

fn get_json(addr: SocketAddr, path: &str) -> Value {
    request_json(addr, "GET", path, "")
}

fn request_json(addr: SocketAddr, method: &str, path: &str, body: &str) -> Value {
    let (status, answer) = try_request_json(addr, method, path, body);
    assert_eq!(status, 200, "{method} {path}: {answer}");
    answer
}

/// One request and its parsed body, whatever the status.
fn try_request_json(addr: SocketAddr, method: &str, path: &str, body: &str) -> (u16, Value) {
    let mut stream = TcpStream::connect(addr).expect("api listens");
    stream
        .set_read_timeout(Some(Duration::from_secs(40)))
        .unwrap();
    write!(
        stream,
        "{method} {path} HTTP/1.1\r\nHost: {addr}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    )
    .unwrap();
    let mut raw = String::new();
    stream.read_to_string(&mut raw).expect("a response");
    let (head, body) = raw.split_once("\r\n\r\n").expect("headers end");
    let status = head
        .lines()
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .and_then(|code| code.parse().ok())
        .expect("status");
    (status, serde_json::from_str(body).expect("JSON body"))
}

fn env(storage: &std::path::Path) -> HashMap<String, String> {
    env_on(storage, "127.0.0.1:9")
}

fn env_on(storage: &std::path::Path, bootstrap: &str) -> HashMap<String, String> {
    [
        ("PEARTUBE_STORAGE", storage.display().to_string()),
        ("PEARTUBE_API_HOST", "127.0.0.1".into()),
        ("PEARTUBE_API_PORT", "0".into()),
        ("PEARTUBE_STREAM_HOST", "127.0.0.1".into()),
        ("PEARTUBE_STREAM_PORT", "0".into()),
        ("PEARTUBE_HTTPS_PORT", "0".into()),
        ("PEARTUBE_BOOTSTRAP", bootstrap.to_owned()),
        ("PEARTUBE_LAN", "0".into()),
    ]
    .into_iter()
    .map(|(k, v)| (k.to_owned(), v))
    .collect()
}

#[tokio::test(flavor = "multi_thread")]
async fn a_relay_founds_its_channel_and_keeps_it_across_restarts() {
    let dir = tempfile::tempdir().unwrap();

    let relay = Relay::listen(env(dir.path())).await.expect("relay listens");
    let ready = relay.start().await.expect("node opens");
    let tracker = ready["tracker"].as_str().unwrap().to_owned();
    assert_eq!(
        ready["writer"], ready["tracker"],
        "a founded channel is written by this relay"
    );
    let addr = relay.addresses()[0];
    let status = tokio::task::spawn_blocking(move || get_json(addr, "/v1/status"))
        .await
        .unwrap();
    assert_eq!(status["tracker"], tracker.as_str());
    assert_eq!(status["writable"], true);
    relay.shutdown().await;

    let settings: Value =
        serde_json::from_str(&std::fs::read_to_string(dir.path().join("settings.json")).unwrap())
            .unwrap();
    assert_eq!(
        settings["channel"],
        tracker.as_str(),
        "the founded channel is saved"
    );

    let again = Relay::listen(env(dir.path()))
        .await
        .expect("relay listens again");
    let reopened = again.start().await.expect("node reopens");
    assert_eq!(
        reopened["tracker"],
        tracker.as_str(),
        "a restart keeps the same channel"
    );
    again.shutdown().await;
}

/// A live setting applies in place and the request returns: applying reads the settings the
/// request just saved.
#[tokio::test(flavor = "multi_thread")]
async fn a_live_setting_applies_without_a_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let relay = Relay::listen(env(dir.path())).await.expect("relay listens");
    let ready = relay.start().await.expect("node opens");
    let addr = relay.addresses()[0];
    let updated = tokio::task::spawn_blocking(move || {
        request_json(addr, "PUT", "/v1/settings", r#"{"acquireConcurrency":2}"#)
    })
    .await
    .unwrap();
    assert_eq!(
        updated["changed"],
        serde_json::json!(["acquireConcurrency"])
    );
    assert_eq!(updated["warning"], Value::Null);
    assert_eq!(
        relay.status().await.expect("still open")["writer"],
        ready["writer"]
    );
    relay.shutdown().await;
}

#[cfg(unix)]
#[derive(PartialEq)]
enum ReopenCase {
    Healthy,
    RestartFails,
    BusyPort,
    Recovers,
}

#[cfg(unix)]
async fn relay_log<R: tokio::io::AsyncBufRead + Unpin>(
    lines: &mut tokio::io::Lines<R>,
    message: &str,
) -> Value {
    tokio::time::timeout(Duration::from_secs(60), async {
        loop {
            let line = lines.next_line().await.unwrap().expect("relay stopped");
            if let Ok(value) = serde_json::from_str::<Value>(&line)
                && value["msg"] == message
            {
                break value;
            }
        }
    })
    .await
    .unwrap_or_else(|_| panic!("relay did not report {message}"))
}

#[cfg(unix)]
async fn check_http_reopen(case: ReopenCase) {
    use peartube_native::private_overlay::{dht::DhtOptions, peeroxide::open_dht, to_hex};
    use std::os::unix::fs::symlink;
    use std::process::Stdio;
    use tokio::io::{AsyncBufReadExt, BufReader};

    let bootstrap = open_dht(DhtOptions {
        host: Some("127.0.0.1".into()),
        port: 0,
        bootstrap: Vec::new(),
    })
    .await
    .unwrap();
    let dir = tempfile::tempdir().unwrap();
    let import = dir.path().join("import");
    let database = dir.path().join("database");
    let link = dir.path().join("corestore");
    std::fs::create_dir(&database).unwrap();
    symlink(&database, &link).unwrap();
    std::fs::create_dir(&import).unwrap();
    std::fs::write(import.join("Film tt1234567.mkv"), b"fixture video").unwrap();
    std::fs::write(
        dir.path().join("settings.json"),
        r#"{"privateGuard":true,"privateGuardPort":0,"lan":true,"lanHost":"127.0.0.1"}"#,
    )
    .unwrap();
    let followed = to_hex(&hypercore::crypto::KeyPair::from_seed(&[0x33; 32]).public_key);
    let mut env = env_on(
        dir.path(),
        &format!("127.0.0.1:{}", bootstrap.address().unwrap().port()),
    );
    for (key, value) in [
        ("PEARTUBE_PRIVATE_HOST", "127.0.0.1"),
        ("PEARTUBE_REPLICATE", "1"),
        ("PEARTUBE_PUBLISH_CHANNEL", "0"),
        ("PEARTUBE_SOURCES", &followed),
        ("PEARTUBE_WATCH_DIR", import.to_str().unwrap()),
    ] {
        env.insert(key.into(), value.into());
    }
    let addr = std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap();
    let mut child = tokio::process::Command::new(env!("CARGO_BIN_EXE_peartube-relay"))
        .kill_on_drop(true)
        .env_clear()
        .envs(env)
        .env("PEARTUBE_API_PORT", addr.port().to_string())
        .env("HOME", dir.path())
        .env("TMPDIR", dir.path())
        .current_dir(dir.path())
        .stdout(Stdio::piped())
        .spawn()
        .unwrap();
    let mut lines = BufReader::new(child.stdout.take().unwrap()).lines();
    let ready = relay_log(&mut lines, "relay ready").await;
    let status = get_json(addr, "/v1/status");
    assert_eq!(status["writer"], ready["writer"]);
    assert_eq!(status["lan"]["host"], "127.0.0.1");
    assert!(status["privateGuard"]["port"].as_u64().unwrap() > 0);
    assert!(
        status["sources"]
            .as_array()
            .unwrap()
            .iter()
            .any(|s| s["key"] == followed)
    );
    let entry_hash = |entries: Value| {
        let row = entries["results"]
            .as_array()
            .unwrap()
            .iter()
            .find(|row| row["id"] == "imdb:tt1234567")
            .expect("imported film");
        row["sha256"].as_str().unwrap().to_owned()
    };
    let hash = entry_hash(get_json(addr, "/v1/entries"));
    // A reopened node must read this entry from storage, not import the file a second time.
    std::fs::remove_file(import.join("Film tt1234567.mkv")).unwrap();
    let restart = ("POST", "/v1/restart", "");
    let settings = ("PUT", "/v1/settings", r#"{"hiverelayReplicas":4}"#);
    if case == ReopenCase::BusyPort {
        let busy = std::net::UdpSocket::bind((addr.ip(), 0)).unwrap();
        let busy_addr = busy.local_addr().unwrap();
        let cause = std::net::UdpSocket::bind(busy_addr)
            .unwrap_err()
            .to_string();
        let patch = json!({ "dhtPort": busy_addr.port() });
        let saved = request_json(addr, "PUT", "/v1/settings", &patch.to_string());
        assert_eq!(saved["changed"], json!(["dhtPort"]));
        assert_eq!(saved["restartNeeded"], json!(["dhtPort"]));
        assert!(
            saved["warning"].as_str().unwrap().contains(&cause),
            "{saved}"
        );
        let file = std::fs::File::open(dir.path().join("settings.json")).unwrap();
        assert_eq!(
            serde_json::from_reader::<_, Value>(file).unwrap()["dhtPort"],
            busy_addr.port()
        );

        // Keep the port occupied beyond the retry. Only the corrective PUT can restore the node.
        tokio::time::sleep(Duration::from_secs(2)).await;
        let pending = get_json(addr, "/v1/settings");
        assert_eq!(pending["settings"]["dhtPort"]["value"], busy_addr.port());
        assert_eq!(pending["restartNeeded"], json!(["dhtPort"]));
        let fixed = request_json(addr, "PUT", "/v1/settings", r#"{"dhtPort":0}"#);
        assert!(fixed.get("warning").is_none(), "{fixed}");
        assert_eq!(get_json(addr, "/v1/settings")["restartNeeded"], json!([]));
        assert_eq!(entry_hash(get_json(addr, "/v1/entries")), hash);
        drop(busy);
    } else if case != ReopenCase::Healthy {
        let (method, path, body) = restart;
        let broken = dir.path().join("not-a-directory");
        std::fs::write(&broken, b"not a directory").unwrap();
        let swap = dir.path().join("swap");
        symlink(broken, &swap).unwrap();
        std::fs::rename(&swap, &link).unwrap();
        let cause = std::fs::create_dir_all(&link).unwrap_err();
        let (status, reply) = try_request_json(addr, method, path, body);
        assert_eq!(status, 400, "{method} {path}: {reply}");
        assert_eq!(reply, json!({"error": format!("io: {cause}")}));
        if case == ReopenCase::RestartFails {
            relay_log(&mut lines, "restart recovery failed").await;
            let (status, reply) = try_request_json(addr, "GET", "/v1/entries", "");
            assert_eq!(status, 503, "{reply}");
            assert_eq!(reply, json!({"error": "The relay node is unavailable"}));
        }
        symlink(&database, &swap).unwrap();
        std::fs::rename(&swap, &link).unwrap();
        if case == ReopenCase::RestartFails {
            request_json(addr, method, path, body);
        }
        tokio::time::timeout(Duration::from_secs(15), async {
            loop {
                let (status, entries) = try_request_json(addr, "GET", "/v1/entries", "");
                if status == 200 {
                    assert_eq!(entry_hash(entries), hash);
                    break;
                }
                assert_eq!(status, 503, "{entries}");
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        })
        .await
        .expect("entry restored");
        assert!(
            get_json(addr, "/v1/status")["privateGuard"]["port"]
                .as_u64()
                .unwrap()
                > 0
        );
    } else {
        for (method, path, body) in [restart, restart, settings] {
            let reply = request_json(addr, method, path, body);
            assert!(reply.get("warning").is_none(), "{reply}");
            assert_eq!(reply["restartNeeded"], json!([]));
            assert_eq!(entry_hash(get_json(addr, "/v1/entries")), hash);
        }
        let settings = get_json(addr, "/v1/settings");
        assert_eq!(settings["settings"]["hiverelayReplicas"]["value"], 4);
        assert_eq!(settings["restartNeeded"], json!([]));
    }
    child.kill().await.unwrap();
    bootstrap.destroy().await.unwrap();
}

#[cfg(unix)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn http_reopens_keep_the_imported_entry() {
    check_http_reopen(ReopenCase::Healthy).await;
}

#[cfg(unix)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn failed_http_reopen_on_restart() {
    check_http_reopen(ReopenCase::RestartFails).await;
}

#[cfg(unix)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn busy_port_http_reopen_can_be_corrected_through_settings() {
    check_http_reopen(ReopenCase::BusyPort).await;
}

#[cfg(unix)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn failed_http_reopen_recovers_without_another_restart() {
    check_http_reopen(ReopenCase::Recovers).await;
}

/// The relay's library from its API: an album folder in the import folder is listed as tracks
/// that carry their own names, the owner removes them and only the files a mirror put there go
/// with them, and the Soulseek provider follows its live settings.
#[tokio::test(flavor = "multi_thread")]
async fn an_album_is_imported_listed_and_removed_and_soulseek_follows_its_settings() {
    let dir = tempfile::tempdir().unwrap();
    let import = dir.path().join("import");
    let tags = |track: &str, title: &str| -> Vec<(String, String)> {
        [
            ("ALBUM", "Blue Train"),
            ("ARTIST", "John Coltrane"),
            ("TITLE", title),
            ("TRACKNUMBER", track),
            (
                "MUSICBRAINZ_ALBUMID",
                "01234567-89ab-cdef-0123-456789abcdef",
            ),
        ]
        .map(|(key, value)| (key.to_owned(), value.to_owned()))
        .to_vec()
    };
    // The owner's album, and a folder as a mirror leaves it: pinned to its release.
    let (owned, mirrored) = (import.join("Blue Train"), import.join("Mirrored"));
    std::fs::create_dir_all(&owned).unwrap();
    std::fs::create_dir_all(&mirrored).unwrap();
    let first = owned.join("01 - Blue Train.flac");
    std::fs::write(&first, network::flac(&tags("1", "Blue Train"), 4000)).unwrap();
    std::fs::write(
        owned.join("02 - Moment.flac"),
        network::flac(&tags("2", "Moment's Notice"), 4100),
    )
    .unwrap();
    std::fs::write(mirrored.join("05 Naima.flac"), "not a tagged file").unwrap();
    std::fs::write(
        mirrored.join(".peartube-album.json"),
        r#"{"release":"discogs:r249504","title":"Giant Steps"}"#,
    )
    .unwrap();

    let mut env = env(&dir.path().join("storage"));
    env.insert("PEARTUBE_WATCH_DIR".into(), import.display().to_string());
    let relay = Relay::listen(env).await.expect("relay listens");
    relay.start().await.expect("node opens");
    let addr = relay.addresses()[0];
    let call = |method: &'static str, path: &'static str, body: Value| async move {
        tokio::task::spawn_blocking(move || {
            let body = if method == "GET" {
                String::new()
            } else {
                body.to_string()
            };
            try_request_json(addr, method, path, &body)
        })
        .await
        .unwrap()
    };
    let plugin = |plugins: &Value, name: &str| -> Value {
        let listed = plugins["plugins"].as_array().expect("plugins");
        let found = listed.iter().find(|plugin| plugin["name"] == name);
        found.unwrap_or_else(|| panic!("no {name} plugin")).clone()
    };

    // The start imported both folders.
    let (_, plugins) = call("GET", "/v1/plugins", Value::Null).await;
    assert_eq!(plugin(&plugins, "watch-dir")["trackedFiles"], 3);
    let soulseek = plugin(&plugins, "soulseek");
    assert_eq!(soulseek["status"], "not configured");
    assert_eq!(soulseek["staging"], "import folder");
    let (_, listing) = call("GET", "/v1/entries", Value::Null).await;
    let release = "mb:0123456789abcdef0123456789abcdef";
    let rows = listing["results"].as_array().expect("results").clone();
    let row = |id: &str| -> Value {
        let found = rows.iter().find(|row| row["id"] == id);
        found
            .unwrap_or_else(|| panic!("no entry {id} in {listing}"))
            .clone()
    };
    let (one, two) = (
        row(&format!("{release}:s01e01")),
        row(&format!("{release}:s01e02")),
    );
    assert_eq!(one["title"], "John Coltrane · Blue Train");
    assert_eq!(
        one["music"],
        json!({ "title": "Blue Train", "artist": "John Coltrane" })
    );
    assert_eq!(two["music"]["title"], "Moment's Notice");
    assert_eq!((&one["season"], &one["episode"]), (&json!(1), &json!(1)));
    assert_eq!(one["seriesKey"], two["seriesKey"], "one album");
    let pinned = row("discogs:r249504:s01e05");
    assert_eq!(pinned["title"], "Giant Steps");
    assert_eq!(pinned["music"], json!({ "title": "Naima" }));

    // The provider is the relay's, and its sign-in is a live setting.
    let search = json!({ "query": "coltrane" });
    let (status, refused) = call("POST", "/v1/providers/soulseek/search", search).await;
    assert_eq!(
        (status, &refused["error"]),
        (400, &json!("Soulseek is not configured"))
    );
    let (status, unknown) = call("POST", "/v1/providers/nope/browse", json!({})).await;
    assert_eq!(
        (status, &unknown["error"]),
        (404, &json!("Unknown provider: nope"))
    );
    let sign_in = json!({ "slskUser": "alice", "slskPass": "hunter2secret" });
    let (status, saved) = call("PUT", "/v1/settings", sign_in).await;
    assert_eq!(status, 200, "{saved}");
    assert_eq!(saved["restartNeeded"], json!([]), "{saved}");
    assert!(
        !saved.to_string().contains("hunter2secret"),
        "the password stays in the file"
    );
    let (_, plugins) = call("GET", "/v1/plugins", Value::Null).await;
    let soulseek = plugin(&plugins, "soulseek");
    assert_eq!(soulseek["status"], "configured");
    assert_eq!(
        (&soulseek["configured"], &soulseek["available"]),
        (&json!(true), &json!(true))
    );
    assert_eq!(
        call("GET", "/v1/jobs", Value::Null).await.1,
        json!({ "jobs": [] })
    );

    // The owner removes the album and the mirrored track. The owner's files stay; the mirror's go.
    let (status, gone) = call(
        "POST",
        "/v1/entries/remove",
        json!({ "seriesKey": one["seriesKey"], "ids": ["discogs:r249504:s01e05"] }),
    )
    .await;
    assert_eq!(status, 200, "{gone}");
    assert_eq!(gone["notRemoved"], json!([]));
    // Each removed track by its own name, with what became of its file.
    let mut fate: Vec<(String, String)> = gone["removed"]
        .as_array()
        .expect("removed")
        .iter()
        .map(|row| {
            let text = |key: &str| row[key].as_str().unwrap_or_default().to_owned();
            (text("title"), text("file"))
        })
        .collect();
    fate.sort();
    let expected = [
        ("Blue Train", "kept"),
        ("Moment's Notice", "kept"),
        ("Naima", "deleted"),
    ];
    assert_eq!(
        fate,
        expected.map(|(title, file)| (title.to_owned(), file.to_owned()))
    );
    assert_eq!(gone["filesDeleted"], 1);
    assert!(first.exists(), "the owner's file stays");
    assert!(
        !mirrored.exists(),
        "the mirrored folder goes with its last file"
    );
    let (_, listing) = call("GET", "/v1/entries", Value::Null).await;
    assert_eq!(listing["results"], json!([]));
    // The files that stayed are not imported again.
    let (_, swept) = call("POST", "/v1/plugins/watch-dir/run", json!({})).await;
    assert_eq!(
        (&swept["scanned"], &swept["ingested"]),
        (&json!(2), &json!(0))
    );
    relay.shutdown().await;
}

/// Sends one request and returns its connection without reading the answer.
fn begin(addr: SocketAddr, method: &str, path: &str, body: &str) -> TcpStream {
    let mut stream = TcpStream::connect(addr).expect("api listens");
    write!(
        stream,
        "{method} {path} HTTP/1.1\r\nHost: {addr}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    )
    .unwrap();
    stream
}

/// Waits until a reopen has taken the node away. From then until the new node is open the relay
/// has none, so a step that is cut now is cut in the middle.
async fn node_taken_away(relay: &Relay) {
    let asked = std::time::Instant::now();
    while relay.status().await.is_some() {
        assert!(
            asked.elapsed() < Duration::from_secs(20),
            "the reopen never took the node away"
        );
        tokio::time::sleep(Duration::from_micros(200)).await;
    }
}

/// A port that was free a moment ago.
fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .and_then(|listener| listener.local_addr())
        .expect("a free port")
        .port()
}

/// The films of an import folder, in MiB and in the order the import takes them.
const FILMS: [usize; 5] = [1, 8, 8, 8, 8];
/// An import that ends with its first large film, so that a reopened relay has none left.
const TWO_FILMS: [usize; 2] = [1, 8];

/// An import folder that keeps the folder import busy for a known time. The import takes the
/// films in name order: when the first one is listed, the import is in the large films, about
/// 0.3 s each in a debug build. A close of the plugin host waits for the film in the import and
/// for no other, so the tests below act in that wait and count the films that were left.
fn import_folder(dir: &std::path::Path, films: &[usize]) -> std::path::PathBuf {
    let folder = dir.join("import");
    std::fs::create_dir_all(&folder).unwrap();
    for (index, mib) in films.iter().enumerate() {
        // Each film has its own bytes: two equal files are one entry.
        let mut state = 0x9E37_79B9_7F4A_7C15u64 + index as u64;
        let mut bytes = vec![0u8; mib << 20];
        for chunk in bytes.chunks_mut(8) {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            chunk.copy_from_slice(&state.to_le_bytes()[..chunk.len()]);
        }
        std::fs::write(folder.join(format!("Film {index}.mp4")), bytes).unwrap();
    }
    folder
}

/// The settings of a relay on `<dir>/data` that imports `<dir>/import`.
fn env_importing(dir: &std::path::Path) -> HashMap<String, String> {
    let mut env = env(&dir.join("data"));
    env.insert(
        "PEARTUBE_WATCH_DIR".into(),
        dir.join("import").display().to_string(),
    );
    env
}

/// The titles the relay lists. `None`: the API does not listen yet. A relay without a node lists
/// nothing.
fn listed(addr: SocketAddr) -> Option<Vec<String>> {
    let mut stream = TcpStream::connect(addr).ok()?;
    stream
        .set_read_timeout(Some(Duration::from_secs(40)))
        .ok()?;
    write!(
        stream,
        "GET /v1/entries?limit=100 HTTP/1.1\r\nHost: {addr}\r\nConnection: close\r\n\r\n"
    )
    .ok()?;
    let mut raw = String::new();
    stream.read_to_string(&mut raw).ok()?;
    let (head, body) = raw.split_once("\r\n\r\n")?;
    if !head.starts_with("HTTP/1.1 200") {
        return None;
    }
    let answer: Value = serde_json::from_str(body).ok()?;
    Some(
        answer["results"]
            .as_array()?
            .iter()
            .map(|entry| entry["title"].as_str().unwrap_or_default().to_owned())
            .collect(),
    )
}

/// Waits until the relay lists at least `count` films, and returns their titles.
async fn listed_at_least(addr: SocketAddr, count: usize) -> Vec<String> {
    let asked = std::time::Instant::now();
    loop {
        let titles = tokio::task::spawn_blocking(move || listed(addr))
            .await
            .unwrap();
        if let Some(titles) = titles.filter(|titles| titles.len() >= count) {
            return titles;
        }
        assert!(
            asked.elapsed() < Duration::from_secs(60),
            "the relay never listed {count} films"
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

/// A relay in its startup import: the first film is listed and a large one is in the import.
/// Returns the relay, its first open (which waits for the plugins to start) and its API.
async fn importing(
    dir: &std::path::Path,
    films: &[usize],
) -> (
    Relay,
    tokio::task::JoinHandle<Result<Value, String>>,
    SocketAddr,
) {
    importing_with(dir, films, env_importing(dir)).await
}

/// `importing`, for a relay with more settings than `env_importing` gives.
async fn importing_with(
    dir: &std::path::Path,
    films: &[usize],
    env: HashMap<String, String>,
) -> (
    Relay,
    tokio::task::JoinHandle<Result<Value, String>>,
    SocketAddr,
) {
    import_folder(dir, films);
    let relay = Relay::listen(env).await.expect("relay listens");
    let addr = relay.addresses()[0];
    let opening = tokio::spawn({
        let relay = relay.clone();
        async move { relay.start().await }
    });
    listed_at_least(addr, 1).await;
    (relay, opening, addr)
}

/// Waits until the relay has saved `key` as `value` in `<dir>/data/settings.json`, and a moment
/// more. The relay saves a change and then puts it in line to apply, so from then on the change
/// is applying or it waits its turn.
async fn saved(dir: &std::path::Path, key: &str, value: Value) {
    let file = dir.join("data").join("settings.json");
    let asked = std::time::Instant::now();
    loop {
        let settings: Value = std::fs::read_to_string(&file)
            .ok()
            .and_then(|text| serde_json::from_str(&text).ok())
            .unwrap_or(Value::Null);
        if settings[key] == value {
            break;
        }
        assert!(
            asked.elapsed() < Duration::from_secs(20),
            "the relay never saved {key}"
        );
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
    tokio::time::sleep(Duration::from_millis(40)).await;
}

/// Opens `<dir>/data` again, without the import folder. It fails when a stopped relay still holds
/// the storage.
async fn opened_again(dir: &std::path::Path, when: &str) -> Relay {
    let relay = Relay::listen(env(&dir.join("data")))
        .await
        .expect("relay listens again");
    relay
        .start()
        .await
        .unwrap_or_else(|error| panic!("{when}: the storage is still held: {error}"));
    relay
}

/// What a stopped relay left in `<dir>/data`: the films that a relay without the import folder
/// lists there.
async fn left_in(dir: &std::path::Path, when: &str) -> Vec<String> {
    let relay = opened_again(dir, when).await;
    let left = listed_at_least(relay.addresses()[0], 1).await;
    relay.shutdown().await;
    left
}

/// Plugins start after the node answers, as in `bin/relay.js`: a restart during the startup
/// import waits for the film in the import, not for the whole import. The import then goes on
/// where it stopped, and every film is imported once.
#[tokio::test(flavor = "multi_thread")]
async fn a_restart_during_the_startup_import_waits_for_one_film_and_imports_each_film_once() {
    let dir = tempfile::tempdir().unwrap();
    let (relay, opening, addr) = importing(dir.path(), &FILMS).await;

    tokio::task::spawn_blocking(move || request_json(addr, "POST", "/v1/restart", ""))
        .await
        .unwrap();
    let at_the_answer = listed_at_least(addr, 1).await;
    assert!(
        at_the_answer.len() < FILMS.len(),
        "the restart waited for the whole import: {at_the_answer:?}"
    );

    // The plugins are listed once they have started, which is after the import.
    let asked = std::time::Instant::now();
    let import = loop {
        let plugins = tokio::task::spawn_blocking(move || get_json(addr, "/v1/plugins"))
            .await
            .unwrap();
        let import = plugins["plugins"]
            .as_array()
            .and_then(|plugins| plugins.iter().find(|plugin| plugin["name"] == "watch-dir"))
            .cloned();
        if let Some(import) = import {
            break import;
        }
        assert!(
            asked.elapsed() < Duration::from_secs(60),
            "the plugins never started after the restart"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    };
    let mut titles = listed_at_least(addr, FILMS.len()).await;
    titles.sort();
    let every_film: Vec<String> = (0..FILMS.len())
        .map(|film| format!("Film {film}"))
        .collect();
    assert_eq!(titles, every_film, "every film is listed once");
    // The second import took only the films that the first one had not reached.
    assert_eq!(
        import["lastResult"]["scanned"],
        json!(FILMS.len()),
        "{import}"
    );
    assert_eq!(
        import["lastResult"]["ingested"],
        json!(FILMS.len() - at_the_answer.len()),
        "{import}"
    );

    opening.await.unwrap().expect("the first open");
    relay.shutdown().await;
    assert_eq!(relay.status().await, None);
}

/// A stop during the startup import (`relay::run`'s `stop`, a signal in the process) ends the
/// import between films and closes the node: the flow ends with exit code 0, and the films after
/// the one in the import are not imported.
#[tokio::test(flavor = "multi_thread")]
async fn a_stop_during_the_startup_import_ends_it_between_films() {
    let dir = tempfile::tempdir().unwrap();
    import_folder(dir.path(), &FILMS);
    let port = free_port();
    let mut env = env_importing(dir.path());
    env.insert("PEARTUBE_API_PORT".into(), port.to_string());
    // The stop arrives when the first film is listed.
    let stop = async move {
        listed_at_least(SocketAddr::from(([127, 0, 0, 1], port)), 1).await;
    };
    let code = peartube_native::relay::run(env, stop).await;
    assert_eq!(code, 0, "a stop during the startup import");
    let left = left_in(dir.path(), "a stop during the startup import").await;
    assert!(
        left.len() < FILMS.len(),
        "the import went on after the stop: {left:?}"
    );
}

/// A live setting for the plugins waits until they have started, and it holds the lifecycle
/// while it waits. A shutdown still ends the startup import between films: it closes the plugin
/// host before it waits for the step in flight.
#[tokio::test(flavor = "multi_thread")]
async fn a_shutdown_ends_the_startup_import_under_a_setting_that_waits_for_the_plugins() {
    let dir = tempfile::tempdir().unwrap();
    let (relay, opening, addr) = importing(dir.path(), &FILMS).await;
    let waiting = begin(addr, "PUT", "/v1/settings", r#"{"replicateMbps":5}"#);
    tokio::time::sleep(Duration::from_millis(40)).await;

    relay.shutdown().await;
    assert_eq!(
        relay.status().await,
        None,
        "no node is left after the shutdown"
    );
    drop(waiting);
    let _ = opening.await.unwrap();
    let left = left_in(dir.path(), "a shutdown under a waiting setting").await;
    assert!(
        left.len() < FILMS.len(),
        "the shutdown waited for the whole import: {left:?}"
    );
}

/// Once a shutdown has begun, a lifecycle step that waited its turn does not begin, as in
/// `bin/relay.js` (`serial`). A live setting for the plugins holds the lifecycle while it waits
/// for them, and a second change waits behind it. The second change turns the `directory`
/// setting on, which lists the followed channel and records that in `directory.json`. After the
/// shutdown that record is still empty: the saved value applies at the next start.
#[tokio::test(flavor = "multi_thread")]
async fn a_change_that_waited_its_turn_does_not_begin_after_a_shutdown() {
    let dir = tempfile::tempdir().unwrap();
    // A followed channel that no peer holds.
    let mut env = env_importing(dir.path());
    env.insert("PEARTUBE_SOURCES".into(), "ab".repeat(32));
    let (relay, opening, addr) = importing_with(dir.path(), &FILMS, env).await;
    let record = dir.path().join("data").join("directory.json");
    let listed_by_the_relay = || -> Value {
        serde_json::from_str(&std::fs::read_to_string(&record).unwrap_or_default())
            .unwrap_or(Value::Null)
    };
    assert_eq!(
        listed_by_the_relay(),
        json!([]),
        "the relay lists no channel while the setting is off"
    );

    let first = begin(addr, "PUT", "/v1/settings", r#"{"replicateMbps":5}"#);
    saved(dir.path(), "replicateMbps", json!(5)).await;
    let behind = begin(addr, "PUT", "/v1/settings", r#"{"directory":true}"#);
    saved(dir.path(), "directory", json!(true)).await;

    relay.shutdown().await;
    assert_eq!(
        relay.status().await,
        None,
        "no node is left after the shutdown"
    );
    let after_the_shutdown = listed_by_the_relay();
    drop((first, behind));
    let _ = opening.await.unwrap();
    // The first change still waited for the plugins when the shutdown came: the import had not
    // ended, so the second change did not have its turn before the shutdown.
    let left = left_in(dir.path(), "a shutdown with a change in line").await;
    assert!(
        left.len() < FILMS.len(),
        "the import ended before the shutdown, so the second change was not in line: {left:?}"
    );
    assert_eq!(
        after_the_shutdown,
        json!([]),
        "the change that waited its turn began after the shutdown"
    );
}

/// A lifecycle step that started always finishes, as a promise does in `bin/relay.js`. The client
/// that asked for a restart, for a setting that reopens the node, or for a recovery onto another
/// channel hangs up while the reopen waits for the film in the import, which is in the middle of
/// its close. The relay still comes back whole, and its shutdown closes the node, so the same
/// storage opens again.
#[tokio::test(flavor = "multi_thread")]
async fn a_reopen_finishes_when_its_client_hangs_up() {
    // The phrase of all-zero entropy: a channel that is not this relay's, so the relay reopens on
    // it. The request is never answered here, because no peer holds that channel.
    let recovery = r#"{"mnemonic":"abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon about"}"#;
    for (method, path, body, same_channel) in [
        ("POST", "/v1/restart", "", true),
        (
            "PUT",
            "/v1/settings",
            r#"{"bootstrap":["127.0.0.1:10"]}"#,
            true,
        ),
        ("POST", "/v1/identity/recover", recovery, false),
    ] {
        let dir = tempfile::tempdir().unwrap();
        let (relay, opening, addr) = importing(dir.path(), &TWO_FILMS).await;
        let channel = relay.status().await.expect("a node is open")["tracker"].clone();

        let asking = begin(addr, method, path, body);
        tokio::time::sleep(Duration::from_millis(40)).await;
        drop(asking);
        // A restart waits for the reopen before it, so its answer comes after both.
        tokio::task::spawn_blocking(move || request_json(addr, "POST", "/v1/restart", ""))
            .await
            .unwrap();
        let now = relay.status().await.expect("a node is open again")["tracker"].clone();
        assert_eq!(
            now == channel,
            same_channel,
            "{method} {path}: the relay came back on {now}, from {channel}"
        );

        relay.shutdown().await;
        assert_eq!(
            relay.status().await,
            None,
            "{method} {path}: no node is left after a shutdown"
        );
        let _ = opening.await.unwrap();
        opened_again(dir.path(), &format!("{method} {path}"))
            .await
            .shutdown()
            .await;
    }
}

/// A shutdown that arrives while a restart is in flight waits for the whole stop: when it returns,
/// no node is left and the storage is free. `bin/relay.js` exited with its store open here when a
/// plugin took long to stop.
#[tokio::test(flavor = "multi_thread")]
async fn a_shutdown_during_a_restart_closes_the_node() {
    // First with the startup import in flight: the restart waits for the film in the import, so
    // the shutdown arrives in the middle of its close on a machine of any speed.
    {
        let dir = tempfile::tempdir().unwrap();
        let (relay, opening, addr) = importing(dir.path(), &TWO_FILMS).await;
        let waiting = begin(addr, "POST", "/v1/restart", "");
        tokio::time::sleep(Duration::from_millis(40)).await;
        relay.shutdown().await;
        assert_eq!(
            relay.status().await,
            None,
            "a node is left after a shutdown during a restart that waits for the import"
        );
        drop(waiting);
        let _ = opening.await.unwrap();
        opened_again(dir.path(), "a shutdown during a restart")
            .await
            .shutdown()
            .await;
    }

    // Then without an import, where the moment depends on the machine: the shutdown arrives as
    // the restart is asked for, and at three moments (ms) after the restart took the node away.
    let dir = tempfile::tempdir().unwrap();
    // Kept to the end: a relay that is dropped lets go of a node it failed to close.
    let mut relays = Vec::new();
    for moment in [None, Some(0), Some(4), Some(9)] {
        let relay = Relay::listen(env(dir.path())).await.expect("relay listens");
        relay.start().await.unwrap_or_else(|error| {
            panic!("the storage is not free before the {moment:?} round: {error}")
        });
        let waiting = begin(relay.addresses()[0], "POST", "/v1/restart", "");
        if let Some(later) = moment {
            node_taken_away(&relay).await;
            tokio::time::sleep(Duration::from_millis(later)).await;
        }
        relay.shutdown().await;
        assert_eq!(
            relay.status().await,
            None,
            "a node is left after a shutdown at {moment:?} of a restart"
        );
        drop(waiting);
        relays.push(relay);
    }
    let relay = Relay::listen(env(dir.path())).await.expect("relay listens");
    relay
        .start()
        .await
        .expect("the last shutdown closed its node");
    relay.shutdown().await;
}

/// A stop that arrives while the node opens (`relay::run`'s `stop`, a signal in the process): the
/// open finishes on its own task, the shutdown closes its node, and the flow ends with exit code
/// 0. The storage opens again at once.
#[tokio::test(flavor = "multi_thread")]
async fn a_stop_while_the_node_opens_closes_it() {
    // The stop arrives before the open, then at three moments (ms) after the open made its store.
    for moment in [None, Some(0), Some(4), Some(9)] {
        let dir = tempfile::tempdir().unwrap();
        let store = dir.path().join("corestore");
        let stop = async {
            let Some(later) = moment else { return };
            while !store.exists() {
                tokio::time::sleep(Duration::from_micros(200)).await;
            }
            tokio::time::sleep(Duration::from_millis(later)).await;
        };
        let code = peartube_native::relay::run(env(dir.path()), stop).await;
        assert_eq!(code, 0, "a stop at {moment:?} of the open");
        let relay = Relay::listen(env(dir.path())).await.expect("relay listens");
        relay.start().await.unwrap_or_else(|error| {
            panic!("a stop at {moment:?} of the open left the storage open: {error}")
        });
        relay.shutdown().await;
    }
}

/// A stop that arrives while the node opens closes the plugin host as soon as it exists, as
/// `bin/relay.js` does in `onstarting`: the folder import never starts. The test runs on one
/// thread and without the channel backfill, so that the task that starts the plugins reaches the
/// folder import before the shutdown gets the node: a host that is still open then has its first
/// film in the import before its close.
#[tokio::test]
async fn a_stop_while_the_node_opens_starts_no_import() {
    let dir = tempfile::tempdir().unwrap();
    import_folder(dir.path(), &TWO_FILMS);
    let mut env = env_importing(dir.path());
    env.insert("PEARTUBE_PUBLISH_CHANNEL".into(), "0".into());
    let store = dir.path().join("data").join("corestore");
    let stop = async {
        while !store.exists() {
            tokio::time::sleep(Duration::from_micros(200)).await;
        }
    };
    let code = peartube_native::relay::run(env, stop).await;
    assert_eq!(code, 0);
    // The host saves a row here for every file it imports.
    let ledger = std::fs::read_to_string(dir.path().join("data").join("mirror-watch.json"))
        .unwrap_or_default();
    let rows: Value = serde_json::from_str(&ledger).unwrap_or(Value::Null);
    assert!(
        rows.as_object().is_none_or(|rows| rows.is_empty()),
        "the folder import started: {ledger}"
    );
}

/// The process itself: a SIGTERM while the node opens ends it with exit code 0, as a clean stop,
/// not by the signal. A relay that was told to stop starts no private guard.
#[cfg(unix)]
#[tokio::test(flavor = "multi_thread")]
async fn the_relay_process_stops_cleanly_on_a_signal_while_its_node_opens() {
    use std::process::{Command, Stdio};

    let dir = tempfile::tempdir().unwrap();
    // The API accepting on its port says the open is next.
    let port = free_port();
    let mut env = env(dir.path());
    env.insert("PEARTUBE_API_PORT".into(), port.to_string());
    let mut relay = Command::new(env!("CARGO_BIN_EXE_peartube-relay"))
        .envs(&env)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .expect("relay process");
    let addr = SocketAddr::from(([127, 0, 0, 1], port));
    let spawned = std::time::Instant::now();
    while TcpStream::connect(addr).is_err() {
        if spawned.elapsed() > Duration::from_secs(20) {
            let _ = relay.kill();
            panic!("the API never listened");
        }
        std::thread::sleep(Duration::from_millis(2));
    }
    let _ = Command::new("kill")
        .arg("-TERM")
        .arg(relay.id().to_string())
        .status();
    let output = tokio::task::spawn_blocking(move || relay.wait_with_output())
        .await
        .unwrap()
        .expect("the relay exits");
    let printed = String::from_utf8_lossy(&output.stdout);
    assert_eq!(
        output.status.code(),
        Some(0),
        "the relay ended with {:?}; it printed: {printed}",
        output.status
    );
    assert!(
        !printed.contains("private guard failed to start"),
        "the relay tried to start its guard after the signal: {printed}"
    );
}

/// A child process that is killed when the test ends before the process does.
#[cfg(unix)]
struct Running(Option<std::process::Child>);

#[cfg(unix)]
impl Drop for Running {
    fn drop(&mut self) {
        if let Some(mut child) = self.0.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

/// The process itself: the startup's guard step and several restarts wait their turn, and a
/// SIGTERM refuses them there. The relay does not report a guard that failed to start, as
/// `bin/relay.js` does not, nor a restart that failed, and it ends with exit code 0.
///
/// The first open waits for the plugins to start. The first restart closes the plugin host,
/// which ends that wait, so the guard step gets in line behind the restarts that were asked for
/// before. The signal comes when a restart has taken the node away.
#[cfg(unix)]
#[tokio::test(flavor = "multi_thread")]
async fn the_relay_process_reports_no_failure_for_steps_that_a_signal_refused() {
    use std::process::{Command, Stdio};

    let dir = tempfile::tempdir().unwrap();
    import_folder(dir.path(), &FILMS);
    let port = free_port();
    let mut env = env_importing(dir.path());
    env.insert("PEARTUBE_API_PORT".into(), port.to_string());
    let mut relay = Running(Some(
        Command::new(env!("CARGO_BIN_EXE_peartube-relay"))
            .envs(&env)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()
            .expect("relay process"),
    ));
    let pid = relay.0.as_ref().expect("the relay runs").id();
    let addr = SocketAddr::from(([127, 0, 0, 1], port));
    // The startup import is in a large film.
    listed_at_least(addr, 1).await;

    let restarts: Vec<TcpStream> = (0..8)
        .map(|_| begin(addr, "POST", "/v1/restart", ""))
        .collect();
    // Between the close of a restart and its open the relay has no node, and it lists nothing.
    let asked = std::time::Instant::now();
    while tokio::task::spawn_blocking(move || listed(addr))
        .await
        .unwrap()
        .is_none_or(|titles| !titles.is_empty())
    {
        assert!(
            asked.elapsed() < Duration::from_secs(20),
            "the first restart never took the node away"
        );
    }
    let _ = Command::new("kill")
        .arg("-TERM")
        .arg(pid.to_string())
        .status();
    let process = relay.0.take().expect("the relay runs");
    let output = tokio::task::spawn_blocking(move || process.wait_with_output())
        .await
        .unwrap()
        .expect("the relay exits");
    drop(restarts);
    let printed = String::from_utf8_lossy(&output.stdout);
    assert_eq!(
        output.status.code(),
        Some(0),
        "the relay ended with {:?}; it printed: {printed}",
        output.status
    );
    for report in ["private guard failed to start", "restart failed"] {
        assert!(
            !printed.contains(report),
            "the relay reported a step that the signal refused ({report}): {printed}"
        );
    }
}

// ---------------------------------------------------------------- cast routes

const DEVICE_ID: &str = "0123456789abcdef0123456789abcdef";
const SEED: &str = "abababababababababababababababababababababababababababababababab";
const PAIR_TOKEN: &str = "cdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcd";
const PAIR_CODE: &str = "ABC123";

/// One DHT node both peers bootstrap from.
struct Testnet {
    _runtime: tokio::runtime::Runtime,
    _udx: libudx::UdxRuntime,
    _task: tokio::task::JoinHandle<Result<(), peeroxide_dht::hyperdht::HyperDhtError>>,
    bootstrap: Value,
}

fn testnet() -> Testnet {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("testnet runtime");
    let udx = libudx::UdxRuntime::new().expect("udx");
    let (task, port) = runtime.block_on(async {
        let mut config = peeroxide_dht::hyperdht::HyperDhtConfig::default();
        config.dht.bootstrap = Vec::new();
        config.dht.host = "127.0.0.1".to_owned();
        config.dht.port = 0;
        config.dht.firewalled = false;
        let (task, handle, _events) = peeroxide_dht::hyperdht::spawn(&udx, config)
            .await
            .expect("testnet node");
        let port = handle.local_port().await.expect("testnet port");
        (task, port)
    });
    Testnet {
        _runtime: runtime,
        _udx: udx,
        _task: task,
        bootstrap: serde_json::json!([format!("127.0.0.1:{port}")]),
    }
}

/// A receiver identity the test can pair with: fixed code, token and device id.
fn seed_identity(dir: &std::path::Path) {
    std::fs::create_dir_all(dir).expect("cast dir");
    let identity = serde_json::json!({
        "deviceId": DEVICE_ID,
        "name": "Kitchen TV",
        "seed": SEED,
        "pairToken": PAIR_TOKEN,
        "pairCode": PAIR_CODE,
    });
    std::fs::write(
        peartube_native::cast::pairing::ReceiverIdentity::file(dir),
        serde_json::to_string_pretty(&identity).expect("json"),
    )
    .expect("identity");
}

async fn receiver_on(bootstrap: &Value, dir: &std::path::Path) -> Arc<CoreNode> {
    let key_pair = peartube_native::cast::receiver::receiver_key_pair_json(dir).expect("key pair");
    create_node(serde_json::json!({
        "storage": dir.join("peer").to_string_lossy(),
        "bootstrap": bootstrap,
        "keyPair": key_pair,
        "storageWal": false
    }))
    .await
    .expect("receiver node")
}

/// `/v1/cast/status` answers from the relay's live controller, and `/v1/cast/pair` reaches a
/// receiver on the network and saves the pairing it hands back.
#[test]
fn the_cast_routes_reach_a_receiver() {
    let testnet = testnet();
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("runtime");
    runtime.block_on(async {
        let dir = tempfile::tempdir().unwrap();
        cast_routes(&testnet.bootstrap, dir.path()).await;
    });
}

async fn cast_routes(bootstrap: &Value, dir: &std::path::Path) {
    let cast_dir = dir.join("tv");
    seed_identity(&cast_dir);
    let node = receiver_on(bootstrap, &cast_dir).await;
    let receiver = Receiver::start(ReceiverOptions {
        storage: cast_dir.clone(),
        peer_storage: Some(cast_dir.join("peer")),
        name: "Kitchen TV".to_owned(),
        port: 0,
        lan_host: None,
        peer: Some(Arc::new(NodePeer::new(node.clone())) as Arc<dyn CastPeer>),
        mdns: false,
        open_auth: false,
        any_port: true,
        on_command: None,
        on_change: None,
    })
    .await
    .expect("receiver starts");
    let receiver_key = receiver.public_key();

    let bootstrap_host = bootstrap[0].as_str().expect("bootstrap host").to_owned();
    let relay = Relay::listen(env_on(dir, &bootstrap_host))
        .await
        .expect("relay listens");
    relay.start().await.expect("node opens");
    let addr = relay.addresses()[0];

    // A live controller answers the status route: the relay's cast stack is open with its node.
    let status = tokio::task::spawn_blocking(move || get_json(addr, "/v1/cast/status"))
        .await
        .unwrap();
    assert!(status["receivers"].is_array(), "{status}");
    assert_eq!(status["session"], Value::Null, "{status}");
    assert_eq!(status["receiverMode"], Value::Null, "{status}");

    // The pair reaches the receiver itself: its code approves and its token comes back.
    let body = format!(r#"{{"publicKey":"{receiver_key}","code":"{PAIR_CODE}"}}"#);
    let deadline = tokio::time::Instant::now() + Duration::from_secs(150);
    let paired = loop {
        let attempt = tokio::task::spawn_blocking({
            let body = body.clone();
            move || try_request_json(addr, "POST", "/v1/cast/pair", &body)
        })
        .await
        .unwrap();
        if attempt.0 == 200 {
            break attempt.1;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "pair: {} {}",
            attempt.0,
            attempt.1
        );
        tokio::time::sleep(Duration::from_secs(2)).await;
    };
    assert_eq!(paired["publicKey"], receiver_key.as_str());
    assert_eq!(paired["token"], PAIR_TOKEN);
    assert_eq!(paired["id"], DEVICE_ID);
    assert_eq!(paired["name"], "Kitchen TV");

    // The saved pairing is what the status route lists.
    let status = tokio::task::spawn_blocking(move || get_json(addr, "/v1/cast/status"))
        .await
        .unwrap();
    assert!(
        status["receivers"]
            .as_array()
            .expect("receivers")
            .iter()
            .any(|row| row["publicKey"] == receiver_key.as_str()),
        "{status}"
    );

    // A stop closes the cast stack with the node; the route is the app's 404 until it reopens.
    relay.shutdown().await;
    receiver.close().await;
    let _ = node.close().await;
    let _ = std::fs::remove_dir_all(dir);
}

// ------------------------------------------------------------------------------- guard mode

/// `PEARTUBE_PRIVATE_RELAY`: a standalone guard forwarder with no Corestore and no storage folder,
/// refusing every channel or content flag, as `bin/relay.js` does.
#[tokio::test(flavor = "multi_thread")]
async fn guard_mode_runs_a_guard_writes_no_storage_and_refuses_content_flags() {
    use peartube_native::private_overlay::dht::DhtOptions;
    use peartube_native::private_overlay::peeroxide::open_dht;

    let dir = tempfile::tempdir().unwrap();
    let storage = dir.path().join("data");
    // A bootstrap node of its own, so the guard's announce completes without the public DHT.
    let bootstrap = open_dht(DhtOptions {
        host: Some("127.0.0.1".into()),
        port: 0,
        bootstrap: Vec::new(),
    })
    .await
    .expect("bootstrap dht");
    let bootstrap = bootstrap
        .address()
        .map(|addr| format!("127.0.0.1:{}", addr.port()))
        .unwrap_or_else(|| "127.0.0.1:9".into());

    let mut env: HashMap<String, String> = HashMap::new();
    env.insert("PEARTUBE_PRIVATE_RELAY".into(), "1".into());
    env.insert("PEARTUBE_PRIVATE_RELAY_PORT".into(), "0".into());
    env.insert("PEARTUBE_STORAGE".into(), storage.display().to_string());
    env.insert("PEARTUBE_BOOTSTRAP".into(), bootstrap);
    env.insert("PEARTUBE_GUARD_SEED".into(), "11".repeat(32));
    assert!(peartube_native::relay::guard_mode(&env));

    let guard = tokio::time::timeout(
        Duration::from_secs(120),
        peartube_native::relay::start_standalone_guard(&env),
    )
    .await
    .expect("the guard starts")
    .expect("the guard is up");
    let ready = guard.ready();
    let expected = peartube_native::private_overlay::to_hex(
        &hypercore::crypto::KeyPair::from_seed(&[0x11; 32]).public_key,
    );
    assert_eq!(
        ready["publicKey"],
        expected.as_str(),
        "the seeded guard key"
    );
    assert_eq!(ready["host"], "127.0.0.1");
    assert!(ready["port"].as_u64().unwrap_or(0) > 0, "{ready}");
    assert!(
        !storage.exists(),
        "guard mode creates no storage folder: {ready}"
    );
    // The heartbeat line is the ready line's key plus `guard.stats()`, and nothing else: JS
    // spreads `guard.stats()` there, which carries no port.
    let heartbeat = guard.heartbeat();
    let mut keys: Vec<&str> = heartbeat
        .as_object()
        .expect("a heartbeat object")
        .keys()
        .map(String::as_str)
        .collect();
    keys.sort_unstable();
    assert_eq!(
        keys,
        [
            "activeCircuits",
            "publicKey",
            "putMutableCount",
            "registeredRoutes",
            "rendezvousTopics",
            "rvRegisterCount",
        ],
        "{heartbeat}"
    );
    guard.close().await;
    assert!(!storage.exists(), "still nothing written on close");

    env.insert("PEARTUBE_CHANNEL".into(), "ab".repeat(32));
    let error = peartube_native::relay::start_standalone_guard(&env)
        .await
        .err()
        .expect("a channel flag is refused");
    assert_eq!(
        error,
        "PEARTUBE_PRIVATE_RELAY runs a standalone Corestore-free guard forwarder and cannot be combined with channel or content flags; run peartube-private-relay separately"
    );
    assert!(!storage.exists());

    // An empty variable is unset, as JS truthiness has it.
    let mut empty: HashMap<String, String> = HashMap::new();
    empty.insert("PEARTUBE_PRIVATE_RELAY".into(), String::new());
    assert!(!peartube_native::relay::guard_mode(&empty));
    empty.insert("PEARTUBE_PRIVATE_RELAY_PORT".into(), "0".into());
    assert!(peartube_native::relay::guard_mode(&empty));
}

/// `PEARTUBE_GUARD_FOLLOW_PARENT`: a guard whose parent dies exits on its own, as
/// `private-relay.js`'s 2 s parent watch does (it calls `shutdown()`, which closes the guard and
/// exits). Without it the process outlived the relay that started it until it was signalled.
#[cfg(unix)]
#[tokio::test(flavor = "multi_thread")]
async fn a_following_guard_leaves_when_its_parent_dies() {
    use peartube_native::private_overlay::dht::DhtOptions;
    use peartube_native::private_overlay::peeroxide::open_dht;
    use std::process::{Command, Stdio};

    // A bootstrap node of its own, so the guard's announce completes without the public DHT.
    let bootstrap = open_dht(DhtOptions {
        host: Some("127.0.0.1".into()),
        port: 0,
        bootstrap: Vec::new(),
    })
    .await
    .expect("bootstrap dht");
    let bootstrap = bootstrap
        .address()
        .map(|addr| format!("127.0.0.1:{}", addr.port()))
        .expect("bootstrap address");
    let storage = tempfile::tempdir().unwrap();

    // `$0` is the relay binary, started in the background so its parent is this short-lived
    // shell rather than the test process. The shell outlives the guard's startup (a debug build
    // takes a moment to reach `main`), then leaves without stopping it.
    let mut parent = Command::new("sh")
        .arg("-c")
        .arg("\"$0\" & echo pid=$!; sleep 4")
        .arg(env!("CARGO_BIN_EXE_peartube-relay"))
        .env("PEARTUBE_PRIVATE_RELAY", "1")
        .env("PEARTUBE_PRIVATE_RELAY_PORT", "0")
        .env("PEARTUBE_GUARD_FOLLOW_PARENT", "1")
        .env("PEARTUBE_PRIVATE_HOST", "127.0.0.1")
        .env("PEARTUBE_BOOTSTRAP", &bootstrap)
        .env("PEARTUBE_GUARD_SEED", "22".repeat(32))
        .env("PEARTUBE_STORAGE", storage.path())
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("parent shell");
    let guard_pid = parent
        .stdout
        .take()
        .map(|stdout| {
            // The guard inherits the pipe, so it stays open until the guard itself exits.
            let (sender, receiver) = std::sync::mpsc::channel();
            std::thread::spawn(move || {
                let mut text = String::new();
                let mut stdout = std::io::BufReader::new(stdout);
                for line in std::io::BufRead::lines(&mut stdout).map_while(Result::ok) {
                    eprintln!("guard: {line}");
                    text.push_str(&line);
                    text.push('\n');
                }
                let _ = sender.send(text);
            });
            receiver
        })
        .expect("stdout");
    parent.wait().expect("the parent shell exits");
    let started = std::time::Instant::now();
    let output = guard_pid
        .recv_timeout(Duration::from_secs(30))
        .expect("the guard exits after its parent, not on a signal");
    let elapsed = started.elapsed();

    let pid: i32 = output
        .lines()
        .find_map(|line| line.strip_prefix("pid="))
        .and_then(|pid| pid.trim().parse().ok())
        .expect("the guard's pid");
    // Whatever happened, never leave the process behind.
    let _ = Command::new("kill")
        .arg("-9")
        .arg(pid.to_string())
        .stderr(Stdio::null())
        .status();
    assert!(
        output.contains("\"private-guard ready\""),
        "the guard started: {output}"
    );
    assert!(
        elapsed < Duration::from_secs(15),
        "the guard left right after its parent: {elapsed:?}"
    );
}
