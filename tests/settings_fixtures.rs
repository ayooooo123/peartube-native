//! Every case in `fixtures/settings-reference.json` must match the app's own output.

use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

use peartube_native::app_settings::{
    AppSettings, app_settings_defaults, node_options, normalize_app_settings,
};
use peartube_native::settings::Settings;
use serde_json::{Value, json};

const FIXTURES: &str = include_str!("../fixtures/settings-reference.json");

fn fixtures() -> Value {
    serde_json::from_str(FIXTURES).expect("fixtures/settings-reference.json")
}

fn cases<'a>(fixtures: &'a Value, key: &str) -> &'a [Value] {
    fixtures[key]
        .as_array()
        .unwrap_or_else(|| panic!("{key} cases"))
}

/// Compares serialized form, so field order is part of the fixture.
fn same_json(actual: &Value, expected: &Value, context: &str) {
    assert_eq!(
        serde_json::to_string(actual).expect("json"),
        serde_json::to_string(expected).expect("json"),
        "{context}"
    );
}

#[test]
fn app_settings_defaults_match() {
    let fixtures = fixtures();
    same_json(
        &serde_json::to_value(app_settings_defaults()).expect("json"),
        &fixtures["appSettingsDefaults"],
        "APP_SETTINGS_DEFAULTS",
    );
}

#[test]
fn app_settings_normalize_matches() {
    let fixtures = fixtures();
    let cases = cases(&fixtures, "appSettings");
    assert!(cases.len() >= 40, "expected 40+ normalize cases");
    for case in cases {
        let name = case["name"].as_str().unwrap_or("case");
        let raw = &case["raw"];
        let require_channel = case["requireChannel"].as_bool().unwrap_or(false);
        let previous = case.get("previous");
        match normalize_app_settings(raw, require_channel, previous) {
            Ok(settings) => same_json(
                &serde_json::to_value(&settings).expect("json"),
                &case["ok"],
                name,
            ),
            Err(error) => assert_eq!(
                error.message(),
                case["err"].as_str().unwrap_or_default(),
                "{name}"
            ),
        }
    }
}

#[test]
fn node_options_match() {
    let fixtures = fixtures();
    for case in cases(&fixtures, "nodeOptions") {
        let settings: AppSettings =
            serde_json::from_value(case["settings"].clone()).expect("settings");
        let options = node_options(
            &settings,
            case["trackersDir"].as_str().expect("trackersDir"),
            case["defaultFolder"].as_str().expect("defaultFolder"),
        );
        same_json(
            &serde_json::to_value(&options).expect("json"),
            &case["value"],
            case["name"].as_str().unwrap_or("case"),
        );
    }
}

fn temp_root(name: &str) -> PathBuf {
    std::env::temp_dir().join(format!(
        "peartube-native-settings-{name}-{}",
        std::process::id()
    ))
}

fn prepare(root: &Path, case: &Value, index: usize) -> PathBuf {
    let dir = root.join(format!("case-{index}"));
    let _ = fs::remove_dir_all(&dir);
    fs::create_dir_all(&dir).expect("temp dir");
    if let Some(initial) = case["initial"].as_str() {
        fs::write(dir.join("settings.json"), initial).expect("initial settings.json");
    }
    dir
}

fn env_of(case: &Value) -> HashMap<String, String> {
    case["env"]
        .as_object()
        .map(|map| {
            map.iter()
                .map(|(key, value)| (key.clone(), value.as_str().unwrap_or_default().to_owned()))
                .collect()
        })
        .unwrap_or_default()
}

fn run_steps(settings: &mut Settings, case: &Value, index: usize) {
    for step in case["steps"].as_array().expect("steps") {
        let op = step["op"].as_str().expect("op");
        let outcome: Result<Value, String> = match op {
            "view" => Ok(settings.view()),
            "get" => Ok(settings.get(step["key"].as_str().expect("key"))),
            "update" => settings
                .update(&step["patch"])
                .map_err(|error| error.message().to_owned()),
            "remember" => settings
                .remember(step["key"].as_str().expect("key"), &step["value"])
                .map(|()| Value::Null)
                .map_err(|error| error.message().to_owned()),
            "snapshot" => Ok(settings.snapshot()),
            "markApplied" => {
                settings.mark_applied(&step["snapshot"]);
                Ok(Value::Null)
            }
            "isLive" => Ok(Value::Bool(
                settings.is_live(step["key"].as_str().expect("key")),
            )),
            "file" => Ok(Value::String(
                settings
                    .file()
                    .file_name()
                    .expect("file name")
                    .to_string_lossy()
                    .into_owned(),
            )),
            other => panic!("case {index}: unknown op {other}"),
        };
        match outcome {
            Ok(value) => same_json(
                &value,
                &step["ok"],
                &format!(
                    "case {index} step {op} in {}",
                    case["name"].as_str().unwrap_or("case")
                ),
            ),
            Err(message) => assert_eq!(
                message,
                step["err"].as_str().expect("err"),
                "case {index} step {op} in {}",
                case["name"].as_str().unwrap_or("case")
            ),
        }
    }
}

/// Writes every case's settings.json with `Settings` and leaves the directories in place.
fn rust_written_cases(fixtures: &Value) -> (PathBuf, Vec<Value>) {
    let cases = cases(fixtures, "settingsFiles");
    let root = temp_root("js");
    let _ = fs::remove_dir_all(&root);
    let mut entries = Vec::new();
    for (index, case) in cases.iter().enumerate() {
        let dir = prepare(&root, case, index);
        let mut settings = Settings::new(&dir, env_of(case));
        run_steps(&mut settings, case, index);
        entries.push(json!({
            "dir": dir.to_str().expect("utf-8 path"),
            "env": case["env"],
            "view": case["reloadedView"],
        }));
    }
    (root, entries)
}

#[test]
fn settings_files_match_byte_for_byte() {
    let fixtures = fixtures();
    let cases = cases(&fixtures, "settingsFiles");
    let root = temp_root("files");
    let _ = fs::remove_dir_all(&root);
    for (index, case) in cases.iter().enumerate() {
        let dir = prepare(&root, case, index);
        let mut settings = Settings::new(&dir, env_of(case));
        run_steps(&mut settings, case, index);
        let file = dir.join("settings.json");
        match case["finalFile"].as_str() {
            Some(expected) => assert_eq!(
                fs::read_to_string(&file).expect("written settings.json"),
                expected,
                "case {index} file"
            ),
            None => assert!(!file.exists(), "case {index}: unexpected settings.json"),
        }
    }
    let _ = fs::remove_dir_all(&root);
}

/// The generator cannot load files Rust writes, so this cross-check runs the JS reference on them.
#[test]
#[ignore = "needs node and `npm install` in fixtures/gen"]
fn rust_written_settings_load_in_js() {
    let Some(app) = std::env::var_os("PEARTUBE_APP")
        .map(PathBuf::from)
        .or_else(|| {
            let sibling = Path::new(env!("CARGO_MANIFEST_DIR")).join("../peartube");
            sibling.is_dir().then_some(sibling)
        })
    else {
        eprintln!("skipping: set PEARTUBE_APP to the app checkout");
        return;
    };
    let (root, entries) = rust_written_cases(&fixtures());
    let request = root.join("cases.json");
    fs::write(&request, serde_json::to_string(&entries).expect("json")).expect("cases.json");
    let output = Command::new("node")
        .arg(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/fixtures/gen/settings.mjs"
        ))
        .arg("verify")
        .arg(&request)
        .env("PEARTUBE_APP", app)
        .output()
        .expect("node");
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let _ = fs::remove_dir_all(&root);
}
