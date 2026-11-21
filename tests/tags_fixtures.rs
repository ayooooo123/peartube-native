use peartube_native::tags::{Tags, album_key, parse_tags};
use serde::Deserialize;
use serde_json::{Value, json};

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Fixtures {
    parse: Vec<Case>,
    cuts: Vec<Cut>,
    album_key: Vec<Key>,
}
#[derive(Deserialize)]
struct Case {
    name: String,
    hex: String,
    expected: Value,
}
#[derive(Deserialize)]
struct Cut {
    name: String,
    hex: String,
    runs: Vec<Run>,
}
#[derive(Deserialize)]
struct Run {
    from: usize,
    expected: Value,
}
#[derive(Deserialize)]
struct Key {
    input: KeyInput,
    expected: String,
}
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct KeyInput {
    album_artist: Option<String>,
    album: Option<String>,
    year: Option<u32>,
}

fn bytes(hex: &str) -> Vec<u8> {
    hex.as_bytes()
        .as_chunks::<2>()
        .0
        .iter()
        .map(|pair| u8::from_str_radix(std::str::from_utf8(pair).unwrap(), 16).unwrap())
        .collect()
}

/// The app's object for the same tags.
fn view(tags: Option<Tags>) -> Value {
    let Some(tags) = tags else {
        return Value::Null;
    };
    json!({
        "container": tags.container,
        "albumArtist": tags.album_artist,
        "album": tags.album,
        "artist": tags.artist,
        "title": tags.title,
        "year": tags.year,
        "disc": tags.disc,
        "track": tags.track,
        "mbid": tags.mbid,
        "discogs": tags.discogs,
    })
}

#[test]
fn tags_fixtures() {
    let fixtures: Fixtures =
        serde_json::from_str(include_str!("../fixtures/tags-reference.json")).unwrap();
    assert!(fixtures.parse.len() >= 50 && fixtures.cuts.len() >= 8);
    for case in fixtures.parse {
        assert_eq!(
            view(parse_tags(&bytes(&case.hex))),
            case.expected,
            "{}",
            case.name
        );
    }
    // Every prefix of a file reads as the app reads it: a tag cut anywhere yields what was read.
    for cut in fixtures.cuts {
        let data = bytes(&cut.hex);
        for len in 0..=data.len() {
            let run = cut.runs.iter().rev().find(|run| run.from <= len).unwrap();
            assert_eq!(
                view(parse_tags(&data[..len])),
                run.expected,
                "{} cut at {len}",
                cut.name
            );
        }
    }
    for key in fixtures.album_key {
        assert_eq!(
            album_key(
                key.input.album_artist.as_deref(),
                key.input.album.as_deref(),
                key.input.year
            ),
            key.expected,
            "{:?} {:?}",
            key.input.album_artist,
            key.input.album
        );
    }
}
