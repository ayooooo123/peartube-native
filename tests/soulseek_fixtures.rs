use peartube_native::providers::soulseek::place::{LoneTrack, place_single_track};
use peartube_native::providers::soulseek::rank::{PeerFile, rank, share_list};
use peartube_native::providers::soulseek::{
    DEFAULT_LISTEN_PORT, DEFAULT_SHARE_MAX_FILES, SOULSEEK_SEARCH_CAP,
};
use peartube_native::providers::staging::{
    ALBUM_PIN, MIRRORED_LIST, clip_bytes, format_album_dir, is_release_id, safe_segment,
};
use peartube_native::tags::Tags;
use serde::Deserialize;
use serde_json::{Value, json};

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Fixtures {
    constants: Value,
    release_ids: Vec<ReleaseId>,
    segments: Vec<Segment>,
    clipped: Vec<Clipped>,
    album_dirs: Vec<AlbumDir>,
    placed: Vec<PlacedCase>,
    candidates: Vec<Ranked>,
    browse: Vec<Browsed>,
}
#[derive(Deserialize)]
struct ReleaseId {
    input: String,
    expected: bool,
}
#[derive(Deserialize)]
struct Segment {
    input: Option<String>,
    expected: String,
}
#[derive(Deserialize)]
struct Clipped {
    input: String,
    max: usize,
    expected: String,
}
#[derive(Deserialize)]
struct AlbumDir {
    input: AlbumDirInput,
    expected: String,
}
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct AlbumDirInput {
    artist: String,
    album: String,
    year: Option<u32>,
    #[serde(default)]
    fallback_title: String,
}
#[derive(Deserialize)]
struct PlacedCase {
    input: PlaceInput,
    expected: Value,
}
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct PlaceInput {
    tags: Option<TagInput>,
    filename: String,
    release: Option<String>,
    track: Option<u32>,
    position: Option<u32>,
    fallback_title: String,
}
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct TagInput {
    album_artist: Option<String>,
    album: Option<String>,
    artist: Option<String>,
    title: Option<String>,
    year: Option<u32>,
    disc: Option<u32>,
    track: Option<u32>,
    mbid: Option<String>,
    discogs: Option<String>,
}
#[derive(Deserialize)]
struct Hit {
    username: String,
    slots: bool,
    queue: u32,
    filename: String,
    size: u64,
    bitrate: Option<u32>,
    sample_rate: Option<u32>,
    bit_depth: Option<u32>,
    length_secs: Option<u32>,
}
#[derive(Deserialize)]
struct Ranked {
    hits: Vec<Hit>,
    cap: usize,
    expected: Value,
}
#[derive(Deserialize)]
struct Browsed {
    records: Vec<Hit>,
    username: String,
    expected: Value,
}

fn peer_files(hits: Vec<Hit>) -> impl Iterator<Item = PeerFile> {
    hits.into_iter().map(|hit| PeerFile {
        username: hit.username,
        slots: hit.slots,
        queue: hit.queue,
        filename: hit.filename,
        size: hit.size,
        bitrate: hit.bitrate,
        sample_rate: hit.sample_rate,
        bit_depth: hit.bit_depth,
        length: hit.length_secs,
    })
}

/// A name as its start and its sizes: the long ones are the cases that count, and two of them
/// side by side do not show where they differ.
fn brief(text: &str) -> String {
    let head: String = text.chars().take(12).collect();
    format!(
        "{head:?} ({} UTF-16 units, {} bytes)",
        text.encode_utf16().count(),
        text.len()
    )
}

/// Every case is run before the test fails, so one run names all that differ.
#[test]
fn soulseek_fixtures() {
    let fixtures: Fixtures =
        serde_json::from_str(include_str!("../fixtures/soulseek-reference.json")).unwrap();
    assert_eq!(
        fixtures.constants,
        json!({
            "SOULSEEK_SEARCH_CAP": SOULSEEK_SEARCH_CAP,
            "DEFAULT_LISTEN_PORT": DEFAULT_LISTEN_PORT,
            "DEFAULT_SHARE_MAX_FILES": DEFAULT_SHARE_MAX_FILES,
            "ALBUM_PIN": ALBUM_PIN,
            "MIRRORED_LIST": MIRRORED_LIST,
        })
    );
    let mut differs: Vec<String> = Vec::new();

    assert!(fixtures.release_ids.len() >= 23);
    for case in fixtures.release_ids {
        if is_release_id(&case.input) != case.expected {
            differs.push(format!(
                "release id {:?}: the app says {}",
                case.input, case.expected
            ));
        }
    }

    assert!(fixtures.segments.len() >= 26);
    for case in fixtures.segments {
        let input = case.input.as_deref().unwrap_or_default();
        let actual = safe_segment(input, "fallback");
        if actual != case.expected {
            differs.push(format!(
                "segment of {}: {} here, {} in the app",
                brief(input),
                brief(&actual),
                brief(&case.expected)
            ));
        }
    }
    assert!(fixtures.clipped.len() >= 13);
    for case in fixtures.clipped {
        let actual = clip_bytes(&case.input, case.max);
        if actual != case.expected {
            differs.push(format!(
                "{} cut to {} bytes: {} here, {} in the app",
                brief(&case.input),
                case.max,
                brief(actual),
                brief(&case.expected)
            ));
        }
    }
    assert!(fixtures.album_dirs.len() >= 6);
    for case in fixtures.album_dirs {
        let input = case.input;
        let actual = format_album_dir(
            &input.artist,
            &input.album,
            input.year,
            &input.fallback_title,
        );
        if actual != case.expected {
            differs.push(format!(
                "album folder of {} / {} / {:?} / {}: {} here, {} in the app",
                brief(&input.artist),
                brief(&input.album),
                input.year,
                brief(&input.fallback_title),
                brief(&actual),
                brief(&case.expected)
            ));
        }
    }

    assert!(fixtures.placed.len() >= 70);
    for case in fixtures.placed {
        let input = case.input;
        let tags = input.tags.map(|tags| Tags {
            container: "flac",
            album_artist: tags.album_artist,
            album: tags.album,
            artist: tags.artist,
            title: tags.title,
            year: tags.year,
            disc: tags.disc,
            track: tags.track,
            mbid: tags.mbid,
            discogs: tags.discogs,
        });
        let placed = place_single_track(LoneTrack {
            tags: tags.as_ref(),
            filename: &input.filename,
            release: input.release.as_deref(),
            track: input.track,
            position: input.position,
            fallback_title: &input.fallback_title,
        });
        let actual = match placed {
            Ok(placed) => json!({
                "folder": placed.folder,
                "file": placed.file,
                "release": placed.release,
                "albumTitle": placed.album_title,
                "trackTitle": placed.track_title,
                "from": placed.from,
            }),
            Err(error) => json!({ "error": error }),
        };
        if actual != case.expected {
            let keys: Vec<&String> = actual
                .as_object()
                .into_iter()
                .flatten()
                .filter(|(key, value)| case.expected.get(key.as_str()) != Some(value))
                .map(|(key, _)| key)
                .collect();
            differs.push(format!(
                "placed {:?}: {keys:?} differ from the app's",
                input.filename
            ));
        }
    }

    // The serialized text is compared, so the order of the keys counts too.
    for case in fixtures.candidates {
        let count = case.hits.len();
        let actual = rank(peer_files(case.hits), case.cap);
        if serde_json::to_string(&actual).unwrap() != serde_json::to_string(&case.expected).unwrap()
        {
            differs.push(format!("ranking of {count} hits, cap {}", case.cap));
        }
    }
    for case in fixtures.browse {
        let actual = share_list(peer_files(case.records), &case.username);
        if serde_json::to_string(&actual).unwrap() != serde_json::to_string(&case.expected).unwrap()
        {
            differs.push(format!("share list of {}", case.username));
        }
    }

    assert!(
        differs.is_empty(),
        "{} cases differ from the app:\n  {}",
        differs.len(),
        differs.join("\n  ")
    );
}
