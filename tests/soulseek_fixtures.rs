use peartube_native::providers::soulseek::place::{LoneTrack, place_single_track};
use peartube_native::providers::soulseek::rank::{PeerFile, rank, share_list};
use peartube_native::providers::soulseek::{
    ALBUM_PIN, DEFAULT_LISTEN_PORT, DEFAULT_SHARE_MAX_FILES, MIRRORED_LIST, SOULSEEK_SEARCH_CAP,
    is_release_id,
};
use peartube_native::tags::Tags;
use serde::Deserialize;
use serde_json::{Value, json};

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Fixtures {
    constants: Value,
    release_ids: Vec<ReleaseId>,
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
    for case in fixtures.release_ids {
        assert_eq!(
            is_release_id(&case.input),
            case.expected,
            "{:?}",
            case.input
        );
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
        assert_eq!(actual, case.expected, "{:?}", input.filename);
    }

    // The serialized text is compared, so the order of the keys counts too.
    for case in fixtures.candidates {
        let count = case.hits.len();
        let actual = rank(peer_files(case.hits), case.cap);
        assert_eq!(
            serde_json::to_string(&actual).unwrap(),
            serde_json::to_string(&case.expected).unwrap(),
            "{count} hits, cap {}",
            case.cap
        );
    }
    for case in fixtures.browse {
        let actual = share_list(peer_files(case.records), &case.username);
        assert_eq!(
            serde_json::to_string(&actual).unwrap(),
            serde_json::to_string(&case.expected).unwrap(),
            "{}",
            case.username
        );
    }
}
