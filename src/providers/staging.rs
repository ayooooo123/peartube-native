//! What every source provider shares about the staging (import) folder: how an album folder is
//! named and pinned, and how the files a relay put there itself are told from the owner's
//! (`src/providers/staging.js`).
//!
//! A name cut at its length limit inside a surrogate pair ends in U+FFFD here; the app's string
//! holds half a pair until the file system call or `toWellFormed` replaces it with the same. Both
//! count as three bytes toward a name's byte limit.

use std::future::Future;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;

use serde_json::{Value, json};

use crate::protocol::{is_js_space, js_trim, truncate_utf16};

/// A mirror told which album it fills pins its folder to that release in this dot file (skipped by
/// the media walk); the importer then announces every file in the folder under it, whatever the
/// peer's tags say. Peer bytes are never rewritten, so a mirrored file's sha256 is the hash of
/// exactly what the peer served.
pub const ALBUM_PIN: &str = ".peartube-album.json";
/// The files this relay mirrored into an album folder, by name, in this dot file. Deleting an
/// entry deletes its file only when the file is listed here (or sits in `<storage>/staging`, where
/// nothing else writes): a file the owner put in the import folder is theirs, and only its
/// announce goes.
pub const MIRRORED_LIST: &str = ".peartube-mirrored.json";

/// `RELEASE_ID`: `mb:<32 hex>`, `discogs:r<1 to 12 digits>`, `album:<20 hex>` or `yt:<20 hex>`.
/// `yt:` is the YouTube provider's namespace: an album saved from there never answers for an
/// `mb:`/`discogs:`/`album:` release, so nobody asking for a release is handed a lossy copy.
pub fn is_release_id(text: &str) -> bool {
    let hex = |text: &str, len: usize| {
        text.len() == len
            && text
                .bytes()
                .all(|byte| matches!(byte, b'0'..=b'9' | b'a'..=b'f'))
    };
    if let Some(id) = text.strip_prefix("mb:") {
        return hex(id, 32);
    }
    if let Some(id) = text
        .strip_prefix("album:")
        .or_else(|| text.strip_prefix("yt:"))
    {
        return hex(id, 20);
    }
    text.strip_prefix("discogs:r").is_some_and(|id| {
        (1..=12).contains(&id.len()) && id.bytes().all(|byte| byte.is_ascii_digit())
    })
}

/// `text` cut to at most `max` UTF-8 bytes, between characters (`clipBytes`). A file name is
/// limited in bytes (255 on most file systems), not in characters: 84 Chinese characters are 252
/// bytes.
pub fn clip_bytes(text: &str, max: usize) -> &str {
    let mut bytes = 0;
    for (at, c) in text.char_indices() {
        bytes += c.len_utf8();
        if bytes > max {
            return &text[..at];
        }
    }
    text
}

/// `<dir>/<name>`, or `<stem> (2)<ext>`… when that name is taken: a mirror never overwrites a file
/// already there.
pub(crate) fn free_path(dir: &Path, name: &str) -> PathBuf {
    let (stem, ext) = split_ext(name);
    let mut candidate = dir.join(name);
    for n in 2..1000 {
        if std::fs::metadata(&candidate).is_err() {
            return candidate;
        }
        candidate = dir.join(format!("{stem} ({n}){ext}"));
    }
    candidate
}

/// A file name as its stem and its extension with the dot (`basename(name, ext)` and
/// `extname(name)`): a name that only starts with its dot, and "..", have no extension.
pub(crate) fn split_ext(name: &str) -> (&str, &str) {
    match name.rfind('.') {
        Some(dot) if dot > 0 && name != ".." => name.split_at(dot),
        _ => (name, ""),
    }
}

/// One folder or file name that every file system takes: no separators, reserved or control
/// characters, no leading dots or `@` (a hidden or system folder), at most 140 UTF-16 units and
/// 180 bytes (which leaves room for a track number, an extension, a `[YouTube]` marker and a
/// ` (2)` suffix under 255 bytes), no trailing dots or spaces (`safeSegment`). `fallback` when
/// nothing is left.
pub fn safe_segment(raw: &str, fallback: &str) -> String {
    let reserved = |c: char| {
        matches!(c, '/' | '\\' | ':' | '*' | '?' | '"' | '<' | '>' | '|') || c <= '\u{1f}'
    };
    let spaced = one_space_for(raw, reserved);
    let shown = spaced.trim_start_matches(|c: char| c == '.' || c == '@' || is_js_space(c));
    let single = one_space_for(shown, is_js_space);
    let cut = truncate_utf16(js_trim(&single), 140);
    let clean = clip_bytes(&cut, 180).trim_end_matches(|c: char| c == '.' || is_js_space(c));
    if clean.is_empty() { fallback } else { clean }.to_owned()
}

/// `text` with every run of characters `matches` accepts replaced by one space.
fn one_space_for(text: &str, matches: impl Fn(char) -> bool) -> String {
    let mut out = String::with_capacity(text.len());
    let mut in_run = false;
    for c in text.chars() {
        let hit = matches(c);
        if !hit {
            out.push(c);
        } else if !in_run {
            out.push(' ');
        }
        in_run = hit;
    }
    out
}

/// The folder an album is mirrored into (`formatAlbumDir`): `Artist - Album (Year)`, else the
/// caller's title (its ` · ` as ` - `), else the album alone.
pub fn format_album_dir(
    artist: &str,
    album: &str,
    year: Option<u32>,
    fallback_title: &str,
) -> String {
    let dated = |name: String| match year {
        Some(year) => format!("{name} ({year})"),
        None => name,
    };
    if !artist.is_empty() && !album.is_empty() {
        return safe_segment(&dated(format!("{artist} - {album}")), "Mirrored Album");
    }
    if !fallback_title.is_empty() {
        // The app also takes the space around each dot; the segment's one-space rule does that here.
        return safe_segment(&fallback_title.replace('·', " - "), "Mirrored Album");
    }
    if !album.is_empty() {
        return safe_segment(&dated(album.to_owned()), "Mirrored Album");
    }
    "Unknown Artist - Mirrored Album".to_owned()
}

/// Adds `names` to the folder's list of files this relay mirrored ([`MIRRORED_LIST`]), which
/// deletion reads.
pub(crate) fn note_mirrored(dir: &Path, names: &[String]) -> std::io::Result<()> {
    let file = dir.join(MIRRORED_LIST);
    let listed = std::fs::read_to_string(&file)
        .ok()
        .and_then(|text| serde_json::from_str::<Value>(&crate::ipc::well_formed(&text)).ok());
    let mut next: Vec<Value> = Vec::new();
    let listed = listed
        .as_ref()
        .and_then(Value::as_array)
        .into_iter()
        .flatten();
    for name in listed.cloned().chain(names.iter().map(|name| json!(name))) {
        // A list or an object is never the same as another one, as in the app's `Set`.
        if name.is_array() || name.is_object() || !next.contains(&name) {
            next.push(name);
        }
    }
    std::fs::write(file, format!("{}\n", Value::Array(next)))
}

/// A provider's finished files moving into the import folder (the app's `place`). The host runs
/// it at most once, and only while no other import holds the folder. What it refuses (a folder
/// that holds other files, 409) reaches the provider as the host's answer.
pub type Place = Box<dyn FnOnce() -> Result<(), PlaceError> + Send>;

/// Whether the save the files belong to was cancelled, or its provider stopped.
pub type Cancelled = Arc<dyn Fn() -> bool + Send + Sync>;

/// Why a host placed or imported nothing: the save was cancelled (`Cancelled`), another import
/// held the folder past the host's limit (503), or `place` itself failed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlaceError {
    pub message: String,
    pub status: Option<u16>,
}

impl PlaceError {
    /// The save was cancelled, its provider stopped or the host closed: nothing was placed.
    pub fn cancelled() -> Self {
        Self {
            message: "Cancelled".to_owned(),
            status: None,
        }
    }
}

/// What one import answers (`scanFolder`'s result), or why nothing was placed.
pub type PlaceFuture = Pin<Box<dyn Future<Output = Result<Value, PlaceError>> + Send>>;

/// The host's `placeAndScan`, as a provider is given it. `place` runs once no other import holds
/// the folder, and the import starts in the same step, so no other import sees a half-placed
/// album. While the host waits nothing is placed: a save that is cancelled, or that the host
/// gives up on, leaves nothing behind.
pub type PlaceAndScan = Arc<dyn Fn(Place, Cancelled) -> PlaceFuture + Send + Sync>;
