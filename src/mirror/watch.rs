//! The `watch-dir` plugin (`scanFolder` and its helpers in `src/mirror.js`): a media file dropped
//! into the import folder is announced once, and a folder of music files becomes one album.
//!
//! Album folders are one level deep. An album's identity is decided once per folder, from every
//! file in it that has tags, before the first track is read: its release id when the tags carry
//! one, else a hash of its names and year. Each track is announced under `<release>:sNNeMM`, with
//! the album as its `title` and its own name in `music`. A folder pinned to a release
//! (`.peartube-album.json`, written by a mirror that was told which album it fills) takes that
//! release over anything its files' tags say.
//!
//! The same sweep runs over `<storage>/staging`, where a mirror puts its files when the import
//! folder cannot take them; its ledger rows carry the `staging:` prefix.

use std::future::Future;
use std::path::{Component, Path, PathBuf};
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde_json::{Map, Value, json};
use sha2::{Digest, Sha256};

use super::{Ctx, FileStat, MirrorError, MirrorFiles, MirrorPlugin};
use crate::acquire::AcquireError;
use crate::app_settings::{js_string, js_truthy};
use crate::node::Node;
use crate::protocol::{MAX_TITLE, is_id, is_js_space, is_media_file, truncate_utf16};
use crate::providers::staging::{
    ALBUM_PIN, Cancelled, MIRRORED_LIST, Place, PlaceError, is_release_id,
};
use crate::tags::{Tags, album_key, parse_tags};

/// The ledger prefix of a file under `<storage>/staging`.
const STAGED: &str = "staging:";
// The tag read is bounded and never through the blob server: a core must not be opened just to name
// a track, and the announce is written from the staged file before the core exists. 1 MiB covers a
// FLAC picture block plus every comment an album carries; a file whose tags sit past it falls back
// to its name.
const TAG_BYTES: usize = 1024 * 1024;

fn sha256_hex(text: &str) -> String {
    let digest = Sha256::digest(text.as_bytes());
    let mut out = String::with_capacity(64);
    for byte in digest {
        out.push_str(&format!("{byte:02x}"));
    }
    out
}

/// `path.basename(path)`.
fn basename(file_path: &str) -> String {
    Path::new(file_path)
        .file_name()
        .map(|name| name.to_string_lossy().into_owned())
        .unwrap_or_default()
}

/// `path.basename(path, path.extname(path))`.
fn stem(file_path: &str) -> String {
    let name = basename(file_path);
    match name.rfind('.') {
        Some(index) if index > 0 => name[..index].to_owned(),
        _ => name,
    }
}

fn ascii_word(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || byte == b'_'
}

/// First `tt` + 6..=10 digits at a word boundary, as the lowercased match text.
fn find_imdb(stem: &str) -> Option<String> {
    let bytes = stem.as_bytes();
    let lower = stem.to_ascii_lowercase();
    let bytes_lower = lower.as_bytes();
    for index in 0..bytes.len() {
        if index > 0 && ascii_word(bytes[index - 1]) {
            continue;
        }
        if bytes_lower.get(index) != Some(&b't') || bytes_lower.get(index + 1) != Some(&b't') {
            continue;
        }
        let mut end = index + 2;
        while end < bytes_lower.len() && bytes_lower[end].is_ascii_digit() {
            end += 1;
        }
        if (6..=10).contains(&(end - index - 2)) {
            return Some(lower[index..end].to_owned());
        }
    }
    None
}

/// First `sNN(eNN)` at a word boundary, as `(NN, NN)`.
fn find_episode(stem: &str) -> Option<(String, String)> {
    let bytes = stem.as_bytes();
    let lower = stem.to_ascii_lowercase();
    let bytes_lower = lower.as_bytes();
    for index in 0..bytes.len() {
        if index > 0 && ascii_word(bytes[index - 1]) {
            continue;
        }
        if bytes_lower.get(index) != Some(&b's') {
            continue;
        }
        let Some(season) = bytes_lower.get(index + 1..index + 3) else {
            continue;
        };
        if !season.iter().all(u8::is_ascii_digit) || bytes_lower.get(index + 3) != Some(&b'e') {
            continue;
        }
        let mut end = index + 4;
        while end < bytes_lower.len() && bytes_lower[end].is_ascii_digit() {
            end += 1;
        }
        if !(2..=3).contains(&(end - index - 4)) {
            continue;
        }
        return Some((
            lower[index + 1..index + 3].to_owned(),
            lower[index + 4..end].to_owned(),
        ));
    }
    None
}

/// What a file is announced as.
#[derive(Debug, Clone, PartialEq)]
pub struct MediaMeta {
    pub id: String,
    pub title: String,
    /// A music track's own name and artist; `title` is then its album.
    pub music: Option<Value>,
}

/// One album folder's identity.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Album {
    /// `mb:<32 hex>`, `discogs:r<n>` or `album:<20 hex>`.
    pub release: String,
    pub title: String,
    /// The artist a pin names, for files with no tags the importer reads; empty when none does.
    pub artist: String,
    /// How many of its files say something about it.
    pub tracks: usize,
    /// The folder is pinned to the release.
    pub pinned: bool,
}

/// `inferMediaMeta`: a canonical PearTube id (`imdb:tt…:s01e02`, `mb:<release>:s01e03`,
/// `album:<hash>:s01e03`, `imdb:tt…` or `clip:<slug>`) from a file name, its own tags, or a
/// sidecar.
///
/// Music comes first when the file sits in an album folder: an album's identity is the release,
/// not the file name, and a folder of "01 - Intro.flac" files names nothing. The album is decided
/// once per folder and handed in, because a compilation names a different artist on every track
/// and a folder where only some rips carry a MusicBrainz id must still be one album. `position` is
/// the place the file sorts at in its folder.
pub fn infer_media_meta(
    file_path: &str,
    sidecar: Option<&Value>,
    tags: Option<&Tags>,
    album: Option<(&Album, usize)>,
) -> MediaMeta {
    let stem = stem(file_path);
    if let Some(id) = sidecar
        .and_then(|sidecar| sidecar.get("id"))
        .and_then(Value::as_str)
    {
        let id = id.to_lowercase();
        if is_id(&id) {
            let title = sidecar
                .and_then(|sidecar| sidecar.get("title"))
                .filter(|title| js_truthy(title))
                .map_or_else(|| stem.clone(), js_string);
            return MediaMeta {
                id,
                title: truncate_utf16(&title, MAX_TITLE),
                music: None,
            };
        }
    }
    if let Some((album, position)) = album
        && let Some(track) = album_track(&stem, tags, album, position)
    {
        return track;
    }
    if let Some(imdb) = find_imdb(&stem) {
        let episode = find_episode(&stem);
        let clean = |with_episode: bool| {
            let mut text = remove_pattern(&stem, &imdb);
            if with_episode && let Some((season, episode)) = &episode {
                text = remove_pattern(&text, &format!("s{season}e{episode}"));
            }
            let text = text
                .chars()
                .map(|ch| {
                    if matches!(ch, '.' | '_' | '-') {
                        ' '
                    } else {
                        ch
                    }
                })
                .collect::<String>();
            let text = collapse_spaces(&text);
            if text.is_empty() { stem.clone() } else { text }
        };
        let (id, title) = match &episode {
            Some((season, episode)) => (format!("imdb:{imdb}:s{season}e{episode}"), clean(true)),
            None => (format!("imdb:{imdb}"), clean(false)),
        };
        return MediaMeta {
            id,
            title,
            music: None,
        };
    }
    let slug: String = stem
        .to_lowercase()
        .chars()
        .filter(char::is_ascii_alphanumeric)
        .collect();
    let suffix = if slug.len() >= 4 {
        slug.chars().take(24).collect()
    } else {
        sha256_hex(&stem)[..12].to_owned()
    };
    let title = collapse_spaces(
        &stem
            .chars()
            .map(|ch| if matches!(ch, '.' | '_') { ' ' } else { ch })
            .collect::<String>(),
    );
    let title = if title.is_empty() { stem } else { title };
    MediaMeta {
        id: format!("clip:{suffix}"),
        title,
        music: None,
    }
}

/// A file in an album folder as a track of that album. It joins the album even when its own tags
/// are missing or partial: its track number is its own, else the leading number in its name
/// ("03 - …"), else the place it sits at. A folder of untagged rips is exactly the case tags exist
/// to rescue, so it must not fall through to `clip:`; nor must a lone untagged track in a folder
/// pinned to its album (a single-track mirror). `None` when nothing gives a number, or when the
/// file is alone, untagged and unpinned.
fn album_track(
    stem: &str,
    tags: Option<&Tags>,
    album: &Album,
    position: usize,
) -> Option<MediaMeta> {
    if tags.is_none() && album.tracks <= 1 && !album.pinned {
        return None;
    }
    // The id suffix is `s<disc>e<track>`: an out-of-range number would make an id `decodeOp`
    // rejects, so it is clamped, and 0 means "not stated".
    let stated = |number: Option<u32>, most: u32| number.filter(|n| *n >= 1).map(|n| n.min(most));
    let track = stated(tags.and_then(|tags| tags.track), 999)
        .or_else(|| stated(leading_number(stem), 999))
        .or_else(|| stated(u32::try_from(position).ok(), 999))?;
    let disc = stated(tags.and_then(|tags| tags.disc), 99).unwrap_or(1);
    let said = |text: Option<&String>| text.filter(|text| !text.is_empty()).cloned();
    let named = said(tags.and_then(|tags| tags.title.as_ref()))
        .or_else(|| name_after_number(stem).map(str::to_owned))
        .unwrap_or_else(|| stem.to_owned());
    let mut music = Map::new();
    music.insert("title".into(), json!(named));
    // Who plays it: the file's own tags, else the artist its folder's pin names. With neither the
    // track names no artist at all.
    let artist = tags
        .and_then(|tags| said(tags.artist.as_ref()).or_else(|| said(tags.album_artist.as_ref())))
        .or_else(|| said(Some(&album.artist)));
    if let Some(artist) = artist {
        music.insert("artist".into(), json!(artist));
    }
    Some(MediaMeta {
        id: format!("{}:s{disc:02}e{track:02}", album.release),
        title: album.title.clone(),
        music: Some(Value::Object(music)),
    })
}

/// One to three digits at the start of `stem` that no letter, digit or underscore follows.
fn leading_number(stem: &str) -> Option<u32> {
    let digits = stem.bytes().take_while(u8::is_ascii_digit).count();
    let ends = stem
        .as_bytes()
        .get(digits)
        .is_none_or(|next| !ascii_word(*next));
    ((1..=3).contains(&digits) && ends)
        .then(|| stem[..digits].parse().ok())
        .flatten()
}

/// A track's name without its number: what follows one to three digits and at least one of
/// `-._ ` (spaces around them allowed), when something on one line does.
fn name_after_number(stem: &str) -> Option<&str> {
    let digits = stem.bytes().take_while(u8::is_ascii_digit).count();
    if !(1..=3).contains(&digits) {
        return None;
    }
    // The app's pattern is `\s*[-._ ]+\s*(.+)$`, greedy: each run gives back what the rest needs.
    let prefixes = |text: &str, matches: fn(char) -> bool| -> Vec<usize> {
        let mut ends = vec![0];
        for (at, c) in text.char_indices() {
            if !matches(c) {
                break;
            }
            ends.push(at + c.len_utf8());
        }
        ends
    };
    let separator = |c: char| matches!(c, '-' | '.' | '_' | ' ');
    let line_end = |c: char| matches!(c, '\n' | '\r' | '\u{2028}' | '\u{2029}');
    let rest = &stem[digits..];
    for space in prefixes(rest, is_js_space).into_iter().rev() {
        let after_space = &rest[space..];
        for marks in prefixes(after_space, separator).into_iter().skip(1).rev() {
            let after_marks = &after_space[marks..];
            for more in prefixes(after_marks, is_js_space).into_iter().rev() {
                let name = &after_marks[more..];
                if !name.is_empty() && !name.contains(line_end) {
                    return Some(name);
                }
            }
        }
    }
    None
}

/// Remove every case-insensitive occurrence of `needle`.
fn remove_pattern(text: &str, needle: &str) -> String {
    let lower = text.to_lowercase();
    let needle_lower = needle.to_lowercase();
    let mut out = String::with_capacity(text.len());
    let mut cursor = 0;
    while let Some(found) = lower[cursor..].find(&needle_lower) {
        let start = cursor + found;
        out.push_str(&text[cursor..start]);
        cursor = start + needle_lower.len();
    }
    out.push_str(&text[cursor..]);
    out
}

fn collapse_spaces(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    let mut space = false;
    for ch in text.trim().chars() {
        if ch == ' ' {
            space = true;
        } else {
            if space && !out.is_empty() {
                out.push(' ');
            }
            space = false;
            out.push(ch);
        }
    }
    out
}

/// One album's identity (`albumIdentity`), from every tagged file in its folder rather than from
/// any single one of them. Each entry is a file's tags and its path, or `None` for a file that has
/// none.
///
/// The release id wins when the folder agrees on one: it is what makes two rips of the same album
/// land on the same tracks, and it survives a rename. The convention tags only some files of a
/// folder, so one id from any file that has it is taken. Without one, the album's names and year
/// are hashed, and `albumKey` folds case, punctuation and accents so a differently-typed folder
/// still agrees.
fn album_identity(files: &[Option<(Tags, &str)>]) -> Option<Album> {
    fn said(text: &Option<String>) -> Option<&str> {
        text.as_deref().filter(|text| !text.is_empty())
    }
    let key_of = |tags: &Tags| {
        let artist = said(&tags.album_artist).or(said(&tags.artist));
        album_key(
            Some(artist.unwrap_or_default()),
            said(&tags.album),
            tags.year,
        )
    };
    let mut mbid = None;
    let mut discogs = None;
    // The albums the files name, in the order first seen, with how many files name each.
    let mut names: Vec<(String, usize)> = Vec::new();
    // Every tagged file names the album, numbered or not (a vinyl rip tags no track numbers); only
    // a numbered one, or a file already announced under a release, vouches for a release id.
    for (tags, _) in files.iter().flatten() {
        let numbered = tags.track.is_some_and(|track| track > 0);
        if mbid.is_none() && numbered {
            mbid = said(&tags.mbid);
        }
        if discogs.is_none() && numbered {
            discogs = said(&tags.discogs);
        }
        if said(&tags.album).is_some() {
            let key = key_of(tags);
            match names.iter_mut().find(|(name, _)| *name == key) {
                Some(entry) => entry.1 += 1,
                None => names.push((key, 1)),
            }
        }
    }
    // The most common album across the folder: a bonus track or a guest credit does not rename the
    // release. The first one named wins a tie.
    let best = names
        .iter()
        .fold(None::<&(String, usize)>, |best, entry| match best {
            Some(best) if best.1 >= entry.1 => Some(best),
            _ => Some(entry),
        })
        .map(|(key, _)| key.as_str());
    if best.is_none() && mbid.is_none() && discogs.is_none() {
        return None;
    }
    let named = files
        .iter()
        .flatten()
        .find(|(tags, _)| said(&tags.album).is_some() && Some(key_of(tags).as_str()) == best);
    let release = match (mbid, discogs, best) {
        (Some(mbid), _, _) => format!("mb:{mbid}"),
        (None, Some(discogs), _) => format!("discogs:r{discogs}"),
        (None, None, best) => format!("album:{}", &sha256_hex(best.unwrap_or_default())[..20]),
    };
    let names: Vec<&str> = named
        .iter()
        .flat_map(|(tags, _)| {
            [
                said(&tags.album_artist).or(said(&tags.artist)),
                said(&tags.album),
            ]
        })
        .flatten()
        .collect();
    let title = truncate_utf16(&names.join(" · "), MAX_TITLE);
    let title = if !title.is_empty() {
        title
    } else {
        Some(named.map(|(_, file)| basename(file)).unwrap_or_default())
            .filter(|name| !name.is_empty())
            .unwrap_or_else(|| "Untitled album".to_owned())
    };
    Some(Album {
        release,
        title,
        artist: String::new(),
        tracks: files.iter().flatten().count(),
        pinned: false,
    })
}

/// What a file already announced as a track of a release vouches for: that release, as tags that
/// name nothing else.
fn imported_release(id: &str) -> Option<Tags> {
    let (release, suffix) = id.rsplit_once(':')?;
    let (season, episode) = suffix.strip_prefix('s')?.split_once('e')?;
    let digits = |text: &str| text.bytes().all(|byte| byte.is_ascii_digit());
    if !is_release_id(release)
        || season.len() != 2
        || !digits(season)
        || !(2..=3).contains(&episode.len())
        || !digits(episode)
    {
        return None;
    }
    Some(Tags {
        container: "",
        album_artist: None,
        album: None,
        artist: None,
        title: None,
        year: None,
        disc: None,
        track: Some(1),
        mbid: release.strip_prefix("mb:").map(str::to_owned),
        discogs: release.strip_prefix("discogs:r").map(str::to_owned),
    })
}

/// One media file under a scanned root.
struct Found {
    /// Its path from the root: `name`, or `folder/name` for a file of an album.
    rel: String,
    /// Its album folder.
    dir: Option<String>,
    full: String,
    stat: FileStat,
}

fn join(dir: &str, name: &str) -> String {
    Path::new(dir).join(name).to_string_lossy().into_owned()
}

/// `path.resolve(path)`: absolute, with `.` and `..` worked out.
fn resolve(path: &str) -> String {
    let absolute = std::path::absolute(path).unwrap_or_else(|_| PathBuf::from(path));
    let mut out = PathBuf::new();
    for part in absolute.components() {
        match part {
            Component::ParentDir => {
                out.pop();
            }
            Component::CurDir => {}
            other => out.push(other),
        }
    }
    out.to_string_lossy().into_owned()
}

/// The sweep flag while a provider's save holds it: given back when the save has been imported or
/// has given up, whichever way it leaves.
struct Holding<'a>(&'a AtomicBool);

impl Drop for Holding<'_> {
    fn drop(&mut self) {
        self.0.store(false, Ordering::SeqCst);
    }
}

/// Shared state the `watch-dir` plugin keeps between runs.
pub struct WatchState {
    pub(super) ledger: Mutex<Map<String, Value>>,
    pub(super) state_file: String,
    /// One sweep at a time; a delete that follows the owner's removal waits on it too, and so does
    /// a provider's finished save.
    pub(super) scanning: AtomicBool,
    /// How long a provider's finished save waits for another import to finish before it gives up
    /// with nothing placed (`importWaitMs`).
    pub(super) import_wait_ms: u64,
    pub(super) watch_dir: Mutex<Option<String>>,
    /// `<storage>/staging`.
    pub(super) staging_dir: String,
    pub(super) closed: Arc<AtomicBool>,
    pub(super) files: Arc<dyn MirrorFiles>,
    pub(super) now_ms: Arc<dyn Fn() -> u64 + Send + Sync>,
}

impl WatchState {
    /// `mirror-watch.json`, as the app writes it: two spaces a level.
    fn save(&self) {
        let text = {
            let ledger = self.ledger.lock().expect("mirror ledger");
            serde_json::to_string_pretty(&*ledger).unwrap_or_default()
        };
        let _ = self.files.write_atomic(&self.state_file, &text);
    }

    pub(super) fn watch_dir(&self) -> Option<String> {
        self.watch_dir.lock().expect("watch dir").clone()
    }

    /// Whether `<storage>/staging` is there to sweep beside the import folder.
    fn has_staging(&self, watch_dir: Option<&str>) -> bool {
        self.files.exists(&self.staging_dir)
            && watch_dir.is_none_or(|dir| resolve(dir) != resolve(&self.staging_dir))
    }

    /// The prefix the ledger rows of a sweep of `dir` carry: none for the import folder.
    pub(super) fn prefix_of(&self, dir: &str) -> &'static str {
        match self.watch_dir() {
            Some(watch_dir) if resolve(&watch_dir) == resolve(dir) => "",
            _ => STAGED,
        }
    }

    /// The first megabyte of a file as tags.
    fn read_tags(&self, full: &str) -> Option<Tags> {
        parse_tags(&self.files.read_head(full, TAG_BYTES)?)
    }

    /// The media files under `root`, in name order, one level of folders deep. A link to a folder
    /// is skipped instead of publishing files from outside the import folder. Dot and `@` names
    /// are skipped, which keeps macOS `._01.flac` and Synology's `@eaDir` out.
    fn list_media(&self, root: &str) -> Vec<Found> {
        let entries = |dir: &str| {
            let mut entries = self.files.list_entries(dir);
            entries.sort_by(|a, b| a.name.encode_utf16().cmp(b.name.encode_utf16()));
            entries.retain(|entry| !entry.name.starts_with(['.', '@']));
            entries
        };
        let stat_of = |path: &str| {
            self.files
                .stat(path)
                .filter(|stat| stat.is_file && stat.size > 0)
        };
        let mut out = Vec::new();
        for entry in entries(root) {
            let full = join(root, &entry.name);
            if entry.is_dir {
                for kid in entries(&full) {
                    if kid.is_dir || !is_media_file(&kid.name) {
                        continue;
                    }
                    let path = join(&full, &kid.name);
                    if let Some(stat) = stat_of(&path) {
                        out.push(Found {
                            rel: format!("{}/{}", entry.name, kid.name),
                            dir: Some(entry.name.clone()),
                            full: path,
                            stat,
                        });
                    }
                }
            } else if is_media_file(&entry.name)
                && let Some(stat) = stat_of(&full)
            {
                out.push(Found {
                    rel: entry.name,
                    dir: None,
                    full,
                    stat,
                });
            }
        }
        out
    }

    /// The release a folder is pinned to (`<folder>/.peartube-album.json`
    /// `{ release, title?, artist? }`), with the title and the artist the pin names (empty when
    /// it names none).
    fn read_album_pin(&self, folder: &str) -> Option<(String, String, String)> {
        let text = self.files.read(&join(folder, ALBUM_PIN))?;
        let pin: Value = serde_json::from_str(&crate::ipc::well_formed(&text)).ok()?;
        let release = pin
            .get("release")?
            .as_str()
            .filter(|id| is_release_id(id))?;
        let named = |key: &str| {
            let text = pin.get(key).and_then(Value::as_str).unwrap_or_default();
            truncate_utf16(text, MAX_TITLE)
        };
        Some((release.to_owned(), named("title"), named("artist")))
    }

    /// One album's identity for the files of a folder (`albumFor`). Files already imported
    /// contribute the id they were stored with, so adding a track to an album that is already on
    /// the channel does not renumber its neighbours when the new file's tags read differently.
    fn album_for(&self, group: &[&Found], root: &str, prefix: &str) -> Option<Album> {
        let said: Vec<Option<(Tags, &str)>> = group
            .iter()
            .map(|file| {
                let imported = {
                    let ledger = self.ledger.lock().expect("mirror ledger");
                    ledger
                        .get(&format!("{prefix}{}", file.rel))
                        .and_then(|row| row.get("id"))
                        .and_then(Value::as_str)
                        .and_then(imported_release)
                };
                imported
                    .or_else(|| self.read_tags(&file.full))
                    .map(|tags| (tags, file.full.as_str()))
            })
            .collect();
        let identity = album_identity(&said);
        let folder = group[0].dir.as_deref().unwrap_or_default();
        let Some((release, title, artist)) = self.read_album_pin(&join(root, folder)) else {
            return identity;
        };
        let title = Some(title)
            .filter(|title| !title.is_empty())
            .or(identity.map(|album| album.title))
            .filter(|title| !title.is_empty())
            .unwrap_or_else(|| folder.to_owned());
        Some(Album {
            release,
            title,
            artist,
            tracks: group.len(),
            pinned: true,
        })
    }

    /// `scanFolder`: one sweep of `dir`, whose ledger rows carry `prefix`. A sweep asked for while
    /// another runs answers `busy` at once.
    pub(super) async fn scan<N>(&self, dir: Option<&str>, ctx: &Ctx<N>, prefix: &str) -> Value
    where
        N: Node + 'static,
        N::Error: From<AcquireError>,
    {
        let Some(dir) = dir.filter(|dir| self.files.exists(dir)) else {
            return empty_scan();
        };
        if self.scanning.swap(true, Ordering::SeqCst) {
            return json!({"scanned": 0, "ingested": 0, "busy": true, "items": []});
        }
        let result = self.scan_inner(dir, ctx, prefix).await;
        self.scanning.store(false, Ordering::SeqCst);
        result
    }

    /// `placeAndScan`: a provider's finished files and their import as one step. `place` moves the
    /// files into the import folder only once no other sweep holds it, and this sweep starts in
    /// the same step, so no other sweep sees a half-placed album. While it waits nothing is
    /// placed: a save that is cancelled, or that gives up after `import_wait_ms` (a first import
    /// of a large folder can run for hours), leaves nothing behind.
    pub(super) async fn place_and_scan<N>(
        &self,
        dir: &str,
        ctx: &Ctx<N>,
        prefix: &str,
        place: Place,
        cancelled: Cancelled,
    ) -> Result<Value, PlaceError>
    where
        N: Node + 'static,
        N::Error: From<AcquireError>,
    {
        let stopped = || self.closed.load(Ordering::SeqCst) || cancelled();
        let end = (self.now_ms)().saturating_add(self.import_wait_ms);
        // Taking the flag is what ends the wait, so no other sweep starts between that and `place`.
        while self.scanning.swap(true, Ordering::SeqCst) {
            if stopped() {
                return Err(PlaceError::cancelled());
            }
            if (self.now_ms)() >= end {
                return Err(PlaceError {
                    message: format!(
                        "Another import has held the folder for {} s. Nothing was saved; try again when it finishes.",
                        self.import_wait_ms.div_ceil(1000)
                    ),
                    status: Some(503),
                });
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        let _held = Holding(&self.scanning);
        // The folder also comes free when the relay closes (the running import stops between
        // files) or the job was cancelled in the last moment of the wait: neither may place
        // anything.
        if stopped() {
            return Err(PlaceError::cancelled());
        }
        place().map_err(|message| PlaceError {
            message,
            status: None,
        })?;
        Ok(if self.files.exists(dir) {
            self.scan_inner(dir, ctx, prefix).await
        } else {
            empty_scan()
        })
    }

    async fn scan_inner<N>(&self, dir: &str, ctx: &Ctx<N>, prefix: &str) -> Value
    where
        N: Node + 'static,
        N::Error: From<AcquireError>,
    {
        let root = resolve(dir);
        let files = self.list_media(&root);
        // Nested folders are albums, so each is identified once; root files are handled one at a
        // time.
        let mut albums: Vec<(&str, Vec<&Found>)> = Vec::new();
        for file in &files {
            let Some(dir) = file.dir.as_deref() else {
                continue;
            };
            match albums.iter_mut().find(|(name, _)| *name == dir) {
                Some((_, group)) => group.push(file),
                None => albums.push((dir, vec![file])),
            }
        }
        let unchanged = |file: &Found| {
            let ledger = self.ledger.lock().expect("mirror ledger");
            ledger
                .get(&format!("{prefix}{}", file.rel))
                .is_some_and(|row| {
                    row.get("size").and_then(Value::as_u64) == Some(file.stat.size)
                        && row.get("mtimeMs").and_then(Value::as_f64)
                            == Some(file.stat.mtime_ms.trunc())
                })
        };
        // An album is identified only when it has a file to import: reading every imported album's
        // tags on each poll would read a megabyte per file for nothing.
        let identities: Vec<(&str, Option<Album>)> = albums
            .iter()
            .filter(|(_, group)| !group.iter().all(|file| unchanged(file)))
            .map(|(name, group)| (*name, self.album_for(group, &root, prefix)))
            .collect();
        let mut scanned = 0u64;
        let mut ingested = Vec::new();
        let mut failed = Vec::new();
        for file in &files {
            if self.closed.load(Ordering::SeqCst) {
                break;
            }
            scanned += 1;
            if unchanged(file) {
                continue;
            }
            let sidecar_path = format!("{}.json", file.full);
            let sidecar = if self.files.exists(&sidecar_path) {
                self.files
                    .read(&sidecar_path)
                    .and_then(|text| serde_json::from_str::<Value>(&text).ok())
            } else {
                None
            };
            // A file with no track number of its own takes the place it holds in the folder, so a
            // folder of untagged rips still numbers its tracks 1..n.
            let place = file.dir.as_deref().and_then(|dir| {
                let (_, group) = albums.iter().find(|(name, _)| *name == dir)?;
                let position = group.iter().position(|other| other.rel == file.rel)? + 1;
                let identity = identities.iter().find(|(name, _)| *name == dir)?;
                Some((identity.1.as_ref()?, position))
            });
            let tags = file.dir.as_ref().and_then(|_| self.read_tags(&file.full));
            let meta = infer_media_meta(&file.full, sidecar.as_ref(), tags.as_ref(), place);
            let name = basename(&file.rel);
            // One bad file must not stop the album or the scan: the failure is reported in the
            // result, not thrown.
            let op = match self.files.open_read(&file.full) {
                Ok(reader) => {
                    ctx.ingest_stream(&meta.id, &meta.title, reader, false, meta.music)
                        .await
                }
                Err(message) => Err(MirrorError::plain(message)),
            };
            let op = match op {
                Ok(op) => op,
                Err(error) => {
                    failed.push(json!({ "file": name, "rel": file.rel, "error": error.message }));
                    continue;
                }
            };
            let field = |key: &str| op.get(key).cloned().unwrap_or(Value::Null);
            let mut row = Map::new();
            row.insert("size".into(), json!(file.stat.size));
            row.insert("mtimeMs".into(), json!(file.stat.mtime_ms.trunc() as i64));
            row.insert("id".into(), field("id"));
            row.insert("blobs".into(), field("blobs"));
            row.insert("sha256".into(), field("sha256"));
            row.insert("ingestedAt".into(), json!((self.now_ms)()));
            self.ledger
                .lock()
                .expect("mirror ledger")
                .insert(format!("{prefix}{}", file.rel), Value::Object(row));
            self.save();
            ingested.push(json!({
                "file": name,
                "rel": file.rel,
                "id": field("id"),
                "title": field("title"),
                "size": field("size"),
                "sha256": field("sha256"),
            }));
        }
        json!({
            "scanned": scanned,
            "ingested": ingested.len(),
            "failed": failed,
            "items": ingested,
        })
    }

    /// Whether this relay mirrored `full`: its album folder's list names it, or that folder carries
    /// a pin (only a mirror writes one; mirrors made before the list existed have the pin and no
    /// list).
    fn mirrored_here(&self, full: &str, root: &str) -> bool {
        let folder = dirname(full);
        if resolve(&folder) == resolve(root) {
            return false;
        }
        let name = basename(full);
        let listed = self
            .files
            .read(&join(&folder, MIRRORED_LIST))
            .and_then(|text| serde_json::from_str::<Value>(&crate::ipc::well_formed(&text)).ok())
            .is_some_and(|list| match list {
                Value::Array(names) => names.iter().any(|item| item.as_str() == Some(&name)),
                // `includes`, which a string answers too.
                Value::String(text) => text.contains(&name),
                _ => false,
            });
        listed || self.files.exists(&join(&folder, ALBUM_PIN))
    }

    /// Removes an album folder that has no files left, with its pin; a folder that still has some
    /// keeps a list of the mirrored ones that are still there.
    fn drop_empty_album_folder(&self, folder: &str, root: &str) {
        if resolve(folder) == resolve(root) {
            return;
        }
        let left: Vec<String> = self
            .files
            .list_entries(folder)
            .into_iter()
            .map(|entry| entry.name)
            .collect();
        if left
            .iter()
            .any(|name| name != ALBUM_PIN && name != MIRRORED_LIST)
        {
            let list = join(folder, MIRRORED_LIST);
            let listed = left
                .iter()
                .any(|name| name == MIRRORED_LIST)
                .then(|| self.files.read(&list))
                .flatten()
                .and_then(|text| {
                    serde_json::from_str::<Value>(&crate::ipc::well_formed(&text)).ok()
                });
            if let Some(Value::Array(listed)) = listed {
                let still: Vec<Value> = listed
                    .into_iter()
                    .filter(|item| left.iter().any(|name| item.as_str() == Some(name)))
                    .collect();
                let _ = self
                    .files
                    .write(&list, &format!("{}\n", Value::Array(still)));
            }
            return;
        }
        for name in &left {
            if self.files.remove_file(&join(folder, name)) != Ok(true) {
                return;
            }
        }
        self.files.remove_dir(folder);
    }

    /// After the owner removed announces: each `(id, sha256)` this relay imported from a folder
    /// loses its file and its ledger row when this relay mirrored that file, so the next scan has
    /// nothing to import again (`forgetImported`). Mirrored means: it sits in `<storage>/staging`,
    /// or [`Self::mirrored_here`]. A file the owner put in the import folder stays, and so does its
    /// row: the row matches the untouched file, which keeps the scan from importing it again until
    /// the file changes. Scans wait while this runs. Returns `id/sha256` with `deleted` or `kept`
    /// for each file it found.
    pub(super) async fn forget_imported(
        &self,
        gone: &[(String, String)],
    ) -> Vec<(String, &'static str)> {
        let want: Vec<String> = gone
            .iter()
            .map(|(id, sha256)| format!("{id}/{sha256}"))
            .collect();
        let mut out: Vec<(String, &'static str)> = Vec::new();
        for _ in 0..600 {
            if !self.scanning.load(Ordering::SeqCst) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        self.scanning.store(true, Ordering::SeqCst);
        let rows: Vec<(String, Value)> = {
            let ledger = self.ledger.lock().expect("mirror ledger");
            ledger
                .iter()
                .map(|(key, row)| (key.clone(), row.clone()))
                .collect()
        };
        let watch_dir = self.watch_dir();
        let mut changed = false;
        for (key, row) in rows {
            let text = |field: &str| row.get(field).map_or_else(|| "undefined".into(), js_string);
            let file_key = format!("{}/{}", text("id"), text("sha256"));
            if !want.contains(&file_key) {
                continue;
            }
            let keep = |out: &mut Vec<(String, &'static str)>| {
                if !out.iter().any(|(seen, _)| *seen == file_key) {
                    out.push((file_key.clone(), "kept"));
                }
            };
            let staged = key.strip_prefix(STAGED);
            let root = match staged {
                Some(_) => Some(self.staging_dir.as_str()),
                None => watch_dir.as_deref(),
            };
            // A row that names a path outside its root is never followed.
            let inside = root.and_then(|root| {
                let full = resolve(&join(root, staged.unwrap_or(&key)));
                let within = format!("{}{}", resolve(root), std::path::MAIN_SEPARATOR);
                full.starts_with(&within).then_some((full, root))
            });
            let Some((full, root)) =
                inside.filter(|(full, root)| staged.is_some() || self.mirrored_here(full, root))
            else {
                keep(&mut out);
                continue;
            };
            if self.files.remove_file(&full).is_err() {
                keep(&mut out);
                continue;
            }
            let _ = self.files.remove_file(&format!("{full}.json"));
            self.ledger
                .lock()
                .expect("mirror ledger")
                .shift_remove(&key);
            changed = true;
            self.drop_empty_album_folder(&dirname(&full), root);
            match out.iter_mut().find(|(seen, _)| *seen == file_key) {
                Some(entry) => entry.1 = "deleted",
                None => out.push((file_key, "deleted")),
            }
        }
        if changed {
            self.save();
        }
        self.scanning.store(false, Ordering::SeqCst);
        out
    }
}

/// `path.dirname(path)`.
fn dirname(path: &str) -> String {
    Path::new(path)
        .parent()
        .map(|parent| parent.to_string_lossy().into_owned())
        .unwrap_or_default()
}

fn empty_scan() -> Value {
    json!({"scanned": 0, "ingested": 0, "items": []})
}

pub(super) struct WatchDirPlugin {
    pub(super) state: Arc<WatchState>,
}

impl WatchDirPlugin {
    /// One sweep of the import folder and of `<storage>/staging`, each when it is there; `None`
    /// when neither is.
    async fn sweep<N>(&self, ctx: &Ctx<N>) -> Option<(Option<Value>, Option<Value>)>
    where
        N: Node + 'static,
        N::Error: From<AcquireError>,
    {
        let watch_dir = self.state.watch_dir();
        let staging = self.state.has_staging(watch_dir.as_deref());
        if watch_dir.is_none() && !staging {
            return None;
        }
        let watched = match &watch_dir {
            Some(dir) => Some(self.state.scan(Some(dir), ctx, "").await),
            None => None,
        };
        let staged = match staging {
            true => {
                let dir = self.state.staging_dir.clone();
                Some(self.state.scan(Some(&dir), ctx, STAGED).await)
            }
            false => None,
        };
        Some((watched, staged))
    }
}

/// The two sweeps as one result: what each scanned and imported, added up.
fn merged(watched: Option<&Value>, staged: Option<&Value>, with_busy: bool) -> Value {
    let both = [watched, staged];
    let count = |key: &str| -> u64 {
        both.iter()
            .flatten()
            .filter_map(|scan| scan.get(key)?.as_u64())
            .sum()
    };
    let items: Vec<Value> = both
        .iter()
        .flatten()
        .filter_map(|scan| scan.get("items")?.as_array())
        .flatten()
        .cloned()
        .collect();
    let busy = both
        .iter()
        .flatten()
        .any(|scan| scan.get("busy") == Some(&Value::Bool(true)));
    let mut out = Map::new();
    out.insert("scanned".into(), json!(count("scanned")));
    out.insert("ingested".into(), json!(count("ingested")));
    if with_busy && busy {
        out.insert("busy".into(), json!(true));
    }
    out.insert("items".into(), Value::Array(items));
    Value::Object(out)
}

impl<N: Node + 'static> MirrorPlugin<N> for WatchDirPlugin
where
    N::Error: From<AcquireError>,
{
    fn name(&self) -> &str {
        "watch-dir"
    }

    fn description(&self) -> &str {
        "Ingests media files dropped into the import folder (PEARTUBE_WATCH_DIR) once, tracking fingerprints in mirror-watch.json"
    }

    fn run<'a>(
        &'a self,
        _payload: Value,
        ctx: &'a Ctx<N>,
    ) -> Pin<Box<dyn Future<Output = Result<Value, MirrorError>> + Send + 'a>> {
        Box::pin(async move {
            match self.sweep(ctx).await {
                Some((watched, staged)) => Ok(merged(watched.as_ref(), staged.as_ref(), true)),
                None => Ok(json!({
                    "skipped": true,
                    "reason": "watch_dir_not_configured",
                    "scanned": 0,
                    "ingested": 0,
                    "items": []
                })),
            }
        })
    }

    fn poll<'a>(
        &'a self,
        ctx: &'a Ctx<N>,
    ) -> Pin<Box<dyn Future<Output = Result<Option<Value>, MirrorError>> + Send + 'a>> {
        Box::pin(async move {
            Ok(self
                .sweep(ctx)
                .await
                .map(|(watched, staged)| merged(watched.as_ref(), staged.as_ref(), false)))
        })
    }

    fn info(&self) -> Value {
        // `/v1/plugins` is public: say whether a folder is watched, never which.
        let tracked = self.state.ledger.lock().expect("mirror ledger").len();
        let watching = self.state.watch_dir().is_some();
        json!({"watching": watching, "trackedFiles": tracked})
    }
}

pub(super) fn load_ledger(files: &dyn MirrorFiles, storage: &str) -> Map<String, Value> {
    let path = format!("{storage}/mirror-watch.json");
    files
        .read(&path)
        .and_then(|text| serde_json::from_str::<Value>(&text).ok())
        .and_then(|value| value.as_object().cloned())
        .unwrap_or_default()
}

/// Whether files can be made in `dir` (made if missing): a read-only mount reports as present but
/// refuses writes.
pub(super) fn writable_dir(files: &dyn MirrorFiles, dir: &str, now_ms: u64) -> bool {
    let probe = join(
        dir,
        &format!(".peartube-write-{now_ms:x}{:x}", std::process::id()),
    );
    files.mkdirs(dir);
    files.write(&probe, "").is_ok() && files.remove_file(&probe) == Ok(true)
}

/// `<storage>/staging`.
pub(super) fn staging_dir(storage: &str) -> String {
    join(storage, "staging")
}

/// `path.resolve`, for the host's choice of where mirrors land.
pub(super) fn resolved(path: &str) -> String {
    resolve(path)
}
