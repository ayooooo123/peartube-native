//! Where a mirrored album or a lone track goes in the import folder, named from what the peer and
//! the file say (`safeSegment`, `peerFolderHint`, `formatAlbumDir` and `placeSingleTrack` in
//! `src/providers/soulseek.js`).
//!
//! A name cut at its length limit inside a surrogate pair ends in U+FFFD here; the app's string
//! holds half a pair until the file system call or `toWellFormed` replaces it with the same.

use sha2::{Digest, Sha256};

use crate::protocol::{MAX_TITLE, is_js_space, js_trim, truncate_utf16};
use crate::tags::{Tags, album_key, year_in};

/// One path segment that every file system takes: no separators, reserved or control characters,
/// no leading dots or `@` (a hidden or system folder), at most 140 UTF-16 units, no trailing dots
/// or spaces (`safeSegment`). `fallback` when nothing is left.
pub(crate) fn safe_segment(raw: &str, fallback: &str) -> String {
    let reserved = |c: char| {
        matches!(c, '/' | '\\' | ':' | '*' | '?' | '"' | '<' | '>' | '|') || c <= '\u{1f}'
    };
    let spaced = one_space_for(raw, reserved);
    let shown = spaced.trim_start_matches(|c: char| c == '.' || c == '@' || is_js_space(c));
    let single = one_space_for(shown, is_js_space);
    let cut = truncate_utf16(js_trim(&single), 140);
    let clean = cut.trim_end_matches(|c: char| c == '.' || is_js_space(c));
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

/// The album a peer's folder names (`peerFolderHint`). `artist` or `album` may be empty.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct FolderHint {
    pub artist: String,
    pub album: String,
    pub year: Option<u32>,
}

/// The album the folder of `file` names: `Artist - Album (Year)`, or `Album` under an `Artist`
/// folder, or `Year - Album` under one. A `CD1`/`Disc 2` folder is a disc of the album above it.
/// `None` for a file with no folder (share roots, `@@…`, do not count as one).
pub(crate) fn peer_folder_hint(file: &str) -> Option<FolderHint> {
    let path = file.replace('\\', "/");
    let parts: Vec<&str> = path
        .split('/')
        .map(js_trim)
        .filter(|part| !part.is_empty() && !part.starts_with("@@"))
        .collect();
    if parts.len() < 2 {
        return None;
    }
    let disc = parts.len() >= 3 && is_disc_folder(parts[parts.len() - 2]);
    let at = parts.len() - if disc { 3 } else { 2 };
    let album_dir = parts[at];
    let parent = if at >= 1 { parts[at - 1] } else { "" };
    let year = year_in(album_dir);
    let unbracketed = without_bracketed_years(album_dir);
    let stripped = js_trim(&unbracketed);
    let split = split_at_dashes(stripped);
    let rest = || js_trim(&split[1..].join(" - ")).to_owned();
    let hint = if split.len() >= 2 && is_year(js_trim(split[0])) {
        FolderHint {
            artist: parent.to_owned(),
            album: rest(),
            year,
        }
    } else if split.len() >= 2 {
        FolderHint {
            artist: js_trim(split[0]).to_owned(),
            album: rest(),
            year,
        }
    } else {
        let album = if stripped.is_empty() {
            album_dir
        } else {
            stripped
        };
        FolderHint {
            artist: parent.to_owned(),
            album: album.to_owned(),
            year,
        }
    };
    Some(hint)
}

/// `cd`, `disc` or `disk` (any case), then one or two digits, spaces between allowed.
fn is_disc_folder(name: &str) -> bool {
    ["cd", "disc", "disk"].iter().any(|word| {
        let Some(number) = name
            .get(..word.len())
            .filter(|head| head.eq_ignore_ascii_case(word))
            .map(|_| name[word.len()..].trim_start_matches(is_js_space))
        else {
            return false;
        };
        (1..=2).contains(&number.len()) && number.bytes().all(|byte| byte.is_ascii_digit())
    })
}

/// Four digits from 1800 to 2099, and nothing else.
fn is_year(text: &str) -> bool {
    let bytes = text.as_bytes();
    bytes.len() == 4
        && bytes.iter().all(u8::is_ascii_digit)
        && matches!(bytes, [b'1', b'8' | b'9', ..] | [b'2', b'0', ..])
}

/// `text` without each `(1997)` or `[1997]`, spaces inside the brackets allowed.
fn without_bracketed_years(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    let mut rest = text;
    while let Some(c) = rest.chars().next() {
        let after = &rest[c.len_utf8()..];
        let close = match c {
            '(' => Some(')'),
            '[' => Some(']'),
            _ => None,
        };
        let past_year = close.and_then(|close| {
            let inner = after.trim_start_matches(is_js_space);
            let year = inner.get(..4).filter(|year| is_year(year))?;
            inner[year.len()..]
                .trim_start_matches(is_js_space)
                .strip_prefix(close)
        });
        match past_year {
            Some(past) => rest = past,
            None => {
                out.push(c);
                rest = after;
            }
        }
    }
    out
}

/// `text.split(/\s+[-–—]\s+/)`: the parts between dashes that have space on both sides.
fn split_at_dashes(text: &str) -> Vec<&str> {
    let mut parts = Vec::new();
    let mut start = 0;
    let mut scan = 0;
    while let Some(found) = text[scan..].find(is_js_space) {
        let space = scan + found;
        let dash = text[space..].trim_start_matches(is_js_space);
        let after = dash.strip_prefix(['-', '–', '—']).unwrap_or(dash);
        let next = after.trim_start_matches(is_js_space);
        if after.len() < dash.len() && next.len() < after.len() {
            parts.push(&text[start..space]);
            start = text.len() - next.len();
            scan = start;
        } else {
            scan = text.len() - dash.len();
        }
    }
    parts.push(&text[start..]);
    parts
}

/// The folder an album is mirrored into (`formatAlbumDir`): `Artist - Album (Year)`, else the
/// caller's title (its ` · ` as ` - `), else the album alone.
pub(crate) fn format_album_dir(
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

/// What names one mirrored track (`placeSingleTrack`'s argument).
#[derive(Debug, Clone, Copy, Default)]
pub struct LoneTrack<'a> {
    /// The file's own tags, when it has any.
    pub tags: Option<&'a Tags>,
    /// Its path on the peer.
    pub filename: &'a str,
    /// The release the caller named.
    pub release: Option<&'a str>,
    /// The track number the caller named.
    pub track: Option<u32>,
    /// The place it sorts at in its folder on the peer.
    pub position: Option<u32>,
    /// The caller's title.
    pub fallback_title: &'a str,
}

/// Where one mirrored track goes (`placeSingleTrack`'s result).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Placed {
    pub folder: String,
    pub file: String,
    /// The release the folder is pinned to.
    pub release: String,
    pub album_title: String,
    pub track_title: String,
    /// What named the album: `release` (the caller), `tags` or `peer folder`.
    pub from: &'static str,
}

/// Where one mirrored track goes, or why it cannot go anywhere (`placeSingleTrack`). A lone file
/// has no neighbours to name its album, so it needs its own: the album from its tags, else from
/// its folder on the peer ("Artist - Album (Year)"); the track number from its tags, else the
/// caller's, else the leading number of its file name, else the place it sorts at in its folder
/// on the peer (vinyl rips are named by side, "A1".."B6"). Its folder is pinned to that release
/// (the one the importer would hash from the same names, so it joins the rest of the album) and
/// the file is named `NN - Title.ext`. Without an album or a number it is an error, never a clip.
pub fn place_single_track(track: LoneTrack<'_>) -> Result<Placed, String> {
    let path = track.filename.replace('\\', "/");
    let leaf = path.rsplit('/').next().unwrap_or_default();
    let (stem, ext) = split_ext(leaf);
    let ext = if ext.is_empty() { ".flac" } else { ext };
    let hint = peer_folder_hint(&path);
    let tags = track.tags;
    let tag_album = said(tags.and_then(|tags| tags.album.as_deref()));
    let tag_artist = said(tags.and_then(|tags| tags.album_artist.as_deref()))
        .or(said(tags.and_then(|tags| tags.artist.as_deref())));
    let tag_year = tags.and_then(|tags| tags.year).filter(|year| *year > 0);
    let mbid = said(tags.and_then(|tags| tags.mbid.as_deref()));
    let discogs = said(tags.and_then(|tags| tags.discogs.as_deref()));
    let release = said(track.release);
    // A tagged album names everything; otherwise the peer's folder does, helped by the tags.
    let (artist, album, year) = match tag_album {
        Some(album) => (tag_artist.unwrap_or_default(), album, tag_year),
        None => (
            said(hint.as_ref().map(|hint| hint.artist.as_str()))
                .or(tag_artist)
                .unwrap_or_default(),
            hint.as_ref().map_or("", |hint| hint.album.as_str()),
            hint.as_ref().and_then(|hint| hint.year).or(tag_year),
        ),
    };
    if album.is_empty() && release.is_none() && mbid.is_none() && discogs.is_none() {
        return Err(format!(
            "Could not place “{leaf}”: it has no album tag and its folder on the peer does not name an album. Mirror its whole folder instead."
        ));
    }
    // One to three leading digits; a longer run is not a track number.
    let lead = stem.bytes().take_while(u8::is_ascii_digit).count();
    let from_name = (1..=3)
        .contains(&lead)
        .then(|| stem[..lead].parse::<u32>().ok())
        .flatten();
    let given = |number: Option<u32>| number.filter(|number| *number > 0);
    let number = given(tags.and_then(|tags| tags.track))
        .or(given(track.track))
        .or(given(from_name))
        .or(given(track.position))
        .unwrap_or(0);
    if !(1..=999).contains(&number) {
        return Err(format!(
            "Could not place “{leaf}”: neither its tags, its file name nor its folder on the peer give a track number."
        ));
    }
    // The title is the name without its number ("03 - ") or its vinyl side ("A1 ").
    let unnumbered = if from_name.is_some() {
        Some(&stem[lead..])
    } else {
        after_side(stem)
    };
    let named = js_trim(unnumbered.map_or(stem, after_separators));
    let named = if named.is_empty() { stem } else { named };
    let title = said(tags.and_then(|tags| tags.title.as_deref())).unwrap_or(named);
    let pin = match (release, mbid, discogs) {
        (Some(release), _, _) => release.to_owned(),
        (None, Some(mbid), _) => format!("mb:{mbid}"),
        (None, None, Some(discogs)) => format!("discogs:r{discogs}"),
        (None, None, None) => {
            let key = album_key(Some(artist), Some(album), year);
            let hash: String = Sha256::digest(key)
                .iter()
                .take(10)
                .map(|byte| format!("{byte:02x}"))
                .collect();
            format!("album:{hash}")
        }
    };
    // A release known only by its id still gets a folder of its own, never one shared with other
    // unnamed albums.
    let unnamed;
    let fallback = if !album.is_empty() {
        ""
    } else if !track.fallback_title.is_empty() {
        track.fallback_title
    } else {
        unnamed = format!("Release {}", pin.replace(':', " "));
        &unnamed
    };
    let prefix = format!("{number:02}");
    let names: Vec<&str> = [artist, album]
        .into_iter()
        .filter(|name| !name.is_empty())
        .collect();
    Ok(Placed {
        folder: format_album_dir(artist, album, year, fallback),
        file: safe_segment(&format!("{prefix} - {title}"), &prefix) + ext,
        album_title: truncate_utf16(&names.join(" · "), MAX_TITLE),
        track_title: title.to_owned(),
        from: if release.is_some() {
            "release"
        } else if tag_album.is_some() || mbid.is_some() || discogs.is_some() {
            "tags"
        } else {
            "peer folder"
        },
        release: pin,
    })
}

/// `text` when it says something: the app reads an empty string as none.
fn said(text: Option<&str>) -> Option<&str> {
    text.filter(|text| !text.is_empty())
}

/// A file name as its stem and its extension with the dot (`basename(name, ext)` and
/// `extname(name)`): a name that only starts with its dot, and "..", have no extension.
pub(crate) fn split_ext(name: &str) -> (&str, &str) {
    match name.rfind('.') {
        Some(dot) if dot > 0 && name != ".." => name.split_at(dot),
        _ => (name, ""),
    }
}

/// `stem` after a vinyl side and number ("A1", "b12"): one letter from A to H, then one or two
/// digits. `None` when it does not start with one.
fn after_side(stem: &str) -> Option<&str> {
    let rest = stem.strip_prefix(|c: char| matches!(c, 'a'..='h' | 'A'..='H'))?;
    let digits = rest.bytes().take_while(u8::is_ascii_digit).count();
    (1..=2).contains(&digits).then(|| &rest[digits..])
}

/// `text` after spaces, then any of `-._ `, then spaces.
fn after_separators(text: &str) -> &str {
    text.trim_start_matches(is_js_space)
        .trim_start_matches(['-', '.', '_', ' '])
        .trim_start_matches(is_js_space)
}
