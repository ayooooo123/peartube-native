//! What a music file says about itself, read from its own header without any network or decoder
//! (`src/tags.js`).
//!
//! Everything here is a claim by the file's encoder, and nothing else: a FLAC or MP3 can carry any
//! album, artist and MusicBrainz id its creator typed. [`parse_tags`] only reads, and only the tags
//! a relay needs to name a track (album artist, album, artist, title, disc and track numbers, year)
//! plus a MusicBrainz release id when the tags carry one. It never fails on malformed input: a
//! block that does not parse ends the walk and yields what was read so far, since a half-tagged
//! album is still better than none.
//!
//! Sizes: an ID3v2.4 tag length is synchsafe (7 bits per byte, four bytes), and v2.3's is a plain
//! 32-bit integer. Reading either as the other is how tag readers report a 180-byte tag as 13 MB,
//! so both are handled and the tag is bounded by the buffer it arrives in.
//!
//! A JavaScript string can hold a lone surrogate (half of a pair cut by the 300-unit limit, or one
//! written into a UTF-16 frame); a Rust string cannot. Here it becomes U+FFFD at once, which is
//! what the app's `toWellFormed` makes of it before a tag leaves the relay.

use crate::protocol::{is_js_space, media_format, truncate_utf16};
use icu_properties::props::{EnumeratedProperty, GeneralCategory, GeneralCategoryGroup};
use unicode_normalization::UnicodeNormalization;

const MAX_TEXT: usize = 300;

/// The tags a catalog needs, from either family of comment blocks, plus what the container is
/// (`parseTags`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Tags {
    pub container: &'static str,
    pub album_artist: Option<String>,
    pub album: Option<String>,
    pub artist: Option<String>,
    pub title: Option<String>,
    pub year: Option<u32>,
    pub disc: Option<u32>,
    pub track: Option<u32>,
    /// A MusicBrainz release id: 32 lowercase hex digits, hyphens removed.
    pub mbid: Option<String>,
    /// A Discogs release id: up to 12 digits.
    pub discogs: Option<String>,
}

/// Tag names and values in the order first written, the way the app's `Map` keeps them: a name
/// written again keeps its place and takes the new value.
#[derive(Default)]
struct TagMap(Vec<(String, String)>);

impl TagMap {
    fn set(&mut self, key: String, value: String) {
        match self.0.iter_mut().find(|(name, _)| *name == key) {
            Some(entry) => entry.1 = value,
            None => self.0.push((key, value)),
        }
    }

    /// The first of `keys` that has a value which is not empty (`map.get(a) || map.get(b)`).
    fn first(&self, keys: &[&str]) -> Option<&str> {
        keys.iter().find_map(|key| {
            let (_, value) = self.0.iter().find(|(name, _)| name == key)?;
            (!value.is_empty()).then_some(value.as_str())
        })
    }
}

/// `parseTags(buf)`: `None` when the bytes are not a tagged FLAC, Ogg or MP3 file.
pub fn parse_tags(buf: &[u8]) -> Option<Tags> {
    let container = media_format(buf)?;
    let map = match container {
        "flac" => flac_comments(buf),
        "ogg" => ogg_comments(buf),
        _ if container == "mp3" || buf.starts_with(b"ID3") => id3_frames(buf),
        _ => return None,
    };
    if map.0.is_empty() {
        return None;
    }
    let text = |keys: &[&str]| map.first(keys).map(str::to_owned);
    // FLAC spells these in the comment; ID3 has dedicated frames (TPOS is a disc, TRCK a track), and
    // both write them as "n" or "n/total", which `num` keeps the first part of.
    let count = |keys: &[&str]| num(map.first(keys)).filter(|n| *n > 0);
    Some(Tags {
        container,
        album_artist: text(&["ALBUMARTIST", "ALBUM ARTIST", "TPE2"]),
        album: text(&["ALBUM", "TALB"]),
        artist: text(&["ARTIST", "TPE1"]),
        title: text(&["TITLE", "TIT2"]),
        year: map
            .first(&["DATE", "YEAR", "TDRC", "TYER"])
            .and_then(year_in),
        disc: count(&["DISCNUMBER", "DISCNO", "DISK", "TPOS"]),
        track: count(&["TRACKNUMBER", "TRACK", "TRCK"]),
        mbid: mbid(&map),
        discogs: map
            .first(&["DISCOGS_RELEASE_ID", "DISCOGSRELEASEID"])
            .and_then(digits),
    })
}

/// The album identity two rips of the same album must share, and two different albums must not
/// (`albumKey`). Names are folded the way a person compares them: case, punctuation, spacing and
/// accents carry no meaning here. Letters and digits of every script are kept: folding to a-z alone
/// made every album named in Chinese, Japanese, Korean, Cyrillic or Greek the same album. The year
/// is part of it because a self-titled album and its remaster are different releases; the disc and
/// track count are not, because a lossless and a lossy rip of one album may drop the bonus disc and
/// still be the same album.
pub fn album_key(album_artist: Option<&str>, album: Option<&str>, year: Option<u32>) -> String {
    let fold = |text: Option<&str>| -> String {
        // Lowered as one string, not letter by letter: a capital sigma takes its final form at the
        // end of a word, as in the app's `toLowerCase`.
        let lowered = text
            .unwrap_or_default()
            .nfkd()
            .collect::<String>()
            .to_lowercase();
        // `\p{L}` and `\p{N}`. A mark is neither, so the app's separate removal of marks is covered.
        lowered
            .chars()
            .filter(|c| {
                let category = GeneralCategory::for_char(*c);
                GeneralCategoryGroup::Letter.contains(category)
                    || GeneralCategoryGroup::Number.contains(category)
            })
            .collect()
    };
    let year = year
        .filter(|year| *year > 0)
        .map(|year| year.to_string())
        .unwrap_or_default();
    format!("{}|{}|{year}", fold(album_artist), fold(album))
}

/// A numeric tag ("5", "05", "5/12") as an integer; a range keeps its first part, which is the
/// number itself. At most six digits are read.
fn num(value: Option<&str>) -> Option<u32> {
    let text = value?.trim_start_matches(is_js_space);
    let digits = text.bytes().take(6).take_while(u8::is_ascii_digit).count();
    text[..digits].parse().ok()
}

/// The year out of "1997", "1997-03-04", a full timestamp or a folder name: the first run of exactly
/// four digits from 1800 to 2099 that no letter, digit or underscore touches. Two-digit years are
/// not guessed at.
pub(crate) fn year_in(text: &str) -> Option<u32> {
    let bytes = text.as_bytes();
    let word = |byte: u8| byte.is_ascii_alphanumeric() || byte == b'_';
    bytes.windows(4).enumerate().find_map(|(at, run)| {
        let fits = matches!(run, [b'1', b'8' | b'9', ..] | [b'2', b'0', ..])
            && run[2].is_ascii_digit()
            && run[3].is_ascii_digit()
            && (at == 0 || !word(bytes[at - 1]))
            && bytes.get(at + 4).is_none_or(|next| !word(*next));
        if !fits {
            return None;
        }
        std::str::from_utf8(run).ok()?.parse().ok()
    })
}

/// The first run of digits in `text`, at most twelve of them.
fn digits(text: &str) -> Option<String> {
    let start = text.find(|c: char| c.is_ascii_digit())?;
    Some(
        text[start..]
            .chars()
            .take(12)
            .take_while(char::is_ascii_digit)
            .collect(),
    )
}

// MusicBrainz ids appear in several spellings across taggers: ffmpeg writes `MUSICBRAINZ_ALBUMID`
// as a TXXX description, Picard writes `MusicBrainz Album Id`, and a file may carry only
// `MUSICBRAINZ_RELEASEGROUPID`. A release id is what a track id needs to resolve against, so it
// wins whatever order the tags are stored in. A release-group id is deliberately NOT accepted: it
// names every edition of an album at once, so using it as a release id splits one album into two
// whenever two rips carry different tag kinds. Descriptions are compared with spaces and
// underscores removed, since each tagger spells the separator differently.
const MB_RELEASE_KEYS: [&str; 3] = [
    "MUSICBRAINZALBUMID",
    "MUSICBRAINZRELEASEID",
    "MUSICBRAINZALBUMRELEASEID",
];

/// The release id of the first release tag that holds one. No release id is the honest answer for
/// a file that carries only a release-group id; the caller falls back to the album hash.
fn mbid(map: &TagMap) -> Option<String> {
    map.0.iter().find_map(|(key, value)| {
        let name: String = key
            .chars()
            .filter(|c| !is_js_space(*c) && *c != '_')
            .collect();
        MB_RELEASE_KEYS
            .contains(&name.as_str())
            .then(|| mb_id(value))
            .flatten()
    })
}

/// The first MusicBrainz id in `text`: 32 hex digits in groups of 8, 4, 4, 4 and 12, each group
/// after the first with or without a hyphen before it. Hyphens are stripped and the digits
/// lowercased because the channel's id shape is lowercase alphanumeric.
fn mb_id(text: &str) -> Option<String> {
    let bytes = text.as_bytes();
    (0..bytes.len()).find_map(|start| {
        let mut at = start;
        let mut id = String::with_capacity(32);
        for (group, len) in [8, 4, 4, 4, 12].into_iter().enumerate() {
            if group > 0 && bytes.get(at) == Some(&b'-') {
                at += 1;
            }
            let run = bytes.get(at..at + len)?;
            if !run.iter().all(u8::is_ascii_hexdigit) {
                return None;
            }
            id.extend(run.iter().map(|byte| char::from(byte.to_ascii_lowercase())));
            at += len;
        }
        Some(id)
    })
}

fn le32(body: &[u8], at: usize) -> Option<i32> {
    let bytes = body.get(at..at.checked_add(4)?)?;
    Some(i32::from_le_bytes(bytes.try_into().ok()?))
}

/// A string with a little-endian 32-bit length before it, read at `at`, which moves past it.
/// `None` when the length is negative as a signed number or runs past the body.
fn prefixed<'a>(body: &'a [u8], at: &mut usize) -> Option<&'a [u8]> {
    let len = usize::try_from(le32(body, *at)?).ok()?;
    let start = *at + 4;
    let text = body.get(start..start.checked_add(len)?)?;
    *at = start + len;
    Some(text)
}

/// The Vorbis comment structure FLAC, Ogg Vorbis and Opus share: a vendor string and a list of
/// `KEY=value`, each with a little-endian 32-bit length. A body cut short (the read stopped inside
/// it) keeps the entries read so far.
fn vorbis_comment(body: &[u8], map: &mut TagMap) {
    let mut at = 0;
    if prefixed(body, &mut at).is_none() {
        return;
    }
    // The count is a bare little-endian 32-bit integer, not a length-prefixed string like the vendor.
    let Some(count) = le32(body, at) else {
        return;
    };
    at += 4;
    for _ in 0..count.clamp(0, 512) {
        let Some(entry) = prefixed(body, &mut at) else {
            break;
        };
        let text = String::from_utf8_lossy(entry);
        // An entry with no "=" or nothing before it names no tag.
        let Some((key, value)) = text.split_once('=').filter(|(key, _)| !key.is_empty()) else {
            continue;
        };
        map.set(key.to_uppercase(), truncate_utf16(value, MAX_TEXT));
    }
}

/// FLAC: "fLaC" then metadata blocks, each a 4-byte header (last flag, 7-bit type, 24-bit length).
/// Type 4 is the Vorbis comment.
fn flac_comments(buf: &[u8]) -> TagMap {
    let mut map = TagMap::default();
    let mut at = 4;
    for _ in 0..64 {
        let Some(head) = buf.get(at..at + 4) else {
            break;
        };
        let len = usize::from(head[1]) << 16 | usize::from(head[2]) << 8 | usize::from(head[3]);
        let Some(body) = buf.get(at + 4..at + 4 + len) else {
            break;
        };
        if head[0] & 0x7f == 4 {
            vorbis_comment(body, &mut map);
            break;
        }
        at += 4 + len;
        if head[0] & 0x80 != 0 {
            break;
        }
    }
    map
}

/// Ogg Vorbis and Opus: pages ("OggS", a 27-byte header, then a segment table). A packet is a run
/// of segments ending in one shorter than 255 bytes, and may go on over the next page. The second
/// packet is the comment header: "\x03vorbis" or "OpusTags", then the Vorbis comment structure.
/// Cover art can make it longer than the bytes read, so a packet cut short is parsed as far as it
/// goes.
fn ogg_comments(buf: &[u8]) -> TagMap {
    let mut map = TagMap::default();
    let mut packet: Vec<u8> = Vec::new();
    let mut packets = 0;
    let mut at = 0;
    'pages: for _ in 0..1024 {
        let Some(header) = buf
            .get(at..at + 27)
            .filter(|page| page.starts_with(b"OggS"))
        else {
            break;
        };
        let count = usize::from(header[26]);
        let Some(table) = buf.get(at + 27..at + 27 + count) else {
            break;
        };
        let mut data = at + 27 + count;
        for len in table.iter().map(|len| usize::from(*len)) {
            let Some(segment) = buf.get(data..data + len) else {
                break 'pages;
            };
            packet.extend_from_slice(segment);
            data += len;
            if len == 255 {
                continue;
            }
            packets += 1;
            if packets == 2 {
                break 'pages;
            }
            packet.clear();
        }
        at = data;
    }
    // Whole, or as much of the second packet as the bytes hold.
    if packets >= 1 {
        if let Some(body) = packet.strip_prefix(b"OpusTags") {
            vorbis_comment(body, &mut map);
        } else if let Some(body) = packet.strip_prefix(b"\x03vorbis") {
            vorbis_comment(body, &mut map);
        }
    }
    map
}

/// ID3v2: a 10-byte header, then frames of a 4-character id and a size. v2.2 uses 3-character ids
/// we do not read.
fn id3_frames(buf: &[u8]) -> TagMap {
    let mut map = TagMap::default();
    if buf.len() < 10 || !buf.starts_with(b"ID3") {
        return map;
    }
    let major = buf[3];
    if major != 3 && major != 4 {
        return map;
    }
    let size = |bytes: &[u8]| -> usize {
        if major == 4 {
            bytes
                .iter()
                .fold(0, |size, byte| size << 7 | usize::from(byte & 0x7f))
        } else {
            bytes
                .iter()
                .fold(0, |size, byte| size << 8 | usize::from(*byte))
        }
    };
    let end = buf.len().min(size(&buf[6..10]).saturating_add(10));
    let mut at = 10;
    for _ in 0..256 {
        if at + 10 > end {
            break;
        }
        let id = &buf[at..at + 4];
        if !id
            .iter()
            .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit())
        {
            break;
        }
        let Some(raw) = size(&buf[at + 4..at + 8])
            .checked_add(at + 10)
            .filter(|frame_end| *frame_end <= end)
            .map(|frame_end| &buf[at + 10..frame_end])
        else {
            break;
        };
        if id == b"TXXX" {
            user_text(raw, &mut map);
        } else if id[0] == b'T' {
            map.set(String::from_utf8_lossy(id).into_owned(), id3_text(raw));
        }
        at += 10 + raw.len();
    }
    map
}

/// A user-defined text frame: "description\0value" in the frame's own encoding. The description,
/// uppercased, is the tag's name.
fn user_text(raw: &[u8], map: &mut TagMap) {
    let Some(&enc) = raw.first() else {
        return;
    };
    let zero = if enc == 1 || enc == 2 { 2 } else { 1 };
    let Some(at) = (1..)
        .step_by(zero)
        .take_while(|at| at + zero <= raw.len())
        .find(|at| raw[*at..at + zero].iter().all(|byte| *byte == 0))
    else {
        return;
    };
    let name = text_of(enc, &raw[1..at]).to_uppercase();
    let value = text_of(enc, &raw[at + zero..]);
    if !name.is_empty() && !value.is_empty() {
        map.set(name, value);
    }
}

/// A text frame: a leading encoding byte, then the text.
fn id3_text(raw: &[u8]) -> String {
    raw.split_first()
        .map(|(enc, body)| text_of(*enc, body))
        .unwrap_or_default()
}

/// ID3 text in one of its encodings: 0 Latin-1, 1 UTF-16 with a byte order mark, 2 big-endian
/// UTF-16, 3 UTF-8. It ends at the first NUL and at 300 UTF-16 units.
fn text_of(enc: u8, body: &[u8]) -> String {
    let text: String = match enc {
        0 => body.iter().map(|byte| char::from(*byte)).collect(),
        1 | 2 => utf16(body, enc == 1),
        3 => String::from_utf8_lossy(body).into_owned(),
        _ => return String::new(),
    };
    truncate_utf16(text.split('\0').next().unwrap_or_default(), MAX_TEXT)
}

/// UTF-16 up to its first NUL. With `bom`, a byte order mark picks the byte order and little-endian
/// is the default; without, it is big-endian.
fn utf16(bytes: &[u8], bom: bool) -> String {
    let (little, body) = match bytes {
        [0xff, 0xfe, rest @ ..] if bom => (true, rest),
        [0xfe, 0xff, rest @ ..] if bom => (false, rest),
        _ => (bom, bytes),
    };
    let units: Vec<u16> = body
        .as_chunks::<2>()
        .0
        .iter()
        .map(|pair| {
            if little {
                u16::from_le_bytes(*pair)
            } else {
                u16::from_be_bytes(*pair)
            }
        })
        .take_while(|unit| *unit != 0)
        .collect();
    String::from_utf16_lossy(&units)
}
