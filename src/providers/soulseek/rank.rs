//! One file a Soulseek peer shares, as the API returns it, and the order search results come in
//! (`normalizeRecord`, `normalizeSoulseekCandidates` and `normalizeSoulseekBrowse` in
//! `src/providers/soulseek.js`).

use icu_collator::CollatorBorrowed;
use icu_collator::options::CollatorOptions;
use serde::Serialize;

use super::SOULSEEK_SEARCH_CAP;
use crate::protocol::{is_media_file, js_trim};

/// One file as the Soulseek client reports it: a search hit or an entry of a peer's share list.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct PeerFile {
    pub username: String,
    /// The peer has an upload slot free.
    pub slots: bool,
    /// How many uploads wait at the peer.
    pub queue: u32,
    /// The peer's path, with its own separators (`@@share\Artist\Album\01.flac`).
    pub filename: String,
    pub size: u64,
    /// kbit/s.
    pub bitrate: Option<u32>,
    /// Hz.
    pub sample_rate: Option<u32>,
    pub bit_depth: Option<u32>,
    /// Seconds.
    pub length: Option<u32>,
}

/// One media file a peer shares, in the shape `search` and `browse` return.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Candidate {
    pub username: String,
    pub slots: bool,
    pub queue: u32,
    pub filename: String,
    pub size: u64,
    pub bitrate: Option<u32>,
    pub sample_rate: Option<u32>,
    pub bit_depth: Option<u32>,
    pub length: Option<u32>,
}

/// A peer's share list (`normalizeSoulseekBrowse`'s result).
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ShareList {
    pub username: String,
    pub files: Vec<Candidate>,
    pub tracks: usize,
    pub size: u64,
}

/// `file` in the shape the API returns, or `None` when it is not media or names no peer or file.
/// An audio attribute a peer reports as 0 is unknown.
fn normalize(file: PeerFile) -> Option<Candidate> {
    let username = js_trim(&file.username);
    let filename = js_trim(&file.filename);
    // `MEDIA_FILE` reads the extension alone, so the peer's separators need no translation here.
    if username.is_empty() || filename.is_empty() || !is_media_file(filename) {
        return None;
    }
    let known = |value: Option<u32>| value.filter(|value| *value > 0);
    Some(Candidate {
        username: username.to_owned(),
        slots: file.slots,
        queue: file.queue,
        filename: filename.to_owned(),
        size: file.size,
        bitrate: known(file.bitrate),
        sample_rate: known(file.sample_rate),
        bit_depth: known(file.bit_depth),
        length: known(file.length),
    })
}

/// Search hits as the API returns them (`normalizeSoulseekCandidates`), at most `cap` and never
/// more than [`SOULSEEK_SEARCH_CAP`]:
///
/// 1. a peer with an open upload slot and an empty queue first;
/// 2. then the shorter queue, and a free slot before none;
/// 3. then the higher bit depth, sample rate and bitrate;
/// 4. then by name, in the order the app's `localeCompare` gives on a host with an English or no
///    locale: the CLDR root collation.
pub fn rank(hits: impl IntoIterator<Item = PeerFile>, cap: usize) -> Vec<Candidate> {
    let mut out: Vec<Candidate> = hits.into_iter().filter_map(normalize).collect();
    let ready = |file: &Candidate| file.slots && file.queue == 0;
    let names = CollatorBorrowed::try_new(Default::default(), CollatorOptions::default())
        .expect("the root collation is compiled in");
    out.sort_by(|a, b| {
        ready(b)
            .cmp(&ready(a))
            .then(a.queue.cmp(&b.queue))
            .then(b.slots.cmp(&a.slots))
            .then(b.bit_depth.cmp(&a.bit_depth))
            .then(b.sample_rate.cmp(&a.sample_rate))
            .then(b.bitrate.cmp(&a.bitrate))
            .then_with(|| names.compare(&a.filename, &b.filename))
    });
    out.truncate(cap.clamp(1, SOULSEEK_SEARCH_CAP));
    out
}

/// A peer's share list as the API returns it (`normalizeSoulseekBrowse`): media files only, in the
/// same shape as search hits, ordered by path (as UTF-16, the way the app compares strings) so a
/// client can group them into folders in one pass.
pub fn share_list(records: impl IntoIterator<Item = PeerFile>, username: &str) -> ShareList {
    let mut files: Vec<Candidate> = records.into_iter().filter_map(normalize).collect();
    files.sort_by(|a, b| a.filename.encode_utf16().cmp(b.filename.encode_utf16()));
    ShareList {
        username: username.to_owned(),
        tracks: files.len(),
        size: files.iter().map(|file| file.size).sum(),
        files,
    }
}
