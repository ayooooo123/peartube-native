//! Port of `src/resolution.js`: container format, duration and tracks (`media_info`) plus the
//! release-name resolution tier (`resolution_label`).

use serde::{Deserialize, Serialize};
use std::{future::Future, io};

use crate::identify::JsNumber;
use crate::protocol::media_format;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct VideoSize {
    pub width: u64,
    pub height: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct VideoTrack {
    pub codec: Option<String>,
    pub width: Option<u64>,
    pub height: Option<u64>,
    pub hdr: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AudioTrack {
    pub codec: Option<String>,
    pub channels: Option<u64>,
    pub language: Option<String>,
    pub name: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SubtitleTrack {
    pub codec: Option<String>,
    pub language: Option<String>,
    pub name: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct MediaInfo {
    pub container: String,
    pub duration: Option<JsNumber>,
    pub video: Vec<VideoTrack>,
    pub audio: Vec<AudioTrack>,
    pub subtitles: Vec<SubtitleTrack>,
}

/// The usual release-name tier, judged by width first so letterboxed pictures (1920x800) keep
/// their tier. Standard definition (480 and 576 lines) is 480p.
pub fn resolution_label(size: VideoSize) -> String {
    let VideoSize { width, height } = size;
    if width >= 3200 || height >= 2000 {
        "2160p".into()
    } else if width >= 1800 || height >= 1000 {
        "1080p".into()
    } else if width >= 1200 || height >= 700 {
        "720p".into()
    } else if width >= 640 || height >= 480 {
        "480p".into()
    } else {
        format!("{height}p")
    }
}

fn u16_be(b: &[u8], p: usize) -> Option<u16> {
    Some(u16::from_be_bytes(
        b.get(p..p.checked_add(2)?)?.try_into().ok()?,
    ))
}

fn u16_le(b: &[u8], p: usize) -> Option<u16> {
    Some(u16::from_le_bytes(
        b.get(p..p.checked_add(2)?)?.try_into().ok()?,
    ))
}

fn be(b: &[u8], p: usize) -> Option<u32> {
    Some(u32::from_be_bytes(
        b.get(p..p.checked_add(4)?)?.try_into().ok()?,
    ))
}

fn le(b: &[u8], p: usize) -> Option<u32> {
    Some(u32::from_le_bytes(
        b.get(p..p.checked_add(4)?)?.try_into().ok()?,
    ))
}

/// `String.fromCharCode(...b.subarray(s, e))`: each byte as its Latin-1 code point.
fn latin1_text(b: &[u8], s: usize, e: usize) -> String {
    if e > b.len() || s > e {
        return String::new();
    }
    b[s..e].iter().map(|&byte| char::from(byte)).collect()
}

/// `b4a.toString(b, 'utf8').replace(/\0+$/, '').slice(0, max)`.
fn utf8_clean(b: Option<&[u8]>, max: usize) -> String {
    let Some(b) = b else {
        return String::new();
    };
    let s = String::from_utf8_lossy(b);
    let trimmed = s.trim_end_matches('\0');
    let mut out = String::new();
    let mut units = 0;
    for ch in trimmed.chars() {
        let len = ch.len_utf16();
        if units + len > max {
            break;
        }
        units += len;
        out.push(ch);
    }
    out
}

fn dim(n: i64) -> Option<u64> {
    (n > 0).then_some(n as u64)
}

fn known(code: String) -> Option<String> {
    if code.is_empty() || code == "und" {
        None
    } else {
        Some(code)
    }
}

fn hdr_label(transfer: u64, dolby_vision: bool) -> Option<String> {
    let mut parts = Vec::new();
    if dolby_vision {
        parts.push("Dolby Vision");
    }
    if transfer == 16 {
        parts.push("HDR10");
    }
    if transfer == 18 {
        parts.push("HLG");
    }
    if parts.is_empty() {
        None
    } else {
        Some(parts.join(" · "))
    }
}

fn mkv_table(id: &str) -> Option<&'static str> {
    Some(match id {
        "V_MPEG4/ISO/AVC" => "H.264",
        "V_MPEGH/ISO/HEVC" => "HEVC",
        "V_AV1" => "AV1",
        "V_VP9" => "VP9",
        "V_VP8" => "VP8",
        "V_MPEG4" => "MPEG-4 Part 2",
        "V_MPEG2" => "MPEG-2",
        "V_MPEG1" => "MPEG-1",
        "V_THEORA" => "Theora",
        "V_MJPEG" => "Motion JPEG",
        "V_PRORES" => "ProRes",
        "A_AAC" => "AAC",
        "A_AC3" => "AC-3",
        "A_EAC3" => "E-AC-3",
        "A_TRUEHD" => "TrueHD",
        "A_MLP" => "MLP",
        "A_DTS" => "DTS",
        "A_DTS/LOSSLESS" => "DTS-HD MA",
        "A_FLAC" => "FLAC",
        "A_OPUS" => "Opus",
        "A_VORBIS" => "Vorbis",
        "A_MPEG/L3" => "MP3",
        "A_MPEG/L2" => "MP2",
        "A_ALAC" => "ALAC",
        "A_PCM" => "PCM",
        "S_TEXT/UTF8" => "SRT",
        "S_TEXT/ASS" => "ASS",
        "S_TEXT/SSA" => "SSA",
        "S_TEXT/WEBVTT" => "WebVTT",
        "S_TEXT" => "Text",
        "S_HDMV/PGS" => "PGS",
        "S_HDMV/TEXTST" => "HDMV Text",
        "S_VOBSUB" => "VobSub",
        "S_DVBSUB" => "DVB",
        _ => return None,
    })
}

fn mkv_codec(id: &str) -> Option<String> {
    if let Some(mapped) = mkv_table(id) {
        return Some(mapped.to_owned());
    }
    let head = id.split('/').next().unwrap_or("");
    if let Some(mapped) = mkv_table(head) {
        return Some(mapped.to_owned());
    }
    if id.is_empty() {
        None
    } else {
        Some(id.to_owned())
    }
}

fn mp4_codec(kind: &str) -> Option<String> {
    let mapped = match kind {
        "avc1" | "avc3" => Some("H.264"),
        "hvc1" | "hev1" | "dvh1" | "dvhe" => Some("HEVC"),
        "av01" | "dav1" => Some("AV1"),
        "vp09" => Some("VP9"),
        "vp08" => Some("VP8"),
        "mp4v" => Some("MPEG-4 Part 2"),
        "jpeg" => Some("Motion JPEG"),
        "apch" | "apcn" | "apcs" | "apco" | "ap4h" | "ap4x" => Some("ProRes"),
        "mp4a" => Some("AAC"),
        "ac-3" => Some("AC-3"),
        "ec-3" => Some("E-AC-3"),
        "mlpa" => Some("TrueHD"),
        "dtsc" | "dtsh" | "dtse" => Some("DTS"),
        "dtsl" => Some("DTS-HD MA"),
        "Opus" => Some("Opus"),
        "fLaC" => Some("FLAC"),
        "alac" => Some("ALAC"),
        ".mp3" => Some("MP3"),
        "lpcm" | "sowt" | "twos" | "ipcm" => Some("PCM"),
        "tx3g" => Some("Timed Text"),
        "wvtt" => Some("WebVTT"),
        "stpp" => Some("TTML"),
        "c608" => Some("CEA-608"),
        _ => None,
    };
    if let Some(mapped) = mapped {
        return Some(mapped.to_owned());
    }
    let trimmed = crate::protocol::js_trim(kind);
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed.to_owned())
    }
}

fn is_mp4_dolby_vision(kind: &str) -> bool {
    matches!(kind, "dvh1" | "dvhe" | "dav1")
}

fn fourcc_codec(cc: &str) -> Option<String> {
    let upper = cc.to_uppercase();
    let mapped = match upper.as_str() {
        "XVID" | "DIVX" | "DX50" | "FMP4" | "MP4V" => Some("MPEG-4 Part 2"),
        "DIV3" => Some("MS MPEG-4 v3"),
        "H264" | "X264" | "AVC1" => Some("H.264"),
        "HEVC" | "H265" | "HVC1" => Some("HEVC"),
        "WVC1" => Some("VC-1"),
        "WMV3" => Some("WMV 9"),
        "MJPG" => Some("Motion JPEG"),
        "MPG2" => Some("MPEG-2"),
        _ => None,
    };
    if let Some(mapped) = mapped {
        return Some(mapped.to_owned());
    }
    let trimmed = crate::protocol::js_trim(cc);
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed.to_owned())
    }
}

fn wave_codec(tag: Option<u16>) -> Option<String> {
    let tag = tag?;
    let mapped = match tag {
        1 | 3 => "PCM",
        0x50 => "MP2",
        0x55 => "MP3",
        0xff | 0x1610 => "AAC",
        0x161 => "WMA",
        0x162 => "WMA Pro",
        0x2000 => "AC-3",
        0x2001 => "DTS",
        0xf1ac => "FLAC",
        _ => return Some(format!("0x{tag:x}")),
    };
    Some(mapped.to_owned())
}

fn uint(b: Option<&[u8]>) -> u64 {
    let Some(b) = b else {
        return 0;
    };
    b.iter().fold(0u64, |v, &x| {
        v.saturating_mul(256).saturating_add(u64::from(x))
    })
}

fn ebml_float(b: Option<&[u8]>) -> f64 {
    let Some(b) = b else {
        return 0.0;
    };
    match b.len() {
        4 => f64::from(f32::from_be_bytes(b.try_into().unwrap())),
        8 => f64::from_be_bytes(b.try_into().unwrap()),
        _ => 0.0,
    }
}

fn vint(b: &[u8], p: usize, marker: bool) -> Option<(u64, usize)> {
    let first = *b.get(p)?;
    let len = first.leading_zeros() as usize + 1;
    if len > 8 {
        return None;
    }
    let bytes = b.get(p..p.checked_add(len)?)?;
    let mut value = if marker {
        u64::from(first)
    } else {
        u64::from(first) & (0xffu64 >> len)
    };
    for &byte in &bytes[1..] {
        value = value * 256 + u64::from(byte);
    }
    if !marker && value == (1u64 << (7 * len)) - 1 {
        return Some((u64::MAX, len));
    }
    Some((value, len))
}

fn header(b: &[u8], p: usize) -> Option<(u64, usize, usize)> {
    let (id, il) = vint(b, p, true)?;
    let (len, sl) = vint(b, p.checked_add(il)?, false)?;
    if len == u64::MAX {
        return None;
    }
    Some((
        id,
        usize::try_from(len).ok()?,
        p.checked_add(il)?.checked_add(sl)?,
    ))
}

/// EBML header allowing unknown-length (`u64::MAX`) segments.
fn segment_header(b: &[u8], p: usize) -> Option<(u64, Option<usize>, usize)> {
    let (id, il) = vint(b, p, true)?;
    let (len, sl) = vint(b, p.checked_add(il)?, false)?;
    let size = if len == u64::MAX {
        None
    } else {
        Some(usize::try_from(len).ok()?)
    };
    Some((id, size, p.checked_add(il)?.checked_add(sl)?))
}

fn children(mut b: &[u8]) -> impl Iterator<Item = (u64, &[u8])> {
    std::iter::from_fn(move || {
        let (id, len, start) = header(b, 0)?;
        let end = start.checked_add(len)?;
        let data = b.get(start..end)?;
        b = &b[end..];
        Some((id, data))
    })
}

fn field(b: &[u8], id: u64) -> Option<&[u8]> {
    children(b).filter(|(i, _)| *i == id).map(|(_, v)| v).last()
}

struct ParsedContainer {
    duration: Option<JsNumber>,
    video: Vec<VideoTrack>,
    audio: Vec<AudioTrack>,
    subtitles: Vec<SubtitleTrack>,
}

async fn bytes<F, Fut>(read: &mut F, head: &[u8], start: usize, len: usize) -> io::Result<Vec<u8>>
where
    F: FnMut(u64, usize) -> Fut,
    Fut: Future<Output = io::Result<Vec<u8>>>,
{
    if let Some(end) = start.checked_add(len)
        && end <= head.len()
    {
        return Ok(head[start..end].to_vec());
    }
    read(start as u64, len).await
}

async fn matroska<F, Fut>(read: &mut F, head: &[u8]) -> io::Result<Option<ParsedContainer>>
where
    F: FnMut(u64, usize) -> Fut,
    Fut: Future<Output = io::Result<Vec<u8>>>,
{
    let Some((_, elen, edata)) = header(head, 0) else {
        return Ok(None);
    };
    let Some((0x18538067, _, segment)) = edata
        .checked_add(elen)
        .and_then(|p| segment_header(head, p))
    else {
        return Ok(None);
    };
    let mut tracks_at: Option<usize> = None;
    let mut duration: Option<JsNumber> = None;
    let mut p = segment;
    for _ in 0..64 {
        let near = p.checked_add(16).is_some_and(|end| end <= head.len());
        let buf = if near {
            &head[p..]
        } else {
            &read(p as u64, 16).await?
        };
        let Some((id, len, offset)) = header(buf, 0) else {
            return Ok(None);
        };
        let Some(start) = p.checked_add(offset) else {
            return Ok(None);
        };
        if id == 0x1549a966 {
            let info = bytes(read, head, start, len.min(64 * 1024)).await?;
            let scale = {
                let s = uint(field(&info, 0x2ad7b1));
                if s > 0 { s as f64 } else { 1e6 }
            };
            let d = ebml_float(field(&info, 0x4489)) * scale / 1e9;
            duration = (d != 0.0 && d.is_finite()).then_some(JsNumber(d));
        }
        if id == 0x1654ae6b {
            let tracks = bytes(read, head, start, len.min(4 * 1024 * 1024)).await?;
            let mut out = matroska_tracks(&tracks);
            out.duration = duration;
            return Ok(Some(out));
        }
        if id == 0x114d9b74 {
            let data = bytes(read, head, start, len.min(1024 * 1024)).await?;
            for (_, seek) in children(&data).filter(|(id, _)| *id == 0x4dbb) {
                if field(seek, 0x53ab).is_some()
                    && uint(field(seek, 0x53ab)) == 0x1654ae6b
                    && let Some(pos_raw) = field(seek, 0x53ac)
                    && let Ok(pos) = usize::try_from(uint(Some(pos_raw)))
                    && let Some(target) = segment.checked_add(pos)
                {
                    tracks_at = Some(target);
                }
            }
        }
        if id != 0x1f43b675 {
            let Some(next) = start.checked_add(len) else {
                return Ok(None);
            };
            p = next;
        } else if let Some(target) = tracks_at.filter(|&target| target > p) {
            p = target;
        } else {
            return Ok(None);
        }
    }
    Ok(None)
}

fn matroska_tracks(tracks: &[u8]) -> ParsedContainer {
    let mut video = Vec::new();
    let mut audio = Vec::new();
    let mut subtitles = Vec::new();
    for (_, entry) in children(tracks).filter(|(id, _)| *id == 0xae) {
        let id = utf8_clean(field(entry, 0x86), 128);
        let lang_bcp = utf8_clean(field(entry, 0x22b59d), 128);
        let language = known(if !lang_bcp.is_empty() {
            lang_bcp
        } else {
            utf8_clean(field(entry, 0x22b59c), 128)
        });
        let name_str = utf8_clean(field(entry, 0x536e), 128);
        let name = (!name_str.is_empty()).then_some(name_str);
        let priv_data = field(entry, 0x63a2);
        match uint(field(entry, 0x83)) {
            1 => {
                let v = field(entry, 0xe0).unwrap_or(&[]);
                let colour = field(v, 0x55b0).unwrap_or(&[]);
                let dolby_vision = children(entry).any(|(cid, cdata)| {
                    cid == 0x41e4
                        && matches!(
                            uint(field(cdata, 0x41e7)),
                            0x64766343 | 0x64767643 | 0x64767743
                        )
                });
                if video.len() < 32 {
                    let codec = if id == "V_MS/VFW/FOURCC"
                        && let Some(priv_data) = priv_data
                    {
                        fourcc_codec(&latin1_text(priv_data, 16, 20))
                    } else {
                        mkv_codec(&id)
                    };
                    let width = {
                        let w = uint(field(v, 0xb0));
                        (w > 0).then_some(w)
                    };
                    let height = {
                        let h = uint(field(v, 0xba));
                        (h > 0).then_some(h)
                    };
                    video.push(VideoTrack {
                        codec,
                        width,
                        height,
                        hdr: hdr_label(uint(field(colour, 0x55ba)), dolby_vision),
                    });
                }
            }
            2 if audio.len() < 32 => {
                let a = field(entry, 0xe1).unwrap_or(&[]);
                let codec = if id == "A_MS/ACM"
                    && let Some(priv_data) = priv_data
                {
                    wave_codec(u16_le(priv_data, 0))
                } else {
                    mkv_codec(&id)
                };
                let ch = uint(field(a, 0x9f));
                audio.push(AudioTrack {
                    codec,
                    channels: Some(if ch > 0 { ch } else { 1 }),
                    language,
                    name,
                });
            }
            0x11 if subtitles.len() < 64 => {
                subtitles.push(SubtitleTrack {
                    codec: mkv_codec(&id),
                    language,
                    name,
                });
            }
            _ => {}
        }
    }
    ParsedContainer {
        duration: None,
        video,
        audio,
        subtitles,
    }
}

fn boxes(mut b: &[u8]) -> impl Iterator<Item = (String, &[u8])> {
    std::iter::from_fn(move || {
        let len = be(b, 0)? as usize;
        if len < 8 {
            return None;
        }
        let whole = b.get(..len)?;
        let kind = latin1_text(whole, 4, 8);
        b = &b[len..];
        Some((kind, &whole[8..]))
    })
}

fn child_box<'a>(b: &'a [u8], kind: &str) -> Option<&'a [u8]> {
    boxes(b).find(|(k, _)| k == kind).map(|(_, d)| d)
}

fn iso_language(v: Option<u16>) -> Option<String> {
    let v = v?;
    if v < 0x400 {
        return None;
    }
    let letters = [(v >> 10) & 31, (v >> 5) & 31, v & 31];
    if letters.iter().all(|&c| (1..=26).contains(&c)) {
        let s: String = letters
            .iter()
            .map(|&c| char::from((c as u8) + 0x60))
            .collect();
        known(s)
    } else {
        None
    }
}

async fn mp4<F, Fut>(read: &mut F, head: &[u8]) -> io::Result<Option<ParsedContainer>>
where
    F: FnMut(u64, usize) -> Fut,
    Fut: Future<Output = io::Result<Vec<u8>>>,
{
    let mut p = 0usize;
    for _ in 0..64 {
        let h = bytes(read, head, p, 16).await?;
        let Some(mut len) = be(&h, 0).map(u64::from) else {
            return Ok(None);
        };
        let mut hdr = 8usize;
        if len == 1 {
            let (Some(hi), Some(lo)) = (be(&h, 8), be(&h, 12)) else {
                return Ok(None);
            };
            len = (u64::from(hi) << 32) | u64::from(lo);
            hdr = 16;
        }
        if len < hdr as u64 {
            return Ok(None);
        }
        if latin1_text(&h, 4, 8) == "moov" {
            let Some(start) = p.checked_add(hdr) else {
                return Ok(None);
            };
            let take = len.min(64 * 1024 * 1024) as usize - hdr;
            let moov = bytes(read, head, start, take).await?;
            return Ok(Some(moov_info(&moov)));
        }
        let Some(next) = usize::try_from(len).ok().and_then(|len| p.checked_add(len)) else {
            return Ok(None);
        };
        p = next;
    }
    Ok(None)
}

fn moov_info(moov: &[u8]) -> ParsedContainer {
    let mut out = ParsedContainer {
        duration: None,
        video: Vec::new(),
        audio: Vec::new(),
        subtitles: Vec::new(),
    };
    for (kind, data) in boxes(moov) {
        if kind == "mvhd" {
            let v1 = data.first() == Some(&1);
            let scale = be(data, if v1 { 20 } else { 12 })
                .map(i64::from)
                .unwrap_or(-1);
            let length: f64 = if v1 {
                let hi = be(data, 24).map(|n| n as f64).unwrap_or(-1.0);
                let lo = be(data, 28).map(|n| n as f64).unwrap_or(-1.0);
                hi * 4_294_967_296.0 + lo
            } else {
                be(data, 16).map(|n| n as f64).unwrap_or(-1.0)
            };
            if scale > 0 && length > 0.0 && length != 4_294_967_295.0 {
                let d = length / (scale as f64);
                if d.is_finite() {
                    out.duration = Some(JsNumber(d));
                }
            }
        }
        if kind == "trak" {
            mp4_track(data, &mut out);
        }
    }
    out
}

fn js_round(x: f64) -> i64 {
    (x + 0.5).floor() as i64
}

fn mp4_track(trak: &[u8], out: &mut ParsedContainer) {
    let tkhd = child_box(trak, "tkhd");
    let Some(mdia) = child_box(trak, "mdia") else {
        return;
    };
    let hdlr = child_box(mdia, "hdlr");
    let mdhd = child_box(mdia, "mdhd");
    let stbl = child_box(child_box(mdia, "minf").unwrap_or(&[]), "stbl");
    let stsd = stbl.and_then(|s| child_box(s, "stsd"));
    let entry = stsd.and_then(|s| s.get(8..)).and_then(|s| boxes(s).next());
    let Some((entry_type, entry_data)) = entry else {
        return;
    };
    let handler = hdlr.map_or_else(String::new, |h| latin1_text(h, 8, 12));
    let codec = mp4_codec(&entry_type);
    let language = mdhd.and_then(|m| {
        let offset = if m.first() == Some(&1) { 32 } else { 20 };
        iso_language(u16_be(m, offset))
    });
    if handler == "vide" {
        let mut transfer = 0u64;
        let mut dolby_vision = is_mp4_dolby_vision(&entry_type);
        if let Some(sub) = entry_data.get(78..) {
            for (btype, bdata) in boxes(sub) {
                if btype == "colr"
                    && latin1_text(bdata, 0, 4) == "nclx"
                    && let Some(tr) = u16_be(bdata, 6)
                {
                    transfer = u64::from(tr);
                }
                if matches!(btype.as_str(), "dvcC" | "dvvC" | "dvwC") {
                    dolby_vision = true;
                }
            }
        }
        let (width, height) = if let Some(tkhd) = tkhd {
            let at = if tkhd.first() == Some(&1) { 88 } else { 76 };
            let w = be(tkhd, at)
                .map(|v| js_round(f64::from(v) / 65536.0))
                .unwrap_or(0);
            let h = be(tkhd, at + 4)
                .map(|v| js_round(f64::from(v) / 65536.0))
                .unwrap_or(0);
            (w, h)
        } else {
            (0, 0)
        };
        if out.video.len() < 32 {
            out.video.push(VideoTrack {
                codec,
                width: dim(width)
                    .or_else(|| u16_be(entry_data, 24).and_then(|v| dim(i64::from(v)))),
                height: dim(height)
                    .or_else(|| u16_be(entry_data, 26).and_then(|v| dim(i64::from(v)))),
                hdr: hdr_label(transfer, dolby_vision),
            });
        }
    } else if handler == "soun" {
        if out.audio.len() < 32 {
            out.audio.push(AudioTrack {
                codec,
                channels: u16_be(entry_data, 16).and_then(|v| dim(i64::from(v))),
                language,
                name: None,
            });
        }
    } else if matches!(handler.as_str(), "sbtl" | "subt" | "text" | "clcp")
        && out.subtitles.len() < 64
    {
        out.subtitles.push(SubtitleTrack {
            codec,
            language,
            name: None,
        });
    }
}

fn avi(head: &[u8]) -> ParsedContainer {
    let mut out = ParsedContainer {
        duration: None,
        video: Vec::new(),
        audio: Vec::new(),
        subtitles: Vec::new(),
    };
    let us_per_frame = le(head, 32).map(i64::from).unwrap_or(-1);
    let frames = le(head, 48).map(i64::from).unwrap_or(-1);
    if us_per_frame > 0 && frames > 0 {
        let d = (us_per_frame as f64) * (frames as f64) / 1e6;
        if d.is_finite() {
            out.duration = Some(JsNumber(d));
        }
    }
    let width = le(head, 64).and_then(|v| dim(i64::from(v)));
    let height = le(head, 68).and_then(|v| dim(i64::from(v)));
    let hdrl_end = match le(head, 16) {
        Some(len) => head.len().min(20usize.saturating_add(len as usize)),
        None => return out,
    };
    let Some(avih_len) = le(head, 28).map(|v| v as usize) else {
        return out;
    };
    let mut p = 32usize
        .saturating_add(avih_len)
        .saturating_add(avih_len & 1);
    while p.saturating_add(12) <= hdrl_end {
        let Some(len) = le(head, p + 4).map(|v| v as usize) else {
            break;
        };
        if latin1_text(head, p, p + 4) == "LIST" && latin1_text(head, p + 8, p + 12) == "strl" {
            let mut strh: Option<&[u8]> = None;
            let mut strf: Option<&[u8]> = None;
            let end = hdrl_end.min(p.saturating_add(8).saturating_add(len));
            let mut q = p + 12;
            while q.saturating_add(8) <= end {
                let Some(size) = le(head, q + 4).map(|v| v as usize) else {
                    break;
                };
                let data_end = head.len().min(q.saturating_add(8).saturating_add(size));
                let slice = head.get(q + 8..data_end).unwrap_or(&[]);
                let tag = latin1_text(head, q, q + 4);
                if tag == "strh" {
                    strh = Some(slice);
                }
                if tag == "strf" {
                    strf = Some(slice);
                }
                q = q
                    .saturating_add(8)
                    .saturating_add(size)
                    .saturating_add(size & 1);
            }
            let stream_type = strh.map_or_else(String::new, |s| latin1_text(s, 0, 4));
            if stream_type == "vids" && out.video.len() < 32 {
                let cc = match strf {
                    Some(sf) => latin1_text(sf, 16, 20),
                    None => strh.map_or_else(String::new, |sh| latin1_text(sh, 4, 8)),
                };
                out.video.push(VideoTrack {
                    codec: fourcc_codec(&cc),
                    width,
                    height,
                    hdr: None,
                });
            }
            if stream_type == "auds"
                && let Some(sf) = strf
                && out.audio.len() < 32
            {
                out.audio.push(AudioTrack {
                    codec: wave_codec(u16_le(sf, 0)),
                    channels: u16_le(sf, 2).and_then(|v| dim(i64::from(v))),
                    language: None,
                    name: None,
                });
            }
        }
        p = p
            .saturating_add(8)
            .saturating_add(len)
            .saturating_add(len & 1);
    }
    out
}

/// Reads byte ranges as `(offset, length)`, returning fewer bytes at EOF, and parses the
/// container's format, duration and tracks (`src/resolution.js` `mediaInfo`).
pub async fn media_info<F, Fut>(mut read: F) -> io::Result<Option<MediaInfo>>
where
    F: FnMut(u64, usize) -> Fut,
    Fut: Future<Output = io::Result<Vec<u8>>>,
{
    let head = read(0, 256 * 1024).await?;
    let Some(container) = media_format(&head) else {
        return Ok(None);
    };
    let found = if container == "mkv" || container == "webm" {
        matroska(&mut read, &head).await?
    } else if container == "mp4" || container == "mov" {
        mp4(&mut read, &head).await?
    } else if container == "avi" && latin1_text(&head, 24, 28) == "avih" {
        Some(avi(&head))
    } else {
        None
    };
    Ok(Some(match found {
        Some(found) => MediaInfo {
            container: container.to_owned(),
            duration: found.duration,
            video: found.video,
            audio: found.audio,
            subtitles: found.subtitles,
        },
        None => MediaInfo {
            container: container.to_owned(),
            duration: None,
            video: Vec::new(),
            audio: Vec::new(),
            subtitles: Vec::new(),
        },
    }))
}

/// Convenience helper returning the first sized video track's `(width, height)`.
pub async fn video_size<F, Fut>(read: F) -> io::Result<Option<VideoSize>>
where
    F: FnMut(u64, usize) -> Fut,
    Fut: Future<Output = io::Result<Vec<u8>>>,
{
    let Some(info) = media_info(read).await? else {
        return Ok(None);
    };
    Ok(info
        .video
        .into_iter()
        .find_map(|v| match (v.width, v.height) {
            (Some(width), Some(height)) if width > 0 && height > 0 => {
                Some(VideoSize { width, height })
            }
            _ => None,
        }))
}
