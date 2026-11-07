use serde::{Deserialize, Serialize};
use std::{future::Future, io};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct VideoSize {
    pub width: u64,
    pub height: u64,
}

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

fn size(width: u64, height: u64) -> Option<VideoSize> {
    (width > 0 && height > 0).then_some(VideoSize { width, height })
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
fn uint(b: &[u8]) -> u64 {
    b.iter().fold(0u64, |v, &x| {
        v.saturating_mul(256).saturating_add(u64::from(x))
    })
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
    Some((
        id,
        usize::try_from(len).ok()?,
        p.checked_add(il)?.checked_add(sl)?,
    ))
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
fn tracks_size(tracks: &[u8]) -> Option<VideoSize> {
    for (_, entry) in children(tracks).filter(|(id, _)| *id == 0xae) {
        if field(entry, 0x83).map(uint) != Some(1) {
            continue;
        }
        let Some(video) = field(entry, 0xe0) else {
            continue;
        };
        if let (Some(w), Some(h)) = (field(video, 0xb0), field(video, 0xba)) {
            return size(uint(w), uint(h));
        }
    }
    None
}
fn boxes(mut b: &[u8]) -> impl Iterator<Item = (&[u8], &[u8])> {
    std::iter::from_fn(move || {
        let len = be(b, 0)? as usize;
        if len < 8 {
            return None;
        }
        let whole = b.get(..len)?;
        b = &b[len..];
        Some((&whole[4..8], &whole[8..]))
    })
}
fn moov_size(moov: &[u8]) -> Option<VideoSize> {
    for (_, track) in boxes(moov).filter(|(kind, _)| *kind == b"trak") {
        let mut tkhd = None;
        let mut video = false;
        for (kind, data) in boxes(track) {
            if kind == b"tkhd" {
                tkhd = Some(data);
            }
            if kind == b"mdia"
                && boxes(data).any(|(k, d)| k == b"hdlr" && d.get(8..12) == Some(b"vide"))
            {
                video = true;
            }
        }
        if !video {
            continue;
        }
        let Some(tkhd) = tkhd else {
            continue;
        };
        let at = if tkhd.first() == Some(&1) { 88 } else { 76 };
        let round = |p| be(tkhd, p).map_or(0, |v| (u64::from(v) + 32768) / 65536);
        return size(round(at), round(at + 4));
    }
    None
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
/// Reads byte ranges as `(offset, length)`, returning fewer bytes at EOF.
pub async fn video_size<F, Fut>(mut read: F) -> io::Result<Option<VideoSize>>
where
    F: FnMut(u64, usize) -> Fut,
    Fut: Future<Output = io::Result<Vec<u8>>>,
{
    let head = read(0, 256 * 1024).await?;
    if be(&head, 0) == Some(0x1a45dfa3) {
        let Some((_, elen, edata)) = header(&head, 0) else {
            return Ok(None);
        };
        let Some((0x18538067, _, segment)) = edata.checked_add(elen).and_then(|p| header(&head, p))
        else {
            return Ok(None);
        };
        let mut p = segment;
        let mut tracks_at = 0;
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
            if len == usize::MAX {
                return Ok(None);
            }
            let Some(start) = p.checked_add(offset) else {
                return Ok(None);
            };
            if id == 0x1654ae6b {
                return Ok(tracks_size(
                    &bytes(&mut read, &head, start, len.min(4 * 1024 * 1024)).await?,
                ));
            }
            if id == 0x114d9b74 {
                let data = bytes(&mut read, &head, start, len.min(1024 * 1024)).await?;
                for (_, seek) in children(&data).filter(|(id, _)| *id == 0x4dbb) {
                    if field(seek, 0x53ab).map(uint) == Some(0x1654ae6b)
                        && let Some(pos) = field(seek, 0x53ac)
                            .and_then(|v| usize::try_from(uint(v)).ok())
                            .and_then(|v| segment.checked_add(v))
                    {
                        tracks_at = pos;
                    }
                }
            }
            if id == 0x1f43b675 {
                if tracks_at <= p {
                    return Ok(None);
                }
                p = tracks_at;
            } else if let Some(next) = start.checked_add(len) {
                p = next;
            } else {
                return Ok(None);
            }
        }
        return Ok(None);
    }
    if head.get(0..4) == Some(b"RIFF")
        && head.get(8..12) == Some(b"AVI ")
        && head.get(24..28) == Some(b"avih")
    {
        return Ok(size(
            u64::from(le(&head, 64).unwrap_or(0)),
            u64::from(le(&head, 68).unwrap_or(0)),
        ));
    }
    if ![b"ftyp", b"moov", b"mdat", b"free", b"wide", b"skip"]
        .iter()
        .any(|v| head.get(4..8) == Some(*v))
    {
        return Ok(None);
    }
    let mut p = 0usize;
    for _ in 0..64 {
        let h = bytes(&mut read, &head, p, 16).await?;
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
        if h.get(4..8) == Some(b"moov") {
            let Some(start) = p.checked_add(hdr) else {
                return Ok(None);
            };
            return Ok(moov_size(
                &bytes(
                    &mut read,
                    &head,
                    start,
                    len.min(64 * 1024 * 1024) as usize - hdr,
                )
                .await?,
            ));
        }
        let Some(next) = usize::try_from(len).ok().and_then(|len| p.checked_add(len)) else {
            return Ok(None);
        };
        p = next;
    }
    Ok(None)
}
