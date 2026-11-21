// Generates tags-reference.json from the app's own src/tags.js: what `parseTags` reads from FLAC, Ogg and ID3
// headers built here byte by byte, every prefix of a few of them, and `albumKey`.
import { writeFileSync } from 'node:fs'
import { load, outFile } from './app.mjs'

const { parseTags, albumKey } = await load('src/tags.js')

const u32le = n => { const b = Buffer.alloc(4); b.writeUInt32LE(n >>> 0); return b }
const u32be = n => { const b = Buffer.alloc(4); b.writeUInt32BE(n >>> 0); return b }
const synchsafe = n => Buffer.from([(n >> 21) & 0x7f, (n >> 14) & 0x7f, (n >> 7) & 0x7f, n & 0x7f])
const latin1 = s => Buffer.from(s, 'latin1')
const utf8 = s => Buffer.from(s, 'utf8')

// The Vorbis comment structure: a vendor string, a count, then `KEY=value` entries. `entries` are strings or raw
// buffers; `count` overrides the stated number of entries.
function vorbis (entries, { vendor = 'peartube-fixture', count = null } = {}) {
  const items = entries.map(e => (Buffer.isBuffer(e) ? e : utf8(e)))
  const parts = [u32le(vendor.length), utf8(vendor), u32le(count === null ? items.length : count)]
  for (const item of items) parts.push(u32le(item.length), item)
  return Buffer.concat(parts)
}
const comments = tags => Object.entries(tags).map(([k, v]) => `${k}=${v}`)

// FLAC: "fLaC", then metadata blocks `[type, body]`; the last one carries the last-block flag unless `last` names
// another index.
function flac (blocks, { last = blocks.length - 1, audio = 16 } = {}) {
  const parts = [latin1('fLaC')]
  blocks.forEach(([type, body], i) => {
    parts.push(Buffer.from([(i === last ? 0x80 : 0) | type, (body.length >> 16) & 0xff, (body.length >> 8) & 0xff, body.length & 0xff]), body)
  })
  parts.push(Buffer.alloc(audio, 0x11))
  return Buffer.concat(parts)
}
const flacTags = (tags, options) => flac([[4, vorbis(comments(tags))]], options)

// Ogg: one page per call, its packets laced into 255-byte segments. `open` leaves the last packet unfinished (its
// final segment is 255 bytes long), so it goes on in the next page.
function oggPage (seq, packets, { open = false } = {}) {
  const lacing = []
  packets.forEach((packet, i) => {
    let left = packet.length
    while (left >= 255) { lacing.push(255); left -= 255 }
    if (!(open && i === packets.length - 1)) lacing.push(left)
  })
  const h = Buffer.alloc(27)
  h.write('OggS', 0, 'latin1')
  h[5] = seq === 0 ? 2 : 0
  h.writeUInt32LE(1, 14)
  h.writeUInt32LE(seq, 18)
  h[26] = lacing.length
  return Buffer.concat([h, Buffer.from(lacing), ...packets])
}
const opusHead = Buffer.concat([latin1('OpusHead'), Buffer.from([1, 2, 0x38, 0x01, 0x80, 0xbb, 0, 0, 0, 0, 0])])
const opusTags = entries => Buffer.concat([latin1('OpusTags'), vorbis(entries)])
const vorbisId = Buffer.concat([Buffer.from([1]), latin1('vorbis'), Buffer.alloc(23, 0x02)])
const vorbisTags = entries => Buffer.concat([Buffer.from([3]), latin1('vorbis'), vorbis(entries), Buffer.from([1])])

// ID3v2: a 10-byte header and frames. A frame is `[id, body]`; sizes are synchsafe in v2.4 and plain in v2.3.
function id3 (frames, { version = 4, size = null, tail = Buffer.from([0xff, 0xfb, 0x90, 0x00]) } = {}) {
  const parts = frames.map(([id, body]) => Buffer.concat([latin1(id), version === 4 ? synchsafe(body.length) : u32be(body.length), Buffer.alloc(2), body]))
  const body = Buffer.concat(parts)
  const stated = size === null ? body.length : size
  const header = Buffer.concat([latin1('ID3'), Buffer.from([version, 0, 0]), version === 4 ? synchsafe(stated) : u32be(stated)])
  return Buffer.concat([header, body, tail])
}
const text = (value, enc = 3) => {
  if (enc === 0) return Buffer.concat([Buffer.from([0]), latin1(value)])
  if (enc === 1) return Buffer.concat([Buffer.from([1, 0xff, 0xfe]), Buffer.from(value, 'utf16le')])
  if (enc === 2) return Buffer.concat([Buffer.from([2]), Buffer.from(value, 'utf16le').swap16()])
  return Buffer.concat([Buffer.from([enc]), utf8(value)])
}
const txxx = (desc, value, enc = 3) => {
  if (enc === 1) return Buffer.concat([Buffer.from([1, 0xff, 0xfe]), Buffer.from(desc, 'utf16le'), Buffer.from([0, 0, 0xff, 0xfe]), Buffer.from(value, 'utf16le')])
  if (enc === 2) return Buffer.concat([Buffer.from([2]), Buffer.from(desc, 'utf16le').swap16(), Buffer.from([0, 0]), Buffer.from(value, 'utf16le').swap16()])
  return Buffer.concat([Buffer.from([enc]), enc === 0 ? latin1(desc) : utf8(desc), Buffer.from([0]), enc === 0 ? latin1(value) : utf8(value)])
}

const MB = '0123456789abcdef0123456789abcdef'
const long = 'a'.repeat(298) + '😀😀'
const cases = {
  // FLAC
  flac_full: flacTags({ ALBUM: 'Blue Train', ALBUMARTIST: 'John Coltrane', ARTIST: 'John Coltrane Sextet', TITLE: "Moment's Notice", DATE: '1958-01-01', DISCNUMBER: '1/2', TRACKNUMBER: '03', MUSICBRAINZ_ALBUMID: MB }),
  flac_lowercase_keys: flacTags({ album: 'Kid A', 'album artist': 'Radiohead', title: 'Idioteque', year: 'Released 2000 (remaster)', discno: ' 2', track: '8 of 10', discogs_release_id: 'r1234567890123456' }),
  flac_alternate_keys: flacTags({ ALBUM: 'Homogenic', DISK: '1', TRACK: '4', DISCOGSRELEASEID: 'id 42', MUSICBRAINZ_RELEASEID: 'B0B1C2D3-1111-2222-3333-444455556666' }),
  flac_duplicates_and_empties: flac([[4, vorbis(['ARTIST=First', 'ARTIST=Second', 'ALBUMARTIST=', 'ALBUM ARTIST=Spaced', 'ALBUM=', 'TITLE=', 'TRACKNUMBER=0', 'DISCNUMBER=x', 'DATE=97', '=novalue', 'noequals', 'YEAR=1999'])]]),
  flac_release_group_only: flacTags({ ALBUM: 'Group', MUSICBRAINZ_RELEASEGROUPID: MB }),
  flac_release_id_spellings: flacTags({ ALBUM: 'Spelled', 'MusicBrainz Album Release Id': `urn:${MB.toUpperCase()}`, MUSICBRAINZ_ALBUMID: 'not an id' }),
  flac_long_values: flacTags({ ALBUM: long, TITLE: 'é'.repeat(400), ARTIST: 'x'.repeat(301) }),
  flac_invalid_utf8: flac([[4, vorbis([Buffer.concat([utf8('ALBUM=Caf'), Buffer.from([0xe9, 0xff, 0xc3]), utf8(' ok')]), Buffer.concat([Buffer.from([0xc3]), utf8('KEY=value')])])]]),
  flac_streaminfo_then_comment: flac([[0, Buffer.alloc(34, 0x01)], [1, Buffer.alloc(8)], [4, vorbis(comments({ ALBUM: 'After', TRACKNUMBER: '7' }))], [6, Buffer.alloc(12, 0x09)]]),
  flac_comment_after_last: flac([[0, Buffer.alloc(34)], [4, vorbis(comments({ ALBUM: 'Hidden' }))]], { last: 0 }),
  flac_comment_block_cut: flacTags({ ALBUM: 'Cut' }).subarray(0, 30),
  flac_count_too_high: flac([[4, vorbis(comments({ ALBUM: 'Counted', TITLE: 'Two' }), { count: 9 })]]),
  flac_count_zero: flac([[4, vorbis(comments({ ALBUM: 'Unread' }), { count: 0 })]]),
  flac_negative_length: flac([[4, Buffer.concat([u32le(4), utf8('vend'), u32le(2), u32le(0x80000001), utf8('ALBUM=Lost')])]]),
  flac_empty_vendor: flac([[4, vorbis(comments({ ALBUM: 'No vendor', TRACKNUMBER: '1' }), { vendor: '' })]]),
  flac_many_comments: flac([[4, vorbis([...Array.from({ length: 511 }, (_, i) => `K${i}=v`), 'ALBUM=At the cap', 'TITLE=Beyond the cap'])]]),
  flac_year_run_on: flacTags({ ALBUM: 'Y', DATE: '19970304' }),
  flac_year_boundaries: flacTags({ ALBUM: 'Y', DATE: 'x1997 y_2003 2004z 1799 2100 20051 (2005) 1999' }),
  flac_year_after_accent: flacTags({ ALBUM: 'Y', DATE: 'é1987é' }),
  flac_number_edges: flacTags({ ALBUM: 'N', TRACKNUMBER: '1234567', DISCNUMBER: '\u00a0\u3000\ufeff3' }),
  flac_number_not_space: flacTags({ ALBUM: 'N', TRACKNUMBER: '\u00854', DISCNUMBER: '-1' }),
  flac_discogs_edges: flacTags({ ALBUM: 'D', DISCOGS_RELEASE_ID: '', DISCOGSRELEASEID: 'https://www.discogs.com/release/249504-Rick-Astley' }),
  flac_discogs_no_digits: flacTags({ ALBUM: 'D', DISCOGS_RELEASE_ID: 'no digits here' }),
  flac_mbid_edges: flacTags({ ALBUM: 'M', MUSICBRAINZ_ALBUMID: '0123456-89abcdef0123456789abcdef0 01234567--89ab-cdef-0123-456789abcdef 0123456789ab-cdef0123-456789abcdef 01234567-89AB-CDEF-0123-456789ABCDEFXYZ' }),
  flac_mbid_short: flacTags({ ALBUM: 'M', MUSICBRAINZ_ALBUMID: '0123456789abcdef0123456789abcde', MUSICBRAINZ_RELEASEID: MB.replace(/0/g, 'F') }),
  flac_mbid_key_spacing: flac([[4, vorbis(['ALBUM=M', `musicbrainz\u00a0album\t_id=${MB}`])]]),
  flac_mbid_key_not_space: flac([[4, vorbis(['ALBUM=M', `musicbrainz\u0085albumid=${MB}`])]]),
  flac_mbid_rewritten_key_keeps_place: flac([[4, vorbis(['ALBUM=M', `MUSICBRAINZ_ALBUMID=${'a'.repeat(32)}`, `MUSICBRAINZ_RELEASEID=${'b'.repeat(32)}`, `MUSICBRAINZ_ALBUMID=${'c'.repeat(32)}`])]]),
  flac_no_comment_block: flac([[0, Buffer.alloc(34)]]),
  flac_header_only: latin1('fLaC'),
  // Ogg
  opus_single_page: Buffer.concat([oggPage(0, [opusHead]), oggPage(1, [opusTags(comments({ ALBUM: 'Selected Ambient Works', ARTIST: 'Aphex Twin', TITLE: 'Xtal', DATE: '1992', TRACKNUMBER: '1' }))]), Buffer.alloc(32, 0x33)]),
  opus_laced: Buffer.concat([oggPage(0, [opusHead]), oggPage(1, [opusTags([...comments({ ALBUM: 'Laced', TRACKNUMBER: '2' }), `COMMENT=${'x'.repeat(700)}`, 'TITLE=After the long one'])])]),
  opus_two_pages: (() => {
    const packet = opusTags([...comments({ ALBUM: 'Continued' }), `COMMENT=${'y'.repeat(600)}`, 'TITLE=On the next page'])
    return Buffer.concat([oggPage(0, [opusHead]), oggPage(1, [packet.subarray(0, 510)], { open: true }), oggPage(2, [packet.subarray(510)])])
  })(),
  opus_cut_in_tags: (() => {
    const whole = Buffer.concat([oggPage(0, [opusHead]), oggPage(1, [opusTags([...comments({ ALBUM: 'Half read', ARTIST: 'Someone' }), `COVER=${'z'.repeat(900)}`, 'TITLE=Never reached'])])])
    return whole.subarray(0, whole.length - 500)
  })(),
  opus_head_and_tags_on_one_page: oggPage(0, [opusHead, opusTags(comments({ ALBUM: 'Same page', TRACKNUMBER: '12/14' }))]),
  vorbis_comment: Buffer.concat([oggPage(0, [vorbisId]), oggPage(1, [vorbisTags(comments({ ALBUM: 'Vorbis', ALBUMARTIST: 'Xiph', DISCNUMBER: '2', TRACKNUMBER: '5', MUSICBRAINZ_ALBUMID: MB }))])]),
  ogg_other_second_packet: Buffer.concat([oggPage(0, [vorbisId]), oggPage(1, [Buffer.concat([Buffer.from([5]), latin1('vorbis'), Buffer.alloc(20)])])]),
  ogg_one_page_only: oggPage(0, [opusHead]),
  ogg_broken_second_page: Buffer.concat([oggPage(0, [opusHead]), latin1('NotAnOggPage'), Buffer.alloc(40)]),
  ogg_segment_table_cut: oggPage(0, [opusHead]).subarray(0, 28),
  // ID3
  id3v24_utf8: id3([['TALB', text('Kid A')], ['TPE2', text('Radiohead')], ['TPE1', text('Radiohead feat. Nobody')], ['TIT2', text('Everything In Its Right Place')], ['TDRC', text('2000-10-02T00:00:00')], ['TRCK', text('1/10')], ['TPOS', text('1/1')], ['TXXX', txxx('MUSICBRAINZ_ALBUMID', MB)]]),
  id3v24_picard: id3([['TALB', text('Kid A')], ['TXXX', txxx('MusicBrainz Album Id', 'b0b1c2d3-1111-2222-3333-444455556666')], ['TXXX', txxx('DISCOGS_RELEASE_ID', '998877')]]),
  id3v23_plain_sizes: id3([['TALB', text('OK Computer')], ['TPE1', text('Radiohead')], ['TYER', text('1997')], ['TRCK', text('3')], ['TIT2', text('t'.repeat(200))]], { version: 3 }),
  id3v24_long_frame: id3([['TALB', text('L'.repeat(200))], ['TIT2', text('after the long frame')]]),
  id3_latin1: id3([['TALB', text('Ágætis byrjun', 0)], ['TPE1', text('Sigur Rós', 0)], ['TRCK', text('4', 0)]], { version: 3 }),
  id3_utf16_bom: id3([['TALB', text('Ágætis byrjun 😀', 1)], ['TPE2', Buffer.concat([Buffer.from([1, 0xfe, 0xff]), Buffer.from('Big Endian', 'utf16le').swap16()])], ['TIT2', Buffer.concat([Buffer.from([1]), Buffer.from('No BOM', 'utf16le')])], ['TXXX', txxx('MusicBrainz Release Id', MB, 1)]]),
  id3_utf16be: id3([['TALB', text('Homogenic', 2)], ['TPE1', text('Björk', 2)], ['TXXX', txxx('DISCOGS_RELEASE_ID', '31337', 2)]]),
  id3_lone_surrogates: id3([['TALB', Buffer.concat([Buffer.from([1, 0xff, 0xfe]), Buffer.from([0x41, 0x00, 0x3d, 0xd8, 0x42, 0x00, 0x00, 0xde, 0x43, 0x00])])], ['TIT2', text('b'.repeat(299) + '😀')]]),
  id3_nul_handling: id3([['TALB', text('First\0Second')], ['TPE1', text('Padded\0\0\0')], ['TIT2', text('\0Hidden')], ['TRCK', Buffer.concat([Buffer.from([1, 0xff, 0xfe]), Buffer.from('7\0ignored', 'utf16le')])]]),
  id3_unknown_encoding: id3([['TALB', Buffer.concat([Buffer.from([4]), utf8('Unread')])], ['TIT2', Buffer.alloc(0)], ['TPE1', text('Read')]]),
  id3_txxx_edges: id3([['TXXX', utf8('\x03no terminator')], ['TXXX', txxx('', 'no description')], ['TXXX', txxx('EMPTY', '')], ['TXXX', Buffer.alloc(0)], ['TXXX', txxx('musicbrainz_albumid', `x${MB}x`)], ['TALB', text('Edges')]]),
  id3_txxx_empty_value_only: id3([['TXXX', txxx('MUSICBRAINZ_ALBUMID', '')], ['TXXX', txxx('', MB)]]),
  id3_txxx_empty_value_keeps_first: id3([['TXXX', txxx('MUSICBRAINZ_ALBUMID', MB)], ['TXXX', txxx('MUSICBRAINZ_ALBUMID', '')], ['TXXX', txxx('DISCOGS_RELEASE_ID', '77', 1)], ['TXXX', txxx('DISCOGS_RELEASE_ID', '', 1)]]),
  id3_bad_frame_id: id3([['TALB', text('Before')], ['tit2', text('lowercase id ends the walk')], ['TPE1', text('Unread')]]),
  id3_frame_past_end: (() => {
    const whole = id3([['TALB', text('Whole')], ['TIT2', text('This frame is cut short by the buffer')]], { tail: Buffer.alloc(0) })
    return whole.subarray(0, whole.length - 5)
  })(),
  id3_stated_size_too_small: id3([['TALB', text('Inside')], ['TIT2', text('Outside the stated size')]], { size: 17 }),
  id3_stated_size_too_large: id3([['TALB', text('Bounded by the buffer')]], { size: 0x0fffffff }),
  id3_non_text_frames: id3([['APIC', Buffer.alloc(40, 0x07)], ['COMM', text('a comment')], ['TALB', text('After a picture')], ['TCON', text('(17)')]]),
  id3v22: Buffer.concat([latin1('ID3'), Buffer.from([2, 0, 0]), synchsafe(10), latin1('TAL'), Buffer.from([0, 0, 4, 0]), utf8('Old')]),
  id3_header_only: Buffer.concat([latin1('ID3'), Buffer.from([4, 0, 0]), synchsafe(0)]),
  id3_short: latin1('ID3\x04'),
  mp3_frame_without_tag: Buffer.concat([Buffer.from([0xff, 0xfb, 0x90, 0x00]), Buffer.alloc(64)]),
  // Not music
  empty: Buffer.alloc(0),
  three_bytes: latin1('fLa'),
  plain_text: latin1('not a container'),
  mp4: Buffer.concat([u32be(24), latin1('ftypisom'), Buffer.alloc(16)])
}

// A Rust string cannot hold a lone surrogate. Every use of a tag passes through `toWellFormed` before it leaves the
// relay (the announce's `music` and `title`) or is folded to ASCII (`albumKey`), so the reference is taken after it.
const wellFormed = tags => tags && Object.fromEntries(Object.entries(tags).map(([k, v]) => [k, typeof v === 'string' ? v.toWellFormed() : v]))

const parse = Object.entries(cases).map(([name, data]) => ({ name, hex: data.toString('hex'), expected: wellFormed(parseTags(data)) }))

// Every prefix of these files: a read that stops anywhere still yields what was read so far, and never throws.
// `runs` lists a prefix length and what every prefix from that length up to the next run's reads as.
const cuts = ['flac_full', 'flac_streaminfo_then_comment', 'opus_laced', 'opus_two_pages', 'vorbis_comment', 'id3v24_utf8', 'id3v23_plain_sizes', 'id3_utf16_bom'].map(name => {
  const data = cases[name]
  const runs = []
  let last
  for (let from = 0; from <= data.length; from++) {
    const expected = wellFormed(parseTags(data.subarray(0, from)))
    const text = JSON.stringify(expected)
    if (text !== last) runs.push({ from, expected })
    last = text
  }
  return { name, hex: data.toString('hex'), runs }
})

const keys = [
  { albumArtist: 'Sigur Rós', album: 'Ágætis byrjun', year: 1999 },
  { albumArtist: 'sigur ros', album: 'Agætis Byrjun!', year: 1999 },
  { albumArtist: 'A', album: 'Same Name', year: 2009 },
  { albumArtist: 'A', album: 'Same Name', year: null },
  { albumArtist: 'A', album: 'Same Name', year: 0 },
  { albumArtist: '', album: 'No Artist', year: 1984 },
  { albumArtist: null, album: null, year: null },
  { albumArtist: 'ＦＵＬＬ　ｗｉｄｔｈ １２３', album: 'ﬁne ﬂow ½ Ⅷ', year: 2001 },
  { albumArtist: '坂本龍一', album: '千のナイフ', year: 1978 },
  { albumArtist: 'İstanbul ΣΊΣΥΦΟΣ ẞ', album: 'Straße K', year: 2010 },
  { albumArtist: 'AC/DC', album: 'Back in Black (Remastered) [Deluxe]', year: 1980 },
  { albumArtist: 'a\u0301\u0300b', album: '\ud83d\ude00 emoji', year: 2020 }
].map(input => ({ input, expected: albumKey(input) }))

writeFileSync(outFile('tags-reference.json'), JSON.stringify({ parse, cuts, albumKey: keys }, null, 2) + '\n')
console.log(`wrote ${outFile('tags-reference.json')}`)
