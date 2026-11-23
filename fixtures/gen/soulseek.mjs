// Generates soulseek-reference.json from the app's own src/providers/soulseek.js and src/providers/staging.js:
// where `placeSingleTrack` puts a lone track, the order `normalizeSoulseekCandidates` ranks search hits in,
// `normalizeSoulseekBrowse`, and what every source provider shares about the import folder (the names of the two
// dot files, which release ids a pin may hold, and how a folder or file name is cut to fit).
import { writeFileSync } from 'node:fs'
import { load, outFile } from './app.mjs'

const soulseek = await load('src/providers/soulseek.js')
const staging = await load('src/providers/staging.js')
const { placeSingleTrack, normalizeSoulseekCandidates, normalizeSoulseekBrowse } = soulseek
const { RELEASE_ID, safeSegment, clipBytes, formatAlbumDir } = staging

// A Rust string cannot hold a lone surrogate; a name cut inside a pair reads the same once it is written to disk
// (the file system call replaces it) or passes `toWellFormed`, so the reference is taken after it.
const wellFormed = value => {
  if (typeof value === 'string') return value.toWellFormed()
  if (Array.isArray(value)) return value.map(wellFormed)
  if (value && typeof value === 'object') return Object.fromEntries(Object.entries(value).map(([k, v]) => [k, wellFormed(v)]))
  return value
}

const MB = '0123456789abcdef0123456789abcdef'
const tagged = (extra = {}) => ({ container: 'flac', albumArtist: null, album: null, artist: null, title: null, year: null, disc: null, track: null, mbid: null, discogs: null, ...extra })
const place = (input) => ({ tags: null, release: null, track: null, position: null, fallbackTitle: '', ...input })

const placeInputs = [
  // The album from the peer's folder
  place({ filename: '@@share\\Music\\Miles Davis - Kind of Blue (1959)\\03 - Blue in Green.flac' }),
  place({ filename: 'Music/Radiohead/Kid A/01. Everything In Its Right Place.mp3' }),
  place({ filename: 'Music/Radiohead/2000 - Kid A/1_Everything.mp3' }),
  place({ filename: 'Music/Björk/1997 – Homogenic/02-Jóga.flac' }),
  place({ filename: 'Music/Björk — Homogenic [1997]/CD1/002 Jóga.flac' }),
  place({ filename: 'Music/Artist - Album - Deluxe  -  Edition ( 2004 )/Disc 2/7 Seven.flac' }),
  place({ filename: 'Music/Artist - Box/disk12/11 Eleven.flac' }),
  place({ filename: 'Music/Artist - Box/CD 3/11 Eleven.flac' }),
  place({ filename: 'Music/Artist - Box/CD123/11 Eleven.flac' }),
  place({ filename: 'Music/Artist - Box/cdx/11 Eleven.flac' }),
  place({ filename: 'CD1/04 Four.flac' }),
  place({ filename: 'Album Only/05 Five.flac' }),
  place({ filename: '  Spaced Artist  /  Spaced Album (1999)  /  06 Six.flac  ' }),
  place({ filename: 'Artist/(1999)/07 Seven.flac' }),
  place({ filename: 'Artist/[ 2001 ] (2002) Album 1999 remaster2003x/08 Eight.flac' }),
  place({ filename: 'Artist/1999 - /09 Nine.flac' }),
  place({ filename: 'Artist/ - Album/09 Nine.flac' }),
  place({ filename: 'Artist/A -B- C - D/10 Ten.flac' }),
  place({ filename: 'Artist/1999 - 2001 - A Space Odyssey/10 Ten.flac' }),
  place({ filename: '@@a/@@b/Artist - Album/01 One.flac' }),
  place({ filename: '@@share/01 One.flac' }),
  place({ filename: '01 One.flac' }),
  place({ filename: '' }),
  place({ filename: 'Artist - Album/' }),
  // The track number and the title
  place({ filename: 'A - B/Title Without Number.flac' }),
  place({ filename: 'A - B/Title Without Number.flac', position: 4 }),
  place({ filename: 'A - B/Title Without Number.flac', track: 12, position: 4 }),
  place({ filename: 'A - B/00 Intro.flac', position: 1 }),
  place({ filename: 'A - B/00 Intro.flac' }),
  place({ filename: 'A - B/0003 Four Digits.flac', position: 3 }),
  place({ filename: 'A - B/123 Three Digits.flac' }),
  place({ filename: 'A - B/7.flac' }),
  place({ filename: 'A - B/07 -._ .flac' }),
  place({ filename: 'A - B/07 \t- \t- Tabbed.flac' }),
  place({ filename: 'A - B/A1 Side One.flac', position: 1 }),
  place({ filename: 'A - B/b12-Side Two.flac', position: 8 }),
  place({ filename: 'A - B/h3.flac', position: 20 }),
  place({ filename: 'A - B/i3 Not A Side.flac', position: 21 }),
  place({ filename: 'A - B/A123 Not A Side.flac', position: 22 }),
  place({ filename: 'A - B/Title.flac', position: 1000 }),
  place({ filename: 'A - B/Title.flac', track: 999 }),
  place({ filename: 'A - B/Title.flac', track: 1000, position: 2 }),
  place({ filename: 'A - B/05 Title', track: 9 }),
  place({ filename: 'A - B/05 Title.FLAC' }),
  place({ filename: 'A - B/05 Title.with.dots.Mp3' }),
  place({ filename: 'A - B/.flac', position: 2 }),
  place({ filename: 'A - B/05 Title.', position: 2 }),
  place({ filename: 'A - B/..', position: 2 }),
  // Tags
  place({ filename: 'x/y/whatever.flac', tags: tagged({ album: 'Blue Train', albumArtist: 'John Coltrane', artist: 'Sextet', title: "Moment's Notice", year: 1958, track: 3 }) }),
  place({ filename: 'x/y/whatever.flac', tags: tagged({ album: 'Blue Train', artist: 'Sextet', track: 3 }) }),
  place({ filename: 'x/y/whatever.flac', tags: tagged({ album: 'Blue Train', track: 3 }) }),
  place({ filename: 'Peer Artist - Peer Album (1970)/04 Peer Title.flac', tags: tagged({ artist: 'Tag Artist', title: 'Tag Title', year: 1971 }) }),
  place({ filename: 'Peer Album/04 Peer Title.flac', tags: tagged({ albumArtist: 'Tag Album Artist', artist: 'Tag Artist', year: 1971 }) }),
  place({ filename: 'Peer Artist - Peer Album (1970)/04 Peer Title.flac', tags: tagged({ album: 'Tag Album', year: 1971, track: 1000 }) }),
  place({ filename: 'Peer Artist - Peer Album (1970)/04 Peer Title.flac', tags: tagged({ album: 'Tag Album', track: 0, title: '' }) }),
  place({ filename: 'lonely.flac', tags: tagged({ mbid: MB, track: 5, title: 'Lonely' }) }),
  place({ filename: 'lonely.flac', tags: tagged({ discogs: '249504', track: 5 }) }),
  place({ filename: 'lonely.flac', tags: tagged({ mbid: MB, discogs: '249504', track: 5 }), fallbackTitle: 'Some Artist · Some Album' }),
  place({ filename: 'lonely.flac', tags: tagged({ track: 5 }) }),
  place({ filename: 'lonely.flac', tags: tagged({ album: 'Tagged', mbid: MB, track: 5 }) }),
  // A release the caller names
  place({ filename: 'lonely.flac', release: `mb:${MB}`, track: 2 }),
  place({ filename: 'lonely.flac', release: 'discogs:r42', track: 2, fallbackTitle: '  Artist  ·  Album · Extra  ' }),
  place({ filename: 'lonely.flac', release: 'discogs:r42', track: 2, fallbackTitle: 'A\u00a0·\u00a0B·C · · D\t·' }),
  place({ filename: 'lonely.flac', release: 'discogs:r42', track: 2, fallbackTitle: '. · @x ·' }),
  place({ filename: 'lonely.flac', release: 'discogs:r42', track: 2, fallbackTitle: ' · ' }),
  place({ filename: 'Peer Artist - Peer Album/03 Three.flac', release: 'album:0123456789abcdef0123' }),
  place({ filename: 'x/03 Three.flac', release: `mb:${MB}`, tags: tagged({ album: 'Tag Album', albumArtist: 'Tag Artist', mbid: 'f'.repeat(32) }) }),
  // Names a file system would refuse, or that hide
  place({ filename: 'x/01.flac', tags: tagged({ album: 'Back in Black: Live?', albumArtist: 'AC/DC', title: 'T.N.T. <live> "1977" | *', track: 1 }) }),
  place({ filename: 'x/01.flac', tags: tagged({ album: '...And Justice for All...', albumArtist: '@home', title: '. hidden .', track: 1 }) }),
  place({ filename: 'x/01.flac', tags: tagged({ album: 'Tabs\tand\nlines\u0000here', albumArtist: 'No\u00a0Break\u3000Wide', title: '\u2028Sep\u2029', track: 1 }) }),
  place({ filename: 'x/01.flac', tags: tagged({ album: 'Back\\slash\\\\twice', albumArtist: 'C:\\Music', title: 'a\\b', track: 1 }) }),
  place({ filename: 'x/01.flac', tags: tagged({ album: 'L'.repeat(200), albumArtist: 'A'.repeat(200), title: 'T'.repeat(200), track: 1 }) }),
  place({ filename: 'x/01.flac', tags: tagged({ album: 'e'.repeat(131) + '😀😀😀', albumArtist: 'A', title: 't'.repeat(134) + '😀', track: 1 }) }),
  place({ filename: 'x/01.flac', tags: tagged({ album: '///', albumArtist: ':::', title: '???', track: 1 }) }),
  place({ filename: 'x/01.flac', tags: tagged({ album: 'ア'.repeat(160), albumArtist: '坂本龍一', title: '千のナイフ', track: 1, year: 1978 }) }),
  place({ filename: 'x/01.flac', tags: tagged({ album: 'Album', albumArtist: 'A'.repeat(310), track: 1 }) })
]

const placed = placeInputs.map(input => ({ input, expected: wellFormed(placeSingleTrack(input)) }))

const hit = (username, filename, extra = {}) => ({ username, slots: false, queue: 0, speed: 0, filename, size: 1000, bitrate: null, sample_rate: null, bit_depth: null, length_secs: null, ...extra })
const ranked = [
  hit('ready', 'a\\ready.flac', { slots: true, queue: 0 }),
  hit('slots-queue-3', 'a\\slots3.flac', { slots: true, queue: 3 }),
  hit('noslots-queue-0', 'a\\noslots0.flac', { slots: false, queue: 0 }),
  hit('noslots-queue-3', 'a\\noslots3.flac', { slots: false, queue: 3 }),
  hit('slots-queue-1', 'a\\slots1.flac', { slots: true, queue: 1 }),
  hit('deep', 'a\\deep.flac', { slots: true, bit_depth: 24, sample_rate: 96000, bitrate: 2800 }),
  hit('deep-slow', 'a\\deep-slow.flac', { slots: true, bit_depth: 24, sample_rate: 44100, bitrate: 2800 }),
  hit('cd', 'a\\cd.flac', { slots: true, bit_depth: 16, sample_rate: 44100, bitrate: 900 }),
  hit('cd-thin', 'a\\cd-thin.flac', { slots: true, bit_depth: 16, sample_rate: 44100, bitrate: 700 }),
  hit('lossy', 'a\\lossy.mp3', { slots: true, bitrate: 320, length_secs: 201 }),
  hit('lossy-low', 'a\\lossy-low.mp3', { slots: true, bitrate: 128, length_secs: 0 }),
  hit('  padded  ', '  a\\padded.ogg  ', { slots: true, size: 0 }),
  hit('not-media', 'a\\cover.jpg', { slots: true }),
  hit('not-media', 'a\\notes.txt', { slots: true }),
  hit('not-media', 'a\\flac', { slots: true }),
  hit('', 'a\\nobody.flac', { slots: true }),
  hit('   ', 'a\\blank.flac', { slots: true }),
  hit('noname', '   ', { slots: true }),
  hit('upper', 'A\\SONG.FLAC', { slots: true }),
  hit('video', 'a\\film.mkv', { slots: true, size: 4_000_000_000 })
]
// Hits equal in everything but the name: the app orders them with `localeCompare`.
const names = ['b.flac', 'B.flac', 'a.flac', 'A.flac', 'é.flac', 'e.flac', 'f.flac', 'Z.flac', 'z.flac', 'a b.flac', 'a-b.flac', 'a_b.flac', 'ab.flac', 'track 2.flac', 'track 10.flac',
  '01 x.flac', '1 x.flac', '_x.flac', '~x.flac', '[x].flac', '(x).flac', 'x,y.flac', 'x;y.flac', 'x.y.flac', "x'y.flac", 'ß.flac', 'ss.flac', 'Æ.flac', 'ae.flac', 'ø.flac', 'o.flac',
  '坂本.flac', 'Ωmega.flac', 'жизнь.flac', '😀.flac', 'a\u0301.flac', 'á.flac', 'ａ.flac', 'a\u0001b.flac', 'ab\u0001.flac', 'music\\a\\1.flac', 'music\\a 2\\1.flac', 'music/a/1.flac']
const ties = names.map((name, i) => hit(`tie${i}`, name, { slots: true, queue: 2, bit_depth: 16, sample_rate: 44100, bitrate: 1000 }))
const many = Array.from({ length: 230 }, (_, i) => hit(`many${i}`, `many\\${String((i * 7919) % 230).padStart(3, '0')} ${i % 3 ? 'x' : 'X'}.flac`, { slots: i % 2 === 0, queue: i % 5, bit_depth: i % 4 === 0 ? 24 : 16 }))

const candidates = [
  { hits: ranked, cap: 200 },
  { hits: ranked, cap: 3 },
  { hits: ranked, cap: 1 },
  { hits: ranked, cap: 0 },
  { hits: ties, cap: 200 },
  { hits: [...ties].reverse(), cap: 200 },
  { hits: many, cap: 200 },
  { hits: many, cap: 500 },
  { hits: [], cap: 200 }
].map(({ hits, cap }) => ({ hits, cap, expected: normalizeSoulseekCandidates(hits, cap) }))

const shared = [
  hit('peer', '@@share\\Music\\B Album\\02 two.flac', { size: 200 }),
  hit('peer', '@@share\\Music\\B Album\\01 one.flac', { size: 100, bit_depth: 16, sample_rate: 44100, bitrate: 900, length_secs: 180 }),
  hit('peer', '@@share\\Music\\B Album\\cover.jpg', { size: 50 }),
  hit('peer', '@@share\\Music\\a album\\01 lower.mp3', { size: 30, bitrate: 320 }),
  hit('peer', '@@share\\Music\\A Album\\01 upper.mp3', { size: 30 }),
  hit('peer', '@@share\\Music\\\ud83d\ude00\\01 astral.flac', { size: 7 }),
  hit('peer', '@@share\\Music\\\uffee\\01 high bmp.flac', { size: 9 }),
  hit('peer', '   ', { size: 1 })
]
const browse = [
  { records: shared, username: 'peer' },
  { records: [], username: 'nobody' }
].map(({ records, username }) => ({ records, username, expected: normalizeSoulseekBrowse(records, username) }))

const releaseIds = [`mb:${MB}`, `mb:${MB.toUpperCase()}`, `mb:${MB}0`, `mb:${MB.slice(1)}`, 'discogs:r1', 'discogs:r123456789012', 'discogs:r1234567890123', 'discogs:r', 'discogs:42',
  'album:0123456789abcdef0123', 'album:0123456789abcdef012', 'album:0123456789ABCDEF0123', `mb:${MB}:s01e02`, ` mb:${MB}`, `mb:${MB}\n`, 'imdb:tt0111161', '',
  'yt:0123456789abcdef0123', 'yt:0123456789abcdef012', 'yt:0123456789abcdef01234', 'yt:0123456789ABCDEF0123', 'yt:', 'YT:0123456789abcdef0123'].map(input => ({ input, expected: RELEASE_ID.test(input) }))

// A name is cut to 140 UTF-16 units and then to 180 UTF-8 bytes, between characters. Only text outside ASCII can
// reach the second limit: 60 three-byte characters are 180 bytes, 61 are not.
const kana = '\u30a2'
const cyr = '\u0416'
const face = '\ud83d\ude00'
const segmentInputs = [
  'plain name',
  kana.repeat(60), kana.repeat(61), kana.repeat(140), kana.repeat(200),
  cyr.repeat(90), cyr.repeat(91), cyr.repeat(140),
  // One, two and three bytes over, where the next character would not fit.
  'a' + kana.repeat(60), 'ab' + kana.repeat(60), 'abc' + kana.repeat(60), 'a' + cyr.repeat(90),
  face.repeat(45), face.repeat(46), face.repeat(70), 'a' + face.repeat(70),
  // The 140-unit cut falls inside a pair, and the half that is left still counts toward the bytes.
  'a'.repeat(139) + face, 'a'.repeat(139) + kana, 'a'.repeat(140) + kana,
  // What is trimmed after the cut: dots and spaces at the end.
  kana.repeat(59) + ' . ' + kana, kana.repeat(59) + '...', '  .@' + kana.repeat(61), kana.repeat(30) + '/' + kana.repeat(40),
  '', '...', null
]
const segments = segmentInputs.map(input => ({ input, expected: wellFormed(safeSegment(input, 'fallback')) }))
const clipped = [[kana.repeat(4), 0], [kana.repeat(4), 2], [kana.repeat(4), 3], [kana.repeat(4), 11], [kana.repeat(4), 12], ['a' + cyr + kana + face, 1], ['a' + cyr + kana + face, 2],
  ['a' + cyr + kana + face, 3], ['a' + cyr + kana + face, 5], ['a' + cyr + kana + face, 6], ['a' + cyr + kana + face, 9], ['a' + cyr + kana + face, 10], ['', 5]
].map(([input, max]) => ({ input, max, expected: clipBytes(input, max) }))
const albumDirs = [
  { artist: kana.repeat(40), album: kana.repeat(40), year: 1978 },
  { artist: 'Artist', album: 'Album', year: 2001 },
  { artist: 'Artist', album: 'Album', year: null },
  { artist: '', album: kana.repeat(70), year: 1999 },
  { artist: '', album: '', year: null, fallbackTitle: kana.repeat(30) + ' \u00b7 ' + kana.repeat(40) },
  { artist: '', album: '', year: null, fallbackTitle: '' }
].map(input => ({ input, expected: formatAlbumDir(input) }))

const reference = {
  constants: {
    SOULSEEK_SEARCH_CAP: soulseek.SOULSEEK_SEARCH_CAP,
    DEFAULT_LISTEN_PORT: soulseek.DEFAULT_LISTEN_PORT,
    DEFAULT_SHARE_MAX_FILES: soulseek.DEFAULT_SHARE_MAX_FILES,
    ALBUM_PIN: staging.ALBUM_PIN,
    MIRRORED_LIST: staging.MIRRORED_LIST
  },
  releaseIds,
  segments,
  clipped,
  albumDirs,
  placed,
  candidates,
  browse
}
// JSON drops a property that is undefined without a word: a name the app moved to another file would thin this
// reference out and the comparison with it.
for (const [name, value] of Object.entries(reference.constants)) {
  if (value === undefined) throw new Error(`the app no longer exports ${name} where this generator reads it`)
}
writeFileSync(outFile('soulseek-reference.json'), JSON.stringify(reference, null, 2) + '\n')
console.log(`wrote ${outFile('soulseek-reference.json')}`)
