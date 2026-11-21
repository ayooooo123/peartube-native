// Generates resolution-reference.json from the app's own src/resolution.js.
import { writeFileSync } from 'node:fs'
import { load, outFile } from './app.mjs'

const { mediaInfo, resolutionLabel } = await load('src/resolution.js')
const box = (type, data) => { const h = Buffer.alloc(8); h.writeUInt32BE(data.length + 8); h.write(type, 4); return Buffer.concat([h, data]) }
const tkhd = Buffer.alloc(84); tkhd.writeUInt32BE(1920 * 65536, 76); tkhd.writeUInt32BE(800 * 65536, 80)
const handler = Buffer.alloc(12); handler.write('vide', 8)
const stsdEntry = Buffer.alloc(78); stsdEntry.writeUInt16BE(1920, 24); stsdEntry.writeUInt16BE(800, 26)
const stsd = Buffer.alloc(8); stsd.writeUInt32BE(1, 4)
const minf = box('minf', box('stbl', box('stsd', Buffer.concat([stsd, box('avc1', stsdEntry)]))))
const moov = box('moov', box('trak', Buffer.concat([box('tkhd', tkhd), box('mdia', Buffer.concat([box('hdlr', handler), minf]))])))
const avi = Buffer.alloc(72); avi.write('RIFF'); avi.write('AVI ', 8); avi.write('avih', 24); avi.writeUInt32LE(640, 64); avi.writeUInt32LE(480, 68)
const el = (id, data) => Buffer.concat([Buffer.from(id, 'hex'), Buffer.from([0x80 | data.length]), data])
const video = el('e0', Buffer.concat([el('b0', Buffer.from([0x05, 0x00])), el('ba', Buffer.from([0x02, 0xd0]))]))
const tracks = el('1654ae6b', el('ae', Buffer.concat([el('83', Buffer.from([1])), video])))
const mkv = Buffer.concat([el('1a45dfa3', Buffer.alloc(0)), Buffer.from('18538067ff', 'hex'), tracks])
const cluster = el('1f43b675', Buffer.alloc(0))
const seek = offset => el('114d9b74', el('4dbb', Buffer.concat([el('53ab', Buffer.from('1654ae6b', 'hex')), el('53ac', Buffer.from([offset]))])))
const seekHead = seek(seek(0).length + cluster.length)
const seekMkv = Buffer.concat([el('1a45dfa3', Buffer.alloc(0)), Buffer.from('18538067ff', 'hex'), seekHead, cluster, tracks])
const tkhd1 = Buffer.alloc(96); tkhd1[0] = 1; tkhd1.writeUInt32BE(1280 * 65536, 88); tkhd1.writeUInt32BE(720 * 65536, 92)
const stsdEntry1 = Buffer.alloc(78); stsdEntry1.writeUInt16BE(1280, 24); stsdEntry1.writeUInt16BE(720, 26)
const minf1 = box('minf', box('stbl', box('stsd', Buffer.concat([stsd, box('hvc1', stsdEntry1)]))))
const moov1 = box('moov', box('trak', Buffer.concat([box('tkhd', tkhd1), box('mdia', Buffer.concat([box('hdlr', handler), minf1]))])))
const extended = Buffer.alloc(16); extended.writeUInt32BE(1); extended.write('free', 4); extended.writeBigUInt64BE(16n, 8)
const audio = Buffer.from(handler); audio.write('soun', 8)
const audioEntry = Buffer.alloc(28); audioEntry.writeUInt16BE(2, 16)
const audioMinf = box('minf', box('stbl', box('stsd', Buffer.concat([stsd, box('mp4a', audioEntry)]))))
const audioOnly = box('moov', box('trak', Buffer.concat([box('tkhd', tkhd), box('mdia', Buffer.concat([box('hdlr', audio), audioMinf]))])))
const invalidMkv = Buffer.concat([el('1a45dfa3', Buffer.alloc(0)), Buffer.from('18538067ff1f43b67580', 'hex')])
const aviList = Buffer.concat([Buffer.from('LIST\0\0\0\0strl', 'ascii'), box('strh', Buffer.from('vidsXVID', 'ascii'))])
aviList.writeUInt32LE(aviList.length - 8, 4)
const fullAvi = Buffer.alloc(72 + aviList.length)
avi.copy(fullAvi)
fullAvi.writeUInt32LE(52 + aviList.length, 16)
fullAvi.writeUInt32LE(40, 28)
aviList.copy(fullAvi, 72)
const zeroAvi = Buffer.from(fullAvi); zeroAvi.writeUInt32LE(0, 64)
const zeroMp4 = Buffer.from(moov); zeroMp4.fill(0, 8 + 8 + 8 + 76, 8 + 8 + 8 + 80); zeroMp4.writeUInt16BE(0, 8 + 8 + 84 + 8 + 8 + 20 + 8 + 8 + 8 + 8 + 8 + 24)
const aliases = Object.fromEntries(['ftyp', 'mdat', 'free', 'wide', 'skip'].map(type => [type, Buffer.concat([box(type, Buffer.alloc(0)), moov])]))
const extra = { seek_mkv: seekMkv, cluster_without_seek: invalidMkv, version1_mp4: moov1, extended_box: Buffer.concat([extended, moov]), audio_only: audioOnly, zero_avi: zeroAvi, zero_mp4: zeroMp4, ...aliases }
const longMdat = Buffer.alloc(262160); longMdat.writeUInt32BE(longMdat.length); longMdat.write('mdat', 4)

// Audio files. FLAC: "fLaC", then metadata blocks `[type, body]`; the last one carries the last-block flag.
const u32be = n => { const b = Buffer.alloc(4); b.writeUInt32BE(n >>> 0); return b }
const flacFile = blocks => Buffer.concat([Buffer.from('fLaC', 'latin1'), ...blocks.map(([type, body], i) => Buffer.concat([Buffer.from([(i === blocks.length - 1 ? 0x80 : 0) | type, (body.length >> 16) & 0xff, (body.length >> 8) & 0xff, body.length & 0xff]), body]))])
// STREAMINFO: 34 bytes; from byte 10, 20 bits of sample rate, 3 of channels - 1, 5 of bits per sample - 1 and 36 of samples.
const streaminfo = ({ rate, channels, depth, samples }) => {
  const b = Buffer.alloc(34, 0x07)
  b.writeBigUInt64BE((BigInt(rate) << 44n) | (BigInt(channels - 1) << 41n) | (BigInt(depth - 1) << 36n) | BigInt(samples), 10)
  return b
}
// One MPEG audio frame: the header, the side information a Layer III encoder leaves empty in its first frame, then a
// Xing or Info header when `tag` names one. Version bits: 3 MPEG-1, 2 MPEG-2, 0 MPEG-2.5; layer bits: 3 Layer I,
// 2 Layer II, 1 Layer III.
const mpeg = ({ version = 3, layer = 1, bitrate = 9, rate = 0, mono = false, tag = null, frames = 0, flags = 1 }) => {
  const header = Buffer.from([0xff, 0xe0 | (version << 3) | (layer << 1) | 1, (bitrate << 4) | (rate << 2), mono ? 0xc0 : 0x40])
  const side = Buffer.alloc(version === 3 ? (mono ? 17 : 32) : (mono ? 9 : 17))
  const xing = tag ? Buffer.concat([Buffer.from(tag, 'latin1'), Buffer.from([0, 0, 0, flags]), u32be(frames)]) : Buffer.alloc(0)
  return Buffer.concat([header, side, xing, Buffer.alloc(48)])
}
const id3 = size => Buffer.concat([Buffer.from('ID3', 'latin1'), Buffer.from([4, 0, 0, (size >> 21) & 0x7f, (size >> 14) & 0x7f, (size >> 7) & 0x7f, size & 0x7f]), Buffer.alloc(size, 0xff)])
const cd = { rate: 44100, channels: 2, depth: 16, samples: 44100 * 215 }
const audioFiles = {
  flac_cd: flacFile([[0, streaminfo(cd)]]),
  flac_hires: flacFile([[0, streaminfo({ rate: 96000, channels: 2, depth: 24, samples: 96000 * 61 + 12345 })]]),
  flac_surround_long: flacFile([[0, streaminfo({ rate: 655350, channels: 8, depth: 32, samples: 2 ** 36 - 1 })]]),
  flac_mono_8bit: flacFile([[0, streaminfo({ rate: 8000, channels: 1, depth: 8, samples: 1 })]]),
  flac_zero_rate: flacFile([[0, streaminfo({ rate: 0, channels: 2, depth: 16, samples: 1000 })]]),
  flac_zero_samples: flacFile([[0, streaminfo({ rate: 44100, channels: 2, depth: 16, samples: 0 })]]),
  flac_streaminfo_second: flacFile([[4, Buffer.alloc(40, 0x01)], [0, streaminfo(cd)], [1, Buffer.alloc(9)]]),
  flac_streaminfo_after_last: Buffer.concat([flacFile([[4, Buffer.alloc(12)]]), Buffer.from([0x00, 0x00, 0x00, 0x22]), streaminfo(cd)]),
  flac_streaminfo_short: flacFile([[0, streaminfo(cd).subarray(0, 17)], [0, streaminfo({ rate: 48000, channels: 2, depth: 24, samples: 48000 })]]),
  flac_streaminfo_18_bytes: flacFile([[0, streaminfo(cd).subarray(0, 18)]]),
  flac_streaminfo_cut: flacFile([[0, streaminfo(cd)]]).subarray(0, 4 + 4 + 17),
  flac_streaminfo_just_there: flacFile([[0, streaminfo(cd)]]).subarray(0, 4 + 4 + 18),
  flac_no_streaminfo: flacFile([[4, Buffer.alloc(12)], [1, Buffer.alloc(4)]]),
  flac_header_only: Buffer.from('fLaC', 'latin1'),
  mp3_xing: mpeg({ tag: 'Xing', frames: 8230 }),
  mp3_info_mono: mpeg({ tag: 'Info', frames: 1000, mono: true, rate: 1 }),
  mp3_no_xing: mpeg({ rate: 2 }),
  mp3_xing_without_frames: mpeg({ tag: 'Xing', frames: 8230, flags: 2 }),
  mp3_xing_zero_frames: mpeg({ tag: 'Xing', frames: 0 }),
  mp3_xing_many_frames: mpeg({ tag: 'Xing', frames: 0xfffffffe }),
  mp3_mpeg2: mpeg({ version: 2, tag: 'Xing', frames: 3000 }),
  mp3_mpeg2_mono: mpeg({ version: 2, tag: 'Info', frames: 3000, mono: true, rate: 2 }),
  mp3_mpeg25: mpeg({ version: 0, tag: 'Xing', frames: 3000, rate: 1 }),
  mp2: mpeg({ layer: 2, tag: 'Xing', frames: 500 }),
  mp2_mpeg2: mpeg({ version: 2, layer: 2, rate: 1 }),
  mp1: mpeg({ layer: 3, rate: 1 }),
  mp3_after_id3: Buffer.concat([id3(300), mpeg({ tag: 'Xing', frames: 4115 })]),
  mp3_id3_only: id3(64),
  mp3_id3_then_nothing: Buffer.concat([id3(64), Buffer.alloc(32)]),
  mp3_id3_ten_bytes: id3(0),
  // The search reads 8192 positions: a frame at offset 8191 is the last one it finds.
  mp3_after_junk: Buffer.concat([Buffer.from([0xff, 0xfb]), Buffer.alloc(8189), mpeg({ tag: 'Xing', frames: 10 })]),
  mp3_past_the_search: Buffer.concat([Buffer.from([0xff, 0xfb]), Buffer.alloc(8190), mpeg({ tag: 'Xing', frames: 10 })]),
  mp3_bad_headers_first: Buffer.concat([
    mpeg({ version: 1 }).subarray(0, 4),
    mpeg({ layer: 0 }).subarray(0, 4),
    mpeg({ bitrate: 0 }).subarray(0, 4),
    mpeg({ bitrate: 15 }).subarray(0, 4),
    mpeg({ rate: 3 }).subarray(0, 4),
    mpeg({ tag: 'Info', frames: 77 })
  ]),
  mp3_xing_cut: mpeg({ tag: 'Xing', frames: 8230 }).subarray(0, 4 + 32 + 11),
  mp3_xing_just_there: mpeg({ tag: 'Xing', frames: 8230 }).subarray(0, 4 + 32 + 12),
  mp3_header_cut: mpeg({}).subarray(0, 4)
}
const cases = { mp4: moov, mov_after_mdat: Buffer.concat([longMdat, moov]), avi: fullAvi, matroska: mkv, truncated_mkv: mkv.subarray(0, 12), truncated_mp4: moov.subarray(0, 30), truncated_avi: fullAvi.subarray(0, 68), unknown: Buffer.from('not a container'), ...extra, ...audioFiles }
const media = []
for (const [name, data] of Object.entries(cases)) media.push({ name, hex: data.toString('hex'), expected: await mediaInfo(async (start, end) => data.subarray(start, end + 1)) })
const labels = []
for (const [width, height] of [[0,0],[639,479],[640,1],[1,480],[1199,699],[1200,1],[1,700],[1799,999],[1800,1],[1,1000],[3199,1999],[3200,1],[1,2000],[1920,800]]) labels.push({ width, height, expected: resolutionLabel({ width, height }) })
writeFileSync(outFile('resolution-reference.json'), JSON.stringify({ media, labels }, null, 2) + '\n')
console.log(`wrote ${outFile('resolution-reference.json')}`)
