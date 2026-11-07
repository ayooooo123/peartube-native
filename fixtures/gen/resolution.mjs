import { readFile, writeFile } from 'node:fs/promises'
const source = await readFile(new URL('./resolution.js', import.meta.url), 'utf8')
const { videoSize, resolutionLabel } = await import('data:text/javascript;base64,' + Buffer.from(source).toString('base64'))
const box = (type, data) => { const h = Buffer.alloc(8); h.writeUInt32BE(data.length + 8); h.write(type, 4); return Buffer.concat([h, data]) }
const tkhd = Buffer.alloc(84); tkhd.writeUInt32BE(1920 * 65536, 76); tkhd.writeUInt32BE(800 * 65536, 80)
const handler = Buffer.alloc(12); handler.write('vide', 8)
const moov = box('moov', box('trak', Buffer.concat([box('tkhd', tkhd), box('mdia', box('hdlr', handler))])))
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
const moov1 = box('moov', box('trak', Buffer.concat([box('tkhd', tkhd1), box('mdia', box('hdlr', handler))])))
const extended = Buffer.alloc(16); extended.writeUInt32BE(1); extended.write('free', 4); extended.writeBigUInt64BE(16n, 8)
const audio = Buffer.from(handler); audio.write('soun', 8)
const audioOnly = box('moov', box('trak', Buffer.concat([box('tkhd', tkhd), box('mdia', box('hdlr', audio))])))
const invalidMkv = Buffer.concat([el('1a45dfa3', Buffer.alloc(0)), Buffer.from('18538067ff1f43b67580', 'hex')])
const zeroAvi = Buffer.from(avi); zeroAvi.writeUInt32LE(0, 64)
const zeroMp4 = Buffer.from(moov); zeroMp4.fill(0, 8 + 8 + 8 + 76, 8 + 8 + 8 + 80)
const aliases = Object.fromEntries(['ftyp', 'mdat', 'free', 'wide', 'skip'].map(type => [type, Buffer.concat([box(type, Buffer.alloc(0)), moov])]))
const extra = { seek_mkv: seekMkv, cluster_without_seek: invalidMkv, version1_mp4: moov1, extended_box: Buffer.concat([extended, moov]), audio_only: audioOnly, zero_avi: zeroAvi, zero_mp4: zeroMp4, ...aliases }
const longMdat = Buffer.alloc(262160); longMdat.writeUInt32BE(longMdat.length); longMdat.write('mdat', 4)
const cases = { mp4: moov, mov_after_mdat: Buffer.concat([longMdat, moov]), avi, matroska: mkv, truncated_mkv: mkv.subarray(0, 12), truncated_mp4: moov.subarray(0, 30), truncated_avi: avi.subarray(0, 68), unknown: Buffer.from('not a container'), ...extra }
const media = []
for (const [name, data] of Object.entries(cases)) media.push({ name, hex: data.toString('hex'), expected: await videoSize(async (start, end) => data.subarray(start, end + 1)) })
const labels = []
for (const [width, height] of [[0,0],[639,479],[640,1],[1,480],[1199,699],[1200,1],[1,700],[1799,999],[1800,1],[1,1000],[3199,1999],[3200,1],[1,2000],[1920,800]]) labels.push({ width, height, expected: resolutionLabel({ width, height }) })
await writeFile(new URL('../resolution-reference.json', import.meta.url), JSON.stringify({ media, labels }, null, 2) + '\n')
