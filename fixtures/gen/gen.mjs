// Generates js-reference.json from the app's own src/protocol.js and src/mnemonic.js.
import { createHash } from 'node:crypto'
import { readFileSync, writeFileSync } from 'node:fs'
import { join } from 'node:path'
import { APP, JS_REF, load, outFile } from './app.mjs'

const protocol = await load('src/protocol.js')
const mnemonic = await load('src/mnemonic.js')

const hex = b => Buffer.from(b).toString('hex')
const sha = (...parts) => {
  const h = createHash('sha256')
  for (const p of parts) h.update(p)
  return h.digest()
}
const attempt = (fn, input) => {
  try {
    return { input, ok: true, value: fn(input) }
  } catch (error) {
    return { input, ok: false, error: error.message }
  }
}
const isMedia = name => protocol.MEDIA_FILE.test(name)

// 20 deterministic phrases: 10 x 12 words, 10 x 24 words.
const derived = []
const identities = []
for (let i = 0; i < 20; i++) {
  const words = i < 10 ? 12 : 24
  const entropy = sha(`peartube:fixtures:mnemonic:v1:${i}`).subarray(0, words === 24 ? 32 : 16)
  const phrase = mnemonic.entropyToMnemonic(entropy)
  const id = mnemonic.deriveIdentity(phrase)
  derived.push({ i, words, id })
  identities.push({
    i,
    words,
    mnemonic: phrase,
    genesisPublicKey: hex(id.genesisKeyPair.publicKey),
    genesisSecretKey: hex(id.genesisKeyPair.secretKey),
    channelKey: id.channelKey,
    ownerPublicKey: id.ownerPublicKey,
    vaultKey: hex(id.vaultKey)
  })
}

const channelFor = i => hex(sha(`peartube:fixtures:channel:v1:${i}`))
const writerFor = i => hex(sha(`peartube:fixtures:writer:v1:${i}`))

const claims = []
for (const { i, id } of derived) {
  for (const [channel, writer] of [
    [id.channelKey, id.ownerPublicKey],
    [channelFor(i), writerFor(i)]
  ]) {
    const sig = id.signClaim(channel, writer)
    claims.push({
      identity: i,
      channel,
      writer,
      owner: id.ownerPublicKey,
      sig,
      valid: mnemonic.verifyClaim(channel, writer, id.ownerPublicKey, sig)
    })
  }
}
{
  const id = derived[0].id
  const channel = channelFor(100)
  const writer = writerFor(100)
  const sig = id.signClaim(channel, writer)
  const tampered = (sig[0] === 'a' ? 'b' : 'a') + sig.slice(1)
  claims.push(
    { identity: 0, channel, writer, owner: id.ownerPublicKey, sig: tampered, valid: mnemonic.verifyClaim(channel, writer, id.ownerPublicKey, tampered) },
    { identity: 0, channel: channel.toUpperCase(), writer, owner: id.ownerPublicKey, sig, valid: mnemonic.verifyClaim(channel.toUpperCase(), writer, id.ownerPublicKey, sig) },
    { identity: 0, channel, writer, owner: identities[1].ownerPublicKey, sig, valid: mnemonic.verifyClaim(channel, writer, identities[1].ownerPublicKey, sig) },
    { identity: 0, channel, writer, owner: id.ownerPublicKey, sig: sig.slice(0, 126), valid: mnemonic.verifyClaim(channel, writer, id.ownerPublicKey, sig.slice(0, 126)) }
  )
}

const epochs = []
for (const { i, id } of derived) {
  for (let epoch = 0; epoch <= 3; epoch++) {
    epochs.push({ identity: i, channel: id.channelKey, epoch, epochKey: hex(id.epochKey(id.channelKey, epoch)) })
  }
}
for (const [identity, channel, epoch] of [
  [0, identities[0].channelKey.toUpperCase(), 1],
  [1, identities[1].channelKey.toUpperCase(), 2],
  [2, channelFor(2), 0],
  [3, channelFor(3), 7]
]) {
  epochs.push({ identity, channel, epoch, epochKey: hex(derived[identity].id.epochKey(channel, epoch)) })
}

const wrapped = []
const videoKeyLengths = [32, 32, 32, 16, 24, 1, 32, 64]
for (let i = 0; i < videoKeyLengths.length; i++) {
  const identity = derived[i % derived.length].id
  const epochKey = identity.epochKey(identity.channelKey, i % 4)
  const videoKey = sha(`peartube:fixtures:video:v1:${i}`).subarray(0, videoKeyLengths[i])
  const box = mnemonic.wrapVideoKey(videoKey, epochKey)
  const opened = mnemonic.unwrapVideoKey(box, epochKey)
  wrapped.push({
    videoKey: hex(videoKey),
    epochKey: hex(epochKey),
    wrapped: box,
    unwrap: opened ? hex(opened) : null
  })
}
{
  const tampered = wrapped[0].wrapped
  const flipped = (tampered[100] === 'a' ? 'b' : 'a') + tampered.slice(101)
  const epochKey = Buffer.from(wrapped[0].epochKey, 'hex')
  const opened = mnemonic.unwrapVideoKey(flipped, epochKey)
  wrapped.push({ videoKey: wrapped[0].videoKey, epochKey: wrapped[0].epochKey, wrapped: flipped, unwrap: opened ? hex(opened) : null })
}

const basePhrase = identities[0].mnemonic
const base24 = identities[10].mnemonic
const words12 = basePhrase.split(' ')
const badLast = words12.slice(0, -1).concat(words12.at(-1) === 'abandon' ? 'zoo' : 'abandon')
const mnemonicCheck = []
for (const phrase of [basePhrase, base24]) {
  mnemonicCheck.push({ phrase, normalized: mnemonic.normalizeMnemonic(phrase), valid: mnemonic.validateMnemonic(phrase) })
}
for (const phrase of [
  basePhrase,
  `  ${basePhrase}  `,
  basePhrase.toUpperCase(),
  words12.join('   '),
  words12.join('\n\t '),
  `\u00a0${basePhrase}\u00a0`,
  base24.toUpperCase(),
  badLast.join(' '),
  'abandon '.repeat(12).trim(),
  words12.slice(0, 11).join(' '),
  words12.concat('abandon').join(' '),
  'abandon '.repeat(25).trim(),
  'abandon',
  '',
  '   ',
  words12.map((w, i) => (i === 3 ? 'notaword' : w)).join(' '),
  words12.map((w, i) => (i === 11 ? 'zoo' : w)).join(' '),
  base24.split(' ').slice(0, 23).join(' '),
  base24.split(' ').concat('abandon').join(' ')
]) {
  mnemonicCheck.push({ phrase, normalized: mnemonic.normalizeMnemonic(phrase), valid: mnemonic.validateMnemonic(phrase) })
}

const K = hex(sha('fixtures:key'))
const K2 = hex(sha('fixtures:key2'))
const KUP = K.toUpperCase()
const watchBody = `${K}:1:2:0:1048576`
const parseUriInputs = [
  K,
  KUP,
  `  ${K}  `,
  `peartube://channel/${K}`,
  `peartube://channel/${K}?name=My%20Channel&foo=bar`,
  `peartube:channel/${K}?alias=plus+space`,
  `peartube://tracker/${K2}?name=%E2%9C%93`,
  `peartube://add/${K}`,
  `peartube://source/${K}?name=0`,
  `peartube://source/${K}?name=`,
  `peartube://source/${K}?name=%ZZ`,
  `peartube://source/${K}?name=a+b`,
  `peartube://channel/${K}?name=%E2%9C%93+ok`,
  `peartube://channel/${K}?name=a%26b`,
  `peartube://channel/${K}#fragment`,
  `peartube://%63hannel/${K}`,
  `peartube://watch/${watchBody}?title=Hello%20World`,
  `peartube://watch/${KUP}:0:1:0:0?id=clip:abc&title=Hi&sha256=${K2}`,
  `peartube://watch/${K}:0:1:0:0?id=series:show:s01e02&source=${K2}&kind=channel&title=Ep`,
  `peartube://watch/${K}:0:1:0:0?source=${K2}&kind=tracker`,
  `peartube://watch/${K}:0:1:0:0?sha256=zzz&title=`,
  `peartube://watch/${K}:0:1:0:0?id=BAD`,
  `peartube://watch/${K}:0:1:0:0?sha256=${KUP}`,
  `peartube://watch/${K}:0:0:0:0`,
  `peartube://watch/${K}:-1:1:0:0`,
  `peartube://watch/${K}:1:2:0`,
  `peartube://watch/${K}:1:2:0:1:2`,
  `peartube://watch/xyz:1:2:0:1`,
  `peartube://watch/${K}:1:2:0:1.5`,
  `peartube://watch/${K}:1:2:0:1e20`,
  `peartube://watch/${K}:::1:2`,
  `peartube://watch/${K}:1:2:0:3`,
  `peartube://watch/${K}:0:1:0:0?title=${'a'.repeat(310)}`,
  `peartube://watch/${K}:0:1:0:0?title=😀${'a'.repeat(298)}`,
  `peartube://watch/${K}:0:1:0:0?title=%F0%9F%98%80`,
  `peartube://watch/${K}:9007199254740992:1:0:0`,
  `peartube://watch/${K}:0:1:0:0?id=clip:abc&title=${'b'.repeat(299)}😀`,
  `PEARTUBE://WATCH/${K}:0:1:0:0?ID=clip:abc`,
  `peartube://access/${K}:1:${K2}`,
  `peartube://access/${KUP}:0x10:${K2}`,
  `peartube://access/${K}:1e3:${K2}`,
  `peartube://access/${K}:9007199254740991:${K2}`,
  `peartube://access/${K}:9007199254740992:${K2}`,
  `peartube://access/${K}:1:${KUP}`,
  `peartube://access/${K}:0:${K2}`,
  `peartube://access/${K}:1.5:${K2}`,
  `peartube://access/${K}:1:${K2}:extra`,
  `peartube://access/${K}:1`,
  `peartube://access/xyz:1:${K2}`,
  `peartube://receiver/${K}`,
  `peartube://receiver/${KUP}?name=TV&token=abc&p2p=0`,
  `peartube://receiver/${K}?id=x`,
  `peartube://receiver/${K}?name=${'x'.repeat(100)}`,
  `peartube://receiver/living-room-tv?name=TV`,
  `peartube://receiver/xyz?id=abc`,
  `peartube://receiver/!!!?id=abc`,
  `peartube://receiver/living%20room`,
  `peartube://receiver/DEVICE_42`,
  `peartube://receiver/xyz`,
  `peartube://receiver/---`,
  `peartube://receiver/!!!`,
  `peartube://receiver/10.0.0.5:8176`,
  `peartube://receiver/10.0.0.5:8176?host=real.host&port=9000`,
  `peartube://receiver/10.0.0.5:8176?port=2.5`,
  `peartube://receiver/10.0.0.5?port=Infinity`,
  `peartube://receiver/10.0.0.5:8176?port=0x10`,
  `peartube://receiver/host:1?port=-5`,
  `peartube://receiver/host:1?host=&port=2`,
  `peartube://receiver/device?p2p=1&port=0`,
  `peartube://receiver/?pk=${K}`,
  `peartube://receiver/`,
  `peartube://cast/device?name=Living%20Room`,
  `peartube://nope/${K}`,
  `peartube:${K}`,
  '',
  '   ',
  `peartube://channel/${K.slice(0, 63)}`
]

const blob = { blockOffset: 0, blockLength: 1, byteOffset: 0, byteLength: 10 }
const formatWatchUri = []
for (const [entry, source, sourceKind] of [
  [{ id: 'clip:abc', title: 'Hello World', blobs: K, blob }, '', ''],
  [{ id: 'clip:abc', title: 'Hello World', sha256: K2, blobs: K, blob }, K, 'channel'],
  [{ id: 'clip:abc', title: 'Hello World', sha256: KUP, blobs: K, blob }, KUP, 'channel'],
  [{ id: 'clip:abc', title: '', blobs: K, blob }, '', ''],
  [{ id: 'clip:abc', title: 'a'.repeat(310), blobs: K, blob }, '', ''],
  [{ id: 'clip:abc', title: '😀'.repeat(200), blobs: K, blob }, '', ''],
  [{ id: 'clip:abc', title: `${'a'.repeat(299)}😀`, blobs: K, blob }, '', ''],
  [{ id: 'id with space&x=1', title: 'q&a', blobs: K, blob }, '', ''],
  [{ id: 'clip:abc', title: 'x', blobs: KUP, blob }, '', ''],
  [{ id: 'clip:abc', title: 'x', sha256: '', blobs: K, blob }, '', ''],
  [{ id: 'clip:abc', title: 'x', source: K, blobs: K, blob }, K, 'tracker']
]) {
  formatWatchUri.push({ entry, source, sourceKind, value: protocol.formatWatchUri(entry, source, sourceKind) })
}

const formatReceiverUri = []
for (const input of [
  {},
  { publicKey: KUP },
  { deviceId: 'Living-Room' },
  { host: '10.0.0.5', port: 8176 },
  { publicKey: K, deviceId: 'pad', host: 'x', port: 1, name: 'TV', token: 'a b&c', p2p: true },
  { name: '  Living   Room  ' },
  { name: 'x'.repeat(100) },
  { port: -5 },
  { port: 2.5 },
  { host: 'Host', port: 0 },
  { deviceId: 'Pad', p2p: true }
]) {
  formatReceiverUri.push({ input, value: protocol.formatReceiverUri(input) })
}

const K128 = hex(sha('fixtures:claim-sig'))
const K144 = hex(sha('fixtures:wrapped')).repeat(9).slice(0, 144)
const announce = {
  type: 'announce',
  id: 'clip:abc',
  title: 'Title',
  size: 10,
  sha256: K,
  blobs: K2,
  blob
}
const decodeOpInputs = [
  '{"type":"init"}',
  `{"type":"init","owner":"${K}"}`,
  `{"type":"init","owner":"${KUP}"}`,
  '{"type":"init","owner":"zz"}',
  '{"type":"init","epoch":1}',
  '{"type":"init","epoch":0}',
  '{"type":"init","epoch":1.0}',
  '{"type":"init","epoch":1e3}',
  '{"type":"init","epoch":1e400}',
  '{"type":"init","epoch":"1"}',
  `{"type":"init","owner":null}`,
  `{"type":"owner","owner":"${K}"}`,
  '{"type":"owner"}',
  '{"type":"epoch","epoch":3}',
  '{"type":"epoch","epoch":1.5}',
  '{"type":"epoch","epoch":9007199254740991}',
  '{"type":"epoch","epoch":9007199254740992}',
  `{"type":"claim","sig":"${K128}"}`,
  `{"type":"claim","sig":"${K128}","claim":"${K128}"}`,
  '{"type":"claim","sig":"abc"}',
  '{"type":"claim","claim":"zzz"}',
  `{"type":"grant","writer":"${K}","role":"mod"}`,
  `{"type":"grant","writer":"${K}"}`,
  `{"type":"grant","writer":"${K}","role":""}`,
  `{"type":"grant","writer":"${K}","role":"admin"}`,
  `{"type":"grant","writer":"${K}","role":0}`,
  `{"type":"grant","writer":"${K}","role":1}`,
  `{"type":"revoke","writer":"${K}"}`,
  `{"type":"revoke","writer":"${KUP}"}`,
  '{"type":"remove","key":"writer/x"}',
  '{"type":"remove","key":5}',
  `{"type":"source","sourceKey":"${K}","kind":"channel","title":"T"}`,
  `{"type":"source","sourceKey":"${K}","kind":"tracker","title":"T"}`,
  `{"type":"source","sourceKey":"${K}","kind":"bad","title":"T"}`,
  `{"type":"source","sourceKey":"${K}","kind":"channel","title":""}`,
  `{"type":"source","sourceKey":"${K}","kind":"channel","title":"${'t'.repeat(300)}"}`,
  `{"type":"source","sourceKey":"${K}","kind":"channel","title":"${'t'.repeat(301)}"}`,
  JSON.stringify(announce),
  JSON.stringify({ ...announce, epoch: 2, wrappedKey: K144 }),
  JSON.stringify({ ...announce, blob: undefined }),
  JSON.stringify({ ...announce, blob: { ...blob, blockLength: 1.5 } }),
  JSON.stringify({ ...announce, blob: null }),
  JSON.stringify({ ...announce, size: -1 }),
  JSON.stringify({ ...announce, size: 1.5 }),
  '{"type":"announce","id":"clip:abc","title":"T","size":1.0,"sha256":"' + K + '","blobs":"' + K2 + '","blob":{"blockOffset":0,"blockLength":1,"byteOffset":0,"byteLength":10}}',
  JSON.stringify({ ...announce, wrappedKey: K144.slice(0, 142) }),
  JSON.stringify({ ...announce, epoch: 0 }),
  JSON.stringify({ ...announce, title: 't'.repeat(301) }),
  JSON.stringify({ ...announce, title: '😀'.repeat(150) }),
  JSON.stringify({ ...announce, title: '😀'.repeat(151) }),
  JSON.stringify({ ...announce, id: 'CLIP:abc' }),
  JSON.stringify({ ...announce, extra: { n: 1e3, list: [1, 2.0, { x: true }] } }),
  'null',
  '[]',
  '"str"',
  '123',
  'true',
  'nope',
  '{"type":',
  JSON.stringify(announce) + 'garbage'
]
const decodeOp = decodeOpInputs.map(input => ({ input, value: protocol.decodeOp(input) }))

const cleanText = []
for (const [input, fallback] of [
  ['', 'fb'],
  [null, 'fb'],
  ['hello', ''],
  ['a'.repeat(301), ''],
  ['a'.repeat(300), ''],
  ['😀'.repeat(200), ''],
  [`${'a'.repeat(299)}😀`, ''],
  [`${'a'.repeat(298)}😀b`, ''],
  ['   ', 'fb'],
  ['\uFEFFhello\uFEFF', ''],
  ['x', 'a'.repeat(400)],
  ['', '😀'.repeat(200)],
  ['abc', null]
]) {
  cleanText.push({ input, fallback, value: protocol.cleanText(input, fallback) })
}

const cleanName = []
for (const [input, fallback] of [
  ['  Hello  ', ''],
  ['   ', 'fb'],
  ['', 'fb'],
  ['a'.repeat(100), ''],
  ['😀'.repeat(50), ''],
  [`${'a'.repeat(79)}😀`, ''],
  [null, 'PearTube TV'],
  ['x', ''],
  ['\u00a0x\u00a0', '']
]) {
  cleanName.push({ input, fallback, value: protocol.cleanName(input, fallback) })
}

const formatAccessUri = [
  { channel: KUP, epoch: 1, epochKey: K2.toUpperCase() },
  { channel: K, epoch: 0, epochKey: K2 },
  { channel: K, epoch: 1234, epochKey: K }
].map(input => ({ ...input, value: mnemonic.formatAccessUri(input.channel, input.epoch, input.epochKey) }))

const describeEntry = [
  ['series:show:s01e02', 'Episode Title'],
  ['series:show:s01e234', 'Dune (2021)'],
  ['clip:abc', 'Dune 2021'],
  ['clip:abc', '  Dune  (2021) '],
  ['clip:abc', '1917'],
  ['clip:abc', 'Movie (1885)'],
  ['clip:abc', 'A (1999) extra'],
  ['clip:abc', 'A (2021)'],
  ['clip:abc', 'A (2021))'],
  ['show:id:s01e02', ''],
  ['ABC:s01e02', 'x'],
  ['show:s01e02', 'T'],
  ['a:b:s01e2', 'T'],
  ['clip:abc', 'Dune\n(2021)'],
  ['clip:abc', 'Dune\r(2021)'],
  ['clip:abc', '😀 (2020)']
].map(([id, title]) => ({ id, title, value: protocol.describeEntry(id, title) }))

const isUniform512 = [
  null,
  { byteLength: 0, blockLength: 0 },
  { byteLength: 1, blockLength: 1 },
  { byteLength: 524288, blockLength: 1 },
  { byteLength: 524289, blockLength: 1 },
  { byteLength: 524289, blockLength: 2 },
  { byteLength: 1, blockLength: 0 },
  { byteLength: 1048576, blockLength: 2 },
  { byteLength: 1048577, blockLength: 2 }
].map(input => ({ input, value: protocol.isUniform512(input) }))

const isSystemViewKey = ['peer/x', 'source/x', 'meta/x', 'writer/x', 'revoked/x', 'clip/x', '', 'Peer/x'].map(
  key => ({ key, value: protocol.isSystemViewKey(key) })
)

const mediaFile = ['film.mp4', 'FILM.MKV', 'song.flac', 'notes.txt', 'noext', 'a.mp4.txt', 'x.m2ts'].map(
  name => ({ name, value: isMedia(name) })
)

const appDeps = JSON.parse(readFileSync(join(APP, 'package.json'), 'utf8')).dependencies

const fixtures = {
  meta: {
    source: 'fixtures/gen/gen.mjs from the PEARTUBE_APP checkout',
    commit: JS_REF,
    node: process.version,
    hypercore: appDeps.hypercore,
    hypercoreCrypto: appDeps['hypercore-crypto'],
    sodiumNative: appDeps['sodium-native']
  },
  identities,
  claims,
  epochs,
  wrapped,
  mnemonicCheck,
  parseUri: parseUriInputs.map(input => attempt(protocol.parseUri, input)),
  formatWatchUri,
  formatReceiverUri,
  decodeOp,
  cleanText,
  cleanName,
  formatAccessUri,
  describeEntry,
  isUniform512,
  isSystemViewKey,
  mediaFile
}

writeFileSync(outFile('js-reference.json'), JSON.stringify(fixtures, null, 2) + '\n')
console.log(`wrote ${outFile('js-reference.json')}`)
for (const [key, value] of Object.entries(fixtures)) {
  if (Array.isArray(value)) console.log(` ${key}: ${value.length}`)
}
