// Generate wire bytes with the app's installed codecs; never reproduce their framing here.
import assert from 'node:assert/strict'
import { load } from './app.mjs'

const { encode, decode } = await load('spec/hyperschema/index.js')
const { SEARCH, CAST, GUARD } = await load('spec/protocols.js')
const max = Number.MAX_SAFE_INTEGER
const zero = n => Buffer.alloc(n)
const fixed = n => Buffer.from(Array.from({ length: n }, (_, i) => i))
const x = Buffer.from('x')
const entry = { key: 'k', id: 'i', title: 't', size: max, sha256: 'h' }
const track = { stream: -1, kind: 'audio', codec: 'a', language: '', title: '' }
const command = { token: '', action: 'volume', entry: null, magnetUrl: '', startPositionMs: 0, positionMs: 0, deltaMs: -1, stream: -1, volume: 0.5, muted: -1 }
const idle = { tracks: [], status: 'idle', mode: '', entry: null, positionMs: 0, durationMs: 0, playing: false, buffering: false, ended: false, p2pPeers: 0, cachedBytes: 0, seeding: false, volume: 1, muted: false, audio: -2, subtitle: -2, error: '', updatedAt: 0, videoDecoder: '', droppedFrames: -1 }
const record = { seq: max, signature: fixed(64), value: x }
const cases = []
function add (name, value) {
  const type = `@peartube/${name}`
  const bytes = encode(type, value)
  assert.deepEqual(decode(type, bytes), value, type)
  cases.push({ name, hex: bytes.toString('hex') })
  return bytes
}
const err = add('remote-error', { code: 'HANDLER_ERROR', message: 'bad' })
add('search-query', { q: '', mid: '', limit: max })
add('search-hit', { uri: 'x' })
add('search-hits', { hits: [] })
add('search-hits', { hits: [{ uri: 'x' }] })
add('cast-pair', { code: 'c' })
add('cast-paired', { deviceId: 'd', name: 'n', publicKey: fixed(32), token: '' })
add('cast-subscribe', { token: '' })
add('cast-entry', entry)
add('cast-track', track)
add('cast-command', command)
add('cast-command', { ...command, entry, deltaMs: -(2 ** 52), stream: 2 ** 52 - 1 })
add('cast-state', idle)
add('cast-state', { ...idle, tracks: [track], entry, playing: true, buffering: true, ended: true, muted: true, seeding: true, volume: 0.5, updatedAt: max })
const hello = add('guard-hello', { role: 'hop' })
const ack = add('guard-ack', { status: 1 })
add('guard-register', { routeToken: fixed(32), ttlMs: max })
add('guard-key', { key: zero(32) })
add('guard-put-mutable', { publicKey: fixed(32), ...record })
add('guard-record', record)
add('guard-get-result', { record: null })
add('guard-get-result', { record })
add('guard-rv-register', { topic: fixed(32), slotId: fixed(16), ttlMs: 0, sealedCard: x })
add('guard-rv-slot', { topic: fixed(32), slotId: fixed(16) })
add('guard-cards', { cards: [Buffer.from('a'), Buffer.from('b')] })
add('guard-dial', { guardKey: fixed(32), routeToken: zero(32), udxId: max, bucket: 'b' })
add('guard-open', { routeToken: fixed(32), udxId: 0 })
add('guard-ready', { status: 2, udxId: 0 })
const dead = add('guard-circuit-dead', { udxId: 42 })
for (const [kind, op, id, value] of [[0, 0, 7, hello], [1, 0, 7, ack], [2, 0, 7, err], [3, 11, 0, dead]]) add('frame', { kind, op, id, value })
console.log(JSON.stringify({ cases, protocols: [SEARCH, CAST, GUARD] }, null, 2))
