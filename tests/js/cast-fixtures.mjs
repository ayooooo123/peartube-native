// Generates `fixtures/cast-reference.json` from the app's own `src/cast.js`.
//
//   node cast-fixtures.mjs > ../fixtures/cast-reference.json
//
// The app repository is found through PEARTUBE_APP, else as a sibling checkout (`../../../peartube`).
// Pure helpers are taken from the app's source text (they are module-private); command and player
// state transitions come from a real `createReceiver` driven over HTTP, so both sides of every
// fixture are the app's own code.
import { readFileSync, writeFileSync, mkdirSync, rmSync } from 'node:fs'
import { join, resolve } from 'node:path'
import { tmpdir } from 'node:os'
import { fileURLToPath } from 'node:url'

const here = fileURLToPath(new URL('.', import.meta.url))
const APP = process.env.PEARTUBE_APP || resolve(here, '../../../peartube')
const CAST_SRC = readFileSync(join(APP, 'src/cast.js'), 'utf8')

const cast = await import(join(APP, 'src/cast.js'))
const { qrSvg } = await import(join(APP, 'src/qr.js'))

const DEVICE_ID = '0123456789abcdef0123456789abcdef'
const SEED = 'ab'.repeat(32)
const PAIR_TOKEN = 'cd'.repeat(16)
const PAIR_CODE = 'ABC123'
const NAME = 'Kitchen TV'

// Pull one `function name (...) { ... }` out of the module source by brace matching.
function extract (name) {
  const start = CAST_SRC.indexOf(`function ${name} (`)
  if (start === -1) throw new Error(`missing ${name}`)
  const open = CAST_SRC.indexOf('{', start)
  let depth = 0
  for (let i = open; i < CAST_SRC.length; i++) {
    if (CAST_SRC[i] === '{') depth++
    else if (CAST_SRC[i] === '}') {
      depth--
      if (depth === 0) return CAST_SRC.slice(start, i + 1)
    }
  }
  throw new Error(`unbalanced ${name}`)
}

const HEX64 = /^[0-9a-f]{64}$/
const RECEIVER_PORT = 8176
const parseHttpRange = new Function(`${extract('parseHttpRange')}; return parseHttpRange`)()
const guessMediaMime = new Function(`${extract('guessMediaMime')}; return guessMediaMime`)()
const receiverAddress = new Function('HEX64', 'RECEIVER_PORT', `${extract('receiverAddress')}; return receiverAddress`)(HEX64, RECEIVER_PORT)

function mask (value) {
  if (Array.isArray(value)) return value.map(mask)
  if (value && typeof value === 'object') {
    const out = {}
    for (const [key, item] of Object.entries(value)) {
      if (key === 'updatedAt' || key === 'storageFreeBytes') out[key] = 0
      else out[key] = mask(item)
    }
    return out
  }
  return value
}

function maskPorts (value, port) {
  const text = JSON.stringify(value)
    .replaceAll(`:${port}`, ':0')
    .replaceAll(`port=${port}`, 'port=0')
  return JSON.parse(text)
}

function tempDir (label) {
  const dir = join(tmpdir(), `peartube-cast-fixture-${label}-${process.pid}`)
  rmSync(dir, { recursive: true, force: true })
  mkdirSync(dir, { recursive: true, mode: 0o700 })
  return dir
}

function seedIdentity (dir) {
  const identity = { deviceId: DEVICE_ID, name: NAME, seed: SEED, pairToken: PAIR_TOKEN, pairCode: PAIR_CODE }
  writeFileSync(join(dir, 'receiver.json'), JSON.stringify(identity, null, 2), { mode: 0o600 })
  return identity
}

async function post (port, path, body) {
  const res = await fetch(`http://127.0.0.1:${port}${path}`, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify(body)
  })
  const text = await res.text()
  return { status: res.status, body: text ? JSON.parse(text) : {} }
}

async function get (port, path) {
  const res = await fetch(`http://127.0.0.1:${port}${path}`)
  const text = await res.text()
  return { status: res.status, body: text ? JSON.parse(text) : {} }
}

const storage = tempDir('receiver')
seedIdentity(storage)
const receiver = await cast.createReceiver({
  storage,
  name: NAME,
  port: 0,
  bindHost: '127.0.0.1',
  openAuth: true,
  anyPort: true
})
const port = receiver.port

// --- pure helpers -------------------------------------------------------------------------
const pairCodes = [' abc-123 ', 'peer123', 'ZzZ9', '!!', '', 'a1 b2-c3_d4'].map(input => ({
  input,
  output: cast.normalizePairCode(input)
}))

const idle = cast.idleCastState()
const idleFixture = { keys: Object.keys(idle), state: mask(idle) }

const rangeHeaders = [
  null, 'bytes=0-9', 'bytes=5-', 'bytes=-3', 'bytes=0-0', 'bytes=99-100',
  'bytes=10-', 'bytes=5-4', 'bytes=-0', 'bytes=abc', 'bytes=1-2,5-6', 'items=0-1', 'bytes='
]
const ranges = rangeHeaders.map(header => ({
  header,
  total: 10,
  result: parseHttpRange(header, 10)
}))

const mime = [
  { title: 'Film.WEBM' },
  { title: 'Film', id: 'clip.mkv' },
  { id: 'show.avi' },
  { title: 'Film' },
  { title: null, id: null }
].map(entry => ({ entry, mime: guessMediaMime(entry) }))

const addressInputs = [
  'a'.repeat(64),
  '192.168.1.5',
  'http://tv.local:9000/tv',
  'TV.LOCAL:1234',
  '',
  ':1234',
  ' 10.0.0.2 '
]
const addresses = addressInputs.map(input => {
  try {
    return { input, ok: true, value: receiverAddress(input) }
  } catch (err) {
    return { input, ok: false, error: err.message }
  }
})

const qrTexts = [
  'x',
  'peartube://receiver/' + 'b'.repeat(64) + '?name=Kitchen%20TV&token=' + PAIR_TOKEN + '&host=192.168.1.5&port=8176&p2p=1',
  'peartube://receiver/0123456789abcdef?name=PearTube%20TV&p2p=0',
  'y'.repeat(300)
]
const qr = qrTexts.map(text => ({ text, svg: qrSvg(text) }))

// --- command transitions ------------------------------------------------------------------
const commandScripts = []
{
  const commands = [
    { action: 'status' },
    { action: 'play' },
    { action: 'pause' },
    { action: 'togglePlay' },
    { action: 'seek', positionMs: 4200 },
    { action: 'skip', deltaMs: -1000 },
    { action: 'selectAudio', stream: 2 },
    { action: 'selectSubtitle', stream: -1 },
    { action: 'volume', volume: 0.35, muted: true },
    { action: 'volume', volume: 2 }
  ]
  const states = []
  for (const command of commands) {
    const res = await post(port, '/cast/v1/command', command)
    states.push({ status: res.status, state: mask(res.body.state), error: res.body.error || null })
  }
  commandScripts.push({ name: 'transport', commands, states })
}
{
  const commands = [{ action: 'teleport' }, { action: '' }]
  const states = []
  for (const command of commands) {
    const res = await post(port, '/cast/v1/command', command)
    states.push({ status: res.status, state: null, error: res.body.error || null })
  }
  commandScripts.push({ name: 'errors', commands, states })
}

// --- player reports -----------------------------------------------------------------------
const reportScripts = []
{
  const patches = [
    { playing: true, positionMs: 1000 },
    { buffering: true, playing: true },
    { playing: true, buffering: false, positionMs: 5000, durationMs: 60000, tracks: [{ id: 1 }], audio: 1, subtitle: -1, videoDecoder: 'h264', droppedFrames: 3 },
    { error: 'Media error 4' },
    { playing: false, positionMs: 9000, durationMs: null },
    { idle: true, status: 'idle', entry: null, playing: false, buffering: false, positionMs: 0, durationMs: null, ended: true }
  ]
  const states = patches.map(patch => mask(receiver.reportState(patch)))
  reportScripts.push({ name: 'player', patches, states })
}

// --- info ---------------------------------------------------------------------------------
const infoBody = (await get(port, '/cast/v1/info')).body
const info = maskPorts(mask(infoBody), port)
const webInfoBody = (await get(port, '/cast/v1/web-info')).body
const webInfo = maskPorts(mask({ ...webInfoBody, qrSvg: '', webToken: '' }), port)
const pairCode = receiver.info().pairCode
const pairingUri = maskPorts({ uri: receiver.info().pairingUri }, port).uri

// --- pairing via a real controller --------------------------------------------------------
const controllerStorage = tempDir('controller')
const controller = cast.createCastController({
  storage: controllerStorage,
  getNode: () => null,
  ensureNode: null,
  getLanHost: () => null,
  apiPort: 8174
})
const record = await controller.pairReceiver({ host: '127.0.0.1', port, code: PAIR_CODE })
const saved = JSON.parse(readFileSync(join(controllerStorage, 'cast-receivers.json'), 'utf8'))
const listing = controller.listReceivers()
const pairing = {
  code: PAIR_CODE,
  record: maskPorts(record, port),
  saved: maskPorts(saved, port),
  listing: maskPorts(listing, port)
}

await controller.close()
await receiver.close()
rmSync(storage, { recursive: true, force: true })
rmSync(controllerStorage, { recursive: true, force: true })

const fixture = {
  pairCodes,
  idle: idleFixture,
  ranges,
  mime,
  addresses,
  qr,
  commandScripts,
  reportScripts,
  info,
  webInfo,
  pairCode,
  pairingUri,
  pairing
}
process.stdout.write(JSON.stringify(fixture, null, 2) + '\n')
