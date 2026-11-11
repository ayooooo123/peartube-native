// Generates `fixtures/cast-reference.json` from the app's own `src/cast.js` and `src/qr.js`.
//
//   node cast-fixtures.mjs > ../fixtures/cast-reference.json
//
// The app repository is found through PEARTUBE_APP, else as a sibling checkout (`../../../peartube`).
// Pure helpers are taken from the app's source text where they are module-private. Command and
// player transitions need a node and the control channel, so they are covered by the live
// JS↔Rust interop tests instead of a fixture.
import { readFileSync } from 'node:fs'
import { join, resolve } from 'node:path'
import { fileURLToPath } from 'node:url'

const here = fileURLToPath(new URL('.', import.meta.url))
const APP = process.env.PEARTUBE_APP || resolve(here, '../../../peartube')
const CAST_SRC = readFileSync(join(APP, 'src/cast.js'), 'utf8')

const cast = await import(join(APP, 'src/cast.js'))
const { qrSvg } = await import(join(APP, 'src/qr.js'))

const PAIR_TOKEN = 'cd'.repeat(16)

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

const parseHttpRange = new Function(`${extract('parseHttpRange')}; return parseHttpRange`)()
const guessMediaMime = new Function(`${extract('guessMediaMime')}; return guessMediaMime`)()

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

const qrTexts = [
  'x',
  'peartube://receiver/' + 'b'.repeat(64) + '?name=Kitchen%20TV&token=' + PAIR_TOKEN + '&p2p=1',
  'peartube://receiver/0123456789abcdef?name=PearTube%20TV&p2p=0',
  'y'.repeat(300)
]
const qr = qrTexts.map(text => ({ text, svg: qrSvg(text) }))

const reference = { pairCodes, idle: idleFixture, ranges, mime, qr }
process.stdout.write(JSON.stringify(reference, null, 2) + '\n')
