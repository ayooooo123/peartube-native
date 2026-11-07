// The real PearTube JS node beside the Rust node, on one local HyperDHT testnet.
//
// Modes (argv[2]):
//   testnet              create a testnet, print its bootstrap nodes, stay up
//   publish <size> <id> <title> <bootstrap-json>
//                        create a node on the given testnet, publish random bytes, print the
//                        announce, its stream URL and SHA-256, stay up
//   follow <bootstrap-json> <channel-hex> <id>
//                        follow a channel until `id` is listed, print the search JSON, stay up
//   fetch <url>          fetch the whole URL and print its SHA-256
//
// Every mode prints one JSON line on stdout and then idle until stdin closes.

import { createRequire } from 'node:module'
import { pathToFileURL } from 'node:url'
import { dirname, join, resolve } from 'node:path'
import { fileURLToPath } from 'node:url'
import { mkdtempSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { Readable } from 'node:stream'
import { createHash, randomBytes } from 'node:crypto'

const here = dirname(fileURLToPath(import.meta.url))
const APP = process.env.PEARTUBE_APP || resolve(here, '../../../../peartube')
const require = createRequire(resolve(APP, 'package.json'))

const { default: createTestnet } = await import(
  pathToFileURL(require.resolve('hyperdht/testnet.js')).href
)
const { createNode } = await import(pathToFileURL(resolve(APP, 'src/index.js')).href)

const args = process.argv.slice(3)
const mode = process.argv[2]
const tmp = () => mkdtempSync(join(tmpdir(), 'peartube-mixed-'))

function out (value) {
  process.stdout.write(JSON.stringify(value) + '\n')
}

function sha256 (bytes) {
  return createHash('sha256').update(bytes).digest('hex')
}

// The start, middle and end of the published file, as the bytes and the hash a reader must get.
function rangeCases (bytes) {
  const size = bytes.length
  const span = Math.min(500_001, size)
  const mid = Math.floor((size - span) / 2)
  const end = Math.max(size - span, 0)
  return [[0, span - 1], [mid, mid + span - 1], [end, size - 1]].map(([start, last]) => ({
    start,
    end: last,
    length: last - start + 1,
    sha256: sha256(bytes.subarray(start, last + 1))
  }))
}

async function until (fn, ms = 120000) {
  const end = Date.now() + ms
  for (;;) {
    const value = await fn()
    if (value) return value
    if (Date.now() > end) throw new Error('timed out')
    await new Promise(resolve => setTimeout(resolve, 250))
  }
}

function keepAlive (cleanup) {
  process.stdin.resume()
  process.stdin.on('end', async () => {
    await cleanup?.()
    process.exit(0)
  })
  process.stdin.on('close', async () => {
    await cleanup?.()
    process.exit(0)
  })
}

if (mode === 'testnet') {
  const testnet = await createTestnet(3)
  out({ bootstrap: testnet.bootstrap })
  keepAlive(() => testnet.destroy())
} else if (mode === 'publish') {
  const [sizeText, id, title, bootstrapJson] = args
  const bootstrap = JSON.parse(bootstrapJson)
  const size = Number(sizeText)
  const bytes = randomBytes(size)
  const node = await createNode({ storage: tmp(), bootstrap })
  const published = await node.publish({ id, title }, Readable.from([bytes]))
  out({
    channel: published.channel,
    writer: published.writer,
    key: published.key,
    entry: published,
    sha256: sha256(bytes),
    size,
    ranges: rangeCases(bytes),
    streamUrl: published.streamUrl,
    entries: await node.search(null, {})
  })
  keepAlive(() => node.close())
} else if (mode === 'follow') {
  const [bootstrapJson, channel, id] = args
  const bootstrap = JSON.parse(bootstrapJson)
  const node = await createNode({ storage: tmp(), bootstrap, tracker: channel })
  const hit = await until(async () => {
    const list = await node.search(id || null)
    const found = id ? list.find(item => item.id === id) : list[0]
    return found || null
  })
  out({ entry: hit, entries: await node.search(null, {}) })
  keepAlive(() => node.close())
} else if (mode === 'search') {
  const [bootstrapJson, channel, id] = args
  const bootstrap = JSON.parse(bootstrapJson)
  const node = await createNode({ storage: tmp(), bootstrap, tracker: channel })
  const list = await until(async () => {
    const found = await node.search(id || null)
    return found.length ? found : null
  })
  out({ entries: list })
  keepAlive(() => node.close())
} else if (mode === 'verify') {
  const [bootstrapJson, channel, id] = args
  const bootstrap = JSON.parse(bootstrapJson)
  const node = await createNode({ storage: tmp(), bootstrap, tracker: channel })
  const hit = await until(async () => {
    const list = await node.search(id)
    return list.find(item => item.id === id) || null
  })
  const res = await fetch(hit.streamUrl)
  const bytes = Buffer.from(await res.arrayBuffer())
  out({ entry: hit, sha256: sha256(bytes), size: bytes.length, status: res.status })
  keepAlive(() => node.close())
} else if (mode === 'fetch') {
  const [url] = args
  const res = await fetch(url)
  const bytes = Buffer.from(await res.arrayBuffer())
  out({ status: res.status, sha256: sha256(bytes), size: bytes.length })
} else {
  throw new Error('unknown mode: ' + mode)
}
