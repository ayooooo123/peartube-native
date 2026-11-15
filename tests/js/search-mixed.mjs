// Global search (src/search.js) between the real PearTube JS node and this port, on one local
// HyperDHT testnet.
//
// Modes (argv[2]):
//   testnet                         create a testnet, print its bootstrap nodes, stay up
//   relay <bootstrap-json>          a JS relay that holds a Matroska-headed film and a file that is
//                                   not media, and answers searches for the film only; print the
//                                   film's id and SHA-256, stay up
//   search <bootstrap-json> <query> a JS node that follows nothing searches until a row comes back,
//                                   plays the first row, prints the rows and the played SHA-256
//   fetch <url>                     fetch the whole URL and print its SHA-256
//
// Every mode prints one JSON line on stdout and then idles until stdin closes.

import { createRequire } from 'node:module'
import { pathToFileURL, fileURLToPath } from 'node:url'
import { dirname, join, resolve } from 'node:path'
import { mkdtempSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { Readable } from 'node:stream'
import { createHash, randomBytes } from 'node:crypto'

const here = dirname(fileURLToPath(import.meta.url))
const APP = process.env.PEARTUBE_APP || resolve(here, '../../../peartube')
const require = createRequire(resolve(APP, 'package.json'))

const { default: createTestnet } = await import(pathToFileURL(require.resolve('hyperdht/testnet.js')).href)
const { createNode } = await import(pathToFileURL(resolve(APP, 'src/index.js')).href)

const mode = process.argv[2]
const args = process.argv.slice(3)
const tmp = () => mkdtempSync(join(tmpdir(), 'peartube-search-mixed-'))
const out = value => process.stdout.write(JSON.stringify(value) + '\n')
const sha256 = bytes => createHash('sha256').update(bytes).digest('hex')

async function until (fn, ms = 120000) {
  const end = Date.now() + ms
  for (;;) {
    const value = await fn()
    if (value) return value
    if (Date.now() > end) throw new Error('timed out')
    await new Promise(resolve => setTimeout(resolve, 500))
  }
}

function keepAlive (cleanup) {
  process.stdin.resume()
  const stop = async () => {
    await cleanup?.()
    process.exit(0)
  }
  process.stdin.on('end', stop)
  process.stdin.on('close', stop)
}

if (mode === 'testnet') {
  const testnet = await createTestnet(3)
  out({ bootstrap: testnet.bootstrap.map(node => `${node.host}:${node.port}`) })
  keepAlive(() => testnet.destroy())
} else if (mode === 'relay') {
  const node = await createNode({ storage: tmp(), bootstrap: JSON.parse(args[0]) })
  const film = randomBytes(1024 * 1024 + 13)
  film.set([0x1a, 0x45, 0xdf, 0xa3], 0)
  const junk = randomBytes(300 * 1024)
  junk[0] = 0
  const filmOp = await node.put({ id: 'imdb:tt0079944', title: 'Stalker (1979)' }, Readable.from([film]))
  await node.append(filmOp)
  const junkOp = await node.put({ id: 'clip:stalkernotes', title: 'Stalker 1979 notes' }, Readable.from([junk]))
  await node.append(junkOp)
  const media = new Set([`${filmOp.blobs}:${filmOp.blob.blockOffset}`])
  node.serveSearch({ isMedia: key => media.has(key) })
  out({ id: filmOp.id, sha256: sha256(film), peerKey: node.peerKey })
  keepAlive(() => node.close())
} else if (mode === 'search') {
  const node = await createNode({ storage: tmp(), bootstrap: JSON.parse(args[0]) })
  const rows = await until(async () => {
    const found = await node.searchGlobal({ query: args[1] })
    return found.length && found
  })
  const opened = await node.openUri(rows[0].magnetUrl)
  const bytes = Buffer.from(await (await fetch(opened.entry.streamUrl)).arrayBuffer())
  out({ rows, sha256: sha256(bytes) })
  keepAlive(() => node.close())
} else if (mode === 'fetch') {
  const bytes = Buffer.from(await (await fetch(args[0])).arrayBuffer())
  out({ sha256: sha256(bytes), size: bytes.length })
  keepAlive()
} else {
  throw new Error(`unknown mode ${mode}`)
}
