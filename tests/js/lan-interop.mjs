// The app's real node (`src/index.js` + `src/lan.js`) beside the Rust LAN port, with no DHT
// reachable: both sides bootstrap to 127.0.0.1:9, so every byte must travel over the isolated
// LAN HyperDHT that mDNS introduced them to.
//
//   node lan-interop.mjs publish <lanHost> <size> <id> <title>
//       create a node whose only discovery is LAN, publish random bytes, print `ready`, stay up
//   node lan-interop.mjs follow <lanHost> <channel> <id> <sha256> <size>
//       follow a channel over LAN only, wait for the entry, stream its blob through the node's
//       own range server, print `result`, close
//
// Both modes print one JSON line, then either idle until stdin closes (publish) or exit (follow).
// Errors print `{"t":"error",...}` and exit non-zero.

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
createRequire(resolve(APP, 'package.json'))
const { createNode } = await import(pathToFileURL(resolve(APP, 'src/index.js')).href)
const { createLan, interfaceAdapter } = await import(pathToFileURL(resolve(APP, 'src/lan.js')).href)

const DEAD_BOOTSTRAP = [{ host: '127.0.0.1', port: 9 }]
const tmp = () => mkdtempSync(join(tmpdir(), 'peartube-lan-'))
const sha256 = bytes => createHash('sha256').update(bytes).digest('hex')

function out (value) {
  process.stdout.write(JSON.stringify(value) + '\n')
}

async function until (fn, ms = 120_000) {
  const end = Date.now() + ms
  for (;;) {
    const value = await fn()
    if (value) return value
    if (Date.now() > end) throw new Error('timed out')
    await new Promise(resolve => setTimeout(resolve, 250))
  }
}

function lanNode (host) {
  return createNode({
    storage: tmp(),
    bootstrap: DEAD_BOOTSTRAP,
    lan: keyPair => createLan({ host, keyPair, adapter: interfaceAdapter(host) })
  })
}

function keepAlive (node) {
  const stop = () => node.close().finally(() => process.exit(0))
  process.stdin.resume()
  process.stdin.on('end', stop)
  process.stdin.on('close', stop)
}

const args = process.argv.slice(3)
const mode = process.argv[2]

try {
  if (mode === 'publish') {
    const [host, sizeText, id, title] = args
    const bytes = randomBytes(Number(sizeText))
    const node = await lanNode(host)
    const published = await node.publish({ id, title }, Readable.from([bytes]))
    out({
      t: 'ready',
      peerKey: node.peerKey,
      channel: published.channel,
      key: published.key,
      blobs: published.blobs,
      entry: published,
      sha256: sha256(bytes),
      size: bytes.length,
      lanStats: node.lanStats(),
      lanEndpoint: node.lanEndpoint()
    })
    keepAlive(node)
  } else if (mode === 'follow') {
    const [host, channel, id, expected, sizeText] = args
    const node = await lanNode(host)
    await node.addSource({ key: channel, kind: 'channel' })
    const hit = await until(async () => {
      const list = await node.search(id)
      return list.find(item => item.id === id) || null
    })
    out({ t: 'entry', entry: hit, lanStats: node.lanStats() })
    const response = await fetch(hit.streamUrl)
    const bytes = Buffer.from(await response.arrayBuffer())
    const status = node.status()
    out({
      t: 'result',
      httpStatus: response.status,
      sha256: sha256(bytes),
      size: bytes.length,
      expected,
      expectedSize: Number(sizeText),
      lanStats: node.lanStats(),
      peers: status.peers,
      lanPeers: status.lanPeers,
      lanRxBytes: status.routes.lan.rxBytes
    })
    await node.close()
    process.exit(0)
  } else {
    throw new Error('unknown mode: ' + mode)
  }
} catch (err) {
  out({ t: 'error', error: err?.stack || String(err) })
  process.exit(1)
}
