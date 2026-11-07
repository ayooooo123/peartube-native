// The real PearTube JS node beside the Rust node, on a local HyperDHT testnet, driven by
// line-delimited JSON commands so one process owns every JS node of a scenario.
//
// Run: node tests/js/network-mixed.mjs serve [testnet-size]
// Startup prints one line: {"bootstrap": [...]}. Then each stdin line is one command
// `{"id": N, "op": "...", ...}` and each stdout line is `{"id": N, "ok": true, "result": ...}`
// or `{"id": N, "ok": false, "error": "..."}`. Closing stdin stops the process.
//
// Ops mirror the app's own API so a Rust test asserts the same things the JS tests assert.

import { createRequire } from 'node:module'
import { pathToFileURL } from 'node:url'
import { dirname, join, resolve } from 'node:path'
import { fileURLToPath } from 'node:url'
import { mkdtempSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { basename } from 'node:path'
import { createServer } from 'node:http'
import { createHash, randomBytes } from 'node:crypto'
import { Readable } from 'node:stream'

const here = dirname(fileURLToPath(import.meta.url))
const APP = process.env.PEARTUBE_APP || resolve(here, '../../../../peartube')
const require = createRequire(resolve(APP, 'package.json'))

const { default: createTestnet } = await import(
  pathToFileURL(require.resolve('hyperdht/testnet.js')).href
)
const { createNode } = await import(pathToFileURL(resolve(APP, 'src/index.js')).href)
const { createAcquirer } = await import(
  pathToFileURL(process.env.PEARTUBE_ACQUIRE || resolve(APP, 'src/acquire.js')).href
)

const mode = process.argv[2]
const tmp = () => mkdtempSync(join(tmpdir(), 'peartube-mixed-'))
const sha256 = bytes => createHash('sha256').update(bytes).digest('hex')

function out (value) {
  process.stdout.write(JSON.stringify(value) + '\n')
}

const nodes = new Map()
const acquirers = new Map()
const testnets = []
const servers = new Map()
const guards = []
const hitsLog = new Map()
const bytesLog = new Map()
/// A resilient source's live counters: the ones returned at creation are a stale snapshot.
const statsLog = new Map()

/// The SHA-256 of any slice of a fixture this harness generated, so the other side can compare
/// bytes it never held.
function hashSlice ({ name, start, end }) {
  const bytes = bytesLog.get(name)
  if (!bytes) throw new Error('unknown fixture: ' + name)
  const last = end === undefined ? bytes.length - 1 : end
  return { sha256: sha256(bytes.subarray(start, last + 1)), length: last - start + 1 }
}

async function node (name) {
  const value = nodes.get(name)
  if (!value) throw new Error('unknown node: ' + name)
  return value
}

// The start, middle and end of a published file, as the hash a reader must get for each.
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

function listen (server) {
  return new Promise((resolveListen, reject) => {
    server.once('error', reject)
    server.listen(0, '127.0.0.1', () => resolveListen(server.address().port))
  })
}

async function fetchRange (url, range) {
  const options = { signal: AbortSignal.timeout(180_000) }
  if (range) options.headers = { Range: range }
  const res = await fetch(url, options)
  const bytes = Buffer.from(await res.arrayBuffer())
  return {
    status: res.status,
    sha256: sha256(bytes),
    size: bytes.length,
    // Small bodies are compared byte for byte by the caller.
    bytesHex: bytes.length <= 64 * 1024 ? bytes.toString('hex') : undefined
  }
}

const ops = {
  async testnet ({ size = 3 } = {}) {
    const testnet = await createTestnet(size)
    testnets.push(testnet)
    return { bootstrap: testnet.bootstrap }
  },

  async node ({ name, storage, bootstrap, tracker = null, mnemonic = null, join = false, privateRouting = false, worklet = false }) {
    const opened = await createNode({
      storage: storage || tmp(),
      ...(bootstrap ? { bootstrap } : {}),
      ...(tracker ? { tracker } : {}),
      ...(mnemonic ? { mnemonic } : {}),
      ...(join ? { join } : {}),
      ...(privateRouting ? { privateRouting } : {}),
      ...(worklet ? { worklet } : {})
    })
    nodes.set(name, opened)
    const status = opened.status()
    return {
      channel: status.channel,
      writer: status.writer,
      tracker: status.tracker,
      blobs: status.blobs,
      writable: status.writable,
      privateRouting: opened.privateRouting
    }
  },

  async closeNode ({ name }) {
    const value = await node(name)
    await value.close()
    nodes.delete(name)
    return { closed: true }
  },

  async status ({ name }) {
    return (await node(name)).status()
  },

  async identity ({ name, revealMnemonic = false }) {
    return (await node(name)).identity({ revealMnemonic })
  },

  async publish ({ name, id, title, size, chunkSize = 0 }) {
    const value = await node(name)
    const bytes = randomBytes(size)
    bytesLog.set(name, bytes)
    const payload = chunkSize > 0
      ? Readable.from(Array.from({ length: Math.ceil(bytes.length / chunkSize) }, (_, i) => bytes.subarray(i * chunkSize, (i + 1) * chunkSize)))
      : Readable.from([bytes])
    const published = await value.publish({ id, title }, payload)
    return {
      entry: published,
      key: published.key,
      channel: published.channel,
      writer: published.writer,
      streamUrl: published.streamUrl,
      sha256: sha256(bytes),
      size,
      blob: published.blob,
      blobs: published.blobs,
      ranges: rangeCases(bytes)
    }
  },

  async search ({ name, id = null }) {
    const value = await node(name)
    return { entries: await value.search(id, {}) }
  },

  async findEntry ({ name, key }) {
    return { entry: await (await node(name)).findEntry(key) }
  },

  async fetch ({ url, range = null }) {
    return fetchRange(url, range)
  },

  async grant ({ name, writer, role = 'writer', alias = '' }) {
    return (await node(name)).grantWriter(writer, { role, alias })
  },

  async revoke ({ name, writer, purgeEntries = false }) {
    return (await node(name)).revokeWriter(writer, { purgeEntries })
  },

  async remove ({ name, key }) {
    await (await node(name)).remove(key)
    return { removed: true }
  },

  // The app's raw optimistic append: `channel.append(bytes, { optimistic })`.
  async rawAppend ({ name, text, optimistic = true }) {
    const value = await node(name)
    await value.channel.append(Buffer.from(text), { optimistic })
    return { appended: true }
  },

  async channel ({ name }) {
    const value = await node(name)
    return {
      closed: Boolean(value.channel.closed),
      writable: Boolean(value.channel.writable),
      length: value.channel.local.length
    }
  },

  async clearCache ({ name }) {
    return (await node(name)).clearCache()
  },

  async gcStorage ({ name }) {
    return (await node(name)).gcStorage()
  },

  async evictBehind ({ name, blobs, blob, playheadByte, keepBehindBytes }) {
    const reclaimed = await (await node(name)).evictBehind(blobs, blob, { playheadByte, keepBehindBytes })
    return { reclaimed }
  },

  async diskBytes ({ dir }) {
    const { readdirSync, statSync } = await import('node:fs')
    const walk = folder => readdirSync(folder, { withFileTypes: true }).reduce((sum, f) => {
      const path = join(folder, f.name)
      return sum + (f.isDirectory() ? walk(path) : statSync(path).size)
    }, 0)
    return { bytes: walk(dir), names: readdirSync(dir), blobs: readdirSync(dir).filter(f => f.endsWith('.blob')).sort() }
  },

  async swarmStatus ({ name, key }) {
    const value = await node(name)
    return { status: value.swarm.status(Buffer.from(key, 'hex')) }
  },

  async exposure ({ name }) {
    const value = await node(name)
    return { report: value.swarm.dht.privateRouting?.exposureReport?.() ?? null }
  },

  // A plain HTTP source: the bytes, or a 403 unless the caller sends the expected Authorization.
  async httpSource ({ name, size, authorization = null, delayMs = 0 }) {
    const bytes = randomBytes(size)
    bytesLog.set(name, bytes)
    const hits = []
    hitsLog.set(name, hits)
    const server = createServer((req, res) => {
      hits.push({ url: req.url, authorization: req.headers.authorization || null, range: req.headers.range || null })
      const send = () => {
        if (authorization !== null && req.headers.authorization !== authorization) {
          res.writeHead(403)
          res.end('forbidden')
          return
        }
        res.end(bytes)
      }
      if (delayMs > 0) setTimeout(send, delayMs)
      else send()
    })
    const port = await listen(server)
    servers.set(name, server)
    return { url: `http://127.0.0.1:${port}/file`, sha256: sha256(bytes), size }
  },

  async httpHits ({ name }) {
    return { hits: hitsLog.get(name) || [] }
  },

  /// A resilient source's counters *now*; the object returned at creation is a snapshot.
  async sourceStats ({ name }) {
    const stats = statsLog.get(name)
    if (!stats) throw new Error('unknown source: ' + name)
    return { ...stats, ranges: [...stats.ranges] }
  },

  // One good file and one that dies mid-transfer with a longer content-length.
  async brokenSource ({ name, size }) {
    const good = randomBytes(size)
    const server = createServer((req, res) => {
      if (req.url === '/broken') {
        res.writeHead(200, { 'content-length': String(10 * 1024 * 1024) })
        res.write(randomBytes(64 * 1024), () => res.socket.destroy())
      } else {
        res.end(good)
      }
    })
    const port = await listen(server)
    servers.set(name, server)
    return {
      brokenUrl: `http://127.0.0.1:${port}/broken`,
      goodUrl: `http://127.0.0.1:${port}/good`,
      sha256: sha256(good),
      size
    }
  },

  // The CDN and redirecting gateway from the app's own acquirer test: cross-origin auth stripped,
  // a header-less start, a stall, a 502 and a slow seek, so the acquirer must Range-resume.
  async resilientSource ({ name, size }) {
    const bytes = randomBytes(size)
    bytesLog.set(name, bytes)
    const stats = { cdnHits: 0, forbiddenHits: 0, authLeaks: 0, ranges: [] }
    const cdn = createServer((req, res) => {
      if (req.headers.authorization) stats.authLeaks++
      if (req.headers.authorization || !String(req.headers['user-agent'] || '').startsWith('VLC/')) {
        res.writeHead(403)
        res.end('forbidden')
        return
      }
      stats.cdnHits++
      stats.ranges.push(req.headers.range || null)
      if (stats.cdnHits === 1) {
        res.writeHead(200, { 'content-length': String(bytes.length), 'accept-ranges': 'bytes', etag: '"v1"' })
        res.write(bytes.subarray(0, 700 * 1024), () => res.socket.destroy())
        return
      }
      const match = String(req.headers.range || '').match(/^bytes=(\d+)-$/)
      if (!match) {
        res.writeHead(400)
        res.end('range required')
        return
      }
      const start = Number(match[1])
      if (stats.cdnHits === 2) return // Hang before headers arrive.
      if (stats.cdnHits === 3) {
        res.writeHead(502)
        res.end('upstream hiccup')
        return
      }
      res.writeHead(206, {
        'content-length': String(bytes.length - start),
        'content-range': `bytes ${start}-${bytes.length - 1}/${bytes.length}`,
        etag: '"v1"'
      })
      res.flushHeaders()
      if (stats.cdnHits === 4) {
        const timer = setTimeout(() => { if (!res.destroyed) res.write(bytes.subarray(start, start + 400 * 1024)) }, 260)
        timer.unref?.()
        return
      }
      res.end(bytes.subarray(start))
    })
    const cdnPort = await listen(cdn)
    servers.set(name + '-cdn', cdn)
    const gateway = createServer((req, res) => {
      if (req.url === '/forbidden') {
        stats.forbiddenHits++
        res.writeHead(403)
        res.end('denied')
        return
      }
      res.writeHead(302, { location: `http://127.0.0.1:${cdnPort}/video.mkv` })
      res.end()
    })
    const gatewayPort = await listen(gateway)
    servers.set(name + '-gateway', gateway)
    statsLog.set(name, stats)
    return {
      sourceUrl: `http://127.0.0.1:${gatewayPort}/source`,
      forbiddenUrl: `http://127.0.0.1:${gatewayPort}/forbidden`,
      sha256: sha256(bytes),
      size,
      stats
    }
  },

  async acquirer ({ name, node: nodeName, file, concurrency, retryDelay, stallTimeout, defaultChannel = false }) {
    const value = await node(nodeName)
    const acquirer = createAcquirer(value, file, {
      ...(concurrency === undefined ? {} : { concurrency }),
      ...(retryDelay === undefined ? {} : { retryDelay }),
      ...(stallTimeout === undefined ? {} : { stallTimeout }),
      defaultChannel
    })
    acquirers.set(name, { acquirer, node: nodeName })
    return { ready: true }
  },

  async acquireAdd ({ name, id, title, url, headers = null, channel = false }) {
    const { acquirer } = acquirers.get(name) || {}
    if (!acquirer) throw new Error('unknown acquirer: ' + name)
    return acquirer.add({ id, title, source: { url, ...(headers ? { headers } : {}) }, channel })
  },

  async acquireGet ({ name, jobId }) {
    return acquirers.get(name).acquirer.get(jobId)
  },

  async acquireList ({ name }) {
    return { jobs: acquirers.get(name).acquirer.list() }
  },

  async acquireClose ({ name }) {
    acquirers.get(name)?.acquirer.close()
    acquirers.delete(name)
    return { closed: true }
  },

  async apiEntries ({ name }) {
    const value = await node(name)
    const { createApi } = await import(pathToFileURL(resolve(APP, 'src/http.js')).href)
    const api = createApi({ node: value, acquirer: null })
    const port = await listen(api)
    try {
      const listed = await (await fetch(`http://127.0.0.1:${port}/v1/entries`)).json()
      return { results: listed.results }
    } finally {
      api.closeAllConnections?.()
      api.close()
    }
  },

  async hashSlice ({ name, start, end }) {
    return hashSlice({ name, start, end })
  },

  /// Two relay guards on the testnet: private routing has no route without them
  /// (`bin/private-relay.js` is the out-of-process equivalent).
  async guards ({ count = 2 } = {}) {
    const { createPrivateGuard } = await import(
      pathToFileURL(resolve(APP, 'src/private-overlay.js')).href
    )
    for (let index = 0; index < count; index++) {
      guards.push(await createPrivateGuard({
        host: '127.0.0.1',
        port: 0,
        bootstrap: testnet.bootstrap,
        seed: randomBytes(32)
      }))
    }
    return { guards: guards.length }
  },

  async ping () {
    return { ok: true }
  }
}

if (mode !== 'serve') throw new Error('usage: network-mixed.mjs serve [size]')

const size = Number(process.argv[3] || 3)
const testnet = await createTestnet(size)
testnets.push(testnet)
out({ bootstrap: testnet.bootstrap, pid: process.pid, dir: basename(here) })

const { createInterface } = await import('node:readline')
const lines = createInterface({ input: process.stdin, crlfDelay: Infinity })
let pending = Promise.resolve()
lines.on('line', line => {
  pending = pending.then(async () => {
    let command
    try {
      command = JSON.parse(line)
    } catch (err) {
      out({ msgId: null, id: null, ok: false, error: 'bad json: ' + err.message })
      return
    }
    // The envelope is `msgId`: `publish`/`search` carry an entry `id` of their own, and a single
    // `id` key cannot be both. A numeric `id` is still accepted as the envelope for old callers.
    const msgId = command.msgId ?? (typeof command.id === 'number' ? command.id : null)
    // With `msgId` the op keeps `id` for itself; a bare numeric `id` was the old envelope.
    const { msgId: _msgId, op, ...args } = command
    if (typeof command.msgId === 'undefined') delete args.id
    const handler = ops[op]
    if (!handler) {
      out({ msgId, id: msgId, ok: false, error: 'unknown op: ' + op })
      return
    }
    try {
      out({ msgId, id: msgId, ok: true, result: await handler(args) })
    } catch (err) {
      out({ msgId, id: msgId, ok: false, error: err?.message || String(err) })
    }
  })
})

async function shutdown () {
  for (const { acquirer } of acquirers.values()) {
    try { acquirer.close() } catch {}
  }
  for (const server of servers.values()) {
    try { server.closeAllConnections?.(); server.close() } catch {}
  }
  for (const guard of guards) {
    try { await guard.close() } catch {}
  }
  for (const value of nodes.values()) {
    try { await value.close() } catch {}
  }
  for (const net of testnets) {
    try { await net.destroy() } catch {}
  }
  process.exit(0)
}

lines.on('close', shutdown)
process.stdin.on('end', shutdown)
