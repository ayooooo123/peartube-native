// Runs the app's own `src/mirror.js` (copied to fixtures/gen/mirror.js) against a scripted node, an in-memory
// filesystem and scripted feed replies. Prints one JSON trace that tests/acquire_mirror.rs must reproduce.
//
// `p2p-replicate` and `hiverelay-seed` are registered by the app but not ported, so `list()` is filtered to the
// five shared plugins before comparing.
import { mkdirSync, readFileSync, writeFileSync } from 'node:fs'
import { createHash } from 'node:crypto'
import { fileURLToPath, pathToFileURL } from 'node:url'
import { dirname, join } from 'node:path'
import * as fsStub from './acquire-mirror-stubs.mjs'
import * as cryptoStub from './acquire-mirror-crypto.mjs'

const here = dirname(fileURLToPath(import.meta.url))
const gen = join(here, '.gen')
mkdirSync(gen, { recursive: true })

const stub = name => `"${pathToFileURL(join(here, name)).href}"`
const gen_url = name => `"${pathToFileURL(join(here, '..', '..', 'fixtures', 'gen', name)).href}"`

function load (file, rewrites, out) {
  let text = readFileSync(join(here, '..', '..', 'fixtures', 'gen', file), 'utf8')
  for (const [from, to] of Object.entries(rewrites)) {
    if (!text.includes(`from ${from}`)) throw new Error(`unstubbed import ${from}`)
    text = text.split(`from ${from}`).join(`from ${to}`)
  }
  const path = join(gen, out)
  writeFileSync(path, text)
  return import(`file://${path}?v=${Math.random()}`)
}

const acquireRewrites = {
  "'node:fs'": stub('acquire-mirror-stubs.mjs'),
  "'node:crypto'": stub('acquire-mirror-crypto.mjs'),
  "'node:http'": stub('acquire-mirror-http.mjs'),
  "'node:https'": stub('acquire-mirror-http.mjs'),
  "'b4a'": stub('acquire-mirror-b4a.mjs'),
  "'streamx'": stub('acquire-mirror-streamx.mjs'),
  "'./identify.js'": stub('acquire-mirror-identify.mjs')
}

const mirrorRewrites = {
  "'node:fs'": stub('acquire-mirror-stubs.mjs'),
  "'./protocol.js'": gen_url('protocol.js'),
  "'./hiverelay.js'": stub('acquire-mirror-plants.mjs'),
  "'./traffic.js'": stub('acquire-mirror-plants.mjs'),
  "'./art.js'": stub('acquire-mirror-plants.mjs')
}

const { createAcquirer } = await load('acquire.js', acquireRewrites, 'mirror-acquire.mjs')
const { createMirrorHost } = await load('mirror.js', mirrorRewrites, 'mirror-host.mjs')

const NOW = 1_700_000_000_000
Date.now = () => NOW

const sha = text => createHash('sha256').update(text).digest('hex')
const announceFor = (meta, bytes) => ({
  type: 'announce',
  id: meta.id,
  title: meta.title,
  size: bytes.length,
  sha256: sha(bytes),
  blobs: 'blobs:' + sha(String(meta.id)).slice(0, 8),
  blob: { blockOffset: 0, blockLength: Math.max(1, Math.ceil(bytes.length / 65536)), byteOffset: 0, byteLength: bytes.length }
})

function makeNode () {
  const node = {
    calls: [],
    entries: [],
    backfill: { backfilled: 0, total: 0 },
    rechunkProgress: [],
    rechunkMigrate: [],
    rechunkResult: { total: 0, migrated: 0, deduped: 0, remaining: 0, migratedBytes: 0 },
    privateRouting: false,
    closing: false,
    async put (meta, stream) {
      const chunks = []
      for await (const chunk of stream) chunks.push(Buffer.from(chunk))
      const bytes = Buffer.concat(chunks)
      node.calls.push({ method: 'put', meta: { ...meta }, start: null, progress: [bytes.length] })
      return announceFor(meta, bytes)
    },
    async append (op) { node.calls.push({ method: 'append', op }) },
    async dropStore (name) { node.calls.push({ method: 'dropStore', name }) },
    async warm (entry) { node.calls.push({ method: 'warm', entry }) },
    async publishSource (sourceKey, title) {
      node.calls.push({ method: 'publishSource', sourceKey, title })
      return { type: 'source', sourceKey, kind: 'channel', title }
    },
    async syncLocalChannel () {
      node.calls.push({ method: 'syncLocalChannel' })
      return node.backfill
    },
    async rechunkLegacyBlobs (options) {
      node.calls.push({
        method: 'rechunk',
        id: options.id ?? null,
        foreground: Boolean(options.foreground),
        limit: Number.isFinite(options.limit) ? options.limit : null
      })
      for (const update of node.rechunkProgress) options.onProgress?.(update)
      for (const migration of node.rechunkMigrate) options.onMigrate?.(migration.old, migration.target)
      return node.rechunkResult
    },
    async search (id) {
      node.calls.push({ method: 'search', id: id ?? null })
      return node.entries.filter(entry => !id || entry.id === id)
    },
    async blobCore () { return null },
    status: () => ({ peers: 0, lanPeers: 0 }),
    peerConnections: () => [],
    listSources: () => []
  }
  return node
}

const local = (id, title) => ({
  key: 'k:' + id,
  id,
  title,
  size: 4,
  sha256: 'c'.repeat(64),
  local: true,
  cachedBytes: 4,
  private: false,
  epoch: 0,
  locked: false,
  source: 'channel',
  sourceKind: 'channel',
  season: null,
  episode: null,
  seriesKey: id,
  baseTitle: title,
  year: null,
  streamUrl: '',
  magnetUrl: ''
})

async function settleRechunk (node) {
  for (let i = 0; i < 200; i++) {
    if (node.calls.some(call => call.method === 'rechunk')) return
    await new Promise(resolve => setImmediate(resolve))
  }
  throw new Error('startup count never ran')
}

const PORTED = ['http-acquire', 'channel-backfill', 'blob-rechunk', 'watch-dir', 'feed-mirror']

async function harness (seedJobs = []) {
  fsStub.reset()
  cryptoStub.reset()
  if (seedJobs.length) fsStub.setFile('/work/jobs.json', JSON.stringify(seedJobs))
  globalThis.__feeds = new Map()
  const node = makeNode()
  const acquirer = createAcquirer(node, '/work/jobs.json', { concurrency: 0, retryDelay: 0, ubitConfigDir: '/work/ubit' })
  const host = await createMirrorHost({
    node,
    storage: '/data',
    acquirer,
    mirrorChannel: false,
    rechunk: false,
    watchDir: null,
    feeds: [],
    pollIntervalMs: 0,
    replicate: false,
    hiverelay: null,
    pluginsDir: '/data/plugins'
  })
  await settleRechunk(node)
  return { node, acquirer, host }
}

const capture = ({ node, acquirer, host }) => {
  const ledgerText = fsStub.existsSync('/data/mirror-watch.json') ? fsStub.readFileSync('/data/mirror-watch.json') : null
  const savedText = fsStub.existsSync('/work/jobs.json') ? fsStub.readFileSync('/work/jobs.json') : '[]'
  return {
    calls: node.calls,
    list: host.list().filter(plugin => PORTED.includes(plugin.name)),
    ledger: ledgerText ? JSON.parse(ledgerText) : null,
    jobs: JSON.parse(savedText),
    acquirerList: acquirer.list()
  }
}

const traces = {}

{
  const ctx = await harness()
  ctx.node.backfill = { backfilled: 2, total: 3 }
  const result = await ctx.host.run('channel-backfill')
  traces.backfill = { ...capture(ctx), result }
  await ctx.host.close()
}

{
  const ctx = await harness()
  globalThis.__feeds.set('http://feed.test/v1/entries', {
    body: [
      { id: 'IMDB:TT1234567', title: 'Film', url: 'http://cdn/film.mp4' },
      { id: 'imdb:tt1234567', title: 'Dup' },
      { id: 'imdb:tt2222222', sha256: 'b'.repeat(64), source: { url: 'http://cdn/b.mp4', headers: { authorization: 'Bearer b' } } },
      { id: 'not an id' },
      { id: 'clip:x', url: 'ftp://cdn/x.mp4' },
      { id: 'imdb:tt3333333', url: 'http://cdn/local.mp4' }
    ]
  })
  ctx.node.entries = [local('imdb:tt3333333', 'Local')]
  const result = await ctx.host.run('feed-mirror', { url: 'http://feed.test/v1/entries', headers: { 'x-token': 't' } })
  traces.feed = { ...capture(ctx), result }
  await ctx.host.close()
}

{
  const ctx = await harness()
  ctx.node.entries = [local('imdb:tt4444444', 'Manifest Local')]
  const result = await ctx.host.run('feed-mirror', {
    headers: { 'x-token': 'm' },
    items: [
      { id: 'imdb:tt4444444', title: 'Present' },
      { id: 'imdb:tt5555555', title: 'New', source: { url: 'http://cdn/new.mp4', headers: { 'x-item': 'i' } } },
      { id: 'imdb:tt6666666', title: 'Bad', source: { url: '/relative' } },
      'not an object'
    ]
  })
  traces.manifest = { ...capture(ctx), result }
  await ctx.host.close()
}

{
  const ctx = await harness()
  fsStub.mkdirSync('/data/import')
  fsStub.setFile('/data/import/Some.Film.2019.mkv', 'film')
  fsStub.setFile('/data/import/Show.S01E02.mkv', 'episode')
  fsStub.setFile('/data/import/Movie.tt1234567.S01E02.1080p.mkv', 'movie')
  fsStub.setFile('/data/import/Clip.mkv', 'clip')
  fsStub.setFile('/data/import/Clip.mkv.json', JSON.stringify({ id: 'imdb:tt7654321', title: 'Sidecar Title' }))
  fsStub.setFile('/data/import/notes.txt', 'ignore me')
  fsStub.setFile('/data/import/empty.mp4', '')
  const first = await ctx.host.run('watch-dir', { dir: '/data/import' })
  const second = await ctx.host.run('watch-dir', { dir: '/data/import' })
  fsStub.setFile('/data/import/Show.S01E02.mkv', 'episode, now longer')
  const third = await ctx.host.run('watch-dir', { dir: '/data/import' })
  traces.watch = { ...capture(ctx), runs: [first, second, third] }
  await ctx.host.close()
}

{
  const ctx = await harness([{
    jobId: 'cccccccc-0000-4000-8000-000000000003',
    id: 'imdb:tt7777777',
    title: 'Done',
    plugin: 'http-acquire',
    channel: false,
    private: false,
    status: 'done',
    created: NOW,
    store: 'video:rechunk',
    source: null,
    total: 4,
    bytes: 4,
    announce: { type: 'announce', id: 'imdb:tt7777777', title: 'Done', size: 4, sha256: 'd'.repeat(64), blobs: 'AAAA', blob: { blockOffset: 5, blockLength: 1, byteOffset: 0, byteLength: 4 } },
    size: 4,
    sha256: 'd'.repeat(64),
    hashState: null,
    hashBytes: 0
  }])
  ctx.node.rechunkProgress = [
    { total: 2, migrated: 0, deduped: 0, remaining: 2, migratedBytes: 0, currentId: 'imdb:tt7777777', currentTitle: 'Done' },
    { total: 2, migrated: 1, deduped: 0, remaining: 1, migratedBytes: 4, currentId: 'imdb:tt8888888', currentTitle: 'Other' }
  ]
  ctx.node.rechunkMigrate = [{ old: 'AAAA:5', target: { blobs: 'BBBB', blob: { blockOffset: 7, blockLength: 1, byteOffset: 0, byteLength: 4 } } }]
  ctx.node.rechunkResult = { total: 2, migrated: 1, deduped: 1, remaining: 1, migratedBytes: 4 }
  const result = await ctx.host.run('blob-rechunk', { id: 'imdb:tt7777777', foreground: true, limit: 5 })
  traces.rechunk = { ...capture(ctx), result }
  await ctx.host.close()
}

{
  const ctx = await harness()
  const error = await ctx.host.run('nope').catch(err => ({ message: err.message, status: err.status ?? null }))
  traces.list = { ...capture(ctx), error }
  await ctx.host.close()
}

process.stdout.write(JSON.stringify(traces))
