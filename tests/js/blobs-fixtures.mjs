// Generates `fixtures/blobs-reference.json` from the app's own `src/node/blobs.js`.
//
//   node blobs-fixtures.mjs > ../../fixtures/blobs-reference.json
//
// The app repository is found through PEARTUBE_APP, else as a sibling checkout (`../../../peartube`).
// Blobs run against the app's real `corestore`/`hypercore`/`hyperblobs` (resolved from its own
// node_modules) with a stub channel, swarm and vault, so every recorded value is the app's own
// output: announce ops, core keys, raw (encrypted) blocks, and the cache ceiling's decisions for a
// scripted recency/progress sequence.
import { createHash } from 'node:crypto'
import { createRequire } from 'node:module'
import { mkdtempSync, rmSync } from 'node:fs'
import { join, resolve } from 'node:path'
import { tmpdir } from 'node:os'
import { fileURLToPath, pathToFileURL } from 'node:url'

const here = fileURLToPath(new URL('.', import.meta.url))
const APP = process.env.PEARTUBE_APP || resolve(here, '../../../peartube')
const require = createRequire(join(APP, 'package.json'))
const Corestore = require('corestore')
const b4a = require('b4a')

const { createBlobs } = await import(pathToFileURL(join(APP, 'src/node/blobs.js')).href)
const { Readable } = await import(pathToFileURL(join(APP, 'node_modules/streamx/index.js')).href)

const PRIMARY = Buffer.alloc(32, 0x11).toString('hex')
const LOCAL = Buffer.alloc(32, 0x22).toString('hex')
const EPOCH_KEY = Buffer.alloc(32, 0x33).toString('hex')
const VIDEO_KEY = Buffer.alloc(32, 0x44).toString('hex')

// Same deterministic bytes on both sides: `byte = (i * 31 + 7) % 251`.
function data (size) {
  const out = Buffer.alloc(size)
  for (let i = 0; i < size; i++) out[i] = (i * 31 + 7) % 251
  return out
}

function stream (buffer) {
  return Readable.from([buffer])
}

// Per-block SHA-256 keeps the reference file small and still pins every byte.
function digest (buf) {
  return createHash('sha256').update(buf).digest('hex')
}

function hex (buf) {
  return Buffer.from(buf).toString('hex')
}

function makeCtx ({ dir, store, ops, epochKey = true }) {
  const ctx = {
    opts: { storage: dir, worklet: false, streamHost: '127.0.0.1', streamPort: 0 },
    life: { closing: false },
    undo: [],
    storage: {
      store,
      coreStorage: { closed: false, flush: async () => {}, compact: async () => {} },
      isMobileWorklet: false,
      storedDisc: new Set()
    },
    net: {
      joinContentSwarm () {},
      leaveContentSwarm () {},
      // The node announces, wants and leaves only what it publishes; the recorded values are the ops and blocks.
      announceContent () {},
      wantContent () {},
      leaveContent () {},
      inflightRange: () => [256, 512],
      downBps: 0,
      upBps: 0
    },
    identity: {
      idVault: epochKey ? { epochKey: () => b4a.from(EPOCH_KEY, 'hex') } : null,
      accessKeys: new Map()
    },
    catalog: { ephemeralWatches: new Map(), findEntry: async () => null, search: async () => [] },
    update () {},
    status () {},
    channel: {
      primaryKey: PRIMARY,
      secondaryWriters: new Set(),
      channel: {
        local: { key: b4a.from(LOCAL, 'hex') },
        update: async () => {},
        view: {
          createReadStream: async function * () {
            for (const op of ops) yield op
          }
        }
      },
      append: async op => {
        const text = JSON.stringify(op)
        ops.push({ key: b4a.from(`${op.type}/${LOCAL}/${op.id || ''}`), value: b4a.from(text) })
      },
      localRole: async () => 'owner',
      currentChannelEpoch: async () => 1,
      markHasEntries () {},
      syncWriterCores: async () => {}
    }
  }
  return ctx
}

async function readCore (store, key, encryptionKey = null) {
  const core = store.get({
    key: b4a.from(key, 'hex'),
    ...(encryptionKey ? { encryption: { key: b4a.from(encryptionKey, 'hex') } } : {})
  })
  await core.ready()
  const blocks = []
  const plain = []
  for (let i = 0; i < core.length; i++) blocks.push(digest(await core.get(i, { raw: true })))
  for (let i = 0; i < core.length; i++) plain.push(digest(await core.get(i)))
  const out = { key, length: core.length, byteLength: core.byteLength, blocks, plain }
  await core.close()
  return out
}

const root = mkdtempSync(join(tmpdir(), 'peartube-blobs-'))
const out = { primaryKey: PRIMARY, localKey: LOCAL, epochKey: EPOCH_KEY, videoKey: VIDEO_KEY }

try {
  // --- fresh, unencrypted ingest -------------------------------------------------------------
  {
    const dir = join(root, 'fresh')
    const store = new Corestore(dir, { primaryKey: b4a.from(PRIMARY, 'hex'), unsafe: true })
    const ops = []
    const blobs = await createBlobs(makeCtx({ dir, store, ops }))
    await blobs.startServer()
    const size = 1300123
    const payload = data(size)
    const published = await blobs.publish(
      { id: 'youtube:fixturefresh', title: 'Fresh Ingest', store: 'video:fixture-fresh', expectedSize: size },
      stream(payload)
    )
    const { streamUrl, key, channel, magnetUrl, ...announce } = published
    out.fresh = {
      size,
      streamUrl: streamUrl.replace(/^http:\/\/[^/]+\//, ''),
      key,
      channel,
      magnetUrl,
      announce,
      core: await readCore(store, announce.blobs),
      ops: ops.map(op => op.value.toString())
    }
    await blobs.server.close()
    await store.close()
  }

  // --- encrypted ingest with a fixed video key ----------------------------------------------
  {
    const dir = join(root, 'encrypted')
    const store = new Corestore(dir, { primaryKey: b4a.from(PRIMARY, 'hex'), unsafe: true })
    const ops = []
    const blobs = await createBlobs(makeCtx({ dir, store, ops }))
    await blobs.startServer()
    const size = 1100000
    const published = await blobs.publish(
      {
        id: 'youtube:fixtureenc',
        title: 'Encrypted Ingest',
        store: 'video:fixture-encrypted',
        private: true,
        epoch: 1,
        storeKey: VIDEO_KEY
      },
      stream(data(size))
    )
    const { streamUrl, key, channel, magnetUrl, ...announce } = published
    const { unwrapVideoKey } = await import(pathToFileURL(join(APP, 'src/mnemonic.js')).href)
    out.encrypted = {
      size,
      streamUrl: streamUrl.replace(/^http:\/\/[^/]+\//, ''),
      key,
      channel,
      magnetUrl,
      announce,
      unwrapped: hex(unwrapVideoKey(announce.wrappedKey, b4a.from(EPOCH_KEY, 'hex')) || Buffer.alloc(0)),
      core: await readCore(store, announce.blobs, VIDEO_KEY),
      ops: ops.map(op => op.value.toString())
    }
    await blobs.server.close()
    await store.close()
  }

  // --- cache ceiling: scripted recency, progress and streaming ------------------------------
  {
    const dir = join(root, 'cache-viewer')
    const writerDir = join(root, 'cache-writer')
    const writerStore = new Corestore(writerDir, { primaryKey: b4a.from(PRIMARY, 'hex'), unsafe: true })
    const store = new Corestore(dir, { primaryKey: b4a.from(PRIMARY, 'hex'), unsafe: true })
    const ops = []
    const blobs = await createBlobs(makeCtx({ dir, store, ops }))
    await blobs.startServer()

    // Real replication into a viewer store: the cores end up with blocks but no secret key, the
    // state a downloaded film is in.
    const scripted = []
    const names = ['cache-a', 'cache-b', 'cache-c', 'cache-d']
    for (const name of names) {
      const source = writerStore.get({ name })
      await source.ready()
      const key = hex(source.key)
      await source.append([data(600 * 1024), data(600 * 1024)])
      const viewer = store.get({ key: b4a.from(key, 'hex') })
      await viewer.ready()
      const s1 = source.replicate(true)
      const s2 = viewer.replicate(false)
      s1.pipe(s2).pipe(s1)
      await viewer.update({ wait: true })
      for (let i = 0; i < 2; i++) await viewer.get(i)
      s1.destroy()
      s2.destroy()
      await viewer.close()
      await source.close()
      scripted.push({ name, key })
    }
    await writerStore.close()

    // A fixed clock, so recency order is the touch order.
    let clock = 1_000_000
    const realNow = Date.now
    Date.now = () => (clock += 1000)

    // Touch order: a, b, c, d — a is the keep hint, so b is the oldest victim.
    for (const entry of scripted) await blobs.blobCore(entry.key)

    // Film `c` is being served at 5 MiB progress: streaming, never evicted.
    const progress = new Map([[scripted[2].key, 5 * 1024 * 1024]])
    const streaming = new Set([scripted[2].key])
    blobs.server.streamProgress = () => progress
    blobs.server.streamingHexes = () => streaming

    const budget = 2 * 1024 * 1024
    blobs.setCacheMaxBytes(budget)
    const result = await blobs.enforceCacheCeiling({ keepHex: scripted[0].key })
    Date.now = realNow

    const remaining = {}
    for (const entry of scripted) {
      const core = store.get({ key: b4a.from(entry.key, 'hex') })
      await core.ready()
      remaining[entry.key] = core.length > 0 ? await core.count(0, core.length) : 0
      await core.close()
    }
    out.cacheTrim = {
      budget,
      cores: scripted,
      blockBytes: 600 * 1024,
      progress: [[scripted[2].key, 5 * 1024 * 1024]],
      streaming: [scripted[2].key],
      keep: scripted[0].key,
      result,
      remaining
    }
    await blobs.server.close()
    await store.close()
  }
} finally {
  rmSync(root, { recursive: true, force: true })
}

process.stdout.write(JSON.stringify(out, null, 2) + '\n')
process.exit(0)
