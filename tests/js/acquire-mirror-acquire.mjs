// Runs the app's own `src/acquire.js` (copied verbatim to fixtures/gen/acquire.js) against a scripted node,
// in-memory fs, scripted HTTP sources and a fake `ubit`. Prints one JSON trace that tests/acquire_mirror.rs
// must reproduce with the Rust port.
//
// The copy is loaded through import rewriting: every module it reaches (fs, crypto, http, b4a, streamx,
// identify) is swapped for a stub beside this file; node:path stays real.
import { mkdirSync, readFileSync, writeFileSync } from 'node:fs'
import { createHash } from 'node:crypto'
import { fileURLToPath, pathToFileURL } from 'node:url'
import { dirname, join } from 'node:path'
import * as fsStub from './acquire-mirror-stubs.mjs'
import * as cryptoStub from './acquire-mirror-crypto.mjs'
import * as httpStub from './acquire-mirror-http.mjs'

const here = dirname(fileURLToPath(import.meta.url))
const gen = join(here, '.gen')
mkdirSync(gen, { recursive: true })

const source = readFileSync(join(here, '..', '..', 'fixtures', 'gen', 'acquire.js'), 'utf8')
const stub = name => `"${pathToFileURL(join(here, name)).href}"`
const rewrites = {
  "'node:fs'": stub('acquire-mirror-stubs.mjs'),
  "'node:crypto'": stub('acquire-mirror-crypto.mjs'),
  "'node:http'": stub('acquire-mirror-http.mjs'),
  "'node:https'": stub('acquire-mirror-http.mjs'),
  "'b4a'": stub('acquire-mirror-b4a.mjs'),
  "'streamx'": stub('acquire-mirror-streamx.mjs'),
  "'./identify.js'": stub('acquire-mirror-identify.mjs')
}
let patched = source
for (const [from, to] of Object.entries(rewrites)) {
  if (!patched.includes(`from ${from}`)) throw new Error(`unstubbed import ${from}`)
  patched = patched.split(`from ${from}`).join(`from ${to}`)
}
const modulePath = join(gen, 'acquire.mjs')
writeFileSync(modulePath, patched)

const { createAcquirer } = await import(`file://${modulePath}?v=${Date.now()}`)

const NOW = 1_700_000_000_000
Date.now = () => NOW

const hex = bytes => Buffer.from(bytes).toString('hex')
const announceFor = (meta, bytes) => ({
  type: 'announce',
  id: meta.id,
  title: meta.title,
  size: bytes.length,
  sha256: createHash('sha256').update(bytes).digest('hex'),
  blobs: 'blobs:' + createHash('sha256').update(String(meta.id)).digest('hex').slice(0, 8),
  blob: {
    blockOffset: 0,
    blockLength: Math.max(1, Math.ceil(bytes.length / 65536)),
    byteOffset: 0,
    byteLength: bytes.length
  }
})

function makeNode () {
  const calls = []
  return {
    calls,
    closing: false,
    async put (meta, source, onProgress) {
      const call = { method: 'put', meta: { ...meta }, start: null, progress: [] }
      delete call.meta.onHashState
      calls.push(call)
      const script = globalThis.__nodePlan?.shift() || {}
      let stream = source
      if (typeof source === 'function') {
        call.start = script.start ?? 0
        stream = await source(call.start)
      }
      if (script.hashState) meta.onHashState?.(Buffer.from('checkpoint'), script.hashState)
      const chunks = []
      for await (const chunk of stream) {
        chunks.push(Buffer.from(chunk))
        const total = (call.start ?? 0) + chunks.reduce((sum, part) => sum + part.length, 0)
        onProgress?.(total)
        call.progress.push(total)
      }
      call.meta = { ...meta }
      delete call.meta.onHashState
      return announceFor(meta, Buffer.concat(chunks))
    },
    async append (op) {
      calls.push({ method: 'append', op })
    },
    async dropStore (name) {
      calls.push({ method: 'dropStore', name })
    }
  }
}

const reset = () => {
  fsStub.reset()
  cryptoStub.reset()
  httpStub.reset()
  globalThis.__pages = []
  globalThis.__nodePlan = []
}

async function settle (acquirer) {
  for (let i = 0; i < 200; i++) {
    const busy = acquirer.list().some(job => ['queued', 'running', 'announcing'].includes(job.status))
    if (!busy) return
    await new Promise(resolve => setImmediate(resolve))
  }
  throw new Error('acquirer did not settle')
}

function seed (jobs) {
  fsStub.setFile('/work/jobs.json', JSON.stringify(jobs))
}

function capture (node, acquirer) {
  const saved = fsStub.readFileSync('/work/jobs.json')
  return {
    calls: node.calls,
    list: acquirer.list(),
    saved: JSON.parse(saved),
    modes: fsStub.modes(),
    http: httpStub.calls()
  }
}

const scenario = async (name, setup, body) => {
  reset()
  setup?.()
  const node = makeNode()
  const acquirer = createAcquirer(node, '/work/jobs.json', {
    concurrency: 1,
    retryDelay: 0,
    ubitConfigDir: '/work/ubit',
    onDone: (job, op) => node.calls.push({ method: 'onDone', job, op })
  })
  await body?.(node, acquirer)
  await settle(acquirer)
  const out = capture(node, acquirer)
  acquirer.close()
  return [name, out]
}

const traces = {}
for (const [name, setup, body] of [
  ['download', () => {
    httpStub.plan([{ statusCode: 200, headers: { 'content-length': '7', etag: '"strong"', 'last-modified': 'Wed, 01 Jan 2025 00:00:00 GMT' }, body: ['abc', 'defg'] }])
    globalThis.__nodePlan.push({ start: 0, hashState: 3 })
  }, (node, acquirer) => {
    acquirer.add({ id: 'imdb:tt1234567', title: 'Film', source: { url: 'http://source.test/film.mp4', headers: { authorization: 'Bearer secret' } } })
  }],
  ['resume', () => {
    seed([{
      jobId: 'aaaaaaaa-0000-4000-8000-000000000001',
      id: 'imdb:tt1234567',
      title: 'Film',
      plugin: 'http-acquire',
      channel: false,
      private: false,
      status: 'running',
      created: NOW,
      store: 'video:resume',
      source: { url: 'http://source.test/film.mp4', headers: {}, page: false },
      total: 10,
      bytes: 4,
      etag: '"v1"',
      lastModified: null,
      hashState: null,
      hashBytes: 0
    }])
    httpStub.plan([{ statusCode: 206, headers: { 'content-range': 'bytes 4-9/10', etag: '"v1"' }, body: ['ghij', 'kl'] }])
    globalThis.__nodePlan.push({ start: 4 })
  }],
  ['terminal', () => {
    httpStub.plan([{ statusCode: 404, headers: {} }])
  }, (node, acquirer) => {
    acquirer.add({ id: 'imdb:tt7654321', title: 'Gone', source: { url: 'http://source.test/gone.mp4' } })
  }],
  ['redirect', () => {
    httpStub.plan([
      { statusCode: 302, headers: { location: 'http://cdn.test/film.mp4' }, body: [] },
      { statusCode: 200, headers: { 'content-length': '3' }, body: ['xyz'] }
    ])
  }, (node, acquirer) => {
    acquirer.add({ id: 'imdb:tt1111111', title: 'Moved', source: { url: 'http://source.test/film.mp4', headers: { authorization: 'Bearer secret', cookie: 'a=b' } } })
  }],
  ['page', () => {
    globalThis.__pages.push({ size: 7, body: 'page-7', progress: [{ bytes: 3, total: 7 }] })
  }, (node, acquirer) => {
    acquirer.add({ id: 'imdb:tt2222222', title: 'Page', source: { url: 'http://page.test/watch/2', page: true } })
  }],
  ['cancel', () => {
    seed([{
      jobId: 'bbbbbbbb-0000-4000-8000-000000000002',
      id: 'imdb:tt3333333',
      title: 'Done',
      plugin: 'http-acquire',
      channel: false,
      private: false,
      status: 'done',
      created: NOW,
      store: 'video:done',
      source: null,
      total: 4,
      bytes: 4,
      announce: { type: 'announce', id: 'imdb:tt3333333', title: 'Done', size: 4, sha256: 'a'.repeat(64), blobs: 'AAAA', blob: { blockOffset: 5, blockLength: 1, byteOffset: 0, byteLength: 4 } },
      size: 4,
      sha256: 'a'.repeat(64),
      hashState: null,
      hashBytes: 0
    }])
  }, async (node, acquirer) => {
    await new Promise(resolve => setTimeout(resolve, 0))
    node.calls.push({ method: 'repoint', updated: acquirer.repointBlob('AAAA:5', { blobs: 'BBBB', blob: { blockOffset: 7, blockLength: 1, byteOffset: 0, byteLength: 4 } }) })
    node.calls.push({ method: 'cancel', result: acquirer.cancel('bbbbbbbb-0000-4000-8000-000000000002') })
    node.calls.push({ method: 'cancel', result: acquirer.cancel('nope') })
    node.calls.push({ method: 'get', result: acquirer.get('bbbbbbbb-0000-4000-8000-000000000002') })
  }]
]) traces[name] = Object.fromEntries([await scenario(name, setup, body)])[name]

process.stdout.write(JSON.stringify(traces))
