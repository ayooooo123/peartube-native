// A wire-faithful HiveRelay stand-in and the app's own `createHiverelaySeeder`, on a local HyperDHT
// testnet, driven by line-delimited JSON so the Rust seeder can be compared request for request.
//
// Run: node tests/js/hiverelay-seed.mjs serve
// Startup prints one line: {"ready":true}. Each stdin line is one command
// `{"id": N, "op": "...", ...}`; each stdout line is `{"id": N, "ok": true, "result": ...}` or
// `{"id": N, "ok": false, "error": "..."}`. Closing stdin stops the process.

import { createRequire } from 'node:module'
import { dirname, join, resolve } from 'node:path'
import { fileURLToPath, pathToFileURL } from 'node:url'
import { createServer } from 'node:http'
import { createHash } from 'node:crypto'
import { mkdtempSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { Readable } from 'node:stream'
const here = dirname(fileURLToPath(import.meta.url))
const APP = process.env.PEARTUBE_APP || resolve(here, '../../../../peartube')
const require = createRequire(resolve(APP, 'package.json'))

const { default: b4a } = await import(pathToFileURL(require.resolve('b4a')).href)
const { default: c } = await import(
  pathToFileURL(require.resolve('compact-encoding')).href
)
const { default: sodium } = await import(
  pathToFileURL(require.resolve('sodium-native')).href
)

const { default: createTestnet } = await import(
  pathToFileURL(require.resolve('hyperdht/testnet.js')).href
)
const { default: Hyperswarm } = await import(
  pathToFileURL(require.resolve('hyperswarm')).href
)
const { default: Protomux } = await import(pathToFileURL(require.resolve('protomux')).href)
const { createNode } = await import(pathToFileURL(resolve(APP, 'src/index.js')).href)
const { createHiverelaySeeder } = await import(
  pathToFileURL(resolve(APP, 'src/hiverelay.js')).href
)

const SEED_PROTOCOL = 'hiverelay-seed'
const SEED_HANDSHAKE = b4a.from('{"major":1,"minor":0}')
const RELAY_DISCOVERY_TOPIC = (() => {
  const out = b4a.alloc(32)
  sodium.crypto_generichash(out, b4a.from('hiverelay-discovery-v1'))
  return out
})()

const seedRequestEncoding = {
  preencode (s, m) {
    c.fixed32.preencode(s, m.appKey)
    c.uint.preencode(s, m.discoveryKeys.length)
    for (const dk of m.discoveryKeys) c.fixed32.preencode(s, dk)
    c.uint.preencode(s, m.replicationFactor)
    c.string.preencode(s, m.geoPreference)
    c.uint.preencode(s, m.maxStorageBytes)
    c.uint.preencode(s, m.bountyRate)
    c.uint.preencode(s, m.ttlSeconds)
    c.fixed32.preencode(s, m.publisherPubkey)
    c.fixed64.preencode(s, m.publisherSignature)
    c.uint.preencode(s, m.revocable)
    c.uint.preencode(s, m.unseedFreezeMs)
    c.uint.preencode(s, m.durability)
  },
  encode (s, m) {
    c.fixed32.encode(s, m.appKey)
    c.uint.encode(s, m.discoveryKeys.length)
    for (const dk of m.discoveryKeys) c.fixed32.encode(s, dk)
    c.uint.encode(s, m.replicationFactor)
    c.string.encode(s, m.geoPreference)
    c.uint.encode(s, m.maxStorageBytes)
    c.uint.encode(s, m.bountyRate)
    c.uint.encode(s, m.ttlSeconds)
    c.fixed32.encode(s, m.publisherPubkey)
    c.fixed64.encode(s, m.publisherSignature)
    c.uint.encode(s, m.revocable)
    c.uint.encode(s, m.unseedFreezeMs)
    c.uint.encode(s, m.durability)
  },
  decode (s) {
    const appKey = c.fixed32.decode(s)
    const n = c.uint.decode(s)
    const discoveryKeys = []
    for (let i = 0; i < n; i++) discoveryKeys.push(c.fixed32.decode(s))
    return {
      appKey,
      discoveryKeys,
      replicationFactor: c.uint.decode(s),
      geoPreference: c.string.decode(s),
      maxStorageBytes: c.uint.decode(s),
      bountyRate: c.uint.decode(s),
      ttlSeconds: c.uint.decode(s),
      publisherPubkey: c.fixed32.decode(s),
      publisherSignature: c.fixed64.decode(s),
      revocable: s.start < s.end ? c.uint.decode(s) : 1,
      unseedFreezeMs: s.start < s.end ? c.uint.decode(s) : 0,
      durability: s.start < s.end ? c.uint.decode(s) : 0
    }
  }
}
const seedAcceptEncoding = {
  preencode (s, m) {
    c.fixed32.preencode(s, m.appKey)
    c.fixed32.preencode(s, m.relayPubkey)
    c.string.preencode(s, m.region)
    c.uint.preencode(s, m.availableStorageBytes)
    c.fixed64.preencode(s, m.relaySignature)
  },
  encode (s, m) {
    c.fixed32.encode(s, m.appKey)
    c.fixed32.encode(s, m.relayPubkey)
    c.string.encode(s, m.region)
    c.uint.encode(s, m.availableStorageBytes)
    c.fixed64.encode(s, m.relaySignature)
  },
  decode: () => ({})
}
const seedUnseedEncoding = { preencode () {}, encode () {}, decode: () => ({}) }
const seedDenyEncoding = {
  preencode (s, m) {
    c.fixed32.preencode(s, m.appKey)
    c.fixed32.preencode(s, m.relayPubkey)
    c.string.preencode(s, m.reasonCode)
    c.string.preencode(s, m.detail)
    c.fixed64.preencode(s, m.relaySignature)
  },
  encode (s, m) {
    c.fixed32.encode(s, m.appKey)
    c.fixed32.encode(s, m.relayPubkey)
    c.string.encode(s, m.reasonCode)
    c.string.encode(s, m.detail)
    c.fixed64.encode(s, m.relaySignature)
  },
  decode: () => ({})
}

const hex = bytes => b4a.from(bytes).toString('hex')

function out (value) {
  process.stdout.write(JSON.stringify(value) + '\n')
}

const nodes = new Map()
const seeders = new Map()
let testnet = null
const standins = new Map()

// One stand-in HiveRelay: HTTP endpoints on 127.0.0.1 plus a Hyperswarm peer that opens the
// `hiverelay-seed` channel and records every request, as p2p-hiverelay v0.26.x does.
async function startStandin ({ apiKey = 'hr-secret', region = 'NA', availableBytes = 50 * 1024 ** 3 }) {
  const keyPair = (() => {
    const publicKey = b4a.alloc(32)
    const secretKey = b4a.alloc(64)
    sodium.crypto_sign_keypair(publicKey, secretKey)
    return { publicKey, secretKey }
  })()
  const pubkeyHex = hex(keyPair.publicKey)
  const swarm = new Hyperswarm({ bootstrap: testnet.bootstrap, keyPair })
  const records = { http: [], p2p: [] }

  swarm.on('connection', conn => {
    const mux = Protomux.from(conn)
    let acceptMsg = null
    const channel = mux.createChannel({
      protocol: SEED_PROTOCOL,
      id: null,
      handshake: c.raw,
      onopen (handshake) {
        if (!handshake.equals(SEED_HANDSHAKE)) process.exit(2)
      },
      onclose () {}
    })
    if (!channel) return
    channel.addMessage({
      encoding: seedRequestEncoding,
      onmessage (msg) {
        records.p2p.push({
          raw: hex(c.encode(seedRequestEncoding, msg)),
          appKey: hex(msg.appKey),
          discoveryKeys: msg.discoveryKeys.map(hex),
          replicationFactor: msg.replicationFactor,
          geoPreference: msg.geoPreference,
          maxStorageBytes: msg.maxStorageBytes,
          bountyRate: msg.bountyRate,
          ttlSeconds: msg.ttlSeconds,
          publisherPubkey: hex(msg.publisherPubkey),
          revocable: msg.revocable,
          unseedFreezeMs: msg.unseedFreezeMs,
          durability: msg.durability
        })
        if (!acceptMsg) return
        const sig = b4a.alloc(64)
        sodium.crypto_sign_detached(
          sig,
          b4a.concat([msg.appKey, keyPair.publicKey, b4a.from(region)]),
          keyPair.secretKey
        )
        acceptMsg.send({
          appKey: msg.appKey,
          relayPubkey: keyPair.publicKey,
          region,
          availableStorageBytes: availableBytes,
          relaySignature: sig
        })
      }
    })
    acceptMsg = channel.addMessage({ encoding: seedAcceptEncoding })
    channel.addMessage({ encoding: seedUnseedEncoding })
    channel.addMessage({ encoding: seedDenyEncoding })
    channel.open(SEED_HANDSHAKE)
  })
  swarm.join(RELAY_DISCOVERY_TOPIC, { server: true, client: false })
  await swarm.flush()

  const server = createServer((req, res) => {
    const json = (code, obj) => {
      res.writeHead(code, { 'content-type': 'application/json' })
      res.end(JSON.stringify(obj))
    }
    if (req.method === 'GET' && (req.url === '/.well-known/hiverelay.json' || req.url === '/api/capabilities')) {
      return json(200, { schemaVersion: 1, pubkey: pubkeyHex, version: '0.24.4', region })
    }
    if (req.method === 'GET' && req.url === '/status') {
      return json(200, { running: true, publicKey: pubkeyHex, region })
    }
    if (req.method === 'POST' && req.url === '/seed-core') {
      const chunks = []
      req.on('data', chunk => chunks.push(chunk))
      req.on('end', () => {
        const body = b4a.toString(b4a.concat(chunks))
        records.http.push({
          authorization: req.headers.authorization || null,
          contentType: req.headers['content-type'] || null,
          body
        })
        if (apiKey && req.headers.authorization !== `Bearer ${apiKey}`) {
          return json(401, { error: 'Unauthorized — API key required for /seed-core' })
        }
        let parsed = null
        try {
          parsed = JSON.parse(body)
        } catch {}
        if (!parsed || !Number.isSafeInteger(parsed.maxStorageBytes) || parsed.maxStorageBytes <= 0) {
          return json(400, { error: 'maxStorageBytes must be a positive safe integer' })
        }
        json(200, { ok: true, coreKey: parsed.coreKey, length: 0 })
      })
      return
    }
    json(404, { error: 'not found' })
  })
  await new Promise(done => server.listen(0, '127.0.0.1', done))
  const standin = {
    url: `http://127.0.0.1:${server.address().port}`,
    pubkey: pubkeyHex,
    records,
    async close () {
      server.closeAllConnections?.()
      await new Promise(done => server.close(() => done()))
      await swarm.destroy({ force: true })
    }
  }
  standins.set(pubkeyHex, standin)
  return standin
}

const trafficStub = {
  markBulk () {},
  unmarkBulk () {},
  info: () => ({ bulkUploadMbps: 0, bulkPeers: 0, bulkPaused: false, servingViewers: false, peersBusy: false })
}

function nodeOf (name) {
  const node = nodes.get(name)
  if (!node) throw new Error('unknown node: ' + name)
  return node
}

const ops = {
  async testnet ({ size = 2 } = {}) {
    testnet = await createTestnet(size)
    return { bootstrap: testnet.bootstrap }
  },

  async standin ({ apiKey = 'hr-secret' } = {}) {
    const standin = await startStandin({ apiKey })
    return { url: standin.url, pubkey: standin.pubkey }
  },

  async requests ({ pubkey }) {
    const standin = standins.get(pubkey)
    if (!standin) throw new Error('unknown stand-in: ' + pubkey)
    // The seeder's run returns once the requests are written; the stand-in decodes them from its
    // own socket afterwards. Read only once the recording has stopped growing.
    let last = -1
    for (let i = 0; i < 40; i++) {
      const count = standin.records.http.length + standin.records.p2p.length
      if (count === last && count > 0) break
      last = count
      await new Promise(resolve => setTimeout(resolve, 100))
    }
    return {
      http: standin.records.http.slice(),
      p2p: standin.records.p2p.slice()
    }
  },

  mark ({ pubkey }) {
    const standin = standins.get(pubkey)
    if (!standin) throw new Error('unknown stand-in: ' + pubkey)
    return { http: standin.records.http.length, p2p: standin.records.p2p.length }
  },

  async node ({ name, storage, tracker = null }) {
    const node = await createNode({
      storage: storage || mkdtempSync(join(tmpdir(), 'peartube-hr-')),
      bootstrap: testnet.bootstrap,
      ...(tracker ? { tracker } : {})
    })
    nodes.set(name, node)
    return { channel: node.status().channel, tracker: node.status().tracker }
  },

  // A published video: the channel core plus one 512 KiB-block blob core, both seedable.
  async publish ({ name, videoId = 'imdb:tt0133093', title = 'The Matrix', size = 1300 * 1024 + 37 }) {
    const node = nodeOf(name)
    const bytes = Buffer.alloc(size)
    for (let i = 0; i < size; i++) bytes[i] = (i * 31 + 7) & 0xff
    const entry = await node.publishChannel({ id: videoId, title }, Readable.from([bytes]))
    return {
      channel: node.status().channel,
      magnetUrl: entry.magnetUrl,
      blobs: entry.blobs,
      sha256: createHash('sha256').update(bytes).digest('hex')
    }
  },

  seedable ({ name, includeCached = true }) {
    return nodeOf(name).seedableCores({ includeCached })
  },

  async seeder ({ name, relays = '', apiKey = null, discover = true, replicas = 3, once = true }) {
    const node = nodeOf(name)
    const seeder = createHiverelaySeeder({
      node,
      relays,
      apiKey,
      discover,
      replicas,
      includeCached: true,
      traffic: trafficStub
    })
    seeders.set(name, seeder)
    const result = await seeder.run({ once })
    return { result, info: seeder.info(), publisherPubkey: seeder.info().publisherPubkey }
  },

  async close ({ name }) {
    const node = nodes.get(name)
    if (node) {
      nodes.delete(name)
      await node.close()
    }
    return { closed: Boolean(node) }
  },

  async stop ({ name }) {
    const seeder = seeders.get(name)
    if (seeder) {
      seeders.delete(name)
      await seeder.stop()
    }
    return { stopped: Boolean(seeder) }
  },

  async closeStandin ({ pubkey }) {
    const standin = standins.get(pubkey)
    if (standin) {
      standins.delete(pubkey)
      await standin.close()
    }
    return { closed: Boolean(standin) }
  },

  async destroy () {
    for (const seeder of [...seeders.values()]) await seeder.stop().catch(() => {})
    seeders.clear()
    for (const standin of [...standins.values()]) await standin.close().catch(() => {})
    standins.clear()
    for (const node of [...nodes.values()]) await node.close().catch(() => {})
    nodes.clear()
    await testnet?.destroy()
    testnet = null
    return { destroyed: true }
  }
}

let buffer = ''
process.stdin.setEncoding('utf8')
process.stdin.on('data', chunk => {
  buffer += chunk
  for (let i = buffer.indexOf('\n'); i !== -1; i = buffer.indexOf('\n')) {
    const line = buffer.slice(0, i)
    buffer = buffer.slice(i + 1)
    if (!line.trim()) continue
    let message
    try {
      message = JSON.parse(line)
    } catch {
      continue
    }
    const run = ops[message.op]
    Promise.resolve(run ? run(message) : Promise.reject(new Error('unknown op: ' + message.op)))
      .then(result => out({ id: message.id, ok: true, result }))
      .catch(error => out({ id: message.id, ok: false, error: String(error?.stack || error) }))
  }
})
process.stdin.on('end', async () => {
  await ops.destroy().catch(() => {})
  process.exit(0)
})

out({ ready: true })
