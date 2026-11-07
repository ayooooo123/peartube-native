// PearTube LAN mDNS helper for the Rust port.
// The record/token/parse functions below are copied from the app's `src/lan.js`
// (TOPIC_PREFIX, TOPICS_PER_TXT, MAX_SHARD_TOPICS, MAX_TXT_BYTES, toBase64Url,
// topicToken, txtStr, buildTxt, encodedTxtBytes, createRecords, parseRecord,
// serviceIPv4), with `b4a` swapped for `Buffer` and nothing else changed, so
// fixtures and interop checks come from the app's own code.
//
// Usage:
//   node lan-mdns.mjs fixtures            -> JSON fixture on stdout
//   node lan-mdns.mjs interop '<json>'    -> NDJSON events, stdin drives lifecycle
import { createHash } from 'node:crypto'
import { isIPv4 } from 'node:net'
import { createInterface } from 'node:readline'
import { Bonjour } from 'bonjour-service'

const SERVICE_TYPE = 'hyperdht-mdns'
const SERVICE_PROTOCOL = 'udp'
const TOPIC_PREFIX = Buffer.from('hyperdht-mdns:')
const TOPICS_PER_TXT = 5
const MAX_SHARD_TOPICS = 32
const MAX_TXT_BYTES = 900

function toBase64Url (buf) {
  return buf.toString('base64').replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/, '')
}

function topicToken (topic) {
  return toBase64Url(createHash('sha256').update(TOPIC_PREFIX).update(topic).digest())
}

function txtStr (v) {
  if (Buffer.isBuffer(v)) return v.toString()
  return typeof v === 'string' || typeof v === 'number' ? String(v) : ''
}

function buildTxt (peerKeyHex, port, tokens) {
  const txt = { v: '1', peerKey: peerKeyHex, port: String(port), tc: String(tokens.length) }
  for (let i = 0; i < tokens.length; i += TOPICS_PER_TXT) {
    txt[`t${i / TOPICS_PER_TXT}`] = tokens.slice(i, i + TOPICS_PER_TXT).join(',')
  }
  return txt
}

function encodedTxtBytes (txt) {
  let sum = 0
  for (const [k, v] of Object.entries(txt)) sum += Buffer.byteLength(k) + Buffer.byteLength(String(v)) + 2
  return sum
}

function createRecords (peerKeyHex, port, tokens, nonce) {
  const sorted = tokens.slice().sort()
  const shards = []
  if (sorted.length === 0) {
    shards.push([])
  } else {
    let cur = []
    for (const tok of sorted) {
      const next = [...cur, tok]
      if (next.length > MAX_SHARD_TOPICS || encodedTxtBytes(buildTxt(peerKeyHex, port, next)) > MAX_TXT_BYTES) {
        shards.push(cur)
        cur = [tok]
      } else {
        cur = next
      }
    }
    shards.push(cur)
  }
  const genHash = createHash('sha256')
  for (const tok of sorted) genHash.update(tok).update('\0')
  const gen = (toBase64Url(genHash.digest()).slice(0, 8) + nonce).slice(0, 11)
  return shards.map((shard, idx) => ({
    name: `hyperdht-mdns-${peerKeyHex.slice(0, 12)}-${gen}-${idx}-${shards.length}`,
    type: SERVICE_TYPE,
    protocol: SERVICE_PROTOCOL,
    port,
    txt: buildTxt(peerKeyHex, port, shard)
  }))
}

function parseRecord (service) {
  const txt = service?.txt
  if (!txt || txtStr(txt.v) !== '1') return null
  const peerKeyHex = txtStr(txt.peerKey).toLowerCase()
  const port = Number(service.port || txtStr(txt.port))
  const tc = Number(txtStr(txt.tc) || 0)
  if (!/^[0-9a-f]{64}$/.test(peerKeyHex)) return null
  if (!Number.isInteger(port) || port <= 0 || port > 65535) return null
  if (!Number.isInteger(tc) || tc < 0 || tc > MAX_SHARD_TOPICS) return null
  const tokens = []
  for (let i = 0; tokens.length < tc; i++) {
    const chunk = txtStr(txt[`t${i}`])
    if (!chunk) return null
    tokens.push(...chunk.split(','))
  }
  if (tokens.length !== tc || tokens.some(t => !/^[A-Za-z0-9_-]{43}$/.test(t))) return null
  let generation = null
  let shard = 0
  let totalShards = 1
  const prefix = `hyperdht-mdns-${peerKeyHex.slice(0, 12)}-`
  if (typeof service.name === 'string' && service.name.startsWith(prefix)) {
    const m = /^([A-Za-z0-9_-]{11})-(\d+)-(\d+)$/.exec(service.name.slice(prefix.length))
    if (m) {
      generation = m[1]
      shard = Number(m[2])
      totalShards = Math.max(1, Number(m[3]) || 1)
    }
  }
  return { peerKeyHex, peerKey: Buffer.from(peerKeyHex, 'hex'), port, tokens, generation, shard, totalShards }
}

function serviceIPv4 (service) {
  if (Array.isArray(service?.addresses)) {
    const addr = service.addresses.find(a => typeof a === 'string' && isIPv4(a))
    if (addr) return addr
  }
  const ref = service?.referer?.address
  return typeof ref === 'string' && isIPv4(ref) ? ref : null
}

function sha256Hex (s) {
  return createHash('sha256').update(s).digest('hex')
}

function fixtures () {
  // 20 deterministic inputs: topic counts walk 0..19 so empty, per-chunk
  // (5, 10, 15) and multi-shard (16+) records are all covered.
  const nonces = ['a0z', '001', 'zzz', 'q9x', 'k7m']
  const ports = [4100, 4321, 5000, 65535, 8080]
  const cases = []
  for (let i = 0; i < 20; i++) {
    const peerKey = sha256Hex(`peartube-peer-${i}`)
    const port = ports[i % ports.length]
    const nonce = nonces[i % nonces.length]
    const topics = Array.from({ length: i }, (_, k) => `topic-${i}-${k}`)
    const tokens = topics.map(t => topicToken(Buffer.from(t))).sort()
    const records = createRecords(peerKey, port, tokens, nonce)
    const parsed = records.map(r => {
      const p = parseRecord({ name: r.name, port: r.port, txt: r.txt })
      return p ? { generation: p.generation, shard: p.shard, totalShards: p.totalShards } : null
    })
    cases.push({ peerKey, port, nonce, topics, records, parsed })
  }
  const negative = [
    ['v2', 4100, { v: '2', peerKey: sha256Hex('neg-0'), port: '4100', tc: '0' }],
    ['short-peer-key', 4100, { v: '1', peerKey: 'abcd', port: '4100', tc: '0' }],
    ['txt-port-ignored-when-service-port-set', 4100, { v: '1', peerKey: sha256Hex('neg-2'), port: '0', tc: '0' }],
    ['txt-port-used-when-service-port-missing', 0, { v: '1', peerKey: sha256Hex('neg-3'), port: '4500', tc: '0' }],
    ['no-port-anywhere', 0, { v: '1', peerKey: sha256Hex('neg-4'), port: '0', tc: '0' }],
    ['tc-too-big', 4100, { v: '1', peerKey: sha256Hex('neg-5'), port: '4100', tc: '33' }],
    ['missing-chunk', 4100, { v: '1', peerKey: sha256Hex('neg-6'), port: '4100', tc: '2', t0: 'a'.repeat(43) }],
    ['short-token', 4100, { v: '1', peerKey: sha256Hex('neg-7'), port: '4100', tc: '1', t0: 'tooshort' }],
    ['tc-mismatch', 4100, { v: '1', peerKey: sha256Hex('neg-8'), port: '4100', tc: '1', t0: `${'a'.repeat(43)},${'b'.repeat(43)}` }]
  ].map(([name, servicePort, txt]) => ({ name, servicePort, txt, parsed: parseRecord({ name, port: servicePort, txt }) }))
  return { cases, negative }
}

function emit (obj) {
  process.stdout.write(JSON.stringify(obj) + '\n')
}

async function interop (cfg) {
  const bonjour = new Bonjour(undefined, err => emit({ t: 'error', message: String(err?.message || err) }))
  const tokens = cfg.topics.map(t => topicToken(Buffer.from(t)))
  const records = createRecords(cfg.peerKey, cfg.port, tokens, cfg.nonce)
  const services = []
  const stopAdvertising = () => Promise.all(services.splice(0).map(svc => new Promise(resolve => svc.stop(resolve))))

  try {
    for (const rec of records) services.push(bonjour.publish(rec))
  } catch (err) {
    emit({ t: 'error', message: String(err?.message || err) })
    process.exit(1)
  }
  emit({ t: 'published', peerKey: cfg.peerKey, port: cfg.port, records: records.map(r => r.name) })

  const describe = svc => {
    const p = parseRecord(svc)
    return {
      t: 'up',
      name: svc.name,
      peerKey: p?.peerKeyHex ?? null,
      port: p?.port ?? null,
      topics: p ? p.tokens : null,
      generation: p?.generation ?? null,
      shard: p?.shard ?? null,
      totalShards: p?.totalShards ?? null,
      host: serviceIPv4(svc)
    }
  }
  const browser = bonjour.find({ type: SERVICE_TYPE, protocol: SERVICE_PROTOCOL })
  browser.on('up', svc => emit(describe(svc)))
  browser.on('down', svc => emit({ t: 'down', name: svc.name, peerKey: parseRecord(svc)?.peerKeyHex ?? null, port: svc.port ?? null }))

  const rl = createInterface({ input: process.stdin })
  for await (const line of rl) {
    if (line.trim() === 'stop-publish') {
      await stopAdvertising()
      emit({ t: 'unpublished', peerKey: cfg.peerKey })
    } else if (line.trim() === 'exit') {
      break
    }
  }
  browser.stop()
  await stopAdvertising()
  bonjour.destroy()
  process.exit(0)
}

const [mode, arg] = process.argv.slice(2)
if (mode === 'fixtures') {
  process.stdout.write(JSON.stringify(fixtures()) + '\n')
} else if (mode === 'interop') {
  await interop(JSON.parse(arg))
} else {
  process.stderr.write('usage: lan-mdns.mjs fixtures|interop <json>\n')
  process.exit(2)
}
