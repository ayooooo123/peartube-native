// A JSON-lines harness that runs the app's own cast stack (`src/cast.js`, `src/cast-control.js`
// and `test/receiver-fixture.js`) on a local HyperDHT testnet, so a Rust test can drive the real
// JS controller and the real JS receiver.
//
//   node cast-interop.mjs serve [testnet-size]
//
// Startup prints one line: {"bootstrap": [...]}. Then each stdin line is one command
// {"id": N, "op": "...", ...} and each stdout line is {"id": N, "ok": true, "result": ...} or
// {"id": N, "ok": false, "error": "..."}. Closing stdin stops the process.
import { createRequire } from 'node:module'
import { dirname, join, resolve } from 'node:path'
import { fileURLToPath, pathToFileURL } from 'node:url'
import { spawn } from 'node:child_process'
import { createHash, randomBytes } from 'node:crypto'
import { createInterface } from 'node:readline'
import { mkdtempSync, readFileSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { Readable } from 'node:stream'

const here = dirname(fileURLToPath(import.meta.url))
const APP = process.env.PEARTUBE_APP || resolve(here, '../../../../peartube')
const require = createRequire(resolve(APP, 'package.json'))

const { default: createTestnet } = await import(pathToFileURL(require.resolve('hyperdht/testnet.js')).href)
const { createNode } = await import(pathToFileURL(resolve(APP, 'src/index.js')).href)
const { createCastController } = await import(pathToFileURL(resolve(APP, 'src/cast.js')).href)
const { openControl } = await import(pathToFileURL(resolve(APP, 'src/cast-control.js')).href)

const tmp = label => mkdtempSync(join(tmpdir(), `peartube-cast-${label}-`))
const sha256 = bytes => createHash('sha256').update(bytes).digest('hex')

function out (value) {
  process.stdout.write(JSON.stringify(value) + '\n')
}

const testnet = await createTestnet(Number(process.argv[3]) || 3)
const bootstrap = testnet.bootstrap
const nodes = new Map()
const controllers = new Map()
const receivers = new Map()
const strangers = new Map()
let nextNode = 1

async function node (name) {
  const value = nodes.get(name)
  if (!value) throw new Error('unknown node: ' + name)
  return value
}

async function controller (name) {
  const value = controllers.get(name)
  if (!value) throw new Error('unknown controller: ' + name)
  return value
}

function waitLine (stream, timeoutMs = 60_000) {
  return new Promise((resolve, reject) => {
    const rl = createInterface({ input: stream })
    const timer = setTimeout(() => reject(new Error('no line from the receiver fixture')), timeoutMs)
    rl.once('line', line => {
      clearTimeout(timer)
      rl.close()
      resolve(JSON.parse(line))
    })
  })
}

const ops = {
  // The app's own sender: a node plus `createCastController` over it.
  async jsController ({ storage = tmp('controller') } = {}) {
    const name = `js-${nextNode++}`
    const peer = await createNode({ storage: tmp('node'), bootstrap })
    const cast = createCastController({
      storage,
      getNode: () => peer,
      getLanHost: () => null
    })
    await cast.setLanHost(null).catch(() => {})
    nodes.set(name, peer)
    controllers.set(name, cast)
    return { name, publicKey: peer.peerKey }
  },
  // The app's own TV: the receiver fixture in its own process, on the same testnet.
  async jsReceiver ({ storage = tmp('receiver'), name = 'Bedroom Roku TV', open = false } = {}) {
    const boot = bootstrap.map(entry => `${entry.host}:${entry.port}`).join(',')
    const args = [
      resolve(APP, 'test/receiver-fixture.js'),
      '--port', '0',
      '--name', name,
      '--storage', storage,
      '--bootstrap', boot
    ]
    if (open) args.push('--open')
    const child = spawn(process.execPath, args, { stdio: ['ignore', 'pipe', 'inherit'] })
    const ready = await waitLine(child.stdout)
    const key = `fixture-${nextNode++}`
    receivers.set(key, { child, storage, ready })
    return { ...ready, storage, name, publicKey: ready.publicKey, pairCode: ready.pairCode }
  },
  // A peer that connects to a receiver without pairing, to prove control is refused.
  async stranger ({ publicKey } = {}) {
    const name = `stranger-${nextNode++}`
    const peer = await createNode({ storage: tmp('stranger'), bootstrap })
    const conn = await new Promise((resolve, reject) => {
      const timer = setTimeout(() => reject(new Error('the stranger did not connect')), 60_000)
      peer.swarm.once('connection', connection => {
        clearTimeout(timer)
        connection.on('error', () => {})
        resolve(connection)
      })
      peer.swarm.joinPeer(Buffer.from(publicKey, 'hex'))
    })
    const control = await openControl(conn)
    const errors = {}
    try {
      await control.call('cmd', { action: 'stop' })
      errors.cmd = null
    } catch (err) {
      errors.cmd = err.message
    }
    for (let i = 0; i < 6; i++) {
      try {
        await control.call('pair', { code: '000000' })
        errors.pair = null
      } catch (err) {
        errors.pair = err.message
      }
    }
    try {
      await control.call('pair', { code: 'ABC123' })
      errors.locked = null
    } catch (err) {
      errors.locked = err.message
    }
    strangers.set(name, { peer, control })
    return errors
  },
  async publish ({ node: name = '', size = 2 * 1024 * 1024 + 333 } = {}) {
    const peer = await node(name)
    const bytes = randomBytes(size)
    const id = `imdb:tt${String(Math.floor(Math.random() * 9_000_000) + 1_000_000)}`
    const published = await peer.publish({ id, title: 'Interop Film' }, Readable.from([bytes]))
    return { key: published.key, id, title: 'Interop Film', size: bytes.length, sha256: sha256(bytes) }
  },
  async castPair ({ controller: name, params }) {
    const cast = await controller(name)
    return cast.pairReceiver(params)
  },
  async castPlay ({ controller: name, params }) {
    const cast = await controller(name)
    return cast.castPlay(params)
  },
  async castAttach ({ controller: name, params }) {
    const cast = await controller(name)
    return cast.castAttach(params)
  },
  async castControl ({ controller: name, params }) {
    const cast = await controller(name)
    return cast.castControl(params)
  },
  async castStop ({ controller: name, params }) {
    const cast = await controller(name)
    return cast.castStop(params || {})
  },
  async castStatus ({ controller: name }) {
    const cast = await controller(name)
    return cast.status()
  },
  async lastPlayback ({ storage }) {
    try {
      return JSON.parse(readFileSync(join(storage, 'last-playback.json'), 'utf8'))
    } catch {
      return null
    }
  },
  async close () {
    for (const { child } of receivers.values()) child.kill('SIGTERM')
    for (const { control, peer } of strangers.values()) {
      control.close()
      await peer.close().catch(() => {})
    }
    for (const cast of controllers.values()) await cast.close().catch(() => {})
    for (const peer of nodes.values()) await peer.close().catch(() => {})
    return { closed: true }
  }
}

let pending = ''
process.stdin.on('data', chunk => {
  pending += chunk.toString()
  for (let index = pending.indexOf('\n'); index !== -1; index = pending.indexOf('\n')) {
    const line = pending.slice(0, index).trim()
    pending = pending.slice(index + 1)
    if (!line) continue
    let message
    try {
      message = JSON.parse(line)
    } catch {
      out({ id: null, ok: false, error: 'Malformed command' })
      continue
    }
    const run = ops[message.op]
    if (!run) {
      out({ id: message.id, ok: false, error: `Unknown op ${message.op}` })
      continue
    }
    Promise.resolve()
      .then(() => run(message.params || {}))
      .then(
        result => out({ id: message.id, ok: true, result: result ?? null }),
        err => out({ id: message.id, ok: false, error: err?.message || String(err) })
      )
  }
})

process.stdin.on('end', () => {
  ops.close().finally(() => {
    testnet.destroy().finally(() => process.exit(0))
  })
})

out({ bootstrap })
