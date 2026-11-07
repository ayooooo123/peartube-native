// Interop driver for the Rust private-overlay port.
//
//   node private-overlay-interop.mjs endpoint     # JS peer + 2 JS guards; prints READY, then RESULT
//   node private-overlay-interop.mjs self-check   # two JS peers + 2 JS guards, no Rust side
//
// The app's own `src/private-overlay.js` runs on a hyperdht testnet. `endpoint` prints one JSON
// line `READY {bootstrap,sourceKey,peerPublicKey}` and then waits for the Rust peer: both sides
// send one `source-request` over the rendezvoused Noise stream and answer the other's, then the
// script prints `RESULT {jsAnsweredRust, jsRequestAcknowledged}` and exits.
import { createRequire } from 'node:module'
import { join, resolve } from 'node:path'
import { fileURLToPath } from 'node:url'

const here = fileURLToPath(new URL('.', import.meta.url))
const APP = process.env.PEARTUBE_APP || resolve(here, '../../../../peartube')
const require = createRequire(join(APP, 'package.json'))

const b4a = require('b4a')
const NoiseSecretStream = require('@hyperswarm/secret-stream')
const createTestnet = require(join(APP, 'node_modules/hyperdht/testnet.js'))

const overlay = await import(join(APP, 'src/private-overlay.js'))

const SOURCE_KEY = b4a.from('11'.repeat(32), 'hex')
const SOURCE_ID = 'interop'
const TIMEOUT_MS = 90_000

function emit (kind, value) {
  process.stdout.write(kind + ' ' + JSON.stringify(value) + '\n')
}

function lineReader (stream, onLine) {
  let buffered = ''
  stream.on('data', chunk => {
    buffered += chunk.toString()
    while (true) {
      const at = buffered.indexOf('\n')
      if (at === -1) break
      onLine(buffered.slice(0, at))
      buffered = buffered.slice(at + 1)
    }
  })
}

/// Both peers run the same exchange: ask, answer, wait for the answer to our ask.
function runExchange (stream, label) {
  const state = { answeredRust: false, acknowledged: false }
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error(`${label}: timed out waiting for the exchange`)), TIMEOUT_MS)
    timer.unref?.()
    stream.on('error', () => {})
    lineReader(stream, line => {
      let message
      try { message = JSON.parse(line) } catch { return }
      if (message.type === 'source-request') {
        stream.write(JSON.stringify({ type: 'source-request-ack', from: label, echo: message.from }) + '\n')
        state.answeredRust = true
      } else if (message.type === 'source-request-ack') {
        state.acknowledged = true
      }
      if (state.answeredRust && state.acknowledged) {
        clearTimeout(timer)
        resolve(state)
      }
    })
    stream.write(JSON.stringify({ type: 'source-request', from: label }) + '\n')
  })
}

async function start (host, bootstrap, seed) {
  const guards = []
  for (const [index, guardSeed] of [seed(1), seed(2)].entries()) {
    const guard = await overlay.createPrivateGuard({
      host,
      port: 0,
      bootstrap,
      seed: guardSeed,
      onControlPeer: peer => {
        process.stderr.write(`guard ${index}: ${peer.role} ${peer.remotePublicKey.slice(0, 12)}\n`)
      }
    })
    guards.push(guard)
  }
  const swarm = await overlay.createPrivateSwarm({
    host,
    port: [0, 0],
    bootstrap,
    keyPair: NoiseSecretStream.keyPair(seed(3))
  })
  return { guards, swarm }
}

const [mode = 'endpoint'] = process.argv.slice(2)

const testnet = await createTestnet(3, { host: '127.0.0.1' })
const host = '127.0.0.1'
const seed = n => b4a.alloc(32, n)

try {
  if (mode === 'endpoint') {
    const { guards, swarm } = await start(host, testnet.bootstrap, seed)
    let done = null
    swarm.on('connection', enc => {
      process.stderr.write('js endpoint: connection from ' + b4a.toString(enc.remotePublicKey, 'hex').slice(0, 12) + '\n')
      if (done) return
      done = runExchange(enc, 'js')
      done.then(state => {
        emit('RESULT', state)
        finish(0)
      }).catch(error => {
        emit('RESULT', { error: error.message })
        finish(1)
      })
    })
    swarm.joinSource(SOURCE_ID, SOURCE_KEY)
    emit('READY', {
      bootstrap: testnet.bootstrap,
      sourceKey: b4a.toString(SOURCE_KEY, 'hex'),
      peerPublicKey: b4a.toString(swarm.keyPair.publicKey, 'hex'),
      guardKeys: guards.map(guard => b4a.toString(guard.publicKey, 'hex'))
    })
  } else if (mode === 'self-check') {
    // Two JS peers, same code path as the Rust test: proves the harness itself works.
    const a = await start(host, testnet.bootstrap, seed)
    const b = await start(host, testnet.bootstrap, n => b4a.alloc(32, n + 16))
    const results = []
    const once = (swarm, label) => new Promise((resolve, reject) => {
      const timer = setTimeout(() => reject(new Error(label + ': no connection')), TIMEOUT_MS)
      timer.unref?.()
      swarm.on('connection', enc => {
        clearTimeout(timer)
        runExchange(enc, label).then(resolve, reject)
      })
    })
    const both = Promise.all([once(a.swarm, 'a'), once(b.swarm, 'b')])
    a.swarm.joinSource(SOURCE_ID, SOURCE_KEY)
    b.swarm.joinSource(SOURCE_ID, SOURCE_KEY)
    results.push(...await both)
    emit('RESULT', { peers: results })
    finish(0)
  } else {
    emit('RESULT', { error: `unknown mode ${mode}` })
    finish(2)
  }
} catch (error) {
  emit('RESULT', { error: error.stack || error.message })
  finish(1)
}

function finish (code) {
  Promise.resolve(testnet.destroy()).finally(() => process.exit(code))
}
