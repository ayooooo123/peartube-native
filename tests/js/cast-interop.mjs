// Interop driver for the Rust cast port.
//
//   node cast-interop.mjs controller-http <host> <port> <code>
//   node cast-interop.mjs controller-noise <host> <port> <publicKey>
//   node cast-interop.mjs receiver-http
//   node cast-interop.mjs receiver-noise
//
// Controller modes print one JSON result on stdout. Receiver modes print `READY {json}` and then
// stay up until killed. The app's own `src/cast.js` and `src/qr.js` are imported from PEARTUBE_APP
// (default: a sibling checkout), so both directions run the real implementation.
import { mkdtempSync, writeFileSync, rmSync } from 'node:fs'
import { join, resolve } from 'node:path'
import { tmpdir } from 'node:os'
import { fileURLToPath } from 'node:url'
import net from 'node:net'

const here = fileURLToPath(new URL('.', import.meta.url))
const APP = process.env.PEARTUBE_APP || resolve(here, '../../../../peartube')
const cast = await import(join(APP, 'src/cast.js'))

const DEVICE_ID = '0123456789abcdef0123456789abcdef'
const SEED = 'ab'.repeat(32)
const PAIR_TOKEN = 'cd'.repeat(16)
const PAIR_CODE = 'ABC123'
const NAME = 'Kitchen TV'

const [mode, ...args] = process.argv.slice(2)

function identityDir () {
  const dir = mkdtempSync(join(tmpdir(), 'peartube-cast-interop-'))
  writeFileSync(join(dir, 'receiver.json'), JSON.stringify({
    deviceId: DEVICE_ID,
    name: NAME,
    seed: SEED,
    pairToken: PAIR_TOKEN,
    pairCode: PAIR_CODE
  }, null, 2), { mode: 0o600 })
  return dir
}

function tcpDht (host, port) {
  return {
    connect: () => net.connect(port, host)
  }
}

function tcpNoiseServer () {
  let server = null
  return {
    createServer (onConnection) {
      server = net.createServer(sock => {
        sock.on('error', () => {})
        onConnection(sock)
      })
      return {
        listen: async () => {
          await new Promise((res, rej) => {
            server.once('error', rej)
            server.listen(0, '127.0.0.1', res)
          })
          return { port: server.address().port }
        },
        close: async () => { server.close() }
      }
    },
    get port () { return server?.address()?.port || 0 }
  }
}

async function controllerOver (dht) {
  const storage = mkdtempSync(join(tmpdir(), 'peartube-cast-controller-'))
  const node = dht ? { dht, swarm: { relayThrough: null }, peerKey: 'e'.repeat(64) } : null
  const controller = cast.createCastController({
    storage,
    getNode: () => node,
    ensureNode: null,
    getLanHost: () => null,
    apiPort: 8174
  })
  const record = dht
    ? await controller.pairReceiver({ publicKey: args[2] })
    : await controller.pairReceiver({ host: args[0], port: Number(args[1]), code: args[2] })
  const attached = await controller.castAttach({ receiverId: record.id })
  const play = await controller.castControl({ action: 'play' })
  const seek = await controller.castControl({ action: 'seek', positionMs: 3000 })
  const paused = await controller.castControl({ action: 'pause' })
  const stop = await controller.castStop({})
  await controller.close()
  rmSync(storage, { recursive: true, force: true })
  return {
    record,
    attach: attached.session,
    play: play.session.state,
    seek: seek.session.state,
    paused: paused.session.state,
    stopped: stop.session
  }
}

async function receiverOver (dht) {
  const storage = identityDir()
  const node = dht
    ? {
        dht,
        peerKey: 'd'.repeat(64),
        status: () => ({ peers: 0, lanPeers: 0, routes: null })
      }
    : null
  const receiver = await cast.createReceiver({
    storage,
    name: NAME,
    port: 0,
    bindHost: '127.0.0.1',
    openAuth: true,
    anyPort: true,
    node,
    allowPeerNode: Boolean(node)
  })
  const info = receiver.info()
  const httpPort = receiver.port
  process.stdout.write('READY ' + JSON.stringify({
    httpPort,
    noisePort: dht ? dht.port : 0,
    publicKey: info.publicKey,
    pairCode: info.pairCode,
    deviceId: info.deviceId
  }) + '\n')
  const done = () => {
    Promise.resolve(receiver.close()).finally(() => {
      rmSync(storage, { recursive: true, force: true })
      process.exit(0)
    })
  }
  process.on('SIGTERM', done)
  process.on('SIGINT', done)
  setInterval(() => {}, 1 << 30)
}

switch (mode) {
  case 'controller-http': {
    process.stdout.write(JSON.stringify(await controllerOver(null)) + '\n')
    process.exit(0)
  }
  case 'controller-noise': {
    process.stdout.write(JSON.stringify(await controllerOver(tcpDht(args[0], Number(args[1])))) + '\n')
    process.exit(0)
  }
  case 'receiver-http':
    await receiverOver(null)
    break
  case 'receiver-noise':
    await receiverOver(tcpNoiseServer())
    break
  default:
    throw new Error(`unknown mode ${mode}`)
}
