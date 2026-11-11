// Runs the app's own `src/traffic.js` gate against a scripted, fake-clock
// scenario, and the `peartube/bulk-busy` peer for the Rust interop test.
//
// usage: node traffic.mjs decisions < scenario.json   -> decision log on stdout
//        node traffic.mjs peer                        -> TCP secret-stream peer,
//                                                        prints "port N" then "open"
//                                                        and "msg <v>" lines
import c from 'compact-encoding'
import net from 'node:net'
import { readFileSync } from 'node:fs'
import SecretStream from '@hyperswarm/secret-stream'

const PROTOCOL = 'peartube/bulk-busy'
const log = (line) => process.stdout.write(line + '\n')
const appRoot = new URL(
  (process.env.PEARTUBE_APP || '../../../../peartube').replace(/\/*$/, '/'),
  import.meta.url
)

// The app's traffic.js imports its own protomux copy; patch that very module.
const { default: Protomux } = await import(new URL('node_modules/protomux/index.js', appRoot))
const { createTrafficGate } = await import(new URL('src/traffic.js', appRoot))

const [mode] = process.argv.slice(2)

if (mode === 'decisions') {
  const scenario = JSON.parse(readFileSync(0, 'utf8'))
  const decisions = await runScenario(scenario)
  process.stdout.write(JSON.stringify(decisions) + '\n')
} else if (mode === 'peer') {
  peer()
} else {
  console.error('usage: node traffic.mjs decisions|peer')
  process.exit(2)
}

async function runScenario (scenario) {
  // Fake channels that never open: the decision table is about byte counters.
  const fakeMux = {
    pair () {},
    opened () { return false },
    createChannel () { return { addMessage: () => ({ send () {} }), open () {} } }
  }
  Protomux.from = () => fakeMux

  let now = 0
  let tickFn = null
  Date.now = () => now
  globalThis.setInterval = (fn) => { tickFn = fn; return { unref () {} } }
  globalThis.clearInterval = () => { tickFn = null }

  const conns = new Map()
  const events = []
  const marks = []
  const listeners = new Set()
  let httpViewing = false
  let gate = null
  // A `peartube/bulk-busy` claim counts only from a peer that served this node blocks; this fake
  // node names that set explicitly so the decision table can cover both answers.
  const sourcePeers = new Set()
  const node = {
    privateRouting: false,
    peerConnections: () => [...conns.values()],
    onConnection: (cb) => { listeners.add(cb); return () => listeners.delete(cb) },
    httpViewing: () => httpViewing,
    markBulkPeer: (hex, bulk) => marks.push([hex, bulk]),
    isSourcePeer: (hex) => sourcePeers.has(hex)
  }

  gate = createTrafficGate({ node, uploadMbps: scenario.uploadMbps })
  const conn = (id) => conns.get(String(id))
  const steps = []
  for (const step of scenario.steps) {
    if (step.ms) now += step.ms
    switch (step.op) {
      case 'conn': {
        const created = {
          id: step.id,
          destroyed: false,
          rawBytesWritten: 0,
          remotePublicKey: step.key ? Buffer.from(step.key, 'hex') : null,
          pause () { events.push(`${step.id}:pause`) },
          resume () { events.push(`${step.id}:resume`) }
        }
        conns.set(String(step.id), created)
        for (const cb of listeners) cb(created)
        break
      }
      case 'write': conn(step.id).rawBytesWritten += step.bytes; break
      case 'tick': tickFn?.(); break
      case 'markBulk': gate.markBulk(conn(step.id)); break
      case 'unmarkBulk': gate.unmarkBulk(conn(step.id)); break
      case 'destroy': conn(step.id).destroyed = true; break
      case 'viewing': httpViewing = Boolean(step.value); break
      case 'sourcePeer': sourcePeers.add(step.key); break
      case 'stop': gate.stop(); break
      default: throw new Error('unknown op ' + step.op)
    }
    steps.push({ events: events.splice(0), marks: marks.splice(0), info: gate.info() })
  }
  return { steps }
}

function peer () {
  const server = net.createServer((socket) => {
    const stream = new SecretStream(false, socket)
    const mux = Protomux.from(stream)
    const create = () => {
      if (mux.opened({ protocol: PROTOCOL })) return
      const channel = mux.createChannel({
        protocol: PROTOCOL,
        id: null,
        messages: [{
          encoding: c.uint,
          onmessage (v) {
            log('msg ' + v)
            channel.messages[0].send(v)
          }
        }],
        onopen () { log('open') }
      })
      if (channel) channel.open()
    }
    mux.pair({ protocol: PROTOCOL }, create)
    create()
    stream.on('error', (err) => { console.error(err); process.exit(4) })
  })
  server.listen(0, '127.0.0.1', () => log('port ' + server.address().port))
}
