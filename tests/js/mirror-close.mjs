// Run the app's host and its real traffic gate on a local DHT. Hold one stop until after the
// caller has had a turn: it is then possible to tell whether close returned or detached early.
import { mkdtempSync, rmSync } from 'node:fs'
import { createRequire } from 'node:module'
import { tmpdir } from 'node:os'
import { dirname, join, resolve } from 'node:path'
import { fileURLToPath, pathToFileURL } from 'node:url'

const app = process.env.PEARTUBE_APP || resolve(dirname(fileURLToPath(import.meta.url)), '../../../peartube')
const load = path => import(pathToFileURL(join(app, path)).href)
const { createNode } = await load('src/index.js')
const { createMirrorHost } = await load('src/mirror.js')
const require = createRequire(join(app, 'package.json'))
const { default: createTestnet } = await import(pathToFileURL(require.resolve('hyperdht/testnet.js')).href)

const reason = e => e instanceof AggregateError
  ? { message: e.message, errors: e.errors.map(reason) }
  : e && { message: e.message, status: e.status ?? null }
const result = p => p.then(() => null, e => e)

const storage = mkdtempSync(join(tmpdir(), 'pt-close-oracle-'))
const testnet = await createTestnet(3)
let node
try {
  node = await createNode({ storage, bootstrap: testnet.bootstrap })
  const on = node.onConnection.bind(node)
  const traces = []
  for (const stops of [
    ['fail', 'slow'], ['throw', 'slow'], ['throw', 'fail', 'slow'],
    ['late failure', 'fail', 'slow'], ['reenter', 'fail'], ['slow']
  ]) {
    const listeners = new Set()
    node.onConnection = fn => {
      listeners.add(fn)
      const off = on(fn)
      return () => { listeners.delete(fn); off() }
    }
    const host = await createMirrorHost({ node, storage, acquirer: null, pollIntervalMs: 0 })
    let release, fromStop, reentered = false, settled = 0
    const gate = new Promise(resolve => { release = resolve })
    const started = [], finished = []
    for (const name of stops) host.register({ name, stop () {
      started.push(name)
      if (name === 'reenter' && !reentered) {
        reentered = true
        fromStop = host.close()
      }
      if (name === 'throw') throw new Error('not async')
      return (async () => {
        if (['slow', 'late failure', 'reenter'].includes(name)) await gate
        finished.push(name)
        if (name === 'fail') throw Object.assign(new Error('disk gone'), { status: 507 })
        if (name === 'late failure') throw new Error('late failure')
      })()
    } })
    const first = host.close()
    const atReturn = started.slice()
    const callers = [first, host.close()]
    if (fromStop) callers.push(fromStop)
    const done = callers.map(p => result(p).then(e => { settled++; return e }))
    await new Promise(resolve => setImmediate(resolve))
    const before = { settled, listening: listeners.size > 0, started: started.slice(), atReturn }
    release()
    const results = await Promise.all(done)
    results.push(await result(host.close()))
    traces.push({ stops, before, callers: results.length, same: results.every(e => e === results[0]),
      started, finished, listening: listeners.size > 0, error: reason(results[0]) })
  }
  process.stdout.write(JSON.stringify(traces))
} finally {
  await node?.close()
  await testnet.destroy()
  rmSync(storage, { recursive: true, force: true })
}
