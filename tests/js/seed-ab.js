// Loopback seeder paired with download_ab, like examples/seed_ab.rs: publish a file through node.put, serve until stdin closes.
// Runs under Bare from a JS app copy's bin/ (`bare bin/seed-ab.js <file>`); the app's import map resolves node:*.
import 'bare-process/global'
import { createReadStream, mkdtempSync, rmSync } from 'node:fs'
import { createNode } from '../src/index.js'
import { createLan } from '../src/lan.js'

const storage = mkdtempSync('/tmp/pt-seed-ab-js-')
const stop = async () => {}
const adapter = { advertise: async () => ({ stop }), browse: async () => ({ stop }) }
const node = await createNode({ storage, bootstrap: [{ host: '127.0.0.1', port: 9 }], worklet: false, streamHost: '127.0.0.1', streamPort: 0,
  lan: keyPair => createLan({ host: '127.0.0.1', keyPair, adapter }) })
const entry = await node.put({ id: 'imdb:tt0000001', title: 'Throughput A/B' }, createReadStream(process.argv[2]))
console.log(JSON.stringify({ event: 'seed', pid: process.pid, peer: node.lanEndpoint(), entry }))
process.stdin.resume()
await new Promise(resolve => process.stdin.once('end', resolve))
await node.close()
rmSync(storage, { recursive: true, force: true })
Bare.exit(0)
