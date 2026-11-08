// Loopback-only JS Corestore seeder for the cache ceiling HTTP regression.
import { createRequire } from 'node:module'
import net from 'node:net'
import readline from 'node:readline'
const require = createRequire(process.env.PEARTUBE_APP + '/package.json')
const Corestore = require('corestore')
const SecretStream = require('@hyperswarm/secret-stream')
const [dir, budgetText, blockText] = process.argv.slice(2)
const budget = Number(budgetText)
const blockSize = Number(blockText)
const store = new Corestore(dir)
const cores = []
for (const [name, size] of [['old', budget - 16 * 1024 * 1024], ['movie', Number(process.env.CACHE_FREEZE_MOVIE_BYTES) || budget * 8]]) {
  const core = store.get({ name })
  await core.ready()
  const count = size / blockSize
  for (let start = 0; start < count; start += 16) {
    const batch = []
    for (let i = start; i < Math.min(count, start + 16); i++) {
      const block = Buffer.allocUnsafe(blockSize)
      let x = i + 1
      for (let j = 0; j < blockSize; j += 4) {
        x ^= x << 13
        x ^= x >>> 17
        x ^= x << 5
        block.writeUInt32LE(x >>> 0, j)
      }
      batch.push(block)
    }
    await core.append(batch)
  }
  cores.push({ name, key: core.key.toString('hex'), length: core.length })
}
const server = net.createServer(socket => {
  const stream = new SecretStream(false, socket)
  stream.on('error', () => {})
  store.replicate(stream)
})
await new Promise(resolve => server.listen(0, '127.0.0.1', resolve))
console.log(JSON.stringify({ port: server.address().port, cores }))
readline.createInterface({ input: process.stdin }).on('close', async () => {
  server.close()
  await store.close()
  process.exit(0)
})
