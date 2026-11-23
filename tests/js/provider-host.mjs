// What the mirror host does for a provider (`placeAndScan` in the app's mirror.js), for the oracles that run one
// provider on its own. The other import is scripted: `hold(ms)` lets it hold the folder that long from now, and
// `limit(ms)` is how long the host waits before it gives up. Nothing is placed while it waits. Then the provider's
// `place` runs, and the scan answers one item for each media file it has not reported before; a file named by
// `refuse(name, error)` is answered as a failed import. The host's own wait is compared in the library oracle;
// this one only gives a provider every answer a host can give.
import { createHash } from 'node:crypto'
import { readFileSync, readdirSync, statSync } from 'node:fs'
import { basename, join } from 'node:path'

const sha = bytes => createHash('sha256').update(bytes).digest('hex')

// `log` takes `{ scan: <folder> }` for each scan, `dir` is the folder providers place into, and `tick` is the step
// of the scripted clock in ms.
export function fakeHost (log, dir, tick) {
  const sleep = ms => new Promise(resolve => setTimeout(resolve, ms))
  const seen = new Set()
  const refused = new Map()
  let heldUntil = 0
  let limit = Infinity
  async function scan () {
    log.push({ scan: basename(dir) })
    await sleep(tick)
    const items = []
    const failed = []
    let scanned = 0
    for (const folder of readdirSync(dir).sort()) {
      if (folder.startsWith('.') || !statSync(join(dir, folder)).isDirectory()) continue
      for (const name of readdirSync(join(dir, folder)).sort()) {
        if (name.startsWith('.') || name.endsWith('.json')) continue
        scanned++
        const rel = `${folder}/${name}`
        if (seen.has(rel)) continue
        if (refused.has(name)) {
          failed.push({ file: name, rel, error: refused.get(name) })
          continue
        }
        seen.add(rel)
        const bytes = readFileSync(join(dir, folder, name))
        items.push({ file: name, rel, size: bytes.length, sha256: sha(bytes) })
      }
    }
    return { scanned, ingested: items.length, failed, items }
  }
  return {
    hold (ms) { heldUntil = Date.now() + ms },
    limit (ms) { limit = ms },
    refuse (name, error) { refused.set(name, error) },
    async placeAndScan (place, _ctx, { cancelled = () => false } = {}) {
      const end = Date.now() + limit
      while (Date.now() < heldUntil) {
        if (cancelled()) throw new Error('Cancelled')
        if (Date.now() >= end) throw Object.assign(new Error(`Another import has held the folder for ${Math.ceil(limit / 1000)} s. Nothing was saved; try again when it finishes.`), { status: 503 })
        await sleep(tick)
      }
      if (cancelled()) throw new Error('Cancelled')
      place()
      return scan()
    }
  }
}
