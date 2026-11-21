// Runs the app's own `src/providers/soulseek.js` (PEARTUBE_APP, else the sibling `../peartube`) through the
// sessions of soulseek-provider-scenario.json. A scripted Soulseek client stands where the addon stands
// (soulseek-network.mjs), the clock and the timers are scripted, and the files are real ones in a temporary folder.
// Prints one JSON trace that tests/soulseek_provider.rs must reproduce.
//
// Time moves in steps of the scenario's `tick`, and every scripted event falls on a step, so a timer fires at the
// same instant here and under tokio's paused clock.
import { mock } from 'node:test'
import { mkdirSync, mkdtempSync, readFileSync, readdirSync, rmSync, statSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { basename, dirname, join, resolve } from 'node:path'
import { fileURLToPath, pathToFileURL } from 'node:url'
import { TICK, fakeAddon, network as scenario, sha, tree } from './soulseek-network.mjs'

const here = dirname(fileURLToPath(import.meta.url))
const APP = process.env.PEARTUBE_APP || resolve(here, '../../../peartube')
const { createSoulseekPlugin } = await import(pathToFileURL(join(APP, 'src/providers/soulseek.js')).href)

const NOW = 1_700_000_000_000
// Lets every promise chain and the plugin's own `await`s run before the clock moves again.
const flush = async () => { for (let i = 0; i < 40; i++) await new Promise(resolve => setImmediate(resolve)) }
const tick = async () => { mock.timers.tick(TICK); await flush() }
const sleep = ms => new Promise(resolve => setTimeout(resolve, ms))

// What the mirror host does for a provider (`placeAndScan` in the app's mirror.js), with the other import scripted:
// `hold(ms)` lets another import hold the folder that long from now, and `limit(ms)` is how long the host waits
// before it gives up. Nothing is placed while it waits. Then the provider's `place` runs, and the scan answers one
// item for each media file it has not reported before. The host's own wait is compared in the library oracle;
// this one only gives the provider every answer a host can give.
function fakeHost (log, dir) {
  const seen = new Set()
  let heldUntil = 0
  let limit = Infinity
  async function scan () {
    log.push({ scan: basename(dir) })
    await sleep(TICK)
    const items = []
    let scanned = 0
    for (const folder of readdirSync(dir).sort()) {
      if (folder.startsWith('.') || !statSync(join(dir, folder)).isDirectory()) continue
      for (const name of readdirSync(join(dir, folder)).sort()) {
        if (name.startsWith('.') || name.endsWith('.json')) continue
        scanned++
        if (seen.has(`${folder}/${name}`)) continue
        seen.add(`${folder}/${name}`)
        const bytes = readFileSync(join(dir, folder, name))
        items.push({ file: name, rel: `${folder}/${name}`, size: bytes.length, sha256: sha(bytes) })
      }
    }
    return { scanned, ingested: items.length, items }
  }
  return {
    hold (ms) { heldUntil = Date.now() + ms },
    limit (ms) { limit = ms },
    async placeAndScan (place, _ctx, { cancelled = () => false } = {}) {
      const end = Date.now() + limit
      while (Date.now() < heldUntil) {
        if (cancelled()) throw new Error('Cancelled')
        if (Date.now() >= end) throw Object.assign(new Error(`Another import has held the folder for ${Math.ceil(limit / 1000)} s. Nothing was saved; try again when it finishes.`), { status: 503 })
        await sleep(TICK)
      }
      if (cancelled()) throw new Error('Cancelled')
      place()
      return scan()
    }
  }
}

async function runSession (session) {
  // Every session has its own clock: the timers a session leaves behind go with it.
  mock.timers.enable({ apis: ['setTimeout', 'setInterval', 'Date'], now: NOW })
  const root = mkdtempSync(join(tmpdir(), 'pt-slsk-oracle-'))
  const storage = join(root, 'storage')
  mkdirSync(storage)
  const log = []
  const host = fakeHost(log, session.importFolder ? join(root, 'import') : join(storage, 'staging'))
  let changes = 0
  const start = Date.now()
  const plug = createSoulseekPlugin({
    storage,
    ...(session.importFolder ? { stagingRoot: join(root, 'import'), stagingLabel: 'import folder' } : {}),
    ...session.options,
    onchange: () => { changes++ },
    ...(session.scan === false ? {} : { placeAndScan: host.placeAndScan }),
    loadAddon: session.backend === 'none'
      ? async () => { throw new Error('No prebuild found for platform') }
      : async () => fakeAddon(session, log)
  })

  // Job ids are random: each is named by the order it was made in.
  const labels = new Map()
  const note = () => { for (const job of plug.listJobs()) if (!labels.has(job.jobId)) labels.set(job.jobId, `job-${labels.size + 1}`) }
  const idOf = n => [...labels].find(([, label]) => label === `job-${n}`)?.[0] || 'no-such-job'
  const out = []
  const put = (step, value) => {
    note()
    out.push({ step, at: Date.now() - start, changes, calls: log.splice(0), value })
    changes = 0
  }
  // A call's outcome, once it has one: the clock moves until then.
  const watch = promise => {
    const state = { done: null }
    promise.then(
      value => { state.done = { result: value } },
      err => { state.done = { error: { message: err?.message ?? String(err), status: err?.status ?? null } } }
    )
    return state
  }
  const settle = async state => {
    await flush()
    note()
    for (let i = 0; !state.done && i < 100_000; i++) {
      await tick()
      note()
    }
    if (!state.done) throw new Error('a call did not settle')
    return state.done
  }
  const pending = new Map()

  await flush()
  for (const [i, step] of session.steps.entries()) {
    if (step.do) {
      const call = () => watch(step.do === 'run' ? plug.run(step.payload) : plug[step.do](step.payload))
      if (step.as) {
        pending.set(step.as, call())
        await flush()
        put(i, { started: step.as })
      } else {
        // `repeat` makes the same call again and again; each has its own record.
        for (let n = 0; n < (step.repeat ?? 1); n++) put(i, await settle(call()))
      }
    } else if (step.await) {
      put(i, await settle(pending.get(step.await)))
    } else if (step.advance !== undefined) {
      for (let t = 0; t < step.advance; t += TICK) {
        await tick()
        note()
      }
      put(i, null)
    } else if (step.jobs) {
      put(i, plug.listJobs())
    } else if (step.get !== undefined) {
      put(i, plug.getJob(idOf(step.get)))
    } else if (step.cancel !== undefined) {
      put(i, plug.cancelJob(idOf(step.cancel)))
    } else if (step.info) {
      put(i, plug.info())
    } else if (step.configure) {
      plug.configure(step.configure)
      await flush()
      put(i, null)
    } else if (step.hold !== undefined) {
      host.hold(step.hold)
      put(i, null)
    } else if (step.limit !== undefined) {
      host.limit(step.limit)
      put(i, null)
    } else if (step.tree) {
      put(i, tree(root))
    } else if (step.stop) {
      await plug.stop()
      await flush()
      put(i, null)
    } else {
      throw new Error(`unknown step ${JSON.stringify(step)}`)
    }
  }
  await plug.stop()
  rmSync(root, { recursive: true, force: true })
  mock.timers.reset()
  let text = JSON.stringify({ name: session.name, trace: out }).split(JSON.stringify(root).slice(1, -1)).join('<root>')
  for (const [uuid, label] of labels) text = text.split(uuid).join(label)
  return JSON.parse(text)
}

const sessions = []
for (const session of scenario.sessions) sessions.push(await runSession(session))
delete process.env.PEARTUBE_SLSK_SHARE
delete process.env.PEARTUBE_SLSK_SHARE_MAX_FILES
console.log(JSON.stringify({ sessions }, null, 1))
