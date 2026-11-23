// Runs the app's own `src/providers/youtube.js` (PEARTUBE_APP, else the sibling `../peartube`) through the sessions
// of youtube-provider-scenario.json. Scripted functions stand where the relay's `ubit` calls and its guarded fetch
// stand (`info`, `download`, `fetchBytes`), a scripted host stands where the mirror host stands (provider-host.mjs),
// the clock and the timers are scripted, and the files are real ones in a temporary folder. Prints one JSON trace
// that tests/youtube_provider.rs must reproduce.
//
// Time moves in steps of the scenario's `tick`, and every scripted event falls on a step, so a timer fires at the
// same instant here and under tokio's paused clock.
import { mock } from 'node:test'
import { mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { basename, dirname, join, resolve } from 'node:path'
import { fileURLToPath, pathToFileURL } from 'node:url'
import { fakeHost } from './provider-host.mjs'
import { sha, tree } from './soulseek-network.mjs'

const here = dirname(fileURLToPath(import.meta.url))
const APP = process.env.PEARTUBE_APP || resolve(here, '../../../peartube')
const { createYoutubePlugin } = await import(pathToFileURL(join(APP, 'src/providers/youtube.js')).href)
const scenario = JSON.parse(readFileSync(join(here, 'youtube-provider-scenario.json'), 'utf8'))
const TICK = scenario.tick

const NOW = 1_700_000_000_000
// Lets every promise chain and the plugin's own `await`s run before the clock moves again.
const flush = async () => { for (let i = 0; i < 40; i++) await new Promise(resolve => setImmediate(resolve)) }
const tick = async () => { mock.timers.tick(TICK); await flush() }
const sleep = ms => new Promise(resolve => setTimeout(resolve, ms))
const failure = script => Object.assign(new Error(script.error), script.status ? { status: script.status } : {})

// The relay's `ubitInfo`, scripted: `searches[<query>]` answers `reply` (else `{ entries }`) after `after` ms, or
// fails with `error` (and `status`). A query the scenario does not name has no results.
const scriptedInfo = log => async (query, { timeoutMs } = {}) => {
  log.push({ info: query, timeoutMs: timeoutMs ?? null })
  const script = scenario.searches[query] || { error: 'No YouTube results' }
  if (script.after) await sleep(script.after)
  if (script.error) throw failure(script)
  return 'reply' in script ? script.reply : { entries: script.entries }
}

// The relay's `downloadPage`, scripted: `videos[<id>]` reports each of `steps` (`[ms after the start, bytes so
// far]`), then `after` ms later (one tick when it names none) fails with `error` or leaves `file`, `bytes` long,
// in the folder it was given. A cancel ends it at once, as killing `ubit` does.
const scriptedDownload = log => (url, outDir, { audio = false, controller = null, onProgress = null } = {}) => new Promise((resolve, reject) => {
  const id = new URL(url).searchParams.get('v')
  log.push({ download: id, audio, into: basename(outDir) })
  if (controller?.aborted) return reject(new Error('Cancelled'))
  const script = scenario.videos[id] || { error: 'Video unavailable' }
  const timers = []
  let over = false
  const end = settle => {
    if (over) return
    over = true
    for (const timer of timers) clearTimeout(timer)
    if (controller) controller.onabort = null
    settle()
  }
  if (controller) controller.onabort = () => end(() => reject(new Error('Cancelled')))
  let last = 0
  for (const [ms, bytes] of script.steps || []) {
    last = ms
    timers.push(setTimeout(() => { if (!over) onProgress?.({ bytes, total: null }) }, ms))
  }
  timers.push(setTimeout(() => end(() => {
    if (script.error) return reject(failure(script))
    const path = join(outDir, script.file)
    writeFileSync(path, Buffer.alloc(script.bytes, id))
    resolve({ path, size: script.bytes })
  }), last + (script.after ?? TICK)))
})

// The relay's guarded fetch, scripted: `thumbs[<id>]` is a reply (`status`, `type`, `bytes`) or an `error`. A
// picture the scenario does not name is not found.
const scriptedFetch = log => async url => {
  log.push({ fetch: url })
  const id = url.split('/')[4]
  const script = scenario.thumbs[id] || { status: 404, type: 'text/html', bytes: 9 }
  if (script.after) await sleep(script.after)
  if (script.error) throw failure(script)
  return { status: script.status, headers: script.type ? { 'content-type': script.type } : {}, body: Buffer.alloc(script.bytes, id) }
}

// A file a step puts under the session's folder.
const writeFile = (root, step) => {
  const path = join(root, step.write)
  mkdirSync(dirname(path), { recursive: true })
  writeFileSync(path, step.text)
}

async function runSession (session) {
  // Every session has its own clock: the timers a session leaves behind go with it.
  mock.timers.enable({ apis: ['setTimeout', 'setInterval', 'Date'], now: NOW })
  const root = mkdtempSync(join(tmpdir(), 'pt-yt-oracle-'))
  const storage = join(root, 'storage')
  mkdirSync(storage)
  for (const step of session.before || []) writeFile(root, step)
  const log = []
  const host = fakeHost(log, join(storage, 'staging'), TICK)
  let changes = 0
  const start = Date.now()
  const plug = createYoutubePlugin({
    storage,
    ...session.options,
    onchange: () => { changes++ },
    ...(session.scan === false ? {} : { placeAndScan: host.placeAndScan }),
    ...(session.backend === false ? {} : { info: scriptedInfo(log), download: scriptedDownload(log) }),
    ...(session.fetch === false ? {} : { fetchBytes: scriptedFetch(log) })
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
      value => { state.done = { result: value ?? null } },
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
  // A picture as its type, its size and its hash.
  const picture = thumb => thumb && { type: thumb.type, bytes: thumb.bytes.byteLength, sha256: sha(thumb.bytes) }

  await flush()
  for (const [i, step] of session.steps.entries()) {
    if (step.do) {
      const call = () => watch(plug[step.do](step.payload, {}))
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
    } else if (step.count) {
      put(i, plug.listJobs().length)
    } else if (step.get !== undefined) {
      put(i, plug.getJob(idOf(step.get)))
    } else if (step.cancel !== undefined) {
      // What a cancel sets off (the save ends, the next one starts) happens now, before the clock moves again.
      const answer = plug.cancelJob(idOf(step.cancel))
      await flush()
      put(i, answer)
    } else if (step.info) {
      put(i, plug.info())
    } else if (step.thumb !== undefined) {
      put(i, await settle(watch(plug.thumbnail(step.thumb).then(picture))))
    } else if (step.thumbs) {
      // Pictures asked for one after another, by a number each: only how many there were is recorded.
      const [from, to] = step.thumbs
      for (let n = from; n < to; n++) await settle(watch(plug.thumbnail(`n${String(n).padStart(10, '0')}`)))
      put(i, to - from)
    } else if (step.hold !== undefined) {
      host.hold(step.hold)
      put(i, null)
    } else if (step.limit !== undefined) {
      host.limit(step.limit)
      put(i, null)
    } else if (step.refuse) {
      host.refuse(step.refuse.name, step.refuse.error)
      put(i, null)
    } else if (step.write !== undefined) {
      writeFile(root, step)
      put(i, null)
    } else if (step.tree) {
      put(i, tree(root))
    } else if (step.stop) {
      // A save that waits for the import folder ends at the host's next look, so the clock moves here too.
      await settle(watch(plug.stop()))
      put(i, null)
    } else {
      throw new Error(`unknown step ${JSON.stringify(step)}`)
    }
  }
  await settle(watch(plug.stop()))
  rmSync(root, { recursive: true, force: true })
  mock.timers.reset()
  // A save's work folder has a random name, and so has every job id.
  let text = JSON.stringify({ name: session.name, trace: out })
    .split(JSON.stringify(root).slice(1, -1)).join('<root>')
    .replace(/youtube-work\/job-[A-Za-z0-9]+/g, 'youtube-work/job-*')
  for (const [uuid, label] of labels) text = text.split(uuid).join(label)
  return JSON.parse(text)
}

const sessions = []
for (const session of scenario.sessions) sessions.push(await runSession(session))
console.log(JSON.stringify({ sessions }, null, 1))
