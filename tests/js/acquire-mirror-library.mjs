// Runs the app's own `src/mirror.js` (PEARTUBE_APP, else the sibling `../peartube`) through the sessions of
// acquire-mirror-library.json, on real files in a temporary folder: the import folder and `<storage>/staging`,
// album folders and their pins, what the owner removed, and the Soulseek provider on the scripted network of
// soulseek-network.mjs. Prints one JSON trace that tests/acquire_mirror.rs must reproduce.
import { chmodSync, existsSync, mkdirSync, mkdtempSync, readFileSync, rmSync, symlinkSync, unlinkSync, utimesSync, writeFileSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { dirname, join, resolve } from 'node:path'
import { fileURLToPath, pathToFileURL } from 'node:url'
import { fakeAddon, flac, sha, tree } from './soulseek-network.mjs'

const here = dirname(fileURLToPath(import.meta.url))
const APP = process.env.PEARTUBE_APP || resolve(here, '../../../peartube')
const scenario = JSON.parse(readFileSync(join(here, 'acquire-mirror-library.json'), 'utf8'))

// The app's mirror host with the modules that reach the network planted, and its files real.
const gen = join(here, '.gen')
mkdirSync(gen, { recursive: true })
const plant = `"${pathToFileURL(join(here, 'acquire-mirror-plants.mjs')).href}"`
const app = path => `"${pathToFileURL(join(APP, path)).href}"`
let source = readFileSync(join(APP, 'src/mirror.js'), 'utf8')
for (const [from, to] of Object.entries({
  "'./protocol.js'": app('src/protocol.js'),
  "'./tags.js'": app('src/tags.js'),
  "'./providers/soulseek.js'": app('src/providers/soulseek.js'),
  "'./hiverelay.js'": plant,
  "'./traffic.js'": plant,
  "'./art.js'": plant
})) {
  if (!source.includes(`from ${from}`)) throw new Error(`unstubbed import ${from}`)
  source = source.split(`from ${from}`).join(`from ${to}`)
}
writeFileSync(join(gen, 'library-host.mjs'), source)
const { createMirrorHost } = await import(pathToFileURL(join(gen, 'library-host.mjs')).href)

const NOW = 1_700_000_000_000
Date.now = () => NOW
// A file's modification time when a step names none, in seconds.
const MTIME = 1_600_000_000
// The ledger file's text, with the time of every file written during the run (a download) named `<now>`: only
// those are later than the scripted clock.
const steady = text => text.replace(/"mtimeMs": (\d+)/g, (row, ms) => Number(ms) >= NOW ? '"mtimeMs": "<now>"' : row)

// A node that stores nothing: what the host asks it to announce goes to `calls`.
function makeNode () {
  const node = {
    calls: [],
    refuse: [],
    privateRouting: false,
    closing: false,
    async put (meta, stream) {
      const chunks = []
      for await (const chunk of stream) chunks.push(Buffer.from(chunk))
      const bytes = Buffer.concat(chunks)
      node.calls.push({ method: 'put', meta: { ...meta }, start: null, progress: [bytes.length] })
      if (node.refuse.includes(meta.id)) throw new Error(`refused ${meta.id}`)
      return {
        type: 'announce',
        id: meta.id,
        title: meta.title,
        size: bytes.length,
        sha256: sha(bytes),
        blobs: 'blobs:' + sha(String(meta.id)).slice(0, 8),
        blob: { blockOffset: 0, blockLength: Math.max(1, Math.ceil(bytes.length / 65536)), byteOffset: 0, byteLength: bytes.length }
      }
    },
    async append (op) { node.calls.push({ method: 'append', op }) },
    async rechunkLegacyBlobs () { return { total: 0, migrated: 0, deduped: 0, remaining: 0, migratedBytes: 0 } },
    async syncLocalChannel () { return { backfilled: 0, total: 0 } },
    async search () { return [] },
    async blobCore () { return null },
    status: () => ({ peers: 0, lanPeers: 0 }),
    peerConnections: () => [],
    listSources: () => []
  }
  return node
}

async function runSession (session) {
  const root = mkdtempSync(join(tmpdir(), 'pt-library-oracle-'))
  const storage = join(root, 'storage')
  // The import folder is `import`, or the folder a session names.
  const importDir = join(root, typeof session.watchDir === 'string' ? session.watchDir : 'import')
  const stagingDir = join(storage, 'staging')
  mkdirSync(storage)
  if (session.watchDir) mkdirSync(importDir, { recursive: true })

  // A step that changes the files; false when the step is something else.
  const fileStep = step => {
    if (step.write !== undefined) {
      const path = join(root, step.write)
      mkdirSync(dirname(path), { recursive: true })
      writeFileSync(path, step.flac ? flac(step.flac, step.pad ?? 0) : step.text)
      utimesSync(path, step.mtime ?? MTIME, step.mtime ?? MTIME)
    } else if (step.link !== undefined) {
      symlinkSync(join(root, step.to), join(root, step.link))
    } else if (step.remove !== undefined) {
      unlinkSync(join(root, step.remove))
    } else {
      return false
    }
    return true
  }
  for (const step of session.before) if (!fileStep(step)) throw new Error(`not a file step ${JSON.stringify(step)}`)
  if (session.readOnly) chmodSync(importDir, 0o555)

  const node = makeNode()
  const network = []
  const host = await createMirrorHost({
    node,
    storage,
    acquirer: null,
    mirrorChannel: false,
    rechunk: false,
    watchDir: session.watchDir ? importDir : null,
    feeds: [],
    pollIntervalMs: 0,
    replicate: false,
    hiverelay: null,
    pluginsDir: join(storage, 'plugins'),
    ...session.soulseek,
    soulseekOpts: { loadAddon: async () => fakeAddon(session, network) }
  })

  // Job ids are random: each is named by the order it was made in.
  const labels = new Map()
  const note = () => { for (const job of host.listProviderJobs()) if (!labels.has(job.jobId)) labels.set(job.jobId, `job-${labels.size + 1}`) }
  const idOf = n => [...labels].find(([, label]) => label === `job-${n}`)?.[0] || 'no-such-job'
  const ledgerFile = join(storage, 'mirror-watch.json')
  const ledger = () => existsSync(ledgerFile) ? readFileSync(ledgerFile, 'utf8') : null
  const out = []
  const put = (step, value) => {
    note()
    out.push({ step, calls: node.calls.splice(0), network: network.splice(0), value })
  }
  const outcome = call => call().then(
    result => ({ result }),
    err => ({ error: { message: err?.message ?? String(err), status: err?.status ?? null } })
  )

  put('start', null)
  for (const [i, step] of session.steps.entries()) {
    if (fileStep(step)) {
      put(i, null)
    } else if (step.refuse) {
      node.refuse = step.refuse
      put(i, null)
    } else if (step.run) {
      put(i, await outcome(async () => host.run(step.run, step.payload)))
    } else if (step.together) {
      // Runs asked for while another is under way.
      put(i, await Promise.all(step.together.map(name => outcome(async () => host.run(name)))))
    } else if (step.configure) {
      host.configure(step.configure)
      for (let n = 0; n < 40; n++) await new Promise(resolve => setImmediate(resolve))
      put(i, null)
    } else if (step.provider) {
      put(i, await outcome(async () => host[`${step.provider}Provider`](step.name, step.payload)))
    } else if (step.jobs) {
      put(i, host.listProviderJobs())
    } else if (step.job !== undefined) {
      put(i, host.getProviderJob(idOf(step.job)))
    } else if (step.cancel !== undefined) {
      put(i, host.cancelProviderJob(idOf(step.cancel)))
    } else if (step.forget) {
      // An announce is named by its ledger key, by its place in the ledger, or outright.
      const rows = JSON.parse(ledger() || '{}')
      const gone = step.forget
        .map(want => typeof want === 'string' ? rows[want] : want.row !== undefined ? Object.values(rows)[want.row] : want)
        .map(row => ({ id: row.id, sha256: row.sha256 }))
      put(i, [...await host.forgetImported(gone)])
    } else if (step.list) {
      put(i, host.list().filter(plugin => plugin.name === 'watch-dir' || plugin.name === 'soulseek'))
    } else if (step.tree) {
      put(i, { import: existsSync(importDir) ? tree(importDir) : null, staging: existsSync(stagingDir) ? tree(stagingDir) : null })
    } else if (step.ledger) {
      put(i, ledger() && steady(ledger()))
    } else {
      throw new Error(`unknown step ${JSON.stringify(step)}`)
    }
  }
  await host.close()
  if (session.readOnly) chmodSync(importDir, 0o755)
  rmSync(root, { recursive: true, force: true })
  let text = JSON.stringify({ name: session.name, trace: out }).split(JSON.stringify(root).slice(1, -1)).join('<root>')
  for (const [uuid, label] of labels) text = text.split(uuid).join(label)
  return JSON.parse(text)
}

const sessions = []
for (const session of scenario.sessions) sessions.push(await runSession(session))
process.stdout.write(JSON.stringify({ sessions }, null, 1), () => process.exit(0))
