// Generates ../settings-reference.json from the app's own settings.js/app-settings.js copies in this folder.
// `node settings.mjs verify <cases.json>` loads settings files the Rust side wrote and checks their views.
import { existsSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs'
import { basename, dirname, join } from 'node:path'
import { tmpdir } from 'node:os'
import { fileURLToPath } from 'node:url'
import { createSettings } from './settings.js'
import { APP_SETTINGS_DEFAULTS, nodeOptions, normalizeAppSettings } from './app-settings.js'

const here = dirname(fileURLToPath(import.meta.url))

if (process.argv[2] === 'verify') {
  const cases = JSON.parse(readFileSync(process.argv[3], 'utf8'))
  let failed = 0
  for (const entry of cases) {
    const view = createSettings(entry.dir, entry.env || {}).view()
    if (JSON.stringify(view) !== JSON.stringify(entry.view)) {
      failed++
      console.error('mismatch', entry.dir)
    }
  }
  if (failed) throw new Error(`${failed}/${cases.length} Rust settings files did not load in JS`)
  console.log(`ok: ${cases.length} Rust settings files loaded in JS`)
  process.exit(0)
}

const K = '0123456789abcdef'.repeat(4)
const K2 = 'fedcba9876543210'.repeat(4)
const KUP = K.toUpperCase()
const MN12 = 'abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon about'
const MN24 = 'abandon '.repeat(23) + 'art'

// --- normalizeAppSettings ---

const appSettings = []
function appCase (name, raw, { requireChannel = false, previous = null } = {}) {
  const entry = { name, raw, requireChannel, previous }
  try {
    entry.ok = normalizeAppSettings(raw, { requireChannel, previous })
  } catch (e) {
    entry.err = e.message
  }
  appSettings.push(entry)
}

appCase('defaults', {})
appCase('missing form fields', {}, { requireChannel: true })
appCase('empty form', { tracker: '', mnemonic: '', sources: '', relayThrough: '', bootstrap: '' }, { requireChannel: true })
appCase('tracker hex', { tracker: K }, { requireChannel: true })
appCase('tracker uppercase hex', { tracker: KUP }, { requireChannel: true })
appCase('tracker channel prefix', { tracker: `channel:${K}` }, { requireChannel: true })
appCase('tracker tracker prefix', { tracker: `tracker:${K}` }, { requireChannel: true })
appCase('tracker peartube prefix', { tracker: `peartube:tracker:${K}` }, { requireChannel: true })
appCase('tracker channel link', { tracker: `peartube://channel/${K}?name=News` }, { requireChannel: true })
appCase('tracker channel link alias query', { tracker: `peartube://channel/${K}?alias=Archive` }, { requireChannel: true })
appCase('tracker access link', { tracker: `peartube://access/${K}?epoch=1&epochKey=${K2}` })
appCase('tracker watch link', { tracker: `peartube://watch/${K}?id=a:b` })
appCase('tracker nonsense', { tracker: 'not-a-key' })
appCase('tracker number', { tracker: 123 })
appCase('tracker falsy zero', { tracker: 0, sources: [K] }, { requireChannel: true })
appCase('tracker null', { tracker: null }, { requireChannel: true })
appCase('sources single hex', { sources: K }, { requireChannel: true })
appCase('sources array', { sources: [K, K2] }, { requireChannel: true })
appCase('sources csv', { sources: `${K}, ${K2}` }, { requireChannel: true })
appCase('sources newline list', { sources: `${K}\n${K2}\r\n` }, { requireChannel: true })
appCase('sources uppercase', { sources: [KUP] }, { requireChannel: true })
appCase('sources saved objects', { sources: [{ key: K, alias: 'Mine' }, { key: K2 }] }, { requireChannel: true })
appCase('sources object without key', { sources: [{ alias: 'Mine' }] }, { requireChannel: true })
appCase('sources duplicate keys', { sources: [K, `${K},${K2},${K}`] }, { requireChannel: true })
appCase('sources channel links', { sources: [`peartube://channel/${K}?name=First`, `peartube://channel/${K2}`] }, { requireChannel: true })
appCase('sources number', { sources: 123 })
appCase('sources array number', { sources: [123] })
appCase('sources empty strings', { sources: ['', ' ', `${K}`] }, { requireChannel: true })
appCase('sources equal tracker', { tracker: K, sources: [K] }, { requireChannel: true })
appCase('previous keeps alias', { sources: [K] }, { previous: { sources: [{ key: K, alias: 'Kept' }] }, requireChannel: true })
appCase('previous keeps uppercase alias', { sources: [KUP] }, { previous: { sources: [{ key: K, alias: 'Kept' }] } })
appCase('previous unknown key', { sources: [K2] }, { previous: { sources: [{ key: K, alias: 'Kept' }] } })
appCase('previous object token keeps alias', { sources: [{ key: K }] }, { previous: { sources: [{ key: K, alias: 'Mine' }] } })
appCase('previous explicit alias wins', { sources: [{ key: K, alias: 'New' }] }, { previous: { sources: [{ key: K, alias: 'Old' }] } })
appCase('previous without sources', { sources: [K] }, { previous: {} })
appCase('relay through csv', { relayThrough: `${K},${K2}` })
appCase('relay through spaces', { relayThrough: `${K} ${K2}` })
appCase('relay through uppercase', { relayThrough: KUP })
appCase('relay through array', { relayThrough: [K, K2] })
appCase('relay through bad key', { relayThrough: `${K},nope` })
appCase('bootstrap list', { bootstrap: '127.0.0.1:8000 peer.example:8100' })
appCase('bootstrap ipv6', { bootstrap: ['[::1]:9000'] })
appCase('bootstrap missing port', { bootstrap: '127.0.0.1' })
appCase('bootstrap port too high', { bootstrap: '127.0.0.1:99999' })
appCase('bootstrap object', { bootstrap: [{ host: 'a' }] })
appCase('mnemonic 12 words messy', { mnemonic: `  ${MN12.replace(/ /g, '   ').toUpperCase()}  ` })
appCase('mnemonic 24 words', { mnemonic: MN24 })
appCase('mnemonic short', { mnemonic: 'abandon abandon abandon' })
appCase('mnemonic bad checksum', { mnemonic: MN12.replace(/about$/, 'abandon') })
appCase('mnemonic number', { mnemonic: 123 })
appCase('mnemonic null', { mnemonic: null })
appCase('mnemonic only satisfies channel', { mnemonic: MN12 }, { requireChannel: true })
appCase('channel and mnemonic', { tracker: K, mnemonic: MN12 }, { requireChannel: true })
appCase('lan discovery true', { lanDiscovery: true })
appCase('lan discovery false', { lanDiscovery: false })
appCase('lan discovery null', { lanDiscovery: null })
appCase('lan discovery string', { lanDiscovery: 'yes' })
appCase('private routing zero string', { privateRouting: '0' })
appCase('cache max gb number', { cacheMaxGb: 5 })
appCase('cache max gb string', { cacheMaxGb: '7' })
appCase('cache max gb negative', { cacheMaxGb: -3 })
appCase('cache max gb fraction', { cacheMaxGb: 12.9 })
appCase('cache max gb empty string', { cacheMaxGb: '' })
appCase('cache max gb null', { cacheMaxGb: null })
appCase('cache max gb zero', { cacheMaxGb: 0 })
appCase('cache max gb garbage', { cacheMaxGb: 'abc' })
appCase('cache max gb boolean', { cacheMaxGb: true })
appCase('full form', {
  tracker: K,
  mnemonic: '',
  sources: [K, `${K2}`],
  relayThrough: [K],
  bootstrap: ['127.0.0.1:8000'],
  lanDiscovery: false,
  privateRouting: true,
  cacheMaxGb: 20
}, { requireChannel: true })

const defaults = JSON.parse(JSON.stringify(APP_SETTINGS_DEFAULTS))

// --- nodeOptions ---

const nodeOptionsCases = []
function nodeCase (name, settings, trackersDir, defaultFolder) {
  nodeOptionsCases.push({ name, settings, trackersDir, defaultFolder, value: nodeOptions(settings, { trackersDir, defaultFolder }) })
}

const fixed = normalizeAppSettings({
  tracker: K,
  sources: [K, `peartube://channel/${K2}?name=Other`],
  relayThrough: [K],
  bootstrap: ['127.0.0.1:8000', '[::1]:8001'],
  lanDiscovery: false,
  privateRouting: true,
  cacheMaxGb: 20
})
nodeCase('fixed tracker', fixed, '/var/peartube/trackers', '.peartube')
nodeCase('duplicate slashes', fixed, '/var//peartube/trackers/', 'sub')
nodeCase('linked device', normalizeAppSettings({ mnemonic: MN12 }), '/data/trackers', 'peartube-android')
nodeCase('default folder', normalizeAppSettings({}), '/data/trackers', '.peartube')
nodeCase('tracker and mnemonic', normalizeAppSettings({ tracker: K, mnemonic: MN12, sources: [K] }), '/data/trackers', 'peartube')
nodeCase('empty lists', normalizeAppSettings({ tracker: K }), '/data/trackers', 'peartube')
nodeCase('large cache', normalizeAppSettings({ tracker: K, cacheMaxGb: 4096 }), '/relay/trackers', 'relay')

// --- createSettings ---

const settingsFiles = []
const tmp = () => mkdtempSync(join(tmpdir(), 'peartube-settings-'))

function seedFile (patch, env = {}) {
  const dir = tmp()
  createSettings(dir, env).update(patch)
  const text = readFileSync(join(dir, 'settings.json'), 'utf8')
  rmSync(dir, { recursive: true })
  return text
}

function fileCase (name, { initial = null, env = {}, steps }) {
  const dir = tmp()
  if (initial) writeFileSync(join(dir, 'settings.json'), initial, { mode: 0o600 })
  const settings = createSettings(dir, env)
  const run = steps.map(step => {
    const out = { ...step }
    try {
      switch (step.op) {
        case 'view': out.ok = settings.view(); break
        case 'get': out.ok = settings.get(step.key); break
        case 'update': out.ok = settings.update(step.patch); break
        case 'remember': out.ok = settings.remember(step.key, step.value) ?? null; break
        case 'snapshot': out.ok = settings.snapshot(); break
        case 'markApplied': settings.markApplied(step.snapshot); out.ok = null; break
        case 'isLive': out.ok = settings.isLive(step.key); break
        case 'file': out.ok = basename(settings.file); break
        default: throw new Error(`unknown op ${step.op}`)
      }
    } catch (e) {
      delete out.ok
      out.err = e.message
    }
    return out
  })
  const file = join(dir, 'settings.json')
  const entry = {
    name,
    env,
    initial,
    steps: run,
    finalFile: existsSync(file) ? readFileSync(file, 'utf8') : null,
    // What a restart reads back: the Rust file must load to the same view JS gets from its own file.
    reloadedView: createSettings(dir, env).view()
  }
  rmSync(dir, { recursive: true })
  settingsFiles.push(entry)
}

fileCase('fresh defaults', {
  steps: [
    { op: 'file' },
    { op: 'view' },
    { op: 'get', key: 'streamPort' },
    { op: 'get', key: 'lan' },
    { op: 'isLive', key: 'lan' },
    { op: 'isLive', key: 'streamPort' },
    { op: 'snapshot' }
  ]
})

fileCase('env fallbacks', {
  env: {
    PEARTUBE_STREAM_PORT: '9100',
    PEARTUBE_LAN_HOST: '0',
    PEARTUBE_PRIVATE_ROUTING: '1',
    PEARTUBE_HIVERELAYS: 'https://relay.example/api,https://other.example/v1=TOKEN',
    PEARTUBE_HIVERELAY_API_KEY: 'k',
    PEARTUBE_DHT_PORT: '9000',
    PEARTUBE_REPLICATE: 'true',
    PEARTUBE_REPLICATE_SOURCES: '',
    PEARTUBE_CACHE_MAX_GB: '25'
  },
  steps: [
    { op: 'view' },
    { op: 'get', key: 'lan' },
    { op: 'get', key: 'replicate' },
    { op: 'get', key: 'replicateSources' },
    { op: 'get', key: 'cacheMaxGb' },
    { op: 'snapshot' }
  ]
})

fileCase('bad env numbers read as null', {
  env: { PEARTUBE_DHT_PORT: 'abc', PEARTUBE_REPLICATE_FREE_GB: '' },
  steps: [
    { op: 'get', key: 'dhtPort' },
    { op: 'get', key: 'replicateFreeGb' },
    { op: 'view' }
  ]
})

fileCase('update basics', {
  steps: [
    { op: 'update', patch: { streamPort: 9100 } },
    { op: 'update', patch: { lan: true, tracker: K } },
    { op: 'view' },
    { op: 'get', key: 'tracker' }
  ]
})

fileCase('clear falls back to env', {
  initial: seedFile({ streamPort: 9000 }),
  env: { PEARTUBE_STREAM_PORT: '9100' },
  steps: [
    { op: 'get', key: 'streamPort' },
    { op: 'update', patch: { streamPort: null } },
    { op: 'view' }
  ]
})

fileCase('invalid patches', {
  initial: seedFile({ streamPort: 9000 }),
  steps: [
    { op: 'update', patch: null },
    { op: 'update', patch: [] },
    { op: 'update', patch: { nope: 1 } },
    { op: 'update', patch: { streamPort: 70000 } },
    { op: 'update', patch: { streamPort: '9000' } },
    { op: 'update', patch: { dhtPort: 1.5 } },
    { op: 'update', patch: { privateGuard: 'yes' } },
    { op: 'update', patch: { streamHost: 'bad host' } },
    { op: 'update', patch: { tracker: 'zz' } },
    { op: 'update', patch: { watchDir: 'relative/dir' } },
    { op: 'update', patch: { hiverelayApiKey: 'x'.repeat(513) } },
    { op: 'update', patch: { feeds: 'not a list' } },
    { op: 'update', patch: { feeds: ['not a url'] } },
    { op: 'update', patch: { feeds: Array.from({ length: 65 }, () => 'https://feed.example') } },
    { op: 'update', patch: { allowedPeers: ['nope'] } },
    { op: 'update', patch: { bootstrap: ['1.2.3.4'] } },
    { op: 'update', patch: { cinemetaUrl: 'ftp://meta.example' } },
    { op: 'update', patch: { acquireConcurrency: 0 } },
    { op: 'update', patch: { relayThrough: [KUP] } },
    { op: 'update', patch: { streamPort: 9100 } },
    { op: 'view' }
  ]
})

fileCase('secrets and url refs', {
  initial: seedFile({
    hiverelays: ['https://a.example/x?token=1', 'https://b.example/y=SECRET'],
    hiverelayApiKey: 'topsecret',
    feeds: ['https://feed.example/f']
  }),
  steps: [
    { op: 'view' },
    { op: 'update', patch: { hiverelays: ['#1', 'https://c.example/z'] } },
    { op: 'get', key: 'hiverelays' },
    { op: 'update', patch: { hiverelayApiKey: null } },
    { op: 'view' }
  ]
})

fileCase('missing url refs drop', {
  initial: seedFile({ feeds: ['https://feed.example/f'] }),
  steps: [
    { op: 'update', patch: { feeds: ['#3'] } },
    { op: 'view' },
    { op: 'update', patch: { feeds: ['#0', '#0'] } },
    { op: 'view' }
  ]
})

fileCase('guard port conflict', {
  initial: seedFile({ privateGuard: true, privateGuardPort: 9000 }),
  steps: [
    { op: 'view' },
    { op: 'update', patch: { dhtPort: 9000 } },
    { op: 'update', patch: { dhtPort: 9000, privateGuardPort: 9001 } },
    { op: 'update', patch: { dhtPort: 9500 } },
    { op: 'update', patch: { privateGuard: false, dhtPort: 9001 } },
    { op: 'view' }
  ]
})

fileCase('remember tracker', {
  steps: [
    { op: 'remember', key: 'tracker', value: K },
    { op: 'view' },
    { op: 'remember', key: 'tracker', value: K2 },
    { op: 'get', key: 'tracker' }
  ]
})

fileCase('snapshot and mark applied', {
  steps: [
    { op: 'update', patch: { streamPort: 9000 } },
    { op: 'view' },
    { op: 'snapshot' },
    { op: 'markApplied', snapshot: {} },
    { op: 'view' },
    { op: 'update', patch: { lan: false, replicateFreeGb: 30 } },
    { op: 'view' }
  ]
})

fileCase('mark applied with snapshot', {
  steps: [
    { op: 'update', patch: { streamPort: 9000 } },
    { op: 'snapshot' },
    { op: 'markApplied', snapshot: { streamPort: '"9000"' } },
    { op: 'view' }
  ]
})

fileCase('null clears a saved list', {
  initial: seedFile({ feeds: ['https://a.example'] }),
  steps: [
    { op: 'update', patch: { feeds: null } },
    { op: 'view' }
  ]
})

fileCase('number formatting', {
  steps: [
    { op: 'update', patch: { replicateMbps: 9000.5, cacheMaxGb: 12, hiverelayReplicas: 3, acquireConcurrency: 8 } },
    { op: 'view' }
  ]
})

fileCase('every field', {
  steps: [
    {
      op: 'update',
      patch: {
        streamHost: '10.0.0.5',
        streamPort: 9000,
        lan: true,
        lanHost: 'peer.local',
        dhtPort: 49737,
        bootstrap: ['127.0.0.1:49737', '[::1]:8080'],
        relayThrough: [K],
        privateRouting: true,
        privateGuard: true,
        privateGuardPort: 49738,
        tracker: K2,
        allowedPeers: [K, KUP],
        publishChannel: false,
        directory: true,
        acquireConcurrency: 16,
        watchDir: '/imports/watched',
        feeds: ['https://feed.example/api?x=1'],
        replicate: true,
        replicateSources: [K],
        replicateFreeGb: 50.5,
        replicateMbps: 120,
        cacheMaxGb: 200,
        hiverelays: ['https://relay.example/v1=SECRET'],
        hiverelayApiKey: 'abc',
        hiverelayDiscover: true,
        hiverelayReplicas: 5,
        hiverelayUploadMbps: 100,
        rechunk: true,
        cinemetaUrl: 'https://meta.example/'
      }
    },
    { op: 'view' },
    { op: 'snapshot' }
  ]
})

fileCase('reload written file', {
  steps: [
    { op: 'update', patch: { streamPort: 9010, tracker: K, feeds: ['https://feed.example/a'] } },
    { op: 'view' }
  ]
})

const fixtures = { appSettingsDefaults: defaults, appSettings, nodeOptions: nodeOptionsCases, settingsFiles }
writeFileSync(join(here, '..', 'settings-reference.json'), JSON.stringify(fixtures, null, 2) + '\n')
console.log('wrote settings-reference.json')
for (const [key, value] of Object.entries(fixtures)) {
  if (Array.isArray(value)) console.log(` ${key}: ${value.length}`)
}
