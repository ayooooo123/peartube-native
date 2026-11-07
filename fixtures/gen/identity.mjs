// Generates ../identity-reference.json from the app's own identity.js/mnemonic.js copies in this folder.
// `node identity.mjs verify <cases.json>` loads identity.json files the Rust side wrote and checks their views.
import { existsSync, mkdtempSync, readFileSync, rmSync, statSync, writeFileSync } from 'node:fs'
import { dirname, join } from 'node:path'
import { tmpdir } from 'node:os'
import { fileURLToPath } from 'node:url'
import { createIdentity } from './identity.js'
import { deriveIdentity } from './mnemonic.js'

const here = dirname(fileURLToPath(import.meta.url))

// The slice of ctx.channel identity.js reads, with the appended ops recorded.
function channelOf (config = {}) {
  const state = {
    primaryKey: config.primaryKey,
    writable: config.writable ?? true,
    localKey: config.localKey || config.primaryKey,
    epoch: config.epoch ?? 1,
    role: config.role ?? null,
    writers: config.writers ?? [],
    appends: []
  }
  return {
    get primaryKey () { return state.primaryKey },
    get channel () { return { writable: state.writable, local: { key: Buffer.from(state.localKey, 'hex') } } },
    append: async op => {
      state.appends.push(op)
      if (op.type === 'epoch') state.epoch = op.epoch
    },
    currentChannelEpoch: async () => state.epoch,
    localRole: async () => state.role,
    listWriters: async () => state.writers,
    state
  }
}

function open (storage, mnemonic, channel) {
  return createIdentity({ opts: { storage, mnemonic }, channel, update () {} })
}

if (process.argv[2] === 'verify') {
  const cases = JSON.parse(readFileSync(process.argv[3], 'utf8'))
  let failed = 0
  for (const entry of cases) {
    const id = open(entry.dir, entry.mnemonic, channelOf(entry.channel))
    const value = { view: await id.identity({ revealMnemonic: true }), accessKeys: Object.fromEntries(id.accessKeys) }
    if (JSON.stringify(value) !== JSON.stringify(entry.value)) {
      failed++
      console.error('mismatch', entry.dir)
    }
  }
  if (failed) throw new Error(`${failed}/${cases.length} Rust identity files did not load in JS`)
  console.log(`ok: ${cases.length} Rust identity files loaded in JS`)
  process.exit(0)
}

const K = '0123456789abcdef'.repeat(4)
const K2 = 'fedcba9876543210'.repeat(4)
const K3 = '89abcdef01234567'.repeat(4)
const KUP = K.toUpperCase()
const MN12 = 'abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon about'
const MN24 = 'abandon '.repeat(23) + 'art'
const MN_OTHER = 'legal winner thank year wave sausage worth useful legal winner thank yellow'
const V12 = deriveIdentity(MN12).channelKey
const V24 = deriveIdentity(MN24).channelKey

const writers = [
  { key: K, role: 'owner', alias: 'primary', self: true },
  { key: K2, role: 'writer', alias: 'phone', self: false }
]

const files = []
const tmp = () => mkdtempSync(join(tmpdir(), 'peartube-identity-'))

async function runStep (id, mock, step) {
  switch (step.op) {
    case 'identity': return await id.identity({ revealMnemonic: step.revealMnemonic ?? false })
    case 'claim': return await id.claimWithMnemonic(step.phrase, { alias: step.alias ?? '' })
    case 'rotate': return await id.rotateEpoch()
    case 'addAccessKey': return await id.addAccessKey(step.request)
    case 'ensureVault': {
      const vault = id.ensureVault()
      return { channelKey: vault.channelKey, ownerPublicKey: vault.ownerPublicKey, mnemonic: vault.mnemonic }
    }
    case 'save': id.saveIdentity(); return null
    case 'foundedKey': return id.foundedKey()
    case 'markFounded': id.markFounded(step.key); return null
    case 'idVault':
      return id.idVault
        ? { channelKey: id.idVault.channelKey, ownerPublicKey: id.idVault.ownerPublicKey, mnemonic: id.idVault.mnemonic }
        : null
    case 'accessKeys': return Object.fromEntries(id.accessKeys)
    case 'appends': return mock.state.appends
    case 'epochState': return mock.state.epoch
    default: throw new Error(`unknown op ${step.op}`)
  }
}

async function fileCase (name, { initial = null, mnemonic = null, channel = {}, steps = [] } = {}) {
  const dir = tmp()
  if (initial !== null) writeFileSync(join(dir, 'identity.json'), initial, { mode: 0o600 })
  const mock = channelOf(channel)
  const entry = { name, initial, mnemonic, channel, steps: [], finalFile: null, finalMode: null, reloadedView: null }
  let id
  try {
    id = open(dir, mnemonic, mock)
  } catch (e) {
    entry.createErr = e.message
    files.push(entry)
    rmSync(dir, { recursive: true })
    return
  }
  for (const step of steps) {
    const out = { ...step }
    try { out.ok = await runStep(id, mock, step) } catch (e) { out.err = e.message }
    entry.steps.push(out)
  }
  const file = join(dir, 'identity.json')
  if (existsSync(file)) {
    entry.finalFile = readFileSync(file, 'utf8')
    entry.finalMode = (statSync(file).mode & 0o777).toString(8)
    const restarted = open(dir, mnemonic, channelOf(channel))
    entry.reloadedView = {
      view: await restarted.identity({ revealMnemonic: true }),
      accessKeys: Object.fromEntries(restarted.accessKeys)
    }
  }
  files.push(entry)
  rmSync(dir, { recursive: true })
}

// --- reading the saved file ---

await fileCase('fresh writable channel', {
  channel: { primaryKey: K, localKey: K, writers },
  steps: [
    { op: 'foundedKey' },
    { op: 'idVault' },
    { op: 'accessKeys' },
    { op: 'identity' }
  ]
})

await fileCase('fresh read-only writer', {
  channel: { primaryKey: K, writable: false, localKey: K2, role: 'mod' },
  steps: [{ op: 'identity' }]
})

await fileCase('owner role on a read-only core', {
  channel: { primaryKey: K, writable: false, localKey: K2, role: 'owner' },
  steps: [{ op: 'identity' }]
})

await fileCase('saved phrase', {
  initial: JSON.stringify({ mnemonic: MN12, founded: '', accessKeys: {} }, null, 2),
  channel: { primaryKey: K },
  steps: [
    { op: 'idVault' },
    { op: 'identity' },
    { op: 'identity', revealMnemonic: true },
    { op: 'save' },
    { op: 'appends' }
  ]
})

await fileCase('saved phrase messy', {
  initial: JSON.stringify({ mnemonic: `  ${MN12.toUpperCase().replace(/ /g, '   ')}  `, founded: '', accessKeys: {} }, null, 2),
  channel: { primaryKey: K },
  steps: [{ op: 'idVault' }, { op: 'identity', revealMnemonic: true }]
})

await fileCase('saved 24-word phrase', {
  initial: JSON.stringify({ mnemonic: MN24, founded: '', accessKeys: {} }, null, 2),
  channel: { primaryKey: K },
  steps: [{ op: 'idVault' }, { op: 'identity', revealMnemonic: true }]
})

await fileCase('saved phrase invalid', {
  initial: JSON.stringify({ mnemonic: MN12.replace(/about$/, 'abandon'), founded: '', accessKeys: {} }, null, 2),
  channel: { primaryKey: K },
  steps: [
    { op: 'idVault' },
    { op: 'identity', revealMnemonic: true },
    { op: 'save' }
  ]
})

await fileCase('saved phrase short', {
  initial: JSON.stringify({ mnemonic: 'abandon abandon abandon', founded: '', accessKeys: {} }, null, 2),
  channel: { primaryKey: K },
  steps: [{ op: 'identity' }]
})

await fileCase('saved phrase is a number', {
  initial: '{"mnemonic":123,"founded":"","accessKeys":{}}',
  channel: { primaryKey: K },
  steps: [{ op: 'identity', revealMnemonic: true }, { op: 'save' }]
})

await fileCase('saved phrase is a boolean', {
  initial: '{"mnemonic":true}',
  channel: { primaryKey: K },
  steps: [{ op: 'identity' }]
})

await fileCase('corrupt file', {
  initial: '{ not json',
  channel: { primaryKey: K },
  steps: [
    { op: 'identity' },
    { op: 'save' },
    { op: 'accessKeys' }
  ]
})

await fileCase('truncated file', {
  initial: '{"mnemonic":"abandon abandon',
  channel: { primaryKey: K },
  steps: [{ op: 'identity' }, { op: 'foundedKey' }]
})

await fileCase('file is json null', {
  initial: 'null',
  channel: { primaryKey: K },
  steps: [{ op: 'identity' }, { op: 'save' }]
})

await fileCase('file is an array', {
  initial: '["x","y"]',
  channel: { primaryKey: K },
  steps: [{ op: 'identity' }, { op: 'save' }]
})

await fileCase('empty object', {
  initial: '{}',
  channel: { primaryKey: K },
  steps: [{ op: 'identity' }, { op: 'save' }]
})

await fileCase('access keys uppercase and junk', {
  initial: JSON.stringify({
    mnemonic: '',
    founded: '',
    accessKeys: {
      [`${K.toUpperCase()}:2`]: K2.toUpperCase(),
      [`${K}:3`]: K3.slice(1),
      [`${K}:4`]: 42,
      [`${K}:5`]: null,
      nokey: K
    }
  }, null, 2),
  channel: { primaryKey: K },
  steps: [{ op: 'accessKeys' }, { op: 'save' }]
})

await fileCase('access keys keep insertion order', {
  initial: JSON.stringify({
    accessKeys: { [`${K2}:1`]: K3, [`${K}:1`]: K2, [`${K2}:1`]: K }
  }),
  channel: { primaryKey: K },
  steps: [{ op: 'accessKeys' }, { op: 'save' }]
})

await fileCase('access keys is a string', {
  initial: '{"accessKeys":"abcdef"}',
  channel: { primaryKey: K },
  steps: [{ op: 'accessKeys' }, { op: 'save' }]
})

await fileCase('access keys is a number', {
  initial: '{"accessKeys":7}',
  channel: { primaryKey: K },
  steps: [{ op: 'accessKeys' }]
})

await fileCase('founded uppercase is ignored', {
  initial: JSON.stringify({ mnemonic: MN12, founded: V12.toUpperCase(), accessKeys: {} }, null, 2),
  channel: { primaryKey: V12 },
  steps: [{ op: 'foundedKey' }, { op: 'identity' }]
})

await fileCase('founded marks this channel even for another phrase', {
  initial: JSON.stringify({ mnemonic: MN12, founded: K, accessKeys: {} }, null, 2),
  channel: { primaryKey: K, epoch: 3 },
  steps: [{ op: 'foundedKey' }, { op: 'identity' }]
})

await fileCase('vault channel is this channel', {
  initial: JSON.stringify({ mnemonic: MN12, founded: K2, accessKeys: {} }, null, 2),
  channel: { primaryKey: V12, epoch: 4 },
  steps: [{ op: 'identity' }, { op: 'identity', revealMnemonic: true }]
})

await fileCase('foreign vault gets no epoch key', {
  initial: JSON.stringify({ mnemonic: MN12, founded: K3, accessKeys: {} }, null, 2),
  channel: { primaryKey: K, epoch: 2, writable: false, role: 'writer' },
  steps: [{ op: 'identity' }]
})

await fileCase('founded unset and vault foreign', {
  initial: JSON.stringify({ mnemonic: MN12, accessKeys: {} }),
  channel: { primaryKey: K },
  steps: [{ op: 'identity' }, { op: 'foundedKey' }]
})

// --- the mnemonic option ---

await fileCase('option wins over saved phrase', {
  initial: JSON.stringify({ mnemonic: MN12, founded: '', accessKeys: {} }, null, 2),
  mnemonic: MN_OTHER,
  channel: { primaryKey: K },
  steps: [{ op: 'idVault' }, { op: 'identity', revealMnemonic: true }, { op: 'save' }]
})

await fileCase('empty option falls back to the file', {
  initial: JSON.stringify({ mnemonic: MN12, founded: '', accessKeys: {} }, null, 2),
  mnemonic: '',
  channel: { primaryKey: K },
  steps: [{ op: 'idVault' }]
})

await fileCase('invalid option throws', { mnemonic: 'not a phrase', channel: { primaryKey: K } })

await fileCase('invalid option checksum throws', {
  mnemonic: MN12.replace(/about$/, 'zoo'),
  channel: { primaryKey: K }
})

await fileCase('invalid 24-word option throws', { mnemonic: MN24.replace(/art$/, 'zoo'), channel: { primaryKey: K } })

// --- claims ---

await fileCase('claim on the matching writable channel', {
  channel: { primaryKey: V12, writable: true, localKey: V12 },
  steps: [
    { op: 'claim', phrase: MN12, alias: 'My TV' },
    { op: 'identity' },
    { op: 'appends' },
    { op: 'idVault' }
  ]
})

await fileCase('claim on the matching read-only channel', {
  channel: { primaryKey: V12, writable: false, localKey: K2, role: 'writer' },
  steps: [
    { op: 'claim', phrase: MN12, alias: '  My \n TV  ' },
    { op: 'identity', revealMnemonic: true },
    { op: 'appends' }
  ]
})

await fileCase('claim on another channel', {
  initial: JSON.stringify({ mnemonic: MN_OTHER, founded: '', accessKeys: {} }, null, 2),
  channel: { primaryKey: K, writable: true, localKey: K },
  steps: [
    { op: 'claim', phrase: MN12 },
    { op: 'appends' },
    { op: 'idVault' }
  ]
})

await fileCase('claim invalid phrase', {
  channel: { primaryKey: K },
  steps: [{ op: 'claim', phrase: 'abandon abandon abandon' }, { op: 'appends' }]
})

await fileCase('claim twice', {
  channel: { primaryKey: V12, writable: false, localKey: K2 },
  steps: [
    { op: 'claim', phrase: MN24 },
    { op: 'claim', phrase: MN12, alias: 'Studio' },
    { op: 'appends' }
  ]
})

await fileCase('claim then rotate', {
  channel: { primaryKey: V12, writable: false, localKey: K2, epoch: 1 },
  steps: [
    { op: 'claim', phrase: MN12, alias: 'Phone' },
    { op: 'rotate' },
    { op: 'identity' },
    { op: 'appends' }
  ]
})

// --- epochs ---

await fileCase('rotate without a vault', {
  channel: { primaryKey: K },
  steps: [{ op: 'rotate' }, { op: 'appends' }]
})

await fileCase('rotate once', {
  initial: JSON.stringify({ mnemonic: MN12, founded: V12, accessKeys: {} }, null, 2),
  channel: { primaryKey: V12, epoch: 1 },
  steps: [
    { op: 'rotate' },
    { op: 'epochState' },
    { op: 'identity' },
    { op: 'appends' }
  ]
})

await fileCase('rotate twice', {
  initial: JSON.stringify({ mnemonic: MN12, founded: V12, accessKeys: {} }, null, 2),
  channel: { primaryKey: V12, epoch: 5 },
  steps: [
    { op: 'rotate' },
    { op: 'rotate' },
    { op: 'identity' },
    { op: 'appends' }
  ]
})

// --- subscriber access keys ---

await fileCase('add access key', {
  channel: { primaryKey: K },
  steps: [
    { op: 'addAccessKey', request: { channel: K2, epoch: 2, key: K3 } },
    { op: 'accessKeys' }
  ]
})

await fileCase('add access key uppercase and spaces', {
  channel: { primaryKey: K },
  steps: [
    { op: 'addAccessKey', request: { channel: ` ${KUP} `, epoch: 1, key: ` ${K2.toUpperCase()} ` } },
    { op: 'accessKeys' }
  ]
})

await fileCase('add access key numeric strings', {
  channel: { primaryKey: K },
  steps: [
    { op: 'addAccessKey', request: { channel: K, epoch: '2', key: K2 } },
    { op: 'addAccessKey', request: { channel: K, epoch: true, key: K3 } }
  ]
})

await fileCase('add access key fractional epoch', {
  channel: { primaryKey: K },
  steps: [{ op: 'addAccessKey', request: { channel: K, epoch: 1.5, key: K2 } }, { op: 'accessKeys' }]
})

await fileCase('add access key epoch zero', {
  channel: { primaryKey: K },
  steps: [{ op: 'addAccessKey', request: { channel: K, epoch: 0, key: K2 } }]
})

await fileCase('add access key negative epoch', {
  channel: { primaryKey: K },
  steps: [{ op: 'addAccessKey', request: { channel: K, epoch: -2, key: K2 } }]
})

await fileCase('add access key epoch null', {
  channel: { primaryKey: K },
  steps: [{ op: 'addAccessKey', request: { channel: K, epoch: null, key: K2 } }]
})

await fileCase('add access key missing epoch', {
  channel: { primaryKey: K },
  steps: [{ op: 'addAccessKey', request: { channel: K, key: K2 } }]
})

await fileCase('add access key short hex', {
  channel: { primaryKey: K },
  steps: [{ op: 'addAccessKey', request: { channel: K, epoch: 1, key: K2.slice(1) } }]
})

await fileCase('add access key missing key', {
  channel: { primaryKey: K },
  steps: [{ op: 'addAccessKey', request: { channel: K, epoch: 1 } }]
})

await fileCase('add access key replaces a value in place', {
  initial: JSON.stringify({ mnemonic: MN12, founded: '', accessKeys: { [`${K}:2`]: K2, [`${K2}:1`]: K3 } }, null, 2),
  channel: { primaryKey: K },
  steps: [
    { op: 'addAccessKey', request: { channel: K, epoch: 2, key: K3 } },
    { op: 'accessKeys' },
    { op: 'save' }
  ]
})

await fileCase('add access key persists across restart', {
  channel: { primaryKey: K },
  steps: [
    { op: 'addAccessKey', request: { channel: K2, epoch: 7, key: K } },
    { op: 'save' }
  ]
})

// --- vault creation and founding ---

await fileCase('ensure vault returns the saved one', {
  initial: JSON.stringify({ mnemonic: MN12, founded: '', accessKeys: {} }, null, 2),
  channel: { primaryKey: K },
  steps: [{ op: 'ensureVault' }, { op: 'save' }]
})

await fileCase('mark founded', {
  initial: JSON.stringify({ mnemonic: MN12, founded: '', accessKeys: {} }),
  channel: { primaryKey: K },
  steps: [
    { op: 'markFounded', key: K },
    { op: 'foundedKey' },
    { op: 'identity' },
    { op: 'save' }
  ]
})

await fileCase('mark founded then restart', {
  channel: { primaryKey: V12 },
  mnemonic: MN12,
  steps: [
    { op: 'markFounded', key: V12 },
    { op: 'save' },
    { op: 'epochState' }
  ]
})

await fileCase('save with nothing set', {
  channel: { primaryKey: K },
  steps: [{ op: 'save' }, { op: 'identity' }, { op: 'accessKeys' }]
})

await fileCase('identity before and after a save', {
  initial: JSON.stringify({ mnemonic: MN12, founded: V12, accessKeys: {} }, null, 2),
  channel: { primaryKey: V12, writable: false, localKey: K2, role: 'mod', epoch: 9 },
  steps: [
    { op: 'identity' },
    { op: 'save' },
    { op: 'identity', revealMnemonic: true }
  ]
})

const fixture = { identityFiles: files }
writeFileSync(join(here, '..', 'identity-reference.json'), JSON.stringify(fixture, null, 2) + '\n')
console.log('wrote identity-reference.json')
console.log(` identityFiles: ${files.length}`)
