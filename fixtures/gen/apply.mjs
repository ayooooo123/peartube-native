// Generates ../apply-reference.json from the app's own apply.js copy in this folder, driven through a stub
// view/host that records put/del/flush, addWriter/removeWriter and onWriterChange. `node apply.mjs` writes the
// reference; tests/apply.rs replays every case against the Rust handler and compares the two call sequences.
import { createHash } from 'node:crypto'
import { writeFileSync } from 'node:fs'
import { dirname, join } from 'node:path'
import { fileURLToPath } from 'node:url'
import b4a from 'b4a'
import { makeApply, makeIsTrusted } from './apply.js'
import { deriveIdentity, entropyToMnemonic } from './mnemonic.js'

const here = dirname(fileURLToPath(import.meta.url))
const hex = b => b4a.toString(b, 'hex')
const sha = (...parts) => {
  const h = createHash('sha256')
  for (const p of parts) h.update(b4a.from(`peartube:fixtures:apply:v1:${p}`))
  return h.digest()
}
const keyOf = name => hex(sha(`key:${name}`))

const CHANNEL = keyOf('channel')
const OWNER = keyOf('owner-pub') // a 64-hex owner key the claims are signed against
const W = Object.fromEntries(
  ['a', 'b', 'c', 'd', 'e', 'f', 'g', 'x'].map(n => [n, keyOf(`writer:${n}`)])
)
const identity = deriveIdentity(entropyToMnemonic(sha('mnemonic').subarray(0, 16)))
const CLAIM_OWNER = identity.ownerPublicKey
const signClaim = (channel, writer) => identity.signClaim(channel, writer)

const viewEntry = (key, value) => ({ key: hex(b4a.from(key)), value: hex(b4a.from(value)) })
const node = (key, value) => ({ key, value: hex(b4a.from(typeof value === 'string' ? value : JSON.stringify(value))) })
const announce = (extra = {}) => ({
  type: 'announce',
  id: 'clip:abc123',
  title: 'Clip',
  size: 4096,
  sha256: keyOf('sha'),
  blobs: keyOf('blobs'),
  blob: { blockOffset: 512, blockLength: 2, byteOffset: 0, byteLength: 4096 },
  ...extra
})

// --- harness -------------------------------------------------------------

// The reference's view: a sorted key/value map with a batch that records and applies ops on flush, exactly the
// surface apply.js touches (get, write().tryPut/tryDelete/flush, createReadStream).
function makeView (entries, calls) {
  const map = new Map(entries.map(e => [b4a.toString(b4a.from(e.key, 'hex')), b4a.from(e.value, 'hex')]))
  return {
    map,
    async get (key) {
      const value = map.get(b4a.toString(key))
      return value ? { key, value } : null
    },
    write () {
      const batch = []
      return {
        tryPut (key, value) {
          calls.push({ t: 'put', key: hex(key), value: hex(value) })
          batch.push(['put', key, value])
        },
        tryDelete (key) {
          calls.push({ t: 'del', key: hex(key) })
          batch.push(['del', key])
        },
        async flush () {
          calls.push({ t: 'flush' })
          for (const [kind, key, value] of batch) {
            if (kind === 'put') map.set(b4a.toString(key), b4a.from(value))
            else map.delete(b4a.toString(key))
          }
        }
      }
    },
    async * createReadStream () {
      for (const [key, value] of [...map].sort(([a], [b]) => Buffer.compare(b4a.from(a), b4a.from(b)))) {
        yield { key: b4a.from(key), value }
      }
    }
  }
}

async function runApply (scenario) {
  const calls = []
  const view = makeView(scenario.view, calls)
  const host = {
    async addWriter (key, opts) {
      calls.push({ t: 'addWriter', key: hex(key), isIndexer: Boolean(opts && opts.isIndexer) })
    },
    async removeWriter (key) {
      calls.push({ t: 'removeWriter', key: hex(key) })
    }
  }
  const apply = makeApply(
    () => CHANNEL,
    (writer, added) => calls.push({ t: 'writerChange', writer, added })
  )
  const nodes = scenario.nodes.map(n => ({ key: b4a.from(n.key, 'hex'), value: b4a.from(n.value, 'hex') }))
  await apply(nodes, view, host)
  const out = [...view.map]
    .sort(([a], [b]) => Buffer.compare(b4a.from(a), b4a.from(b)))
    .map(([key, value]) => ({ key: hex(b4a.from(key)), value: hex(value) }))
  return { ...scenario, calls, result: out }
}

async function isTrusted (scenario) {
  const calls = []
  const view = makeView(scenario.view, calls)
  const trusted = await makeIsTrusted(() => CHANNEL)(b4a.from(scenario.key, 'hex'), scenario.reference ? view : null)
  return { ...scenario, trusted }
}

// channel.js `update`: mirrorLanding(changes) then ctx.update(). The landing set lives for the channel's life.
function runUpdates (scenario) {
  const calls = []
  const landed = new Set()
  const mirrorLanding = changes => {
    for (const name of ['system', 'view']) {
      const c = changes && changes[name]
      if (!c || !c.to || !c.to.key || c.incremental) continue
      if (landed.has(c.to.key)) continue
      landed.add(c.to.key)
      calls.push({ t: 'mirror', key: c.to.key })
    }
    calls.push({ t: 'update' })
  }
  for (const step of scenario.steps) mirrorLanding(step)
  return { ...scenario, calls }
}

// --- apply scenarios -----------------------------------------------------

const init = (extra = {}) => node(CHANNEL, { type: 'init', owner: CLAIM_OWNER, epoch: 1, ...extra })
const grant = (writer, extra = {}) => node(CHANNEL, { type: 'grant', writer, role: 'writer', ...extra })
const revoke = (writer, extra = {}) => node(CHANNEL, { type: 'revoke', writer, ...extra })

const scenarios = [
  {
    name: 'owner-init',
    view: [],
    nodes: [init()]
  },
  {
    name: 'owner-init-bare',
    view: [],
    nodes: [node(CHANNEL, { type: 'init' })]
  },
  {
    name: 'stranger-init-legacy',
    view: [],
    nodes: [node(W.a, { type: 'init', epoch: 2 })]
  },
  {
    name: 'claim-valid',
    view: [],
    nodes: [
      node(CHANNEL, { type: 'init', owner: CLAIM_OWNER }),
      node(W.a, { type: 'claim', sig: signClaim(CHANNEL, W.a), alias: 'laptop' })
    ]
  },
  {
    name: 'claim-bad-signature',
    view: [],
    nodes: [
      node(CHANNEL, { type: 'init', owner: CLAIM_OWNER }),
      node(W.a, { type: 'claim', sig: keyOf('blobs') + keyOf('sha'), alias: 'forged' })
    ]
  },
  {
    name: 'claim-signed-for-another-channel',
    view: [],
    nodes: [
      node(CHANNEL, { type: 'init', owner: CLAIM_OWNER }),
      node(W.b, { type: 'claim', sig: signClaim(keyOf('elsewhere'), W.b) })
    ]
  },
  {
    name: 'claim-signed-by-another-key',
    view: [],
    nodes: [
      node(CHANNEL, { type: 'init', owner: OWNER }),
      node(W.c, { type: 'claim', sig: signClaim(CHANNEL, W.c) })
    ]
  },
  {
    name: 'claim-revoked',
    view: [viewEntry(`revoked/${W.a}`, '1')],
    nodes: [
      node(CHANNEL, { type: 'init', owner: CLAIM_OWNER }),
      node(W.a, { type: 'claim', sig: signClaim(CHANNEL, W.a) })
    ]
  },
  {
    name: 'claim-without-owner-is-legacy',
    view: [],
    nodes: [node(W.d, { type: 'claim', sig: signClaim(CHANNEL, W.d) })]
  },
  {
    name: 'announce-carries-a-claim',
    view: [],
    nodes: [
      node(CHANNEL, { type: 'init', owner: CLAIM_OWNER }),
      node(W.e, announce({ claim: signClaim(CHANNEL, W.e), alias: 'tv' }))
    ]
  },
  {
    name: 'grant-and-revoke-with-purge',
    view: [
      viewEntry(`peer/${W.b}`, 'peer'),
      viewEntry(`source/${W.b}/${keyOf('src')}`, 'source'),
      viewEntry(`clip/${W.b}/1`, 'entry'),
      viewEntry('clip/other/1', 'other'),
      viewEntry('meta/epoch', '1')
    ],
    nodes: [
      init(),
      grant(W.b, { role: 'mod', alias: 'moderator' }),
      node(W.b, announce()),
      revoke(W.b, { purgeEntries: true })
    ]
  },
  {
    name: 'revoke-own-key-is-ignored',
    view: [],
    nodes: [init(), revoke(CHANNEL)]
  },
  {
    name: 'revoked-writer-ops-are-ignored',
    view: [viewEntry(`revoked/${W.f}`, '1')],
    nodes: [init(), node(W.f, announce()), node(W.f, { type: 'remove', key: 'clip/other/1' })]
  },
  {
    name: 'optimistic-outsider',
    view: [viewEntry(`writer/${W.g}`, JSON.stringify({ role: '' }))],
    nodes: [init(), node(W.x, announce()), node(W.g, announce())]
  },
  {
    name: 'legacy-writer-announce',
    view: [],
    nodes: [node(W.d, announce())]
  },
  {
    name: 'mod-can-remove-anything',
    view: [],
    nodes: [init(), grant(W.e, { role: 'mod' }), node(W.e, { type: 'remove', key: 'clip/other/1' })]
  },
  {
    name: 'writer-removes-own-key-only',
    view: [],
    nodes: [
      init(),
      grant(W.f),
      node(W.f, { type: 'remove', key: `clip/${W.f}/1` }),
      node(W.f, { type: 'remove', key: `clip/${W.e}/1` })
    ]
  },
  {
    name: 'remove-protected-prefixes',
    view: [],
    nodes: [
      init(),
      node(CHANNEL, { type: 'remove', key: 'meta/owner' }),
      node(CHANNEL, { type: 'remove', key: 'writer/x' }),
      node(CHANNEL, { type: 'remove', key: 'revoked/x' })
    ]
  },
  {
    name: 'source-op',
    view: [],
    nodes: [
      init(),
      node(CHANNEL, { type: 'source', sourceKey: keyOf('src'), kind: 'channel', title: 'Other channel' })
    ]
  },
  {
    name: 'epoch-bumps-upward-only',
    view: [],
    nodes: [
      init(),
      node(CHANNEL, { type: 'epoch', epoch: 5 }),
      node(CHANNEL, { type: 'epoch', epoch: 3 }),
      node(CHANNEL, { type: 'epoch', epoch: 6 })
    ]
  },
  {
    name: 'epoch-from-existing-view',
    view: [viewEntry('meta/epoch', '4')],
    nodes: [node(CHANNEL, { type: 'epoch', epoch: 7 }), node(CHANNEL, { type: 'epoch', epoch: 4 })]
  },
  {
    name: 'owner-op-sets-owner-once',
    view: [],
    nodes: [node(CHANNEL, { type: 'init' }), node(CHANNEL, { type: 'owner', owner: OWNER })]
  },
  {
    name: 'role-read-from-view-record',
    view: [viewEntry(`writer/${W.a}`, JSON.stringify({ role: 'owner', alias: 'old' }))],
    nodes: [node(W.a, { type: 'grant', writer: W.b, role: 'writer', alias: ' ' })]
  },
  {
    name: 'malformed-ops',
    view: [],
    nodes: [
      node(CHANNEL, '{not json'),
      node(CHANNEL, '[]'),
      node(CHANNEL, '"init"'),
      node(CHANNEL, { type: 'grant', writer: 'zz', role: 'writer' }),
      node(CHANNEL, { type: 'grant', writer: W.a, role: 'admin' }),
      node(CHANNEL, { type: 'revoke', writer: W.a.slice(1) }),
      node(CHANNEL, { type: 'remove' }),
      node(CHANNEL, { type: 'source', sourceKey: keyOf('src'), kind: 'peer', title: 'x' }),
      node(CHANNEL, { type: 'announce', id: 'nope', title: 'x', size: 1 }),
      node(CHANNEL, { type: 'claim', sig: keyOf('sha') }),
      node(CHANNEL, { type: 'init', epoch: 0 }),
      node(CHANNEL, { type: 'unknown' })
    ]
  },
  {
    name: 'mixed-batch',
    view: [viewEntry(`revoked/${W.c}`, '1')],
    nodes: [
      init({ epoch: 2 }),
      node(W.a, { type: 'claim', sig: signClaim(CHANNEL, W.a), alias: 'phone' }),
      node(W.a, announce()),
      grant(W.b, { role: 'writer', alias: 'laptop' }),
      node(W.b, announce({ id: 'show:s01e02' })),
      node(W.c, announce()),
      node(W.a, { type: 'epoch', epoch: 9 }),
      node(W.a, { type: 'remove', key: `clip/${W.b}/1` }),
      revoke(W.b)
    ]
  }
]

// --- isTrusted scenarios -------------------------------------------------

const trustedScenarios = [
  { name: 'own-channel', key: CHANNEL, view: [], reference: true },
  { name: 'no-reference', key: W.a, view: [], reference: false },
  { name: 'linked-owner', key: W.a, view: [viewEntry(`writer/${W.a}`, JSON.stringify({ role: 'owner' }))], reference: true },
  { name: 'plain-writer', key: W.b, view: [viewEntry(`writer/${W.b}`, JSON.stringify({ role: 'writer' }))], reference: true },
  {
    name: 'revoked-owner',
    key: W.c,
    view: [viewEntry(`writer/${W.c}`, JSON.stringify({ role: 'owner' })), viewEntry(`revoked/${W.c}`, '1')],
    reference: true
  },
  { name: 'unknown-writer', key: W.d, view: [], reference: true },
  { name: 'bad-record', key: W.e, view: [viewEntry(`writer/${W.e}`, 'not json')], reference: true }
]

// --- update scenarios ----------------------------------------------------

const head = (name, length) => ({ key: keyOf(name), length })
const change = (from, to, incremental = true) => ({ flushes: 1, from, to, incremental })
const updateScenarios = [
  {
    name: 'incremental-changes-are-not-mirrored',
    steps: [{ view: change(head('v0', 1), head('v1', 2)), system: change(head('s0', 1), head('s1', 2)) }]
  },
  {
    name: 'landing-mirrors-view-and-system',
    steps: [{ view: change(head('v0', 1), head('v1', 2), false), system: change(head('s0', 1), head('s1', 2), false) }]
  },
  {
    name: 'landing-mirrors-once',
    steps: [
      { view: change(head('v0', 1), head('v1', 2), false), system: change(head('s0', 1), head('s1', 2), false) },
      { view: change(head('v1', 2), head('v1', 2), false), system: change(head('s1', 2), head('s1', 2), false) }
    ]
  },
  {
    name: 'empty-changes',
    steps: [{}, { view: null, system: null }]
  }
]

const fixture = {
  channel: CHANNEL,
  ownerPublicKey: CLAIM_OWNER,
  apply: await Promise.all(scenarios.map(runApply)),
  isTrusted: await Promise.all(trustedScenarios.map(isTrusted)),
  update: updateScenarios.map(runUpdates)
}
writeFileSync(join(here, '..', 'apply-reference.json'), `${JSON.stringify(fixture, null, 1)}\n`)
console.log(`ok: ${fixture.apply.length} apply, ${fixture.isTrusted.length} isTrusted, ${fixture.update.length} update scenarios`)
