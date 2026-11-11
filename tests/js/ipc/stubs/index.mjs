// The worklet's view of the P2P node: scripted answers that the Rust fakes repeat exactly.
import { clone, fixtures } from '../state.mjs'

export const idleStatus = () => ({
  tracker: '', channel: '', writer: '', writable: false, blobs: '', blobBytes: 0, cachedBytes: 0, cacheMaxBytes: 0,
  peers: 0, lanPeers: 0, downBps: 0, upBps: 0, rxBytes: 0, txBytes: 0,
  routes: { private: bucket(), public: bucket(), lan: bucket() },
  inflightBlocks: 0, rtt: 0, retransmits: 0, sources: [],
  reciprocity: { tier: 'leecher', seededBytes: 0, contributingPeers: 0, ratio: 0, inflightMax: 0 }
})

const bucket = () => ({ peers: 0, rxBytes: 0, txBytes: 0, downBps: 0, upBps: 0 })

export const followedSources = list =>
  list.filter(s => !s.writable && (!s.primary || s.alias)).map(({ key, kind, alias }) => ({ key, kind, alias }))

const node = {
  lan: null,
  swarm: {
    listeners: [],
    on (event, listener) { if (event === 'update') node.swarm.listeners.push(listener) },
    off (event, listener) { node.swarm.listeners = node.swarm.listeners.filter(l => l !== listener) },
    async suspend () {},
    async resume () {}
  },
  suspend: async () => {},
  resume: async () => {},
  status: () => clone(fixtures.status),
  privateRouting: false,
  setCacheMaxBytes: bytes => bytes,
  hasLan: () => Boolean(node.lan),
  setLan: async factory => { node.lan = factory ? await factory({ publicKey: fixtures.lanKey.publicKey, secretKey: fixtures.lanKey.secretKey }) : null },
  discoveredSources: async () => clone(fixtures.discovered),
  search: async () => clone(fixtures.entries),
  findEntry: async () => clone(fixtures.entries[0]),
  warm: async () => {},
  addSource: async () => clone(fixtures.source),
  removeSource: async () => ({ removed: true }),
  listSources: () => clone(fixtures.sources),
  focusSource: async () => fixtures.focus,
  setPlayerActive: async () => ({ ok: true }),
  remove: async () => {},
  identity: async () => clone(fixtures.identity),
  revokeWriter: async () => clone(fixtures.revoke),
  rotateEpoch: async () => clone(fixtures.epoch),
  clearCache: async () => clone(fixtures.cacheCleared),
  openUri: async () => clone(fixtures.openUri),
  close: async () => {}
}

export async function createNode (options) {
  node.options = options
  return node
}
