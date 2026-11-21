// Generates http-reference.json by running the app's own API server (PEARTUBE_APP, else the sibling
// `../peartube`), driven with the stub node and extensions below. Run: node http.mjs
import { createServer, request } from 'node:http'
import { writeFileSync } from 'node:fs'
import { load, outFile } from './app.mjs'

const { createApi } = await load('src/http.js')

const HOST = 'relay.local:8174'
const STREAM = 'http://127.0.0.1:9/?key=aaa&blob=bbb&type=video%2Fmp4'
const KEY = 'a'.repeat(64)

const STATUS = {
  tracker: 'trk', channel: 'chan', writer: 'wr', writable: true, blobs: 'bl', blobBytes: 3,
  cachedBytes: 4, cacheMaxBytes: 5, peers: 1, lanPeers: 0, downBps: 0, upBps: 0, rxBytes: 6, txBytes: 7,
  routes: {
    private: { peers: 1, rxBytes: 6, txBytes: 7, downBps: 0, upBps: 0 },
    public: { peers: 0, rxBytes: 0, txBytes: 0, downBps: 0, upBps: 0 },
    lan: { peers: 0, rxBytes: 0, txBytes: 0, downBps: 0, upBps: 0 }
  },
  inflightBlocks: 0, rtt: 0, retransmits: 0, sources: [], reciprocity: { tier: 'leecher', seededBytes: 0, contributingPeers: 0, ratio: 0, inflightMax: 0 }
}

const SOURCE = { key: KEY, kind: 'channel', alias: 'home', primary: true, writable: true }
const MOVIE = {
  key: 'k1', id: 'imdb:tt1234567', title: 'Film', size: 10485760, sha256: 'h', local: false,
  cachedBytes: 0, private: false, epoch: 0, locked: false, source: 'source-key-1', sourceKind: 'channel',
  season: null, episode: null, seriesKey: 'film', baseTitle: 'Film', year: '2019', streamUrl: STREAM,
  magnetUrl: 'pear://watch/film'
}
const EPISODE = {
  ...MOVIE, key: 'k2', id: 'imdb:tt7654321:s01e02', title: 'Show', local: true, source: 'source-key-2',
  sourceKind: 'followed', season: 1, episode: 2, seriesKey: 'show', baseTitle: 'Show', year: null
}
const IDENTITY = {
  channel: 'chan', writer: 'wr', role: 'owner', writable: true, mnemonicSet: true,
  writers: [{ key: 'wr', role: 'owner', alias: 'main', self: true }], epoch: 1
}

const publicKey = 'b'.repeat(64)
const node = {
  blobServer: { host: '127.0.0.1', port: 0 },
  status: () => STATUS,
  search: async (id = null) => {
    if (id === null) return [MOVIE, EPISODE]
    if (id === 'imdb:tt1234567') return [MOVIE]
    if (id === 'imdb:tt1234567:s01e02' || id === 'imdb:tt7654321') return [EPISODE]
    return []
  },
  listSources: () => [SOURCE],
  discoveredSources: async () => [{ key: publicKey, kind: 'channel', title: 'Disc', via: 'trk' }],
  publishedSources: async () => [{ key: publicKey, kind: 'channel', title: 'Pub' }],
  addSource: async spec => ({ ...SOURCE, key: publicKey, alias: spec?.alias ?? '' }),
  removeSource: async key => ({ removed: key === publicKey }),
  remove: async () => {},
  publishSource: async ({ sourceKey, title }) => ({ type: 'source', sourceKey: sourceKey.toLowerCase(), kind: 'channel', title }),
  publishChannel: async payload => ({
    type: 'announce', id: payload.id, title: 'Film', size: 3, sha256: 'h', blobs: 'bl',
    blob: { blockOffset: 0, blockLength: 1, byteOffset: 0, byteLength: 3 },
    key: 'k1', channel: 'chan', streamUrl: STREAM, magnetUrl: 'pear://watch/film'
  }),
  unpublishSource: async key => ({ unpublished: true, key }),
  openUri: async () => ({ action: 'watch', entry: MOVIE }),
  identity: async ({ revealMnemonic } = {}) => (revealMnemonic ? { ...IDENTITY, mnemonic: 'a b c' } : IDENTITY),
  grantWriter: async (writer, { role, alias }) => ({ granted: true, writer, role, alias }),
  revokeWriter: async (writer, { purgeEntries }) => ({ revoked: true, writer, purgeEntries }),
  rotateEpoch: async () => ({ epoch: 2, epochKey: 'ek', accessUri: 'pear://access' }),
  claimWithMnemonic: async () => ({ claimed: true, reopenNeeded: false, channel: 'chan' }),
  clearCache: async () => ({ clearedBytes: 1, reclaimedBytes: 2 })
}

const acquirer = {
  add: async request => ({ jobId: 'job-1', id: request.id, title: request.title }),
  list: () => [{ jobId: 'job-1', state: 'queued' }],
  get: id => (id === '0123456789abcdef0123456789abcdef0123' ? { jobId: id, state: 'queued' } : null),
  cancel: id => ({ jobId: id, state: 'cancelled' })
}

const mirror = {
  list: () => [{ name: 'http-acquire' }],
  run: async (plugin, payload) => ({ plugin, payload })
}

// A library the owner removes from: this channel's album and films, and another channel's film. `stuck` and
// `unseen-stuck` cannot be removed.
const ALBUM = 'mb:' + '0'.repeat(32)
const JOBS = [1, 2, 3].map(n => [8, 4, 4, 4, 12].map(length => String(n).repeat(length)).join('-'))
function libraryNode () {
  const own = (key, id, sha256, extra = {}) => ({ ...MOVIE, key, id, sha256, source: 'chan', ...extra })
  const track = n => own(`t${n}`, `${ALBUM}:s01e0${n}`, `s${n}`, {
    title: 'Album',
    seriesKey: ALBUM,
    baseTitle: 'Album',
    year: null,
    season: 1,
    episode: n,
    ...(n === 3 ? {} : { music: { title: `Track ${n}`, artist: 'Artist' } })
  })
  let rows = [
    track(1), track(2), track(3),
    own('own-film', 'imdb:tt0000001', 'f1'),
    own('stuck', 'imdb:tt0000002', 'f2'),
    { ...MOVIE, key: 'theirs', id: 'imdb:tt0000003', sha256: 'f3', source: 'other' }
  ]
  return {
    ...node,
    search: async () => rows,
    remove: async key => {
      if (key === 'stuck' || key === 'unseen-stuck') throw new Error(`Cannot remove ${key}`)
      rows = rows.filter(row => row.key !== key)
    }
  }
}

// A mirror host with one provider, `soulseek`, one mirror under way, and files for the first album track and the
// film (deleted) and for the second track (kept).
const providerJob = status => ({ jobId: JOBS[0], plugin: 'soulseek', status })
const provider = (call, name, payload) => {
  if (name !== 'soulseek') throw Object.assign(new Error(`Unknown provider: ${name}`), { status: 404 })
  return { call, name, payload }
}
const libraryMirror = {
  ...mirror,
  searchProvider: async (name, payload) => provider('search', name, payload),
  browseProvider: async (name, payload) => provider('browse', name, payload),
  mirrorProvider: async (name, payload) => provider('mirror', name, payload),
  listProviderJobs: () => [providerJob('transferring')],
  getProviderJob: id => (id === JOBS[0] ? providerJob('transferring') : null),
  cancelProviderJob: id => (id === JOBS[0] ? providerJob('removed') : null),
  forgetImported: async gone => new Map(gone.flatMap(({ id, sha256 }) => {
    const file = { s1: 'deleted', f1: 'deleted', s2: 'kept' }[sha256]
    return file ? [[`${id}/${sha256}`, file]] : []
  }))
}

const cast = {
  status: () => ({ receivers: [], session: null, receiverMode: null }),
  pairReceiver: async () => ({ deviceId: 'd1', name: 'TV' }),
  removeReceiver: id => ({ removed: true, id }),
  castPlay: async () => ({ ok: true, action: 'play' }),
  castControl: async () => ({ ok: true, action: 'control' }),
  castStop: async () => ({ ok: true, action: 'stop' })
}

const artPoster = async imdb => (imdb === 'tt1234567' ? 'https://img.test/poster.jpg' : null)
const artBackdrop = async () => null

const full = {
  node,
  acquirer,
  mirror,
  cast,
  posterUrl: artPoster,
  backdropUrl: artBackdrop,
  identify: async url => ({ title: 'Identified', url }),
  extraStatus: () => ({ lan: null, apiUrl: 'http://relay.local:8174' }),
  get settings () { return null },
  onSourcesChanged: () => {}
}
const token = { ...full, apiToken: 'secret' }
const library = {
  ...full,
  node: libraryNode(),
  // This acquirer knows one job; any other is a provider's or nobody's.
  acquirer: { ...acquirer, cancel: id => (acquirer.get(id) ? acquirer.cancel(id) : null) },
  mirror: libraryMirror
}
const bare = {
  node: null,
  fallbackStatus: { tracker: 'trk' },
  fallbackJobs: () => [{ jobId: 'fallback' }, { jobId: JOBS[2], state: 'queued' }],
  posterUrl: null,
  backdropUrl: null
}

const json = (method, path, body, headers = {}) => ({
  method,
  path,
  headers: { host: HOST, ...headers },
  body: body === undefined ? undefined : JSON.stringify(body)
})
const raw = (method, path, body, headers = {}) => ({
  method,
  path,
  headers: { host: HOST, ...headers },
  body
})

const scenarios = {
  node: [
    json('GET', '/v1/status'),
    raw('GET', '/nope'),
    json('POST', '/v1/entries/remove', { key: 'abc' }),
    json('POST', '/v1/entries/remove', { key: '  ' }),
    raw('POST', '/v1/entries/remove', '{oops', { 'content-type': 'application/json' }),
    json('GET', '/v1/search'),
    json('GET', '/v1/search?id=imdb:tt1234567&limit=5&offset=2'),
    json('GET', '/v1/entries?source=source-key-1&q=film'),
    json('GET', '/v1/sources'),
    json('POST', '/v1/sources', { uri: 'pear://channel/' + publicKey }),
    json('DELETE', '/v1/sources/' + publicKey),
    json('POST', '/v1/channel/publish', { type: 'source', sourceKey: 'ABCDEF', title: 'T' }),
    json('POST', '/v1/channel/publish', { id: 'imdb:tt1234567' }),
    json('DELETE', '/v1/channel/sources/' + publicKey),
    json('POST', '/v1/open', { uri: 'peartube://watch/' + KEY + ':0:1:0:3?id=imdb%3Att1234567&title=Film' }),
    json('GET', '/v1/resolve?uri=' + encodeURIComponent('peartube://watch/' + KEY + ':0:1:0:3?id=imdb%3Att1234567&title=Film')),
    json('POST', '/v1/cache/clear', {}),
    json('POST', '/v1/acquire', { id: 'i', title: 't', source: 's', channel: 'c' }),
    json('GET', '/v1/jobs'),
    json('GET', '/v1/jobs/0123456789abcdef0123456789abcdef0123'),
    json('DELETE', '/v1/jobs/0123456789abcdef0123456789abcdef0123'),
    json('GET', '/v1/jobs/' + JOBS[1]),
    json('GET', '/v1/plugins'),
    json('POST', '/v1/plugins/http-acquire/run', { url: 'http://x' }),
    json('POST', '/v1/mirror', {}),
    json('GET', '/manifest.json'),
    json('GET', '/catalog/movie/peartube-movies.json'),
    json('GET', '/catalog/series/peartube-series.json'),
    json('GET', '/catalog/movie/peartube-movies.json', undefined, { 'x-forwarded-proto': 'https' }),
    json('GET', '/stream/movie/tt1234567.json'),
    json('GET', '/stream/series/tt1234567:1:2.json'),
    json('GET', '/v1/playlist.m3u'),
    json('GET', '/v1/poster/tt1234567'),
    json('GET', '/v1/backdrop/tt1234567'),
    json('GET', '/v1/poster/nope'),
    raw('OPTIONS', '/v1/status'),
    json('POST', '/v1/identify', { url: 'ftp://x' }),
    json('POST', '/v1/identify', { url: ' http://x ' }),
    json('GET', '/v1/identity?reveal=1'),
    json('POST', '/v1/identity/writers', { writer: 'w2' }),
    json('DELETE', '/v1/identity/writers/' + KEY + '?purge=1'),
    json('POST', '/v1/identity/epoch'),
    json('POST', '/v1/identity/recover', { mnemonic: 'a b c' }),
    json('GET', '/v1/identity', undefined, { origin: 'http://evil.test' }),
    json('GET', '/v1/settings'),
    json('GET', '/v1/status', undefined, { host: 'relay.test:8174' }),
    json('POST', '/v1/entries/remove', { key: 'abc' }, { host: 'relay.test:8174' }),
    json('POST', '/v1/cast/pair', {}),
    json('GET', '/v1/blob?key=a&blob=b')
  ],
  token: [
    json('GET', '/v1/status'),
    json('POST', '/v1/entries/remove', { key: 'abc' }),
    json('POST', '/v1/entries/remove', { key: 'abc' }, { authorization: 'Bearer nope' }),
    json('POST', '/v1/entries/remove', { key: 'abc' }, { authorization: 'Bearer secret' }),
    json('GET', '/v1/settings'),
    json('GET', '/v1/identity'),
    json('GET', '/v1/search?id=x'),
    json('POST', '/v1/providers/soulseek/search', {}),
    raw('OPTIONS', '/v1/status')
  ],
  library: [
    json('POST', '/v1/entries/remove', {}),
    json('POST', '/v1/entries/remove', { key: 7, seriesKey: ' ', ids: 'x', keys: [' ', 3] }),
    json('POST', '/v1/entries/remove', [1, 2]),
    json('POST', '/v1/entries/remove', { key: ' ghost ' }),
    json('POST', '/v1/entries/remove', { key: 'stuck' }),
    json('POST', '/v1/entries/remove', { key: 'theirs' }),
    json('POST', '/v1/entries/remove', { ids: [' imdb:tt0000001 ', 9, '', 'imdb:tt0000003'] }),
    json('POST', '/v1/entries/remove', { keys: ['t3', 'unseen', 'unseen-stuck', 'theirs'] }),
    json('POST', '/v1/entries/remove', { seriesKey: ` ${ALBUM.toUpperCase()} ` }),
    json('POST', '/v1/entries/remove', { seriesKey: ALBUM }),
    json('GET', '/v1/jobs'),
    json('GET', '/v1/jobs/' + JOBS[0]),
    json('DELETE', '/v1/jobs/' + JOBS[0]),
    json('GET', '/v1/jobs/' + JOBS[1]),
    json('DELETE', '/v1/jobs/' + JOBS[1]),
    json('DELETE', '/v1/jobs/0123456789abcdef0123456789abcdef0123'),
    json('GET', '/v1/jobs/' + JOBS[0].replace('1', 'A')),
    json('POST', '/v1/providers/soulseek/search', { query: 'x' }),
    json('POST', '/v1/providers/soulseek/browse', { username: 'bob' }),
    json('POST', '/v1/providers/soulseek/mirror', { username: 'bob', files: [] }),
    json('POST', '/v1/providers/nope/search', {}),
    json('POST', '/v1/providers/a%20b/browse', {}),
    json('POST', '/v1/providers/soulseek/other', {}),
    json('POST', '/v1/providers//search', {}),
    json('GET', '/v1/providers/soulseek/search'),
    raw('POST', '/v1/providers/soulseek/search', '{oops', { 'content-type': 'application/json' })
  ],
  bare: [
    json('GET', '/v1/status'),
    json('GET', '/v1/search?id=x'),
    json('GET', '/v1/entries'),
    json('GET', '/v1/sources'),
    json('GET', '/v1/jobs'),
    json('GET', '/v1/jobs/' + JOBS[2]),
    json('DELETE', '/v1/jobs/' + JOBS[2]),
    json('GET', '/v1/jobs/' + JOBS[1]),
    json('POST', '/v1/providers/soulseek/search', {}),
    json('POST', '/v1/entries/remove', { key: 'abc' }),
    json('POST', '/v1/acquire', { id: 'i' }),
    json('GET', '/v1/plugins'),
    json('POST', '/v1/mirror', {}),
    json('GET', '/v1/playlist.m3u'),
    json('GET', '/v1/identity')
  ],
  cast: [
    json('GET', '/v1/cast/status'),
    json('POST', '/v1/cast/pair', {}),
    json('DELETE', '/v1/cast/receivers/abc'),
    json('POST', '/v1/cast/play', {}),
    json('POST', '/v1/cast/control', {}),
    json('POST', '/v1/cast/stop', {})
  ]
}

const states = { node: full, token, library, bare, cast: full }
const KEEP = [
  'content-type', 'location', 'cache-control', 'vary', 'etag', 'content-disposition',
  'access-control-allow-origin', 'access-control-allow-methods', 'access-control-allow-headers',
  'access-control-expose-headers', 'cross-origin-resource-policy', 'cross-origin-opener-policy',
  'cross-origin-embedder-policy'
]

function send (port, req) {
  return new Promise((resolve, reject) => {
    const reqStream = request({ host: '127.0.0.1', port, method: req.method, path: req.path, headers: req.headers }, res => {
      const chunks = []
      res.on('data', chunk => chunks.push(chunk))
      res.on('end', () => {
        const headers = {}
        for (const name of KEEP) if (res.headers[name] !== undefined) headers[name] = res.headers[name]
        resolve({
          method: req.method,
          path: req.path,
          requestHeaders: req.headers,
          requestBody: req.body === undefined ? null : req.body,
          status: res.statusCode,
          headers,
          body: Buffer.concat(chunks).toString('utf8')
        })
      })
    })
    reqStream.once('error', reject)
    if (req.body !== undefined) reqStream.write(req.body)
    reqStream.end()
  })
}

async function run (state, requests) {
  const srv = createApi(state)
  await new Promise(resolve => srv.listen(0, '127.0.0.1', resolve))
  const port = srv.address().port
  const out = []
  for (const req of requests) out.push(await send(port, req))
  await new Promise(resolve => srv.close(resolve))
  return out
}

const reference = { host: HOST, scenarios: [] }
for (const [name, requests] of Object.entries(scenarios)) {
  reference.scenarios.push({ name, responses: await run(states[name], requests) })
}
writeFileSync(outFile('http-reference.json'), JSON.stringify(reference, null, 2) + '\n')
console.log(`wrote ${outFile('http-reference.json')} (${reference.scenarios.length} scenarios)`)
