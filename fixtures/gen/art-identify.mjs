// Generates art-identify-reference.json from the app's own src/art.js and src/identify.js.
// `node art-identify.mjs` writes the reference; the Rust port replays every case against the same routes,
// subprocess plans and recorded subprocess output in tests/art_identify.rs.
import { chmodSync, copyFileSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs'
import { createServer } from 'node:http'
import { tmpdir } from 'node:os'
import { dirname, join } from 'node:path'
import { fileURLToPath } from 'node:url'
import { load, outFile } from './app.mjs'

const { send, metaFor, posterUrl, backdropUrl } = await load('src/art.js')
const {
  parseName, fileName, cinemetaSearch, candidateEntry, identify, probe, ubitInfo, downloadPage, norm, strong
} = await load('src/identify.js')

const here = dirname(fileURLToPath(import.meta.url))
const fakeUbit = join(here, 'fake-ubit.mjs')

const captured = async run => {
  try {
    return { out: await run() }
  } catch (error) {
    return { error: { message: error.message, status: error.status ?? null, code: error.code ?? null } }
  }
}

// A one-shot http server that records what was asked for. {@BASE} stands in for the port.
async function serve (routes = []) {
  const requests = []
  const server = createServer((req, res) => {
    requests.push({ method: req.method, path: req.url, userAgent: String(req.headers['user-agent'] || '') })
    const route = routes.find(r => r.path === req.url)
    res.on('error', () => {})
    const reply = () => {
      try {
        if (!route) {
          res.writeHead(404)
          res.end()
          return
        }
        res.writeHead(route.status || 200, route.headers || {})
        res.end(req.method === 'HEAD' ? undefined : route.body || '')
      } catch {}
    }
    if (route?.delayMs) setTimeout(reply, route.delayMs)
    else reply()
  })
  await new Promise(resolve => server.listen(0, '127.0.0.1', resolve))
  const base = `http://127.0.0.1:${server.address().port}`
  return { base, requests, close: () => new Promise(resolve => server.close(resolve)) }
}

// Puts a fake `ubit` on PATH and answers with the plan it was given.
function fakeUbitDir (plan) {
  const dir = mkdtempSync(join(tmpdir(), 'peartube-ubit-'))
  copyFileSync(fakeUbit, join(dir, 'ubit'))
  chmodSync(join(dir, 'ubit'), 0o755)
  writeFileSync(join(dir, 'plan.json'), JSON.stringify(plan))
  return dir
}

const withUbit = async (plan, run) => {
  const dir = fakeUbitDir(plan)
  const path = process.env.PATH
  const ubitBin = process.env.UBIT_BIN
  process.env.PATH = `${dir}:${path}`
  process.env.UBIT_BIN = join(dir, 'ubit')
  try {
    return await run(dir)
  } finally {
    process.env.PATH = path
    if (ubitBin === undefined) delete process.env.UBIT_BIN
    else process.env.UBIT_BIN = ubitBin
    rmSync(dir, { recursive: true, force: true })
  }
}

const observed = (dir, base, outDir) => {
  let seen
  try {
    seen = JSON.parse(readFileSync(join(dir, 'observed.json'), 'utf8'))
  } catch {
    return { argv: null, configDir: null }
  }
  return {
    argv: seen.argv.map(arg => String(arg).split(outDir || '\0').join('{OUT}').split(base || '\0').join('{BASE}')),
    configDir: seen.configDir
  }
}

const fill = (value, base) => JSON.parse(JSON.stringify(value).split('{BASE}').join(base))

// The port is different on every run, so recorded values keep a {BASE} placeholder.
const template = (value, base) => JSON.parse(JSON.stringify(value).split(base).join('{BASE}'))

const fixture = { source: 'fixtures/gen/art-identify.mjs from the PEARTUBE_APP checkout' }

// --- parseName ---
fixture.parseName = [
  'The.Matrix.1999.1080p.BluRay.x264-GROUP.mkv',
  "Charlotte's Web 2006 720p.mp4",
  'some_show_S01E02_1080p.mkv',
  'Show.1x02.HDTV.avi',
  'movie.2020.mkv.mp4',
  'https://example.com/a/b/The%20Big%20Film%202019%20720p.mkv?x=1',
  '',
  '1080p.mkv',
  'Movie (2019) [1080p] [x265].mkv',
  'Movie.2019.2160p.WEB-DL.DDP5.1.Atmos.HEVC.mkv',
  'Show S1 E2.mp4',
  'ShowS01E02.mkv',
  'A Film 1998 480p AAC.mp3',
  'Amélie.2001.1080p.mkv',
  '1920x1080.mkv',
  'The.Show.S10E100.720p.mkv',
  'file.MKV',
  'x265.only.mkv',
  'Movie.2019.1080p.eac3.x265.mkv',
  'https%3A%2F%2Fexample.com%2Fa%2FFilm%202021.mp4',
  'trailing.dots..',
  'Movie.1948.480p.DivX.mp3',
  'https://example.com/a/%zz.mkv',
  'https://example.com/',
  'https://example.com/a/Film.2020.mkv?token=1'
].map(input => ({ input, out: parseName(input) }))

// --- fileName ---
fixture.fileName = [
  { url: 'https://cdn.example/dld/abc?filename=Film.2020.1080p.mkv', headers: {} },
  { url: 'https://cdn.example/dld/abc', headers: { 'content-disposition': "attachment; filename*=UTF-8''Film%20Name.mkv" } },
  { url: 'https://cdn.example/dld/abc', headers: { 'content-disposition': 'attachment; filename="film.mkv"' } },
  { url: 'https://cdn.example/dld/abc', headers: {} },
  { url: 'https://cdn.example/dld/abc?filename=notes.txt', headers: { 'content-disposition': "attachment; filename*=UTF-8''Real.mkv" } },
  { url: 'https://cdn.example/a/My%20Film.mkv', headers: {} },
  { url: 'https://cdn.example/dld/abc?filename=A+B.mkv', headers: {} },
  { url: 'https://cdn.example/a/%zz.mkv', headers: {} },
  { url: 'https://cdn.example/dl/100%25?filename=', headers: {} },
  { url: 'https://cdn.example/dl/100%', headers: {} },
  { url: 'https://cdn.example/dld/abc', headers: { 'content-disposition': 'attachment; filename=bare.mkv' } },
  // The bare token stops at whitespace (`[^;\s]+`), so a spaced release name is no name at all...
  { url: 'https://cdn.example/dld/abc', headers: { 'content-disposition': 'attachment; filename=Dune Part Two.mkv' } },
  // ...and it is never trimmed: a name that starts with a space does not match.
  { url: 'https://cdn.example/dld/abc', headers: { 'content-disposition': 'attachment; filename= Dune.2021.mkv' } },
  { url: 'https://cdn.example/dld/abc', headers: { 'content-disposition': 'attachment; filename= "x.mkv"' } },
  { url: 'not a url', headers: {} },
  { url: 'https://cdn.example/dld/abc', headers: { 'content-disposition': 'attachment; filename*=utf-8\'\'Lower.MKV' } }
].map(test => {
  try {
    return { ...test, out: fileName(test.url, test.headers) }
  } catch (error) {
    return { ...test, error: error.message }
  }
})

// --- norm / strong / candidateEntry ---
fixture.norm = [
  'The Matrix', 'the matrix', 'AN American Werewolf', 'Amélie', '  Æon Flux! ', 'A', 'an', 'The  Matrix  1999',
  'Das Boot (1981)', 'Movie: Part 2', 'theatre', 'THE THE'
].map(input => ({ input, out: norm(input) }))

const candidates = [
  { imdb: 'tt0133093', type: 'movie', name: 'The Matrix', year: 1999, poster: 'https://p.jpg' },
  { imdb: 'tt0234215', type: 'movie', name: 'The Matrix Reloaded', year: 2003, poster: null },
  { imdb: 'tt0106062', type: 'series', name: 'Matrix', year: 1993, poster: null },
  { imdb: 'tt9999999', type: 'movie', name: 'The Matrix', year: null, poster: null }
]
const parseds = [
  { title: 'The Matrix', year: 1999, season: null, episode: null, resolution: '', codec: '', audio: '', ext: '' },
  { title: 'The Matrix', year: 2003, season: null, episode: null, resolution: '', codec: '', audio: '', ext: '' },
  { title: 'The Matrix', year: null, season: 2, episode: 5, resolution: '', codec: '', audio: '', ext: '' },
  { title: 'Something Else', year: null, season: null, episode: null, resolution: '', codec: '', audio: '', ext: '' }
]
fixture.strong = []
fixture.candidateEntry = []
for (const candidate of candidates) {
  for (const parsed of parseds) {
    fixture.strong.push({ candidate, parsed, out: strong(candidate, parsed) })
    fixture.candidateEntry.push({ candidate, parsed, out: candidateEntry(candidate, parsed) })
  }
}

// --- the clip id derive (pure, so the hash of the link url is fixture-testable) ---
const { createHash } = await import('node:crypto')
const clipFor = (kind, pageId, url) =>
  'clip:' + createHash('sha256').update(kind === 'page' ? pageId : url).digest('hex').slice(0, 16)
fixture.clipIds = [
  { kind: 'file', url: 'https://example.com/Film.2020.mkv', pageId: null },
  { kind: 'page', url: 'https://example.com/watch?v=1', pageId: 'https://vid.example/watch?v=1' }
].map(test => ({ ...test, out: clipFor(test.kind, test.pageId, test.url) }))

// --- send: request shapes, redirects, byte cap, timeout ---
fixture.send = []
const sendCases = [
  {
    name: 'get with default user agent',
    route: { path: '/plain', status: 200, headers: { 'content-type': 'text/plain' }, body: 'hello' },
    call: { url: '{BASE}/plain', opts: {} },
    expect: { status: 200, urlPath: '/plain', contentType: 'text/plain', body: 'hello' }
  },
  {
    name: 'caller headers win over the default user agent',
    route: { path: '/hdr', status: 201, headers: { 'content-type': 'application/json' }, body: '{}' },
    call: { url: '{BASE}/hdr', opts: { headers: { 'user-agent': 'Custom/1', 'x-extra': '1' } } },
    expect: { status: 201, urlPath: '/hdr', contentType: 'application/json', body: '{}' },
    userAgent: 'Custom/1'
  },
  {
    name: 'head discards the body',
    route: { path: '/head', status: 200, headers: { 'content-type': 'video/mp4', 'content-length': '4' }, body: 'body' },
    call: { url: '{BASE}/head', opts: { method: 'HEAD' } },
    expect: { status: 200, urlPath: '/head', contentType: 'video/mp4', body: '' }
  },
  {
    name: 'follows five redirects',
    routes: [
      { path: '/r1', status: 302, headers: { location: '/r2' } },
      { path: '/r2', status: 302, headers: { location: '/r3' } },
      { path: '/r3', status: 302, headers: { location: '/r4' } },
      { path: '/r4', status: 302, headers: { location: '/r5' } },
      { path: '/r5', status: 302, headers: { location: '/r6' } },
      { path: '/r6', status: 200, headers: { 'content-type': 'text/plain' }, body: 'end' }
    ],
    call: { url: '{BASE}/r1', opts: {} },
    expect: { status: 200, urlPath: '/r6', contentType: 'text/plain', body: 'end' }
  },
  {
    name: 'stops after five redirects',
    routes: [1, 2, 3, 4, 5, 6].map(n => ({ path: `/s${n}`, status: 302, headers: { location: `/s${n + 1}` } })),
    call: { url: '{BASE}/s1', opts: {} },
    expect: { status: 302, urlPath: '/s6', contentType: null, body: '' }
  },
  {
    name: 'times out',
    route: { path: '/slow', status: 200, body: 'late', delayMs: 2000 },
    call: { url: '{BASE}/slow', opts: { timeoutMs: 120 } },
    error: 'Timed out'
  },
  { name: 'rejects a non-http scheme', call: { url: 'ftp://example.com/x', opts: {} }, error: 'Only http(s) links can be added' },
  { name: 'rejects a bad link', call: { url: 'not a url', opts: {} }, error: 'Not a valid link' }
]
for (const test of sendCases) {
  const server = await serve(test.routes || (test.route ? [test.route] : []))
  const opts = fill(test.call.opts, server.base)
  const result = await captured(() => send(fill(test.call.url, server.base), opts))
  const entry = {
    name: test.name,
    url: test.call.url,
    routes: test.routes || (test.route ? [test.route] : []),
    method: opts.method || 'GET',
    userAgent: test.userAgent || 'PearTube',
    maxBytes: opts.maxBytes ?? null,
    timeoutMs: opts.timeoutMs ?? null,
    requests: server.requests
  }
  if (test.error) entry.error = { message: test.error, status: null, code: null }
  else {
    const { status, headers, url, body } = result.out
    entry.out = { status, urlPath: new URL(url).pathname + new URL(url).search, contentType: headers['content-type'] ?? null, body }
  }
  if (test.expect) {
    const exp = test.expect
    const same = entry.out?.status === exp.status && entry.out?.urlPath === exp.urlPath &&
      entry.out?.contentType === (exp.contentType ?? null) && entry.out?.body === exp.body
    if (!same) throw new Error(`${test.name}: unexpected ${JSON.stringify(entry.out)}`)
  }
  fixture.send.push(entry)
  await server.close()
}

// --- cinemetaSearch ---
fixture.cinemeta = []
const metaBody = JSON.stringify({
  metas: [
    { imdb_id: 'tt0133093', name: 'The Matrix', releaseInfo: '1999-03-31', poster: 'https://p.jpg' },
    { imdb_id: 'tt0234215', name: 'The Matrix Reloaded', releaseInfo: '2003-05-15', poster: 'https://p2.jpg' },
    { imdb_id: 'tt0106062', name: 'Matrix', releaseInfo: '1993-01-01' },
    { id: 'tt10838180', name: 'The Matrix Resurrections', year: '2021', poster: 42 },
    { imdb_id: 'tt0111161', name: 'The Shawshank Redemption', releaseInfo: '1994-09-22' },
    { imdb_id: 'bad', name: 'The Matrix', releaseInfo: '1999-01-01' },
    { imdb_id: 'tt0317219', name: 'Cars', releaseInfo: '2006-06-09' },
    { imdb_id: 'tt3606756', name: 'Incredibles 2', releaseInfo: '2018-06-15' },
    { name: 'no id' }
  ]
})
const cinemetaCases = [
  { name: 'movie search ranks the exact title and year first', title: 'The Matrix', year: 1999, season: null, status: 200, body: metaBody },
  { name: 'a season makes it a series search', title: 'The Matrix', year: null, season: 2, status: 200, body: metaBody },
  { name: 'a non-200 answer is no candidates', title: 'The Matrix', year: 1999, season: null, status: 500, body: 'boom' },
  { name: 'broken json is no candidates', title: 'The Matrix', year: 1999, season: null, status: 200, body: 'not json' },
  { name: 'no base is no candidates', title: 'The Matrix', year: 1999, season: null, base: '' },
  { name: 'no title is no candidates', title: '', year: 1999, season: null }
]
for (const test of cinemetaCases) {
  const server = await serve(test.body === undefined
    ? []
    : [{ path: `/catalog/${test.season ? 'series' : 'movie'}/top/search=${encodeURIComponent(test.title)}.json`, status: test.status, headers: { 'content-type': 'application/json' }, body: test.body }])
  const base = test.base === undefined ? server.base : test.base
  const out = await cinemetaSearch(base, { title: test.title, year: test.year, season: test.season })
  fixture.cinemeta.push({
    name: test.name,
    title: test.title,
    year: test.year,
    season: test.season,
    hasBase: base !== '',
    status: test.status ?? null,
    body: test.body ?? null,
    requests: server.requests,
    out
  })
  await server.close()
}

// --- metaFor / poster / backdrop ---
fixture.meta = []
const metaCases = [
  {
    name: 'a movie poster is enough',
    imdb: 'tt0111161',
    steps: [{ routes: [{ path: '/meta/movie/tt0111161.json', status: 200, body: JSON.stringify({ meta: { name: 'The Shawshank Redemption', poster: 'https://img/p.jpg', background: 'https://img/b.jpg' } }) }] }]
  },
  {
    name: 'a posterless movie falls through to the series answer',
    imdb: 'tt0106062',
    steps: [{
      routes: [
        { path: '/meta/movie/tt0106062.json', status: 200, body: JSON.stringify({ meta: { name: 'Matrix', poster: '' } }) },
        { path: '/meta/series/tt0106062.json', status: 200, body: JSON.stringify({ meta: { name: 'Matrix', poster: 'https://img/s.jpg', background: 'https://img/sb.jpg' } }) }
      ]
    }]
  },
  {
    name: 'an empty meta falls through',
    imdb: 'tt0209144',
    steps: [{
      routes: [
        { path: '/meta/movie/tt0209144.json', status: 200, body: JSON.stringify({ meta: null }) },
        { path: '/meta/series/tt0209144.json', status: 200, body: JSON.stringify({ meta: { name: 'Memento', poster: 'https://img/m.jpg' } }) }
      ]
    }]
  },
  {
    name: 'broken json and a 500 leave no meta and are cached as a failure',
    imdb: 'tt1375666',
    steps: [
      { routes: [] },
      { routes: [{ path: '/meta/movie/tt1375666.json', status: 200, body: JSON.stringify({ meta: { poster: 'https://img/late.jpg' } }) }] }
    ]
  },
  { name: 'an imdb id that is not tt-digits is never asked for', imdb: 'tt12', steps: [{ routes: [] }] },
  {
    name: 'a number poster is not art',
    imdb: 'tt0816692',
    steps: [{ routes: [{ path: '/meta/movie/tt0816692.json', status: 200, body: JSON.stringify({ meta: { poster: 7, background: 'https://img/i.jpg' } }) }] }]
  }
]
for (const test of metaCases) {
  const steps = []
  for (const step of test.steps) {
    const server = await serve(step.routes)
    const meta = await metaFor(server.base, test.imdb)
    const poster = await posterUrl(server.base, test.imdb)
    const backdrop = await backdropUrl(server.base, test.imdb)
    steps.push({ routes: step.routes, meta: meta ?? null, poster: poster ?? null, backdrop: backdrop ?? null, requests: server.requests })
    await server.close()
  }
  fixture.meta.push({ name: test.name, imdb: test.imdb, steps })
}

// --- ubitInfo: recorded subprocess output ---
fixture.ubitInfo = []
const infoCases = [
  {
    name: 'a readable page',
    url: 'https://vid.example/watch?v=1',
    plan: { stdout: [JSON.stringify({ title: 'A Clip', formats: [{ height: 720 }] })], exit: 0 }
  },
  {
    name: 'DRM is refused',
    url: 'https://vid.example/watch?v=drm',
    plan: { stdout: [JSON.stringify({ title: 'Locked', isDRM: true })], exit: 0 }
  },
  { name: 'no metadata at all', url: 'https://vid.example/bad', plan: { stdout: ['not json'], exit: 0 } },
  { name: 'an empty answer', url: 'https://vid.example/empty', plan: { stdout: [], exit: 0 } },
  {
    name: 'stderr carries the status',
    url: 'https://vid.example/gone',
    plan: { stdout: [], stderr: ['warning: starting\n', 'error: HTTP 404: not found\n'], exit: 1 }
  },
  { name: 'a DRM complaint is 451', url: 'https://vid.example/drm2', plan: { stdout: [], stderr: ['error: DRM protected stream\n'], exit: 1 } },
  { name: 'a silent failure', url: 'https://vid.example/silent', plan: { stdout: [], exit: 1 } },
  {
    name: 'a long message is cut to 200',
    url: 'https://vid.example/long',
    plan: { stdout: [], stderr: [`error: ${'x'.repeat(300)}\n`], exit: 1 }
  },
  { name: 'a config dir reaches the subprocess', url: 'https://vid.example/conf', configDir: '/tmp/ubit-config', plan: { stdout: ['{}'], exit: 0 } }
]
for (const test of infoCases) {
  const result = await withUbit(test.plan, async dir => {
    const got = await captured(() => ubitInfo(test.url, { configDir: test.configDir || '' }))
    return { ...got, observed: observed(dir, null, null) }
  })
  fixture.ubitInfo.push({ ...test, ...result })
}
// A missing binary is the one case no plan can cover.
{
  const empty = mkdtempSync(join(tmpdir(), 'peartube-noubit-'))
  const path = process.env.PATH
  const ubitBin = process.env.UBIT_BIN
  process.env.PATH = empty
  process.env.UBIT_BIN = join(empty, 'ubit')
  let result
  try {
    result = await captured(() => ubitInfo('https://vid.example/absent', {}))
  } finally {
    process.env.PATH = path
    if (ubitBin === undefined) delete process.env.UBIT_BIN
    else process.env.UBIT_BIN = ubitBin
  }
  fixture.ubitInfo.push({ name: 'ubit is not installed', url: 'https://vid.example/absent', plan: null, argv: null, configDir: null, ...result })
}

// --- downloadPage: recorded NDJSON ---
fixture.download = []
const okPlan = {
  stdout: [
    '{"event":"progress","downloaded":10,"total":100}\n{"event":"pro',
    'cess","stage":"mux"}\n',
    'not json at all\n',
    '{"event":"progress","downloaded":100,"total":0}\n',
    '{"event":"done","path":"/out/Film.2020.mkv","size":1234}\n'
  ],
  exit: 0
}
const downloadCases = [
  { name: 'progress, a split line and a done event', plan: okPlan, out: { path: '/out/Film.2020.mkv', size: 1234 }, progress: [{ bytes: 10, total: 100 }, { bytes: 100, total: null }] },
  {
    name: 'an error event outranks the exit code',
    plan: { stdout: ['{"event":"error","error":"HTTP 403 forbidden"}\n'], exit: 1 },
    error: { message: 'HTTP 403 forbidden', status: 403, code: null }
  },
  {
    name: 'stderr when no event explains it',
    plan: { stdout: [], stderr: ['error: HTTP 410 gone\n'], exit: 1 },
    error: { message: 'HTTP 410 gone', status: 410, code: null }
  },
  { name: 'exit zero without a done path', plan: { stdout: ['{"event":"progress","downloaded":5}\n'], exit: 0 }, error: { message: 'ubit failed to download this page', status: null, code: null } },
  { name: 'a fractional size is no size', plan: { stdout: ['{"event":"done","path":"/out/a.mkv","size":12.5}\n'], exit: 0 }, out: { path: '/out/a.mkv', size: null } },
  { name: 'a zero size is a size', plan: { stdout: ['{"event":"done","path":"/out/b.mkv","size":0}\n'], exit: 0 }, out: { path: '/out/b.mkv', size: 0 } },
  { name: 'a done path that is not a string', plan: { stdout: ['{"event":"done","path":7,"size":3}\n'], exit: 0 }, error: { message: 'ubit failed to download this page', status: null, code: null } },
  { name: 'a stall is Source stalled', plan: { stdout: ['{"event":"progress","downloaded":1}\n'], hangAfterStdout: 1 }, stallTimeout: 120, error: { message: 'Source stalled', status: null, code: null } },
  { name: 'cancel before the start', plan: null, cancelled: 'before', error: { message: 'Cancelled', status: null, code: null } },
  { name: 'cancel while running', plan: { stdout: ['{"event":"progress","downloaded":1}\n'], hangAfterStdout: 1 }, cancelled: 'progress', error: { message: 'Cancelled', status: null, code: null } },
  { name: 'a config dir reaches the subprocess', plan: { stdout: ['{"event":"done","path":"/out/c.mkv","size":1}\n'], exit: 0 }, configDir: '/tmp/ubit-config', out: { path: '/out/c.mkv', size: 1 } }
]
for (const test of downloadCases) {
  const outDir = mkdtempSync(join(tmpdir(), 'peartube-out-'))
  const url = 'https://vid.example/page'
  const progress = []
  const run = async dir => {
    const controller = { aborted: test.cancelled === 'before', onabort: null }
    const onProgress = value => {
      progress.push(value)
      if (test.cancelled === 'progress' && !controller.aborted) {
        controller.aborted = true
        controller.onabort?.()
      }
    }
    const got = await captured(() => downloadPage(url, outDir, {
      configDir: test.configDir || '',
      stallTimeout: test.stallTimeout ?? 30_000,
      controller: test.cancelled ? controller : null,
      onProgress
    }))
    return { ...got, observed: observed(dir, url, outDir) }
  }
  const result = test.plan ? await withUbit(test.plan, run) : await run(null)
  rmSync(outDir, { recursive: true, force: true })
  fixture.download.push({
    name: test.name,
    url,
    configDir: test.configDir || '',
    stallTimeout: test.stallTimeout ?? 30_000,
    cancelled: test.cancelled ?? null,
    plan: test.plan,
    progress,
    ...result
  })
}

// --- probe and identify over a local server plus the fake subprocess ---
fixture.probe = []
const pageInfo = {
  title: 'The Matrix 1999',
  webpageURL: 'https://vid.example/watch?v=1',
  site: 'YouTube',
  author: 'uploader',
  duration: 136.5,
  thumbnail: 'https://img/t.jpg',
  formats: [{ height: 720, filesize: 400 }, { height: 1080, filesize: 1234567 }]
}
const probeCases = [
  {
    name: 'a video content type is a file',
    url: '{BASE}/dld/Film.2020.1080p.mkv',
    routes: [{ path: '/dld/Film.2020.1080p.mkv', status: 200, headers: { 'content-type': 'video/x-matroska', 'content-length': '12345' } }]
  },
  {
    name: 'a media name without a length',
    url: '{BASE}/dld/Film.mp4',
    routes: [{ path: '/dld/Film.mp4', status: 200, headers: { 'content-type': 'application/octet-stream' } }]
  },
  {
    name: 'a 404 on a media name is an error',
    url: '{BASE}/dld/Film.mkv',
    routes: [{ path: '/dld/Film.mkv', status: 404, headers: { 'content-type': 'text/plain' } }]
  },
  {
    name: 'an html page goes through ubit',
    url: '{BASE}/watch',
    routes: [{ path: '/watch', status: 200, headers: { 'content-type': 'text/html' }, body: '<html></html>' }],
    plan: { stdout: [JSON.stringify(pageInfo)], exit: 0 }
  },
  {
    name: 'text html with a media name still goes through ubit',
    url: '{BASE}/dld/Film.mkv',
    routes: [{ path: '/dld/Film.mkv', status: 200, headers: { 'content-type': 'text/html; charset=utf-8' } }],
    plan: { stdout: [JSON.stringify({ title: 'Film' })], exit: 0 }
  },
  {
    name: 'a missing title leaves empty fields',
    url: '{BASE}/watch2',
    routes: [{ path: '/watch2', status: 200, headers: { 'content-type': 'text/html' } }],
    plan: { stdout: [JSON.stringify({ formats: [] })], exit: 0 }
  }
]
for (const test of probeCases) {
  const server = await serve(test.routes)
  const run = async dir => {
    const got = await captured(() => probe(fill(test.url, server.base), {}))
    return { ...got, observed: dir ? observed(dir, server.base, null) : { argv: null, configDir: null } }
  }
  const result = test.plan ? await withUbit(test.plan, run) : await run(null)
  fixture.probe.push({
    name: test.name,
    url: test.url,
    headerRoutes: test.routes,
    plan: test.plan ?? null,
    requests: server.requests,
    ...(result.out === undefined ? result : { ...result, out: template(result.out, server.base) })
  })
  await server.close()
}

fixture.identify = []
const identifyCases = [
  {
    name: 'a page with a year gets movie suggestions and a match',
    url: '{BASE}/watch',
    routes: [
      { path: '/watch', status: 200, headers: { 'content-type': 'text/html' } },
      {
        path: '/catalog/movie/top/search=The%20Matrix.json',
        status: 200,
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({
          metas: [
            { imdb_id: 'tt0234215', name: 'The Matrix Reloaded', releaseInfo: '2003-05-15', poster: 'https://p2.jpg' },
            { imdb_id: 'tt0133093', name: 'The Matrix', releaseInfo: '1999-03-31', poster: 'https://p.jpg' },
            { imdb_id: 'tt0111161', name: 'The Shawshank Redemption', releaseInfo: '1994-09-22' }
          ]
        })
      }
    ],
    plan: { stdout: [JSON.stringify(pageInfo)], exit: 0 },
    stableId: true
  },
  {
    name: 'an episode is searched as a series and announced with sNNeNN',
    url: '{BASE}/watch',
    routes: [
      { path: '/watch', status: 200, headers: { 'content-type': 'text/html' } },
      {
        path: '/catalog/series/top/search=Some%20Show.json',
        status: 200,
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({ metas: [{ imdb_id: 'tt0944947', name: 'Some Show', releaseInfo: '2011-04-17' }] })
      }
    ],
    plan: {
      stdout: [JSON.stringify({ title: 'Some Show S02E05 1080p', webpageURL: 'https://vid.example/watch?v=2', formats: [{ height: 2160 }] })],
      exit: 0
    },
    stableId: true
  },
  {
    name: 'a page title without a year or episode gets no suggestions',
    url: '{BASE}/watch',
    routes: [{ path: '/watch', status: 200, headers: { 'content-type': 'text/html' } }],
    plan: { stdout: [JSON.stringify({ title: 'My Holiday Vlog', webpageURL: 'https://vid.example/watch?v=3' })], exit: 0 },
    stableId: true
  },
  {
    name: 'a direct file is parsed and searched, hashing the link',
    url: '{BASE}/dld/The.Matrix.1999.1080p.mkv',
    routes: [
      { path: '/dld/The.Matrix.1999.1080p.mkv', status: 200, headers: { 'content-type': 'video/mp4', 'content-length': '99' } },
      {
        path: '/catalog/movie/top/search=The%20Matrix.json',
        status: 200,
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({ metas: [{ imdb_id: 'tt0133093', name: 'The Matrix', releaseInfo: '1999-03-31' }] })
      }
    ],
    plan: null,
    stableId: false
  }
]
for (const test of identifyCases) {
  const server = await serve(test.routes)
  const run = async () => captured(() => identify(fill(test.url, server.base), { cinemetaUrl: server.base }))
  const result = test.plan ? await withUbit(test.plan, run) : await run()
  fixture.identify.push({
    name: test.name,
    url: test.url,
    routes: test.routes,
    plan: test.plan,
    stableId: test.stableId,
    requests: server.requests,
    ...(result.out === undefined ? result : { ...result, out: template(result.out, server.base) })
  })
  await server.close()
}

writeFileSync(outFile('art-identify-reference.json'), `${JSON.stringify(fixture, null, 2)}\n`)
console.log(`wrote ${outFile('art-identify-reference.json')}`)
