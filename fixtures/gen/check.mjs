// Regenerates every fixtures/*-reference.json from the PearTube app checkout these fixtures belong to and
// compares the result with what is committed. `node check.mjs` exits 1 when a reference differs or a
// generator fails; `node check.mjs --write` replaces the committed references instead.
//
// The app checkout is PEARTUBE_APP, else the sibling `../peartube` (see app.mjs). Run it before bumping
// JS_REF: a difference is either a Rust port to catch up or a debt entry in PARITY.md.
import { spawnSync } from 'node:child_process'
import { existsSync, mkdtempSync, readdirSync, readFileSync, rmSync, writeFileSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { dirname, join, resolve } from 'node:path'
import { fileURLToPath } from 'node:url'
import { APP, JS_REF } from './app.mjs'

const here = dirname(fileURLToPath(import.meta.url))
const root = resolve(here, '..', '..')
const fixtures = join(root, 'fixtures')
const write = process.argv.includes('--write')

// A reference is the app's output. These fields only record how the file was produced: the app commit and
// Node version that wrote it, and the generator's own description. A checkout that happens to have another
// Node version is not a disagreement with the app.
const PROVENANCE = {
  'js-reference.json': ['meta'],
  'art-identify-reference.json': ['source']
}

// One entry per committed reference: the generator script, its directory (which JS dependency set it runs
// with), and whether the generator prints its reference to stdout instead of writing a file.
const sources = [
  { file: 'apply-reference.json', script: 'fixtures/gen/apply.mjs' },
  { file: 'art-identify-reference.json', script: 'fixtures/gen/art-identify.mjs' },
  { file: 'blobs-reference.json', script: 'tests/js/blobs-fixtures.mjs', stdout: true },
  { file: 'cast-reference.json', script: 'tests/js/cast-fixtures.mjs', stdout: true },
  { file: 'hiverelay-reference.json', script: 'fixtures/gen/hiverelay.mjs' },
  { file: 'http-reference.json', script: 'fixtures/gen/http.mjs' },
  { file: 'identity-reference.json', script: 'fixtures/gen/identity.mjs' },
  { file: 'js-reference.json', script: 'fixtures/gen/gen.mjs' },
  { file: 'resolution-reference.json', script: 'fixtures/gen/resolution.mjs' },
  { file: 'settings-reference.json', script: 'fixtures/gen/settings.mjs' }
]

const head = spawnSync('git', ['-C', APP, 'rev-parse', 'HEAD'], { encoding: 'utf8' })
const checkout = head.status === 0 ? head.stdout.trim() : ''
console.log(`app: ${APP} (JS_REF ${JS_REF})`)
if (checkout && checkout !== JS_REF) {
  console.error(`app checkout is at ${checkout}, not JS_REF ${JS_REF}; check out the pinned commit`)
  process.exit(1)
}
if (checkout) {
  const status = spawnSync('git', ['-C', APP, 'status', '--porcelain'], { encoding: 'utf8' })
  if (status.status === 0 && status.stdout.trim()) {
    console.log('note: the checkout has uncommitted changes; comparing against its working tree')
  }
}

const uncovered = readdirSync(fixtures)
  .filter(name => name.endsWith('-reference.json'))
  .filter(name => !sources.some(source => source.file === name))
if (uncovered.length > 0) {
  console.error(`no generator for ${uncovered.join(', ')}; add it to check.mjs`)
  process.exit(1)
}

const out = mkdtempSync(join(tmpdir(), 'peartube-fixtures-'))
const results = []
try {
  for (const source of sources) {
    const path = join(root, source.script)
    const run = spawnSync(process.execPath, [path], {
      cwd: dirname(path),
      env: { ...process.env, PEARTUBE_APP: APP, PEARTUBE_FIXTURE_OUT: out },
      encoding: 'utf8',
      maxBuffer: 256 * 1024 * 1024
    })
    if (run.status !== 0) {
      const reason = (run.stderr || run.stdout || '').trim().split('\n').slice(-6).join('\n  ')
      results.push({ source, error: reason || `exit ${run.status}${run.signal ? ` (${run.signal})` : ''}` })
      continue
    }
    let generated
    if (source.stdout) generated = Buffer.from(run.stdout)
    else {
      const target = join(out, source.file)
      if (!existsSync(target)) {
        results.push({ source, error: `generator wrote no ${source.file}` })
        continue
      }
      generated = readFileSync(target)
    }
    const committed = join(fixtures, source.file)
    const current = existsSync(committed) ? readFileSync(committed) : null
    results.push({ source, committed, current, generated })
  }
} finally {
  rmSync(out, { recursive: true, force: true })
}

let failed = 0
for (const result of results) {
  const name = `fixtures/${result.source.file}`
  if (result.error) {
    failed++
    console.error(`ERROR ${name}: ${result.source.script} failed\n  ${result.error}`)
    continue
  }
  if (write) {
    const changed = !same(result.source.file, result.current, result.generated)
    writeFileSync(result.committed, result.generated)
    console.log(`${changed ? 'wrote' : 'unchanged'} ${name}`)
    continue
  }
  if (same(result.source.file, result.current, result.generated)) {
    console.log(`ok   ${name}`)
    continue
  }
  failed++
  console.error(`DIFF ${name}`)
  for (const line of summarize(result.source.file, result.current, result.generated)) console.error(`  ${line}`)
}

if (write) {
  console.log(`regenerated ${results.length} references from JS_REF ${JS_REF}`)
  process.exit(0)
}
console.log(`${results.length} references checked: ${results.length - failed} match, ${failed} differ`)
process.exit(failed ? 1 : 0)

// The most useful part of a mismatch: where the first difference is, and which top-level sections
// (each a protocol function, fixture group or scenario set) moved.
function summarize (file, current, generated) {
  if (!current) return [`not committed; ${generated.length} generated bytes`]
  const lines = [`committed ${current.length} bytes, generated ${generated.length} bytes`]
  let a
  let b
  try {
    a = behaviour(file, current)
    b = behaviour(file, generated)
  } catch {
    let at = 0
    while (at < current.length && at < generated.length && current[at] === generated[at]) at++
    lines.push(`first byte difference at ${at}`)
    return lines
  }
  const first = firstDifference(a, b, '')
  lines.push(first ? `first difference: ${first.path}: ${first.committed} -> ${first.generated}` : 'JSON equal')
  const keys = new Set([...Object.keys(a || {}), ...Object.keys(b || {})])
  const moved = [...keys].filter(key => firstDifference(a?.[key], b?.[key], ''))
  if (moved.length > 0) lines.push(`sections: ${moved.join(', ')}`)
  return lines
}

function behaviour (file, buffer) {
  const value = JSON.parse(buffer.toString('utf8'))
  if (value && typeof value === 'object' && !Array.isArray(value)) {
    for (const key of PROVENANCE[file] || []) delete value[key]
    // An identify clip id hashes the link, and the scripted server's port differs between runs. The Rust
    // test ignores the id of these cases as well (`stableId: false`).
    if (file === 'art-identify-reference.json') {
      for (const row of value.identify || []) {
        if (row.stableId === false && row.out && row.out.plain) row.out.plain.id = null
      }
    }
  }
  // `wrapVideoKey` draws a random nonce, so a wrapped box's bytes are never the same twice: only its length
  // and the app's own unwrap result compare.
  return nonces(value)
}

function nonces (value) {
  if (Array.isArray(value)) return value.map(nonces)
  if (typeof value === 'string') return value.replace(/\\*"wrappedKey\\*":\\*"[0-9a-f]+/g, 'wrappedKey:<box>')
  if (!value || typeof value !== 'object') return value
  for (const [key, item] of Object.entries(value)) {
    if ((key === 'wrapped' || key === 'wrappedKey') && typeof item === 'string') value[key] = item.length
    else value[key] = nonces(item)
  }
  return value
}

function same (file, current, generated) {
  if (!current) return false
  try {
    return JSON.stringify(behaviour(file, current)) === JSON.stringify(behaviour(file, generated))
  } catch {
    return current.equals(generated)
  }
}

function firstDifference (a, b, path) {
  if (JSON.stringify(a) === JSON.stringify(b)) return null
  if (Array.isArray(a) && Array.isArray(b)) {
    const shared = Math.min(a.length, b.length)
    for (let i = 0; i < shared; i++) {
      const found = firstDifference(a[i], b[i], `${path}[${i}]`)
      if (found) return found
    }
    return { path, committed: `${a.length} cases`, generated: `${b.length} cases` }
  }
  if (a && b && typeof a === 'object' && typeof b === 'object') {
    for (const key of new Set([...Object.keys(a), ...Object.keys(b)])) {
      if (!(key in a)) return { path: `${path}.${key}`, committed: 'absent', generated: short(b[key]) }
      if (!(key in b)) return { path: `${path}.${key}`, committed: short(a[key]), generated: 'absent' }
      const found = firstDifference(a[key], b[key], `${path}.${key}`)
      if (found) return found
    }
  }
  return { path: path || '<root>', committed: short(a), generated: short(b) }
}

function short (value) {
  const text = JSON.stringify(value)
  return text === undefined ? 'undefined' : text.length > 200 ? `${text.slice(0, 200)}…` : text
}
