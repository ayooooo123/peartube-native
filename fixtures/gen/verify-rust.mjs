// Cross-check: unwraps boxes produced by the Rust `wrap_video_key` with the app's own src/mnemonic.js.
import { readFileSync } from 'node:fs'
import { load } from './app.mjs'

const { unwrapVideoKey } = await load('src/mnemonic.js')

const file = process.argv[2]
if (!file) throw new Error('usage: node verify-rust.mjs <cases.json>')
const cases = JSON.parse(readFileSync(file, 'utf8'))
if (!Array.isArray(cases) || cases.length === 0) throw new Error('no cases')

let failed = 0
for (const c of cases) {
  const opened = unwrapVideoKey(c.wrapped, Buffer.from(c.epochKey, 'hex'))
  if (!opened || opened.toString('hex') !== c.videoKey) {
    failed++
    console.error('mismatch', c)
  }
}
if (failed) throw new Error(`${failed}/${cases.length} Rust wrappings failed`)
console.log(`ok: ${cases.length} Rust wrappings opened in JS`)
