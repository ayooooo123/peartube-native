// Exports libsodium's `crypto_hash_sha256_state` at several offsets of one deterministic payload, using the
// app's installed sodium-native, so tests/blobs.rs can prove the Rust checkpoint format is byte-compatible:
// a state exported here resumes in Rust to the payload's digest, and a state Rust exports finalises here.
//
//   node sha256-state.mjs                     -> { size, points: [{ bytes, state, digest }] }
//   node sha256-state.mjs states.json         -> [{ bytes, digest }] for { size, states: [{ bytes, state }] }
//
// `state` is base64 of the 104-byte struct; `digest` is the hex SHA-256 of the whole payload, so a state at
// `bytes` only has to be foldable over the rest.
import { createRequire } from 'node:module'
import { readFileSync } from 'node:fs'
import { join } from 'node:path'

const require = createRequire(import.meta.url)
const app = process.env.PEARTUBE_APP || join(process.env.HOME || '', 'projects', 'peartube')
const sodium = require(join(app, 'node_modules', 'sodium-native'))

// The same payload tests/blobs.rs builds.
const SIZE = 2 * 1024 * 1024 + 12345
const data = Buffer.alloc(SIZE)
for (let i = 0; i < SIZE; i++) data[i] = (i * 31 + 7) % 251

const stateBytes = sodium.crypto_hash_sha256_STATEBYTES
const digestBytes = sodium.crypto_hash_sha256_BYTES
if (stateBytes !== 104) throw new Error(`unexpected state size ${stateBytes}`)

const hash = consume => {
  const state = Buffer.alloc(stateBytes)
  sodium.crypto_hash_sha256_init(state)
  if (consume > 0) sodium.crypto_hash_sha256_update(state, data.subarray(0, consume))
  return state
}
const digestOf = state => {
  const digest = Buffer.alloc(digestBytes)
  sodium.crypto_hash_sha256_final(state, digest)
  return digest.toString('hex')
}

if (process.argv[2]) {
  const input = JSON.parse(readFileSync(process.argv[2], 'utf8'))
  if (input.size !== SIZE) throw new Error(`unexpected payload size ${input.size}`)
  const out = input.states.map(({ bytes, state }) => {
    const resumed = Buffer.from(state, 'base64')
    if (resumed.length !== stateBytes) throw new Error(`state at ${bytes} is ${resumed.length} bytes`)
    if (bytes < SIZE) sodium.crypto_hash_sha256_update(resumed, data.subarray(bytes))
    return { bytes, digest: digestOf(resumed) }
  })
  process.stdout.write(JSON.stringify(out))
} else {
  const points = [1, 63, 64, 65, 512 * 1024 - 1, 512 * 1024, 700_000, 1024 * 1024 + 123, SIZE - 1]
  const whole = digestOf(hash(SIZE))
  const out = points.map(bytes => {
    const state = hash(bytes)
    // the snapshot must resume where it stopped
    const resumed = Buffer.from(state)
    sodium.crypto_hash_sha256_update(resumed, data.subarray(bytes))
    if (digestOf(resumed) !== whole) throw new Error(`sodium state at ${bytes} does not resume`)
    return { bytes, state: state.toString('base64'), digest: whole }
  })
  process.stdout.write(JSON.stringify({ size: SIZE, points: out }))
}
