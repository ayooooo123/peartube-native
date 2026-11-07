// Deterministic `node:crypto` ids for the acquire scenario harness: UUID call N and random-bytes call N are
// fixed sequences so the Rust fake RNG can produce the same job ids and store keys.
let uuids = 0
let blocks = 0

export function reset () {
  uuids = 0
  blocks = 0
}

export function randomUUID () {
  uuids += 1
  const tail = String(uuids).padStart(12, '0')
  return '00000000-0000-4000-8000-' + tail
}

export function randomBytes (size) {
  blocks += 1
  const out = Buffer.alloc(size)
  for (let i = 0; i < size; i++) out[i] = (blocks * 31 + i) & 0xff
  return out
}
