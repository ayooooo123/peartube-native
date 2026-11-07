// Generates the reference vectors in `tests/private_overlay.rs` from the app's own
// `src/private-overlay.js`.
//
//   node private-overlay-fixtures.mjs
//
// The app repository is found through PEARTUBE_APP, else as a sibling checkout
// (`../../../peartube`). Everything emitted here is produced by the overlay's own code: values are
// taken from the exported `diversityBucketForAddress`, and the module-private encoders, signers and
// card helpers are lifted out of the source text with brace matching.
//
// Everything is deterministic except `sealedBox.sealedHex`, whose nonce is random on every run;
// the test only requires that the port opens whatever blob this emits.
import { createHash } from 'node:crypto'
import { readFileSync } from 'node:fs'
import { createRequire } from 'node:module'
import { join, resolve } from 'node:path'
import { fileURLToPath } from 'node:url'

const here = fileURLToPath(new URL('.', import.meta.url))
const APP = process.env.PEARTUBE_APP || resolve(here, '../../../../peartube')
const require = createRequire(join(APP, 'package.json'))

const b4a = require('b4a')
const c = require('compact-encoding')
const sodium = require('sodium-native')
const HyperDHT = require('hyperdht')
const hypercoreCrypto = require('hypercore-crypto')

const src = readFileSync(join(APP, 'src/private-overlay.js'), 'utf8')

function endOfBlock (open) {
  let depth = 0
  for (let i = open; i < src.length; i++) {
    if (src[i] === '{') depth++
    else if (src[i] === '}') {
      depth--
      if (depth === 0) return i + 1
    }
  }
  throw new Error('unbalanced')
}

// The `{` that opens the body: the first one outside any parentheses or brackets, so a
// destructured parameter list does not get mistaken for the body.
function bodyBrace (start) {
  let parens = 0
  let brackets = 0
  for (let i = start; i < src.length; i++) {
    const ch = src[i]
    if (ch === '(') parens++
    else if (ch === ')') parens--
    else if (ch === '[') brackets++
    else if (ch === ']') brackets--
    else if (ch === '{' && parens === 0 && brackets === 0) return i
  }
  throw new Error('missing body')
}

function fn (name) {
  const start = src.indexOf(`function ${name} (`)
  if (start === -1) throw new Error(`missing ${name}`)
  return src.slice(start, endOfBlock(bodyBrace(start)))
}

// `const x = ...`, whether the value is a primitive, an object literal or an arrow body.
function decl (name) {
  const start = src.indexOf(`const ${name} = `)
  if (start === -1) throw new Error(`missing ${name}`)
  const newline = src.indexOf('\n', start)
  if (!src.slice(start, newline).includes('=>') && !src.slice(start, newline).includes('= {')) {
    return src.slice(start, newline)
  }
  return src.slice(start, endOfBlock(bodyBrace(start)))
}

const helpers = new Function('HyperDHT', 'hypercoreCrypto', 'c', 'sodium', 'b4a', 'createHash', `
${decl('CARD_DOMAIN')}
${decl('MAX_CARD_BYTES')}
${decl('sha256')}
${decl('rand')}
${decl('u32')}
${decl('mutSignEnc')}
${decl('frameEnc')}
${decl('bufArrEnc')}
const [, , NS_MUTABLE_PUT] = hypercoreCrypto.namespace('hyperswarm/dht', [4, 5, 6, 0, 1, 10])
${fn('signDhtMutable')}
${fn('verifyDhtMutable')}
${fn('sealBox')}
${fn('openBox')}
${fn('encodeSignedCard')}
${fn('decodeAndVerifyCard')}
return {
  CARD_DOMAIN, MAX_CARD_BYTES, mutSignEnc, frameEnc, bufArrEnc,
  signDhtMutable, verifyDhtMutable, sealBox, openBox,
  encodeSignedCard, decodeAndVerifyCard, sha256, u32
}
`)(HyperDHT, hypercoreCrypto, c, sodium, b4a, createHash)

const overlay = await import(join(APP, 'src/private-overlay.js'))
const hex = b => b4a.toString(b, 'hex')

const SEED = 'aa'.repeat(32)
const SOURCE_KEY = b4a.from('11'.repeat(32), 'hex')
const EPOCH = '497'
const keyPair = HyperDHT.keyPair(b4a.from(SEED, 'hex'))
const guardKey = b4a.from('22'.repeat(32), 'hex')
const routeToken = b4a.from('33'.repeat(32), 'hex')
const NOW = 1_700_000_000_000
const EXPIRES = NOW + 600_000

const guardTopics = [0, 1, 2, 3].map(i => hex(HyperDHT.hash(b4a.from(`peartube/private-guard/v1/${i}`))))

const buckets = [
  ['10.1.2.3', 9000, false],
  ['10.1.2.3', null, false],
  ['127.0.0.1', 9000, false],
  ['127.0.0.1', 9000, true],
  ['127.0.0.1', 0, true],
  ['127.0.0.1', null, true],
  ['::1', 9000, true],
  ['::1', 9000, false],
  ['0.1.2.3', 5, false],
  ['::ffff:192.168.4.5', 443, false],
  ['2001:DB8:1::5', 80, false],
  ['fe80::1', null, false],
  ['example.com', 80, false],
  ['', 80, false],
  ['1.2.3.4.5', 80, false],
  ['256.1.2.3', 80, false],
  ['10.1.2.3', 70000, false],
  ['::ffff:127.0.0.1', 7001, true]
].map(([host, port, allowLoopback]) => ({
  host,
  port,
  allowLoopback,
  bucket: overlay.diversityBucketForAddress(host, port, allowLoopback)
}))

const frames = [
  [1, 0, ''],
  [19, 7, 'aabbcc'],
  [8, 4294967295, '00'.repeat(200)]
].map(([op, id, payloadHex]) => {
  const payload = b4a.from(payloadHex, 'hex')
  return { op, id, payloadHex, encodedHex: hex(c.encode(helpers.frameEnc, { op, id, payload })) }
})

const bufArrs = [
  [],
  ['', 'aa', 'bbcc']
].map(itemsHex => {
  const items = itemsHex.map(h => b4a.from(h, 'hex'))
  return { itemsHex, encodedHex: hex(c.encode(helpers.bufArrEnc, items)) }
})

const card = helpers.encodeSignedCard({
  innerKeyPair: keyPair,
  guardPublicKey: guardKey,
  routeToken,
  seq: 3,
  expiresAt: EXPIRES
})

const sealKey = helpers.sha256('peartube/private-peer-seal/v1/', keyPair.publicKey)
const sealed = helpers.sealBox(b4a.from('private-route-card-fixture'), sealKey)

const value = b4a.from('signed mutable value')
const mutableSig = helpers.signDhtMutable(42, value, keyPair.secretKey)

const out = {
  guards: {
    seed: SEED,
    publicKeyHex: hex(keyPair.publicKey),
    guardTopics,
    peerSealKeyHex: hex(sealKey),
    rvSealKeyHex: hex(helpers.sha256('peartube/private-rv-seal/v1/', SOURCE_KEY)),
    rvTopicHex: hex(helpers.sha256('peartube/private-rv/v1/', SOURCE_KEY, EPOCH))
  },
  buckets,
  frames,
  bufArrs,
  cards: {
    seed: SEED,
    guardPublicKeyHex: hex(guardKey),
    routeTokenHex: hex(routeToken),
    seq: 3,
    expiresAt: EXPIRES,
    now: NOW,
    cardHex: hex(card),
    // Every verdict below is the app's own `decodeAndVerifyCard`.
    cases: [
      { label: 'accepted', now: NOW, expectedKeyHex: hex(keyPair.publicKey) },
      { label: 'expired', now: EXPIRES },
      { label: 'too-far', now: EXPIRES - 1_800_001 },
      { label: 'wrong-key', now: NOW, expectedKeyHex: '44'.repeat(32) },
      { label: 'window-edge', now: EXPIRES - 1_800_000 }
    ].map(entry => ({
      ...entry,
      verified: Boolean(helpers.decodeAndVerifyCard(
        card,
        entry.now,
        entry.expectedKeyHex ? b4a.from(entry.expectedKeyHex, 'hex') : null
      ))
    }))
  },
  sealedBox: {
    sealKeyHex: hex(sealKey),
    plainHex: hex(b4a.from('private-route-card-fixture')),
    sealedHex: hex(sealed)
  },
  mutable: {
    seed: SEED,
    publicKeyHex: hex(keyPair.publicKey),
    seq: 42,
    valueHex: hex(value),
    signatureHex: hex(mutableSig),
    tamperedSignatureHex: hex(Buffer.from(mutableSig).fill(0, 0, 1))
  }
}

console.log(JSON.stringify(out, null, 2))
