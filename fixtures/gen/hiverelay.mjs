import { writeFileSync } from 'node:fs'
import c from 'compact-encoding'
import sodium from 'sodium-native'
import { encodeHyperdriveHeader, encodeHyperdriveSingleRootNode, seedRequestEncoding, seedAcceptEncoding, unseedRequestEncoding, seedDenyEncoding, signSeedRequest, computeBoundBytes, blake2bTopic, SEED_HANDSHAKE } from './hiverelay.js'
import net from 'node:net'
import SecretStream from '@hyperswarm/secret-stream'
import Protomux from 'protomux'

const hex = b => Buffer.from(b).toString('hex')
const unhex = s => Buffer.from(s, 'hex')
const kp = { publicKey: Buffer.alloc(32), secretKey: Buffer.alloc(64) }
sodium.crypto_sign_seed_keypair(kp.publicKey, kp.secretKey, Buffer.alloc(32, 7))
const request = { appKey: Buffer.alloc(32, 1), discoveryKeys: [Buffer.alloc(32, 2), Buffer.alloc(32, 3)], replicationFactor: 3, geoPreference: '[]', maxStorageBytes: 33554432, bountyRate: 42, ttlSeconds: 2592000, publisherPubkey: kp.publicKey, publisherSignature: Buffer.alloc(64), revocable: 1, unseedFreezeMs: 999, durability: 2 }
request.publisherSignature = signSeedRequest(request, kp)
if (process.argv[2] === '--peer') {
  const server = net.createServer(socket => {
    const stream = new SecretStream(false, socket)
    const mux = Protomux.from(stream)
    const channel = mux.createChannel({
      protocol: 'hiverelay-seed', handshake: c.raw,
      onopen (handshake) {
        if (!handshake.equals(SEED_HANDSHAKE)) process.exit(2)
        channel.messages[0].send(request)
      },
      messages: [{ encoding: seedRequestEncoding, onmessage (msg) {
        if (!msg.publisherSignature.equals(signSeedRequest(msg, kp))) process.exit(3)
        process.stdout.write('received\n')
      } }]
    })
    channel.open(SEED_HANDSHAKE)
    stream.on('error', err => { console.error(err); process.exit(4) })
  })
  server.listen(0, '127.0.0.1', () => process.stdout.write(`port ${server.address().port}\n`))
} else if (process.argv[2] === '--verify') {
  const encoded = unhex(process.argv[3])
  const msg = c.decode(seedRequestEncoding, encoded)
  if (!sodium.crypto_sign_verify_detached(msg.publisherSignature, (() => {
    const dkHash = Buffer.alloc(32)
    if (msg.discoveryKeys.length) sodium.crypto_generichash(dkHash, Buffer.concat(msg.discoveryKeys))
    const meta = Buffer.alloc(40)
    meta.writeUInt8(msg.replicationFactor, 0); meta.writeUInt8(msg.revocable ? 1 : 0, 1); meta.writeUInt8(msg.durability || 0, 2)
    meta.writeBigUInt64BE(BigInt(msg.maxStorageBytes), 8); meta.writeBigUInt64BE(BigInt(msg.ttlSeconds), 16); meta.writeUInt32BE(msg.bountyRate || 0, 24); meta.writeBigUInt64BE(BigInt(msg.unseedFreezeMs || 0), 28)
    return Buffer.concat([Buffer.from('hiverelay.seed-request.v3\0'), msg.appKey, dkHash, meta])
  })(), msg.publisherPubkey)) process.exit(1)
} else {
  const beacons = [0, 1, 255].map((n, i) => ({ feed: hex(Buffer.alloc(32, n)), seq: i + 1, path: i === 1 ? 'manifest.json' : '/manifest.json', blob: { blockOffset: i * 10, blockLength: 1, byteOffset: i * 1234, byteLength: 234 + i }, header: hex(encodeHyperdriveHeader(Buffer.alloc(32, n))), root: hex(encodeHyperdriveSingleRootNode(i + 1, i === 1 ? 'manifest.json' : '/manifest.json', { blockOffset: i * 10, blockLength: 1, byteOffset: i * 1234, byteLength: 234 + i })) }))
  const empty = { ...request, discoveryKeys: [], appKey: Buffer.alloc(32, 9) }
  empty.publisherSignature = signSeedRequest(empty, kp)
  const http = [null, 'api-token'].map(apiKey => {
    const body = JSON.stringify({ coreKey: 'abcd', maxStorageBytes: computeBoundBytes(1234) })
    const headers = { accept: 'application/json', ...(apiKey ? { authorization: `Bearer ${apiKey}` } : {}), 'content-type': 'application/json', 'content-length': String(Buffer.byteLength(body)) }
    return { apiKey, method: 'POST', url: 'https://relay.example/seed-core', headers, body }
  })
  const appKey = Buffer.alloc(32, 1), relayPubkey = Buffer.alloc(32, 4), relaySignature = Buffer.alloc(64, 5)
  const messages = {
    accept: hex(c.encode(seedAcceptEncoding, { appKey, relayPubkey, region: 'eu-west', availableStorageBytes: 500000, relaySignature })),
    unseed: hex(c.encode(unseedRequestEncoding, { appKey, timestamp: 123456, publisherPubkey: kp.publicKey, publisherSignature: request.publisherSignature })),
    deny: hex(c.encode(seedDenyEncoding, { appKey, relayPubkey, reasonCode: 'capacity', detail: 'full', relaySignature }))
  }
  const reference = { handshake: hex(SEED_HANDSHAKE), topic: hex(blake2bTopic('hiverelay-discovery-v1')), epoch: hex(blake2bTopic('hiverelay-discovery-v1-epoch-123')), publisher: hex(kp.publicKey), signature: hex(request.publisherSignature), request: hex(c.encode(seedRequestEncoding, request)), emptyRequest: hex(c.encode(seedRequestEncoding, empty)), messages, beacons, http, bounds: [0, 1, 16777216, 134217728, 2000000000].map(n => [n, computeBoundBytes(n)]) }
  writeFileSync(new URL('../hiverelay-reference.json', import.meta.url), JSON.stringify(reference, null, 2) + '\n')
}
