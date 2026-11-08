// Paired with examples/download_ab.rs. Use a private PEARTUBE_APP copy.
// consume <config.json> | seed <bytes> [config.json] (waits for stdin EOF).
import assert from 'node:assert/strict'
import { readFileSync, writeFileSync, mkdtempSync, rmSync } from 'node:fs'
import { createHash, randomBytes } from 'node:crypto'
import { createRequire } from 'node:module'
import { request } from 'node:http'
import { resolve } from 'node:path'
import { pathToFileURL } from 'node:url'
const app = process.env.PEARTUBE_APP
assert(app, 'PEARTUBE_APP must name a private app copy')
const require = createRequire(resolve(app, 'package.json'))
const { createNode } = await import(pathToFileURL(resolve(app, 'src/index.js')))
const { createLan, tuneUdxSocket } = await import(pathToFileURL(resolve(app, 'src/lan.js')))
const out = value => console.log(JSON.stringify(value))
const storage = mkdtempSync('/tmp/pt-download-ab-js-')
const mode = process.argv[2]
if (mode === 'seed') {
  const DHT = require('hyperdht')
  const Corestore = require('corestore')
  const store = new Corestore(storage)
  const core = store.get({name:'throughput-ab'})
  await core.ready()
  const size = Number(process.argv[3])
  assert(Number.isSafeInteger(size) && size > 0)
  const digest = createHash('sha256')
  for (let offset = 0; offset < size; offset += 524288 * 16) {
    const batch = []
    for (let i = offset; i < Math.min(size, offset + 524288 * 16); i += 524288) {
      const block = randomBytes(Math.min(524288,size-i))
      digest.update(block)
      batch.push(block)
    }
    await core.append(batch)
  }
  const dht = new DHT({host:'127.0.0.1',bootstrap:[],ephemeral:false,firewalled:false})
  await dht.fullyBootstrapped()
  tuneUdxSocket(dht.io.serverSocket)
  tuneUdxSocket(dht.io.clientSocket)
  const keyPair = DHT.keyPair()
  const server = dht.createServer(socket => {
    socket.on('error',()=>{})
    tuneUdxSocket(socket.rawStream?.socket)
    store.replicate(socket)
  })
  await server.listen(keyPair)
  const hash = digest.digest('hex')
  const peer={host:'127.0.0.1',port:dht.address().port,publicKey:keyPair.publicKey.toString('hex')}
  const entry={type:'announce',id:'imdb:tt0000001',title:'Throughput A/B',size,sha256:hash,blobs:core.key.toString('hex'),
    blob:{blockOffset:0,byteOffset:0,blockLength:core.length,byteLength:size}}
  if (process.argv[4]) writeFileSync(process.argv[4],JSON.stringify({host:peer.host,worklet:true,peers:[peer],start:0,bytes:size,sha256:hash,entry}))
  out({event:'seed',storage,peer,entry})
  process.stdin.resume()
  await new Promise(resolve=>process.stdin.once('end',resolve))
  await server.close()
  await dht.destroy({force:true})
  await store.close()
} else {
  assert.equal(mode,'consume')
  const config = JSON.parse(readFileSync(process.argv[3]))
  // Explicit dials only: ambient mDNS would silently add other seeders to the A/B.
  const stop = async () => {}
  const adapter = {advertise:async()=>({stop}),browse:async()=>({stop})}
  const node = await createNode({storage,bootstrap:[{host:'127.0.0.1',port:9}],streamHost:'127.0.0.1',streamPort:0,
    worklet:config.worklet ?? true,lan:keyPair=>createLan({host:config.host,keyPair,adapter})})
  let sampler
  try {
    const allowed = new Set(config.peers.map(p=>p.publicKey))
    node.onConnection(socket=>{if (!allowed.has(socket.remotePublicKey?.toString('hex'))) socket.destroy()})
    node.setAllowedPeers([...allowed])
    for (const p of config.peers) node.dialLanPeer(p)
    const deadline = Date.now()+30000
    while ([...node.peerConnections()].filter(p=>p.connected).length < config.peers.length) {
      assert(Date.now() < deadline,'LAN peer connect timeout')
      await new Promise(resolve=>setTimeout(resolve,50))
    }
    const before = node.status()
    assert.equal(before.cachedBytes,0,'consumer must start uncached')
    const url = node.streamUrl(config.entry)
    out({event:'ready',pid:process.pid,storage,url,status:before})
    const cpu = process.cpuUsage()
    const wall = process.hrtime.bigint()
    const digest = createHash('sha256')
    let count = 0
    if (config.trace) sampler = setInterval(()=>out({event:'sample',bytes:count,status:node.status(),
      connections:[...node.peerConnections()].map(p=>({key:p.remotePublicKey?.toString('hex'),host:p.rawStream?.remoteHost,
        rxBytes:p.rawStream?.bytesReceived}))}),500)
    await new Promise((resolve,reject)=>{
      const req=request(url,{headers:{range:`bytes=${config.start}-${config.start+config.bytes-1}`}},res=>{
        try { assert.equal(res.statusCode,206) } catch(err) { reject(err);res.destroy();return }
        res.on('data',data=>{count+=data.length;digest.update(data)})
        res.on('error',reject)
        res.on('end',resolve)
      })
      const timer=setTimeout(()=>req.destroy(new Error('range timed out')),config.timeoutMs || 180000)
      req.once('close',()=>clearTimeout(timer))
      req.on('error',reject)
      req.end()
    })
    clearInterval(sampler)
    const seconds=Number(process.hrtime.bigint()-wall)/1e9
    const used=process.cpuUsage(cpu)
    const cpuSeconds=(used.user+used.system)/1e6
    const hash=digest.digest('hex')
    assert.equal(count,config.bytes)
    if (config.sha256) assert.equal(hash,config.sha256)
    out({event:'result',backend:'js',bytes:count,seconds,MBps:count/seconds/1e6,cpu_seconds:cpuSeconds,
      cpu_s_per_GiB:cpuSeconds*1073741824/count,sha256:hash,status:node.status(),
      peers:[...node.peerConnections()].map(p=>({key:p.remotePublicKey?.toString('hex'),host:p.rawStream?.remoteHost,
        rxBytes:p.rawStream?.bytesReceived,rtt:p.rawStream?.rtt,retransmits:p.rawStream?.retransmits}))})
  } finally {
    clearInterval(sampler)
    await node.close()
  }
}
rmSync(storage,{recursive:true,force:true})
process.exit(0)
