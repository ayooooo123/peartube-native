// The scripted Soulseek network of soulseek-provider-scenario.json (`scripts`, `search`, `peers`), for the oracles
// that run the app's Soulseek plugin: a stand-in for the `peartube-soulseek-bare` addon, the files its peers send,
// and a listing of what ends up on disk. tests/support/soulseek_network.rs is the same network for the Rust port.
import { createHash } from 'node:crypto'
import { readFileSync, readdirSync, statSync, writeFileSync } from 'node:fs'
import { basename, dirname, join } from 'node:path'
import { fileURLToPath } from 'node:url'

const here = dirname(fileURLToPath(import.meta.url))
export const network = JSON.parse(readFileSync(join(here, 'soulseek-provider-scenario.json'), 'utf8'))
export const TICK = network.tick

export const sha = bytes => createHash('sha256').update(bytes).digest('hex')
const sleep = ms => new Promise(resolve => setTimeout(resolve, ms))

// A FLAC file whose only metadata block is a Vorbis comment, then `pad` bytes.
export function flac (tags, pad) {
  const u32 = n => { const b = Buffer.alloc(4); b.writeUInt32LE(n); return b }
  const entries = tags.map(([key, value]) => Buffer.from(`${key}=${value}`, 'utf8'))
  const vendor = Buffer.from('peartube-test', 'utf8')
  const parts = [u32(vendor.length), vendor, u32(entries.length)]
  for (const entry of entries) parts.push(u32(entry.length), entry)
  const comment = Buffer.concat(parts)
  const header = Buffer.from([0x84, (comment.length >> 16) & 0xff, (comment.length >> 8) & 0xff, comment.length & 0xff])
  return Buffer.concat([Buffer.from('fLaC', 'latin1'), header, comment, Buffer.alloc(pad, 0x11)])
}

const peerOf = user => network.peers[user] || {}
const fileOf = (user, filename) => peerOf(user).files?.[filename] || peerOf(user).file || { script: 'silent' }
const record = (user, r) => ({
  username: r.username ?? user,
  slots: r.slots ?? true,
  queue: r.queue ?? 0,
  speed: 0,
  filename: r.filename,
  size: r.size ?? 0,
  bitrate: r.bitrate ?? null,
  sample_rate: r.sample_rate ?? null,
  bit_depth: r.bit_depth ?? null,
  length_secs: r.length_secs ?? null
})

// The stand-in for `peartube-soulseek-bare`: what the plugin asks of it goes to `log`. `session.login` is `ok`
// (the default), `error:<message>` or anything else for a rejected sign-in; `session.boundPort` is the port the
// client reports instead of the one it was given.
export function fakeAddon (session, log) {
  class Soulseek {
    constructor (opts) {
      // The addon reads its share options from the environment when a client is made.
      log.push({ created: { listenPort: opts.listenPort, share: process.env.PEARTUBE_SLSK_SHARE === '1', shareMaxFiles: Number(process.env.PEARTUBE_SLSK_SHARE_MAX_FILES) } })
      this.listenPort = opts.listenPort
      this.loggedIn = false
    }

    state () {
      return { connected: this.loggedIn, logged_in: this.loggedIn, listen_port: session.boundPort ?? this.listenPort }
    }

    async login (host, port, user, pass) {
      log.push({ login: [host, port, user, pass] })
      await sleep(TICK)
      const how = session.login ?? 'ok'
      if (how.startsWith('error:')) throw new Error(how.slice(6))
      this.loggedIn = how === 'ok'
      return { logged_in: this.loggedIn }
    }

    async search (query, waitMs) {
      log.push({ search: [query, waitMs] })
      const script = network.search[query] || { hits: [] }
      await sleep(TICK)
      if (script.error) throw new Error(script.error)
      return script.hits.map(hit => record('', hit))
    }

    async browse (user, timeoutMs) {
      log.push({ browse: [user, timeoutMs] })
      const script = peerOf(user).browse || { records: [] }
      await sleep(TICK)
      if (script.error) throw new Error(script.error)
      return script.records.map(r => record(user, r))
    }

    download (user, filename, dest, onProgress) {
      log.push({ download: [user, filename, basename(dest)] })
      const file = fileOf(user, filename)
      const bytes = file.tags ? flac(file.tags, file.pad ?? 0) : Buffer.alloc(0)
      return new Promise((resolve, reject) => {
        for (const step of network.scripts[file.script]) {
          setTimeout(() => {
            if (step.error) return reject(new Error(step.error))
            if (step.done) {
              // A download the plugin gave up on has no folder left to write into.
              try { writeFileSync(dest, bytes) } catch { return }
              return resolve({ path: dest, size: bytes.length })
            }
            onProgress({ phase: step.phase, bytes: Math.floor(bytes.length * (step.permille ?? 0) / 1000), total: step.phase === 'transferring' ? bytes.length : 0, speed: 0, queue: step.queue ?? null })
          }, step.at)
        }
      })
    }

    close () { log.push({ close: true }) }
  }
  return { Soulseek }
}

// Every folder and file under `root`: a `.json` file with its text, any other file with its size and hash.
export function tree (root) {
  const out = []
  const walk = (dir, rel) => {
    for (const name of readdirSync(dir).sort()) {
      const full = join(dir, name)
      const path = rel ? `${rel}/${name}` : name
      if (statSync(full).isDirectory()) {
        out.push({ path: `${path}/` })
        walk(full, path)
      } else if (name.endsWith('.json')) {
        out.push({ path, text: readFileSync(full, 'utf8') })
      } else {
        const bytes = readFileSync(full)
        out.push({ path, size: bytes.length, sha256: sha(bytes) })
      }
    }
  }
  walk(root, '')
  return out
}
