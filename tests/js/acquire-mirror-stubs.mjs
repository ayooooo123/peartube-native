// In-memory `node:fs` for the acquire/mirror scenario harnesses: the same paths and modes the app uses,
// recorded so the Rust port's fake filesystem can be compared call for call.
const state = { files: new Map(), dirs: new Set(['/work', '/data']), logs: [], tempCounter: 0 }

export function reset () {
  state.files.clear()
  state.dirs = new Set(['/work', '/data'])
  state.logs = []
  state.tempCounter = 0
}

export function setFile (path, text) {
  state.files.set(path, String(text))
}

export function appendLog (entry) {
  state.logs.push(entry)
}

export function modes () {
  return state.logs.filter(entry => entry[0] === 'write').map(entry => [entry[1], entry[2]])
}

export function readFileSync (path) {
  if (!state.files.has(path)) {
    const error = new Error("ENOENT: no such file or directory, open '" + path + "'")
    error.code = 'ENOENT'
    throw error
  }
  return state.files.get(path)
}

export function writeFileSync (path, data, options = {}) {
  state.files.set(path, String(data))
  appendLog(['write', path, options.mode ?? null])
}

export function renameSync (from, to) {
  if (!state.files.has(from)) throw new Error("ENOENT: rename '" + from + "'")
  state.files.set(to, state.files.get(from))
  state.files.delete(from)
  appendLog(['rename', from, to])
}

export function rmSync (path) {
  for (const key of [...state.files.keys()]) {
    if (key === path || key.startsWith(path + '/')) state.files.delete(key)
  }
  state.dirs.delete(path)
  appendLog(['rm', path])
}

export function existsSync (path) {
  return state.files.has(path) || state.dirs.has(path)
}

export function readdirSync (dir) {
  const prefix = dir.endsWith('/') ? dir : dir + '/'
  const names = new Set()
  for (const key of state.files.keys()) {
    if (key.startsWith(prefix)) names.add(key.slice(prefix.length).split('/')[0])
  }
  for (const folder of state.dirs) {
    if (folder.startsWith(prefix)) names.add(folder.slice(prefix.length).split('/')[0])
  }
  return [...names].filter(Boolean)
}

export function mkdirSync (path) {
  const parts = path.split('/').filter(Boolean)
  let current = ''
  for (const part of parts) {
    current += '/' + part
    state.dirs.add(current)
  }
}

export function mkdtempSync (prefix) {
  state.tempCounter += 1
  const dir = prefix + String(state.tempCounter).padStart(6, '0')
  state.dirs.add(dir)
  appendLog(['mkdtemp', dir])
  return dir
}

export function statSync (path) {
  if (state.files.has(path)) {
    return { isFile: () => true, size: Buffer.byteLength(state.files.get(path)), mtimeMs: 1000.5 }
  }
  if (state.dirs.has(path)) return { isFile: () => false, size: 0, mtimeMs: 1000.5 }
  throw new Error("ENOENT: stat '" + path + "'")
}

export function createReadStream (path) {
  const text = state.files.get(path) ?? ''
  return (async function * () {
    yield Buffer.from(text)
  })()
}

export function statfsSync () {
  return { bavail: 1000, bsize: 4096 }
}
