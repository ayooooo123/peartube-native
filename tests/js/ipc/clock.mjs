// A fake clock and timer table, so the worklet's timers run on the scenario's time rather than the
// wall clock. Installed before `worker.js` loads.
import { scenario } from './state.mjs'

const realSetImmediate = setImmediate
let now = scenario.start
let nextId = 1
let timers = []

const later = ms => {
  const id = nextId++
  timers.push({ id, at: now + ms, repeat: null, fn: null })
  return { id, record: timers[timers.length - 1] }
}

export function installClock () {
  Date.now = () => now
  globalThis.setTimeout = (fn, ms = 0) => {
    const { id, record } = later(ms)
    record.fn = fn
    return id
  }
  globalThis.clearTimeout = id => {
    timers = timers.filter(timer => timer.id !== id)
  }
  globalThis.setInterval = (fn, ms = 0) => {
    const { id, record } = later(ms)
    record.fn = fn
    record.repeat = ms
    return id
  }
  globalThis.clearInterval = globalThis.clearTimeout
}

/** Let every resolved promise chain finish (a real macrotask boundary). */
export const drain = () => new Promise(resolve => realSetImmediate(resolve))

/**
 * Jump to `now + ms`, then run every timer that was due, in deadline then insertion order. Time
 * moves once, not to each timer's own deadline, so a long jump fires each interval once — the same
 * model the Rust test clock uses.
 */
export async function advance (ms) {
  now += ms
  for (;;) {
    const due = timers
      .filter(timer => timer.at <= now)
      .sort((a, b) => a.at - b.at || a.id - b.id)[0]
    if (!due) break
    timers = timers.filter(timer => timer !== due)
    due.fn()
    if (due.repeat !== null) timers.push({ ...due, id: nextId++, at: now + due.repeat })
    await drain()
  }
  await drain()
}
