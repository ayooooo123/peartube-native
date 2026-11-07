// Drives the real `worklet/worker.js` through the parity scenario and prints the lines it wrote.
// `cargo test --test ipc` runs this and compares it with the Rust backend on the same scenario.
import { EventEmitter } from 'node:events'
import { advance, drain, installClock } from './ipc/clock.mjs'
import { installHooks, workerUrl } from './ipc/hooks.mjs'
import { clone, interfaces, out, scenario } from './ipc/state.mjs'

const bare = new EventEmitter()
const ipc = new EventEmitter()
ipc.write = chunk => out.push(String(chunk))
// `Bare.on(...)` chains; `Bare.emit('appEvent', msg)` reaches whatever listens.
globalThis.Bare = {
  IPC: ipc,
  on (event, listener) { bare.on(event, listener); return globalThis.Bare },
  emit: (event, ...args) => bare.emit(event, ...args)
}

installClock()
installHooks()
await import(workerUrl)

for (const step of scenario.steps) {
  if (step.interfaces) {
    interfaces.current = clone(step.interfaces)
  } else if (step.suspend !== undefined) {
    bare.emit('suspend', step.suspend)
    await drain()
  } else if (step.resume) {
    bare.emit('resume')
    await drain()
  } else if (step.advance !== undefined) {
    await advance(step.advance)
  } else if (step.line !== undefined) {
    ipc.emit('data', Buffer.from(step.line.endsWith('\n') ? step.line : `${step.line}\n`))
    await drain()
  }
}

await drain()
process.stdout.write(out.join(''))
