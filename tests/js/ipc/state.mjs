// Shared state for the parity stubs: the scenario the Rust test also drives, the lines the worker
// wrote, and the interfaces the fake `bare-os` reports.
import { readFileSync } from 'node:fs'
import { fileURLToPath } from 'node:url'

const path = process.env.PC_SCENARIO || fileURLToPath(new URL('../ipc-scenario.json', import.meta.url))
export const scenario = JSON.parse(readFileSync(path, 'utf8'))
export const fixtures = scenario.fixtures
export const out = []
export const hostEvents = []

export const clone = value => JSON.parse(JSON.stringify(value))

// A record with one interface list per step, so `moveLan` sees the device move.
export const interfaces = { current: clone(scenario.interfaces) }
