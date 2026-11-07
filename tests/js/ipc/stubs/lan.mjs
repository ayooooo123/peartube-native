// LAN discovery: the address picker is the app's, the binding is a stub the fake node records.
import { fixtures } from '../state.mjs'

const PRIVATE = /^(10\.|192\.168\.|172\.(1[6-9]|2\d|3[01])\.)/

export function lanAddress (interfaces) {
  const names = Object.keys(interfaces).filter(name => /^(wl|en|eth)/.test(name))
  for (const name of names.sort((a, b) => b.startsWith('wl') - a.startsWith('wl'))) {
    for (const { family, address, internal } of interfaces[name]) {
      if ((family === 'IPv4' || family === 4) && !internal && PRIVATE.test(address)) return address
    }
  }
  return null
}

export function interfaceAdapter (host) {
  return { host }
}

export function createLan ({ host, adapter }) {
  return { host, adapter, bindings: 0, async close () {} }
}

export const fixturesLanKey = fixtures.lanKey
