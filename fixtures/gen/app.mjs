// The one place generators find the PearTube app checkout and the commit they are pinned to. Nothing in this
// folder is a copy of app code: every generator loads the app's own modules from `PEARTUBE_APP`, else the
// sibling `../peartube` checkout CI creates.
import { createRequire } from 'node:module'
import { readFileSync } from 'node:fs'
import { dirname, join, resolve } from 'node:path'
import { fileURLToPath, pathToFileURL } from 'node:url'

const here = dirname(fileURLToPath(import.meta.url))

export const APP = resolve(process.env.PEARTUBE_APP || join(here, '..', '..', '..', 'peartube'))
export const JS_REF = readFileSync(join(here, '..', '..', 'JS_REF'), 'utf8').trim()

/** Imports one module from the app checkout, e.g. `await load('src/protocol.js')`. */
export function load (path) {
  return import(pathToFileURL(join(APP, path)).href)
}

/** A CommonJS dependency of the app, resolved from its own node_modules. */
export function appRequire (name) {
  return createRequire(join(APP, 'package.json'))(name)
}

/** Where a reference is written: `fixtures/`, or `PEARTUBE_FIXTURE_OUT` when check.mjs regenerates it. */
export function outFile (name) {
  return join(process.env.PEARTUBE_FIXTURE_OUT || join(here, '..'), name)
}
