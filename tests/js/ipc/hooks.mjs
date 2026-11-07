// Redirects the worklet's imports to scripted stubs, leaving `protocol.js` and `app-settings.js`
// real so their JSON is the app's own.
import { registerHooks } from 'node:module'
import { fileURLToPath, pathToFileURL } from 'node:url'

const here = new URL('./', import.meta.url)

export const appRoot = process.env.PEARTUBE_APP || fileURLToPath(new URL('../../../../../peartube', import.meta.url))
export const workerUrl = pathToFileURL(`${appRoot}/worklet/worker.js`).href

const shared = {
  'bare-process/global': './stubs/bare-process.mjs',
  b4a: './stubs/b4a.mjs',
  'bare-os': './stubs/bare-os.mjs'
}

const fromWorklet = {
  '../src/index.js': './stubs/index.mjs',
  '../src/http.js': './stubs/http.mjs',
  '../src/lan.js': './stubs/lan.mjs',
  '../src/cast.js': './stubs/cast.mjs'
}

function redirect (specifier, parent) {
  if (shared[specifier]) return shared[specifier]
  if (specifier === './mnemonic.js' && parent.endsWith('/src/app-settings.js')) return './stubs/mnemonic.mjs'
  if (parent === workerUrl && fromWorklet[specifier]) return fromWorklet[specifier]
  return null
}

export function installHooks () {
  registerHooks({
    resolve (specifier, context, nextResolve) {
      const stub = redirect(specifier, context.parentURL || '')
      if (stub) return { url: new URL(stub, here).href, format: 'module', shortCircuit: true }
      return nextResolve(specifier, context)
    }
  })
}
