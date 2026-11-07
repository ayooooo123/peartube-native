// The loopback API: `listenLoopback`'s bind, retried on an ephemeral port, as the worker expects.
import { EventEmitter } from 'node:events'
import { fixtures } from '../state.mjs'

export function createApi () {
  const server = new EventEmitter()
  server.address = () => ({ port: fixtures.apiPort })
  server.listen = port => {
    if (port === fixtures.preferredApiPort) server.emit('error', new Error('EADDRINUSE'))
    else server.emit('listening')
  }
  server.close = callback => callback()
  server.closeAllConnections = () => {}
  server.notify = () => {}
  return server
}
