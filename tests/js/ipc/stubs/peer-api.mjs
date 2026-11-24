// The worklet's loopback API wrapper: exercises the stubbed `http.mjs` bind fallback
// (`preferredPort` -> ephemeral `fixtures.apiPort`) without opening real storage, sockets, or mirrors.
import { createApi } from './http.mjs'

function listenLoopback (srv, preferredPort) {
  return new Promise(resolve => {
    const tryBind = port => {
      const onError = () => {
        srv.removeListener('listening', onListen)
        if (port !== 0) tryBind(0)
        else resolve(null)
      }
      const onListen = () => {
        srv.removeListener('error', onError)
        const addr = srv.address()
        resolve(addr?.port ? `http://127.0.0.1:${addr.port}` : null)
      }
      srv.once('error', onError)
      srv.once('listening', onListen)
      srv.listen(port, '127.0.0.1')
    }
    tryBind(preferredPort)
  })
}

export async function openPeerApi ({ preferredPort }) {
  const server = createApi()
  const url = await listenLoopback(server, preferredPort)
  if (!url) server.closeAllConnections()
  return {
    url,
    server: url ? server : null,
    notify: () => {},
    async close () {
      if (!url) return
      await new Promise(resolve => {
        server.close(() => resolve())
        server.closeAllConnections()
      })
    }
  }
}
