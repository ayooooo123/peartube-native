// Scripted `node:http`/`node:https` for the acquire scenario harness: each request takes the next planned
// reply and records the URL and headers it carried.
let state = { plan: [], calls: [] }

export function reset () {
  state = { plan: [], calls: [] }
}

export function plan (replies) {
  state.plan = replies.slice()
}

export function calls () {
  return state.calls
}

export function request (url, options, callback) {
  const href = typeof url === 'string' ? url : String(url.href || url)
  state.calls.push({ url: href, headers: { ...(options?.headers || {}) } })
  const spec = state.plan.shift() || { statusCode: 500, headers: {} }
  const response = {
    statusCode: spec.statusCode,
    headers: spec.headers || {},
    resume () {},
    destroy () {},
    async * [Symbol.asyncIterator] () {
      for (const chunk of spec.body || []) yield Buffer.from(chunk)
    }
  }
  const req = {
    destroy () {},
    once () {},
    end () {
      process.nextTick(() => callback(response))
    }
  }
  return req
}
