// Stand-ins for the mirror host's neighbours so the copied `mirror.js` can run headless: no HiveRelay seeder,
// no traffic gate, and a feed fetcher that reads its reply from a scripted plan.
export function createHiverelaySeeder () {
  return {
    name: 'hiverelay-seed',
    description: '',
    enabled: () => false,
    async run () { return {} },
    async poll () { return null },
    async stop () {},
    info: () => ({ enabled: false })
  }
}

export function createTrafficGate () {
  return {
    stop () {},
    peersBusy: () => false
  }
}

export async function send (url, options = {}) {
  const plan = globalThis.__feeds?.get(url)
  if (!plan) return { status: 404, url, headers: [], body: '' }
  return {
    status: plan.status ?? 200,
    url,
    headers: [],
    body: typeof plan.body === 'string' ? plan.body : JSON.stringify(plan.body)
  }
}
