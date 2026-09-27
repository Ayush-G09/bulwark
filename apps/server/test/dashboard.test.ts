import { afterAll, afterEach, beforeEach, describe, expect, it } from 'vitest'
import WebSocket from 'ws'
import Redis from 'ioredis'
import type { Config } from '../src/config'
import { createServer } from '../src/server'
import type { BulwarkServer } from '../src/server'
import { redisAvailable, REDIS_URL } from './redis'

const available = await redisAvailable()
const redis = available ? new Redis(REDIS_URL) : (null as never)
afterAll(async () => { if (available) await redis.quit() })

let srv: BulwarkServer
let base = ''
let wsBase = ''
const config = (over: Partial<Config> = {}): Config => ({
  port: 0, redisUrl: REDIS_URL, instanceId: `test-${Math.random()}`, queuePrefix: `test:jobs:${Math.random()}`,
  policies: { api: { kind: 'token-bucket', capacity: 3, refillPerSec: 0.0001 } },
  queue: { concurrency: 1, maxAttempts: 3, backoffMs: 10, maxBackoffMs: 1000, visibilityTimeoutMs: 2000 },
  reapIntervalMs: 200,
  ...over,
})

beforeEach(async () => {
  if (!available) return
  srv = createServer(config(), redis)
  const port = await srv.listen(0)
  base = `http://127.0.0.1:${port}`
  wsBase = `ws://127.0.0.1:${port}`
})
afterEach(async () => { if (available) await srv.close() })

const post = (path: string, body: unknown) => fetch(`${base}${path}`, { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify(body) })
function connect(url: string): Promise<{ ws: WebSocket; messages: any[] }> {
  return new Promise((resolve, reject) => {
    const ws = new WebSocket(`${url}/dashboard/live`)
    const messages: any[] = []
    ws.on('message', (d) => messages.push(JSON.parse(d.toString())))
    ws.on('open', () => resolve({ ws, messages }))
    ws.on('error', reject)
  })
}
const waitFor = async (cond: () => boolean, ms = 4000) => {
  const t0 = Date.now()
  while (!cond()) { if (Date.now() - t0 > ms) throw new Error('timed out waiting'); await new Promise((r) => setTimeout(r, 10)) }
}

describe.skipIf(!available)('the dashboard: live events', () => {
  it('serves the dashboard page', async () => {
    const r = await fetch(`${base}/`)
    expect(r.status).toBe(200)
    expect(r.headers.get('content-type')).toContain('text/html')
    expect(await r.text()).toContain('Bulwark')
  })

  it('a connected client sees a rate-limit check as it happens', async () => {
    const { ws, messages } = await connect(wsBase)
    const key = `k1-${Math.random()}`
    await post('/limit', { policy: 'api', key })
    await waitFor(() => messages.length > 0)
    ws.terminate()
    expect(messages[0]).toMatchObject({ type: 'limit', policy: 'api', key, allowed: true })
  })

  it('a connected client sees a refusal too, with the retry information implied by remaining: 0', async () => {
    const { ws, messages } = await connect(wsBase)
    const key = `k2-${Math.random()}`
    for (let i = 0; i < 4; i++) await post('/limit', { policy: 'api', key })
    await waitFor(() => messages.length >= 4)
    ws.terminate()
    // continuous refill means a fraction of a token can trickle back in during the few ms between requests, so
    // "empty" is a tiny positive remainder, not exactly 0 — the same reality Phase 1's own tests account for
    expect(messages.at(-1).allowed).toBe(false)
    expect(messages.at(-1).remaining).toBeLessThan(0.01)
  })

  it('a connected client sees a job’s lifecycle: claimed then completed', async () => {
    const { ws, messages } = await connect(wsBase)
    await post('/jobs', { type: 'sleep', ms: 10 })
    await waitFor(() => messages.some((m) => m.event === 'completed'))
    ws.terminate()
    const types = messages.filter((m) => m.type === 'job').map((m) => m.event)
    expect(types).toEqual(['claimed', 'completed'])
  })

  it('a job that fails is shown failing, then dead', async () => {
    const { ws, messages } = await connect(wsBase)
    await post('/jobs', { type: 'fail', message: 'nope', maxAttempts: 1 })
    await waitFor(() => messages.some((m) => m.event === 'dead'))
    ws.terminate()
    const dead = messages.find((m) => m.event === 'dead')
    expect(dead).toMatchObject({ error: 'nope', attempt: 1 })
  })
})

describe.skipIf(!available)('the dashboard: two unrelated deployments sharing one Redis do not see each other', () => {
  it('a server with a different queuePrefix (so a different event channel) never appears on this one’s dashboard', async () => {
    const other = createServer({ ...config(), instanceId: 'unrelated-deployment' }, new Redis(REDIS_URL)) // config() makes a fresh, different queuePrefix
    const portOther = await other.listen(0)
    try {
      const { ws, messages } = await connect(wsBase)
      await fetch(`http://127.0.0.1:${portOther}/limit`, { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify({ policy: 'api', key: `isolated-${Math.random()}` }) })
      await new Promise((r) => setTimeout(r, 300)) // give a wrongly-shared channel every chance to leak the event
      ws.terminate()
      expect(messages.some((m) => m.instanceId === 'unrelated-deployment')).toBe(false)
    } finally { await other.close() }
  })
})

describe.skipIf(!available)('the dashboard: the actual point of this phase — cluster-wide, not per-process', () => {
  it('a rate-limit check made through instance A is seen live on instance B’s dashboard', async () => {
    const shared = { ...config(), instanceId: 'shared-test' }
    const a = createServer(shared, redis)
    const portA = await a.listen(0)
    const b = createServer(shared, new Redis(REDIS_URL))
    const portB = await b.listen(0)
    const key = `cross-${Math.random()}`
    try {
      const { ws, messages } = await connect(`ws://127.0.0.1:${portB}`)
      await fetch(`http://127.0.0.1:${portA}/limit`, { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify({ policy: 'api', key }) })
      await waitFor(() => messages.length > 0)
      ws.terminate()
      expect(messages[0]).toMatchObject({ type: 'limit', key, instanceId: 'shared-test' })
    } finally { await Promise.all([a.close(), b.close()]) }
  })

  it('a job enqueued and run on instance A is seen live on instance C’s dashboard', async () => {
    const shared = { ...config(), instanceId: 'worker-a' }
    const a = createServer(shared, redis)
    const portA = await a.listen(0)
    const c = createServer({ ...shared, instanceId: 'viewer-c', queue: { ...shared.queue, concurrency: 0 } }, new Redis(REDIS_URL))
    const portC = await c.listen(0)
    try {
      const { ws, messages } = await connect(`ws://127.0.0.1:${portC}`)
      await fetch(`http://127.0.0.1:${portA}/jobs`, { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify({ type: 'sleep', ms: 10 }) })
      await waitFor(() => messages.some((m) => m.event === 'completed'))
      ws.terminate()
      // every event came from instance A's own worker; instance C had none of its own (concurrency: 0), proving it only ever saw A's activity via the relay
      expect(messages.every((m) => m.instanceId === 'worker-a')).toBe(true)
    } finally { await Promise.all([a.close(), c.close()]) }
  })
})
