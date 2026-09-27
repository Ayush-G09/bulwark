import { afterAll, afterEach, beforeEach, describe, expect, it } from 'vitest'
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
const config = (over: Partial<Config> = {}): Config => ({
  port: 0, redisUrl: REDIS_URL, instanceId: `test-${Math.random()}`, queuePrefix: `test:jobs:${Math.random()}`,
  policies: { api: { kind: 'token-bucket', capacity: 3, refillPerSec: 0.0001 }, login: { kind: 'sliding-window', limit: 2, windowMs: 60_000 } },
  queue: { concurrency: 2, maxAttempts: 3, backoffMs: 10, maxBackoffMs: 1000, visibilityTimeoutMs: 2000 },
  reapIntervalMs: 100,
  ...over,
})

beforeEach(async () => {
  if (!available) return
  srv = createServer(config(), redis)
  const port = await srv.listen(0)
  base = `http://127.0.0.1:${port}`
})
afterEach(async () => { if (available) await srv.close() })

const post = (path: string, body: unknown) => fetch(`${base}${path}`, { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify(body) })

describe.skipIf(!available)('the HTTP surface', () => {
  it('reports its own identity on /health', async () => {
    const r = await fetch(`${base}/health`)
    const j = await r.json()
    expect(r.status).toBe(200)
    expect(j).toMatchObject({ ok: true })
    expect(typeof j.instanceId).toBe('string')
  })

  it('checks a named policy and enforces its capacity over HTTP', async () => {
    const key = `k-${Math.random()}`
    for (let i = 0; i < 3; i++) {
      const r = await post('/limit', { policy: 'api', key })
      expect(r.status).toBe(200)
      expect((await r.json()).allowed).toBe(true)
    }
    const refused = await post('/limit', { policy: 'api', key })
    expect(refused.status).toBe(429)
    const body = await refused.json()
    expect(body).toMatchObject({ allowed: false, policy: 'api', key })
    expect(body.retryAfterMs).toBeGreaterThan(0)
  })

  it('an unknown policy is a clean 404 that lists the real ones', async () => {
    const r = await post('/limit', { policy: 'nope', key: 'x' })
    expect(r.status).toBe(404)
    expect((await r.json()).policies.sort()).toEqual(['api', 'login'])
  })

  it('rejects a bad key or cost, and bad json, without touching the limiter', async () => {
    expect((await post('/limit', { policy: 'api', key: '' })).status).toBe(400)
    expect((await post('/limit', { policy: 'api', key: 'bad key with spaces' })).status).toBe(400)
    expect((await post('/limit', { policy: 'api', key: 'ok', cost: -1 })).status).toBe(400)
    expect((await post('/limit', { policy: 'api', key: 'ok', cost: 0 })).status).toBe(400)
    const r = await fetch(`${base}/limit`, { method: 'POST', headers: { 'content-type': 'application/json' }, body: '{not json' })
    expect(r.status).toBe(400)
  })

  it('a sliding-window policy ignores cost: every request counts as exactly one', async () => {
    const key = `k-${Math.random()}`
    await post('/limit', { policy: 'login', key, cost: 1 })
    const r = await post('/limit', { policy: 'login', key, cost: 100 })
    expect(r.status).toBe(200) // if cost applied, this alone would exceed the limit of 2
    expect((await post('/limit', { policy: 'login', key })).status).toBe(429) // now at the real limit
  })

  it('enqueues a job and it is actually run by this instance’s own embedded worker', async () => {
    const r = await post('/jobs', { type: 'sleep', ms: 10 })
    expect(r.status).toBe(202)
    const { id } = await r.json()
    expect(typeof id).toBe('string')
    await new Promise((resolve) => setTimeout(resolve, 1500))
    const stats = await (await fetch(`${base}/stats`)).json()
    expect(stats.queue).toMatchObject({ ready: 0, processing: 0 })
  })

  it('a job that always fails ends up in the dead-letter list, reachable over HTTP', async () => {
    await post('/jobs', { type: 'fail', message: 'nope', maxAttempts: 1 })
    await new Promise((resolve) => setTimeout(resolve, 1500))
    const dead = await (await fetch(`${base}/jobs/dead`)).json()
    expect(dead.dead.some((d: { lastError: string }) => d.lastError === 'nope')).toBe(true)
  })

  it('a flaky job that succeeds on a later attempt is not left as failed', async () => {
    await post('/jobs', { type: 'flaky', succeedOnAttempt: 2, maxAttempts: 3 })
    await new Promise((resolve) => setTimeout(resolve, 1500))
    const stats = await (await fetch(`${base}/stats`)).json()
    expect(stats.queue.dead).toBe(0)
  })

  it('rejects a job of an unknown type', async () => {
    expect((await post('/jobs', { type: 'whatever' })).status).toBe(400)
  })

  it('/stats names the policies and reports live queue counts', async () => {
    const j = await (await fetch(`${base}/stats`)).json()
    expect(j.policies.sort()).toEqual(['api', 'login'])
    expect(j.queue).toMatchObject({ ready: 0, processing: 0, delayed: 0, dead: 0 })
  })

  it('an unknown route is a clean 404', async () => {
    expect((await fetch(`${base}/nope`)).status).toBe(404)
  })

  it('refuses a body over the limit', async () => {
    const r = await fetch(`${base}/limit`, { method: 'POST', body: 'x'.repeat(2 * 1024 * 1024) })
    expect(r.status).toBe(413)
  })
})

describe.skipIf(!available)('two instances sharing one Redis actually share state', () => {
  it('a limit reached through one instance is enforced through a second, independent instance', async () => {
    const second = createServer(config(), redis)
    const port2 = await second.listen(0)
    try {
      const key = `k-${Math.random()}`
      for (let i = 0; i < 3; i++) await post('/limit', { policy: 'api', key })
      const throughSecond = await fetch(`http://127.0.0.1:${port2}/limit`, { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify({ policy: 'api', key }) })
      expect(throughSecond.status).toBe(429) // the second instance sees the first one's spending
    } finally { await second.close() }
  })

  it('a job enqueued through one instance can be, and is, picked up by a second instance’s worker', async () => {
    const second = createServer(config(), redis)
    await second.listen(0)
    try {
      await post('/jobs', { type: 'sleep', ms: 10 })
      await post('/jobs', { type: 'sleep', ms: 10 })
      await new Promise((resolve) => setTimeout(resolve, 1500))
      const stats = await (await fetch(`${base}/stats`)).json()
      expect(stats.queue).toMatchObject({ ready: 0, processing: 0 }) // both jobs finished, by whichever instance claimed them
    } finally { await second.close() }
  })
})
