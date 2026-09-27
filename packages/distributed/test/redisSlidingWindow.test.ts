import { afterAll, describe, expect, it } from 'vitest'
import { RedisSlidingWindowLimiter } from '../src/redisSlidingWindow'
import { client, redisAvailable } from './redis'

const available = await redisAvailable()
const redis = available ? client() : (null as never)
afterAll(async () => { if (available) await redis.quit() })

const limiter = (limit: number, windowMs: number, now = { t: 0 }) => new RedisSlidingWindowLimiter(redis, { limit, windowMs, now: () => now.t, keyPrefix: `test:${Math.random()}:` })

describe.skipIf(!available)('RedisSlidingWindowLimiter: matches the in-process algorithm', () => {
  it('allows exactly `limit` requests in a window, then refuses', async () => {
    const l = limiter(3, 1000)
    for (let i = 0; i < 3; i++) expect((await l.check('a', 0)).allowed).toBe(true)
    expect(await l.check('a', 0)).toMatchObject({ allowed: false, remaining: 0 })
  })
  it('a request that expires makes room for exactly one more', async () => {
    const l = limiter(2, 1000)
    await l.check('a', 0)
    await l.check('a', 500)
    expect((await l.check('a', 900)).allowed).toBe(false)
    expect((await l.check('a', 1001)).allowed).toBe(true)
    expect((await l.check('a', 1002)).allowed).toBe(false)
  })
  it('is a true sliding window: no boundary burst', async () => {
    const l = limiter(2, 1000)
    await l.check('a', 999); await l.check('a', 999)
    expect((await l.check('a', 1000)).allowed).toBe(false)
    expect((await l.check('a', 1999)).allowed).toBe(true)
  })
  it('says how long until the oldest request ages out', async () => {
    const l = limiter(1, 1000)
    await l.check('a', 0)
    expect((await l.check('a', 400)).retryAfterMs).toBe(600)
  })
  it('two limiters sharing a key prefix see the same state', async () => {
    const prefix = `test:shared:${Math.random()}:`
    const a = new RedisSlidingWindowLimiter(redis, { limit: 2, windowMs: 1000, keyPrefix: prefix })
    const b = new RedisSlidingWindowLimiter(redis, { limit: 2, windowMs: 1000, keyPrefix: prefix })
    await a.check('k')
    await a.check('k')
    expect((await b.check('k')).allowed).toBe(false)
  })
  it('rejects a non-positive limit or window', () => {
    expect(() => new RedisSlidingWindowLimiter(redis, { limit: 0, windowMs: 1000 })).toThrow()
    expect(() => new RedisSlidingWindowLimiter(redis, { limit: 1, windowMs: 0 })).toThrow()
  })
  it('reset clears a key', async () => {
    const l = limiter(1, 1000)
    await l.check('a', 0)
    await l.reset('a')
    expect((await l.check('a', 0)).allowed).toBe(true)
  })
})

describe.skipIf(!available)('RedisSlidingWindowLimiter: the actual point of this phase', () => {
  it('under real concurrent requests from many separate connections, never admits more than the limit', async () => {
    const prefix = `test:race:${Math.random()}:`
    const connections = Array.from({ length: 10 }, () => client())
    try {
      const results = await Promise.all(Array.from({ length: 50 }, (_, i) => {
        const rl = new RedisSlidingWindowLimiter(connections[i % connections.length], { limit: 20, windowMs: 60_000, keyPrefix: prefix })
        return rl.check('shared')
      }))
      expect(results.filter((r) => r.allowed).length).toBe(20)
    } finally { await Promise.all(connections.map((c) => c.quit())) }
  })
})
