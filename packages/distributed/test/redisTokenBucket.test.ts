import { afterAll, describe, expect, it } from 'vitest'
import { RedisTokenBucketLimiter } from '../src/redisTokenBucket'
import { client, redisAvailable } from './redis'

const available = await redisAvailable()
const redis = available ? client() : (null as never)
afterAll(async () => { if (available) await redis.quit() })

const limiter = (capacity: number, refillPerSec: number, now = { t: 0 }) => new RedisTokenBucketLimiter(redis, { capacity, refillPerSec, now: () => now.t, keyPrefix: `test:${Math.random()}:` })

describe.skipIf(!available)('RedisTokenBucketLimiter: matches the in-process algorithm', () => {
  it('starts full, allows a burst up to capacity, then refuses', async () => {
    const l = limiter(3, 1)
    expect(await l.check('a')).toMatchObject({ allowed: true, remaining: 2 })
    expect(await l.check('a')).toMatchObject({ allowed: true, remaining: 1 })
    expect(await l.check('a')).toMatchObject({ allowed: true, remaining: 0 })
    expect(await l.check('a')).toMatchObject({ allowed: false, remaining: 0 })
  })
  it('refills continuously with elapsed time', async () => {
    const now = { t: 0 }
    const l = limiter(10, 5, now)
    for (let i = 0; i < 10; i++) await l.check('a')
    now.t += 400 // 2 tokens back
    expect(await l.check('a')).toMatchObject({ allowed: true, remaining: 1 })
  })
  it('never refills past capacity', async () => {
    const now = { t: 0 }
    const l = limiter(5, 100, now)
    await l.check('a')
    now.t += 10_000
    expect(await l.check('a')).toMatchObject({ allowed: true, remaining: 4 })
  })
  it('a request can cost more than one token, all-or-nothing', async () => {
    const l = limiter(10, 1)
    expect(await l.check('a', 4)).toMatchObject({ allowed: true, remaining: 6 })
    expect(await l.check('a', 7)).toMatchObject({ allowed: false, remaining: 6 })
  })
  it('says how long until there will be enough tokens', async () => {
    const l = limiter(5, 2)
    for (let i = 0; i < 5; i++) await l.check('a')
    expect((await l.check('a', 3)).retryAfterMs).toBe(1500)
  })
  it('keys, and separately created limiters sharing the prefix, see the same state', async () => {
    const prefix = `test:shared:${Math.random()}:`
    const a = new RedisTokenBucketLimiter(redis, { capacity: 2, refillPerSec: 1, keyPrefix: prefix })
    const b = new RedisTokenBucketLimiter(redis, { capacity: 2, refillPerSec: 1, keyPrefix: prefix })
    await a.check('k')
    await a.check('k')
    expect((await b.check('k')).allowed).toBe(false) // b sees a's spend, because it is the same Redis key
  })
  it('reset gives a key a full bucket again', async () => {
    const l = limiter(3, 1)
    await l.check('a', 3)
    await l.reset('a')
    expect(await l.check('a')).toMatchObject({ allowed: true, remaining: 2 })
  })
  it('rejects a non-positive capacity, refill rate, or cost', () => {
    expect(() => new RedisTokenBucketLimiter(redis, { capacity: 0, refillPerSec: 1 })).toThrow()
    expect(() => new RedisTokenBucketLimiter(redis, { capacity: 1, refillPerSec: 0 })).toThrow()
    expect(limiter(5, 1).check('a', 0)).rejects.toThrow()
  })
})

describe.skipIf(!available)('RedisTokenBucketLimiter: the actual point of this phase', () => {
  it('under real concurrent requests from many separate connections, never admits more than capacity allows', async () => {
    const prefix = `test:race:${Math.random()}:`
    const connections = Array.from({ length: 10 }, () => client())
    try {
      // 50 concurrent requests for 20 tokens, from 10 separate real Redis connections, racing on one shared bucket.
      // A refill rate this small means the starting capacity is all that could possibly be admitted within the test.
      const results = await Promise.all(Array.from({ length: 50 }, (_, i) => {
        const rl = new RedisTokenBucketLimiter(connections[i % connections.length], { capacity: 20, refillPerSec: 0.0001, keyPrefix: prefix })
        return rl.check('shared')
      }))
      expect(results.filter((r) => r.allowed).length).toBe(20) // exactly capacity, however many raced for it at once
    } finally { await Promise.all(connections.map((c) => c.quit())) }
  })
})
