import { describe, expect, it } from 'vitest'
import { TokenBucketLimiter } from './tokenBucket'

const limiter = (capacity: number, refillPerSec: number, now = { t: 0 }) => new TokenBucketLimiter({ capacity, refillPerSec }, { now: () => now.t })

describe('TokenBucketLimiter: the basics', () => {
  it('starts full and allows a burst up to capacity, then refuses', () => {
    const l = limiter(3, 1)
    expect(l.check('a')).toMatchObject({ allowed: true, remaining: 2 })
    expect(l.check('a')).toMatchObject({ allowed: true, remaining: 1 })
    expect(l.check('a')).toMatchObject({ allowed: true, remaining: 0 })
    expect(l.check('a')).toMatchObject({ allowed: false, remaining: 0 })
  })
  it('refills continuously with elapsed time, not in ticks', () => {
    const now = { t: 0 }
    const l = limiter(10, 5, now) // 5 tokens/sec
    for (let i = 0; i < 10; i++) l.check('a')
    expect(l.check('a').allowed).toBe(false)
    now.t += 400 // 2 tokens back
    expect(l.check('a')).toMatchObject({ allowed: true, remaining: 1 })
    expect(l.check('a')).toMatchObject({ allowed: true, remaining: 0 })
    expect(l.check('a').allowed).toBe(false)
  })
  it('never refills past capacity, however long it has been idle', () => {
    const now = { t: 0 }
    const l = limiter(5, 100, now)
    l.check('a')
    now.t += 10_000
    expect(l.check('a')).toMatchObject({ allowed: true, remaining: 4 }) // capacity 5, one spent, not 500
  })
  it('a request can cost more than one token, and is all-or-nothing', () => {
    const l = limiter(10, 1)
    expect(l.check('a', 4)).toMatchObject({ allowed: true, remaining: 6 })
    expect(l.check('a', 7)).toMatchObject({ allowed: false, remaining: 6 }) // not partially spent
    expect(l.check('a', 6)).toMatchObject({ allowed: true, remaining: 0 })
  })
  it('keys are independent: one key being empty never affects another', () => {
    const l = limiter(1, 1)
    l.check('a')
    expect(l.check('a').allowed).toBe(false)
    expect(l.check('b').allowed).toBe(true)
  })
  it('says how long until there will be enough tokens', () => {
    const l = limiter(5, 2) // 2/sec
    for (let i = 0; i < 5; i++) l.check('a')
    const r = l.check('a', 3) // needs 3, has 0: 1500ms
    expect(r.retryAfterMs).toBe(1500)
  })
  it('a clock that jumps backward never refunds tokens', () => {
    const now = { t: 10_000 }
    const l = limiter(5, 1, now)
    l.check('a', 5) // empty
    now.t = 0 // NTP correction, or a clock skew across machines
    expect(l.check('a').allowed).toBe(false)
    now.t = 11_000 // one real second later than the first check
    expect(l.check('a')).toMatchObject({ allowed: true, remaining: 0 })
  })
  it('peek reports the level without spending anything or creating the key', () => {
    const l = limiter(5, 1)
    expect(l.peek('never-touched')).toBe(5)
    expect(l.size).toBe(0) // peek alone must not create state
    l.check('a', 2)
    expect(l.peek('a')).toBe(3)
    expect(l.peek('a')).toBe(3) // peeking twice does not spend twice
  })
  it('reset gives a key a full bucket again', () => {
    const l = limiter(3, 1)
    l.check('a', 3)
    l.reset('a')
    expect(l.check('a')).toMatchObject({ allowed: true, remaining: 2 })
  })
  it('sweep only drops idle, full buckets, never a key that is idle but still recovering', () => {
    const now = { t: 0 }
    const l = limiter(10, 0.5, now) // slow refill: 10s only brings back 5 tokens
    l.check('a', 9) // 1 left; 10s of idle refill (5) brings it to 6, not full
    l.check('b', 6) // 4 left; 10s of idle refill (5) brings it to 9, not full either
    now.t = 10_000
    l.sweep(5000)
    expect(l.size).toBe(2) // neither is actually full yet, so neither is swept
    l.check('b', 9) // spend down to exactly what remains, proving it truly was not full
    expect(l.peek('b')).toBeCloseTo(0, 6)
  })
  it('sweep drops a bucket once it has genuinely refilled to full while idle', () => {
    const now = { t: 0 }
    const l = limiter(3, 1, now)
    l.check('a', 1) // 2 left, refills to 3 well within 10s
    now.t = 10_000
    l.sweep(5000)
    expect(l.size).toBe(0)
    expect(l.peek('a')).toBe(3) // forgotten, so it starts fresh at capacity again
  })
  it('rejects a non-positive capacity, refill rate, or cost', () => {
    expect(() => new TokenBucketLimiter({ capacity: 0, refillPerSec: 1 })).toThrow()
    expect(() => new TokenBucketLimiter({ capacity: 1, refillPerSec: 0 })).toThrow()
    expect(() => limiter(5, 1).check('a', 0)).toThrow()
    expect(() => limiter(5, 1).check('a', -1)).toThrow()
  })
})

describe('TokenBucketLimiter: property test against a naive simulation', () => {
  function rng(seed: number) {
    let a = seed >>> 0
    return () => { a = (a + 0x6d2b79f5) >>> 0; let t = a; t = Math.imul(t ^ (t >>> 15), t | 1); t ^= t + Math.imul(t ^ (t >>> 7), t | 61); return ((t ^ (t >>> 14)) >>> 0) / 4294967296 }
  }
  it('matches a simple "add continuously, cap at capacity, spend if enough" model over random request timing', () => {
    const r = rng(12345)
    for (let trial = 0; trial < 50; trial++) {
      const capacity = 1 + Math.floor(r() * 20)
      const refillPerSec = 0.5 + r() * 10
      const now = { t: 0 }
      const l = new TokenBucketLimiter({ capacity, refillPerSec }, { now: () => now.t })
      let model = capacity
      let lastT = 0
      for (let i = 0; i < 200; i++) {
        now.t += Math.floor(r() * 500)
        const cost = 1 + Math.floor(r() * 3)
        model = Math.min(capacity, model + ((now.t - lastT) / 1000) * refillPerSec)
        lastT = now.t
        const modelAllowed = model >= cost
        if (modelAllowed) model -= cost
        const got = l.check('k', cost, now.t)
        expect(got.allowed, `trial ${trial} step ${i}`).toBe(modelAllowed)
        expect(got.remaining, `trial ${trial} step ${i}`).toBeCloseTo(model, 6)
      }
    }
  })
})
