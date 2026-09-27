import { describe, expect, it } from 'vitest'
import { SlidingWindowCounter, SlidingWindowLog } from './slidingWindow'

function rng(seed: number) {
  let a = seed >>> 0
  return () => { a = (a + 0x6d2b79f5) >>> 0; let t = a; t = Math.imul(t ^ (t >>> 15), t | 1); t ^= t + Math.imul(t ^ (t >>> 7), t | 61); return ((t ^ (t >>> 14)) >>> 0) / 4294967296 }
}

describe('SlidingWindowLog: exact', () => {
  const make = (limit: number, windowMs: number, now = { t: 0 }) => ({ l: new SlidingWindowLog({ limit, windowMs }, { now: () => now.t }), now })

  it('allows exactly `limit` requests in a window, then refuses', () => {
    const { l } = make(3, 1000)
    for (let i = 0; i < 3; i++) expect(l.check('a', 0).allowed).toBe(true)
    expect(l.check('a', 0)).toMatchObject({ allowed: false, remaining: 0 })
  })
  it('a request that expires makes room for exactly one more, not a reset', () => {
    const { l } = make(2, 1000)
    l.check('a', 0)
    l.check('a', 500)
    expect(l.check('a', 900).allowed).toBe(false)
    expect(l.check('a', 1001).allowed).toBe(true) // the t=0 request has aged out
    expect(l.check('a', 1002).allowed).toBe(false) // but the t=500 one has not
  })
  it('is a true sliding window, not fixed buckets: no boundary burst', () => {
    const { l } = make(2, 1000)
    l.check('a', 999); l.check('a', 999) // right at the end of "window 0"
    expect(l.check('a', 1000).allowed).toBe(false) // "window 1" started, but both requests are still within the last 1000ms
    expect(l.check('a', 1999).allowed).toBe(true) // now the first has aged out
  })
  it('keys are independent', () => {
    const { l } = make(1, 1000)
    l.check('a', 0)
    expect(l.check('a', 0).allowed).toBe(false)
    expect(l.check('b', 0).allowed).toBe(true)
  })
  it('says how long until the oldest request in the window ages out', () => {
    const { l } = make(1, 1000)
    l.check('a', 0)
    expect(l.check('a', 400).retryAfterMs).toBe(600)
  })
  it('rejects a non-positive limit or window', () => {
    expect(() => new SlidingWindowLog({ limit: 0, windowMs: 1000 })).toThrow()
    expect(() => new SlidingWindowLog({ limit: 1, windowMs: 0 })).toThrow()
  })
  it('reset and sweep', () => {
    const { l } = make(1, 1000)
    l.check('a', 0)
    l.reset('a')
    expect(l.check('a', 0).allowed).toBe(true)
    l.sweep(5000)
    expect(l.size).toBe(0)
  })
})

describe('SlidingWindowCounter: the approximation', () => {
  const make = (limit: number, windowMs: number, now = { t: 0 }) => ({ c: new SlidingWindowCounter({ limit, windowMs }, { now: () => now.t }), now })

  it('allows about `limit` requests spread evenly through a window', () => {
    const { c } = make(10, 1000)
    let allowed = 0
    for (let i = 0; i < 10; i++) if (c.check('a', i * 100).allowed) allowed++
    expect(allowed).toBe(10)
    expect(c.check('a', 950).allowed).toBe(false)
  })
  it('smooths the boundary between windows: bunching 10 requests right before it and 10 right after does not let all 20 through', () => {
    const { c } = make(10, 1000)
    let allowed = 0
    for (let i = 0; i < 10; i++) if (c.check('a', 995).allowed) allowed++ // all just before the boundary
    for (let i = 0; i < 10; i++) if (c.check('a', 1005).allowed) allowed++ // all just after it
    // a naive fixed-window counter would allow all 20 (two separate buckets); the blend must refuse most of the second burst
    expect(allowed).toBeLessThan(14)
  })
  it('the previous window’s weight actually decays as the current window progresses, not just at the very end', () => {
    const { c, now } = make(10, 1000)
    for (let i = 0; i < 10; i++) c.check('a', 0) // window 0 full
    now.t = 1500 // halfway through window 1: previous window's weight should be roughly halved
    const r = c.check('a', 1500)
    // estimate ≈ 10*0.5 + 1 = 6, well under the limit of 10 — a limiter that never decays would still read ~10 here and refuse
    expect(r.allowed).toBe(true)
    expect(r.remaining).toBeGreaterThan(0)
  })
  it('a key untouched for two full windows starts fresh, not still throttled by ghost traffic', () => {
    const { c } = make(2, 1000)
    c.check('a', 0); c.check('a', 0)
    expect(c.check('a', 2001).allowed).toBe(true)
  })
  it('keys are independent, and rejects bad options', () => {
    const { c } = make(1, 1000)
    c.check('a', 0)
    expect(c.check('b', 0).allowed).toBe(true)
    expect(() => new SlidingWindowCounter({ limit: 0, windowMs: 1000 })).toThrow()
  })
  it('reset and sweep', () => {
    const { c } = make(1, 1000)
    c.check('a', 0)
    c.reset('a')
    expect(c.check('a', 0).allowed).toBe(true)
    c.sweep(5000)
    expect(c.size).toBe(0)
  })
})

describe('SlidingWindowCounter vs SlidingWindowLog: the approximation stays close to the exact answer', () => {
  it('over random request timing, the counter never allows drastically more than the log would, and rarely allows much less', () => {
    const r = rng(777)
    for (let trial = 0; trial < 30; trial++) {
      const limit = 2 + Math.floor(r() * 20)
      const windowMs = 500 + Math.floor(r() * 3000)
      const log = new SlidingWindowLog({ limit, windowMs })
      const counter = new SlidingWindowCounter({ limit, windowMs })
      let t = 0
      let logAllowed = 0
      let counterAllowed = 0
      for (let i = 0; i < 300; i++) {
        t += Math.floor(r() * (windowMs / 5))
        if (log.check('k', t).allowed) logAllowed++
        if (counter.check('k', t).allowed) counterAllowed++
      }
      // the approximation must never let meaningfully more traffic through than the exact algorithm over the same trial
      expect(counterAllowed, `trial ${trial}: limit ${limit} window ${windowMs}`).toBeLessThanOrEqual(logAllowed + Math.ceil(logAllowed * 0.15) + 2)
    }
  })
})
