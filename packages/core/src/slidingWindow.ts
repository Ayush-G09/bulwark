import type { RateLimitResult } from './tokenBucket'

export interface SlidingWindowOptions {
  /** requests allowed per window */
  limit: number
  windowMs: number
}

interface State { count: number; windowStart: number; prevCount: number }

/**
 * The "sliding window counter" approximation: exact request timestamps are not kept (that would be the sliding window
 * log, below); instead each key has a current fixed window and remembers the previous one's count, and blends them by
 * how far into the current window we are. This assumes requests were spread evenly through the previous window, which
 * is usually close enough and costs O(1) memory per key instead of O(requests).
 *
 * `SlidingWindowLog` is the exact version, used in tests as ground truth to measure how far the approximation drifts,
 * and is a fine choice itself for a key with a modest request rate.
 */
export class SlidingWindowCounter {
  private readonly state = new Map<string, State>()
  private readonly now: () => number

  constructor(private readonly opts: SlidingWindowOptions, deps: { now?: () => number } = {}) {
    if (opts.limit <= 0) throw new Error('limit must be positive')
    if (opts.windowMs <= 0) throw new Error('windowMs must be positive')
    this.now = deps.now ?? Date.now
  }

  private estimate(s: State, at: number): number {
    const intoWindow = Math.min(1, Math.max(0, (at - s.windowStart) / this.opts.windowMs))
    return s.prevCount * (1 - intoWindow) + s.count
  }

  check(key: string, at = this.now()): RateLimitResult {
    let s = this.state.get(key)
    if (!s) { s = { count: 0, windowStart: at, prevCount: 0 }; this.state.set(key, s) }
    else {
      const elapsed = at - s.windowStart
      if (elapsed >= this.opts.windowMs * 2) { s.prevCount = 0; s.count = 0; s.windowStart = at } // idle long enough that the old window tells us nothing
      else if (elapsed >= this.opts.windowMs) { s.prevCount = s.count; s.count = 0; s.windowStart += this.opts.windowMs }
    }
    const estimated = this.estimate(s, at)
    if (estimated >= this.opts.limit) {
      // roughly how long until the blended estimate drops below the limit, assuming no more requests arrive
      const over = estimated - this.opts.limit
      const retryAfterMs = s.prevCount > 0 ? Math.ceil((over / s.prevCount) * this.opts.windowMs) : this.opts.windowMs
      return { allowed: false, remaining: 0, retryAfterMs: Math.max(1, retryAfterMs) }
    }
    s.count++
    return { allowed: true, remaining: Math.max(0, Math.floor(this.opts.limit - estimated - 1)) }
  }

  reset(key: string) { this.state.delete(key) }
  get size() { return this.state.size }
  sweep(olderThan: number) { for (const [k, s] of this.state) if (s.windowStart + this.opts.windowMs * 2 < olderThan) this.state.delete(k) }
}

/** The exact sliding window: keeps every timestamp in the last `windowMs`. O(requests in the window) per key; a clear
 * and simple reference to check the counter's approximation against, and fine to use directly at a modest request rate. */
export class SlidingWindowLog {
  private readonly hits = new Map<string, number[]>()
  private readonly now: () => number

  constructor(private readonly opts: SlidingWindowOptions, deps: { now?: () => number } = {}) {
    if (opts.limit <= 0) throw new Error('limit must be positive')
    if (opts.windowMs <= 0) throw new Error('windowMs must be positive')
    this.now = deps.now ?? Date.now
  }

  check(key: string, at = this.now()): RateLimitResult {
    const cutoff = at - this.opts.windowMs
    let list = this.hits.get(key)
    if (!list) { list = []; this.hits.set(key, list) }
    let dropped = 0
    while (dropped < list.length && list[dropped] <= cutoff) dropped++
    if (dropped > 0) list.splice(0, dropped)
    if (list.length >= this.opts.limit) {
      return { allowed: false, remaining: 0, retryAfterMs: Math.max(1, list[0] + this.opts.windowMs - at) }
    }
    list.push(at)
    return { allowed: true, remaining: this.opts.limit - list.length }
  }

  reset(key: string) { this.hits.delete(key) }
  get size() { return this.hits.size }
  sweep(olderThan: number) { for (const [k, list] of this.hits) if (list.length === 0 || list[list.length - 1] < olderThan - this.opts.windowMs) this.hits.delete(k) }
}
