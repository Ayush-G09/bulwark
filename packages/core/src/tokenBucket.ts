export interface RateLimitResult {
  allowed: boolean
  /** tokens left in the bucket right after this call */
  remaining: number
  /** if refused, how long until enough tokens exist for this same request (ms) */
  retryAfterMs?: number
}

export interface TokenBucketOptions {
  /** the bucket's size: the largest burst a key can spend at once */
  capacity: number
  /** tokens added per second */
  refillPerSec: number
}

interface Bucket {
  tokens: number
  /** last time this bucket was brought up to date */
  updatedAt: number
}

/**
 * A token bucket per key. Tokens refill continuously (not in ticks), computed lazily from elapsed time whenever the
 * key is next touched, so idle keys cost nothing and there is no background timer to get wrong. A request of `cost`
 * tokens is only ever accepted if the bucket can pay for all of it at once: a request never partially succeeds.
 *
 * Single-threaded correctness: JavaScript runs `check` to completion before anything else touches the same bucket, so
 * "read the bucket, decide, write the bucket" is naturally atomic within one process. That guarantee ends the moment
 * state is shared across processes (see the distributed limiter) — this class alone is not safe to call from two
 * processes against the same storage without more care.
 */
export class TokenBucketLimiter {
  private readonly buckets = new Map<string, Bucket>()
  private readonly now: () => number

  constructor(private readonly opts: TokenBucketOptions, deps: { now?: () => number } = {}) {
    if (opts.capacity <= 0) throw new Error('capacity must be positive')
    if (opts.refillPerSec <= 0) throw new Error('refillPerSec must be positive')
    this.now = deps.now ?? Date.now
  }

  private bucketFor(key: string, at: number): Bucket {
    let b = this.buckets.get(key)
    if (!b) { b = { tokens: this.opts.capacity, updatedAt: at }; this.buckets.set(key, b) }
    else if (at > b.updatedAt) {
      const elapsedSec = (at - b.updatedAt) / 1000
      b.tokens = Math.min(this.opts.capacity, b.tokens + elapsedSec * this.opts.refillPerSec)
      b.updatedAt = at
    }
    // a clock that goes backward (NTP adjustment) never refunds tokens or moves updatedAt backward
    return b
  }

  check(key: string, cost = 1, at = this.now()): RateLimitResult {
    if (cost <= 0) throw new Error('cost must be positive')
    const b = this.bucketFor(key, at)
    if (b.tokens >= cost) {
      b.tokens -= cost
      return { allowed: true, remaining: b.tokens }
    }
    const short = cost - b.tokens
    return { allowed: false, remaining: b.tokens, retryAfterMs: Math.ceil((short / this.opts.refillPerSec) * 1000) }
  }

  /** The current token count for a key, without spending anything or creating the key if it does not exist yet. */
  peek(key: string, at = this.now()): number {
    const existing = this.buckets.get(key)
    if (!existing) return this.opts.capacity
    return this.bucketFor(key, at).tokens
  }

  /** Forgets a key, so its next request starts with a full bucket again. */
  reset(key: string) { this.buckets.delete(key) }

  get size() { return this.buckets.size }

  /** Drops buckets that are full and have been untouched since before `olderThan`, so idle keys do not leak memory
   * forever. A bucket's fill level is brought up to date first, so one that has simply refilled since its last request
   * is correctly recognised as full, not skipped because of a stale token count. */
  sweep(olderThan: number, at = this.now()) {
    for (const [k, b] of this.buckets) {
      if (b.updatedAt >= olderThan) continue
      if (this.bucketFor(k, at).tokens >= this.opts.capacity) this.buckets.delete(k)
    }
  }
}
