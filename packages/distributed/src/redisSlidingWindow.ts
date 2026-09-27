import type { Redis } from 'ioredis'
import type { RateLimitResult } from '@bulwark/core'

export interface RedisSlidingWindowOptions {
  limit: number
  windowMs: number
  keyPrefix?: string
  now?: () => number
}

/**
 * The exact sliding window (`@bulwark/core`'s `SlidingWindowLog`), shared across processes via one Redis sorted set
 * per key: each request is a member scored by its own timestamp. "Drop everything older than the window, count what's
 * left, and add this request if there's room" is three separate Redis calls if done naively — a race between two
 * processes' counts is exactly the bug this phase exists to rule out. A Lua script makes the whole read-decide-write
 * one atomic step, the same way the token bucket above does.
 *
 * A sorted set, not a list, because it needs to both drop by score (age) and count in one structure; members are made
 * unique with a random suffix so two requests landing on the same millisecond do not collide and silently vanish.
 */
const SCRIPT = `
local key = KEYS[1]
local limit = tonumber(ARGV[1])
local windowMs = tonumber(ARGV[2])
local now = tonumber(ARGV[3])
local member = ARGV[4]

local cutoff = now - windowMs
redis.call('ZREMRANGEBYSCORE', key, '-inf', cutoff)
local count = redis.call('ZCARD', key)

local allowed = 0
local retryAfterMs = 0
if count < limit then
  redis.call('ZADD', key, now, member)
  allowed = 1
  count = count + 1
else
  local oldest = redis.call('ZRANGE', key, 0, 0, 'WITHSCORES')
  retryAfterMs = math.max(1, tonumber(oldest[2]) + windowMs - now)
end

redis.call('PEXPIRE', key, windowMs)
return { allowed, limit - count, retryAfterMs }
`

let counter = 0

export class RedisSlidingWindowLimiter {
  private readonly now: () => number
  private readonly prefix: string
  private sha: string | undefined

  constructor(private readonly redis: Redis, private readonly opts: RedisSlidingWindowOptions) {
    if (opts.limit <= 0) throw new Error('limit must be positive')
    if (opts.windowMs <= 0) throw new Error('windowMs must be positive')
    this.now = opts.now ?? Date.now
    this.prefix = opts.keyPrefix ?? 'bulwark:sw:'
  }

  private async run(key: string, now: number, member: string): Promise<[number, number, number]> {
    try {
      if (!this.sha) this.sha = (await this.redis.script('LOAD', SCRIPT)) as string
      return (await this.redis.evalsha(this.sha, 1, key, this.opts.limit, this.opts.windowMs, now, member)) as [number, number, number]
    } catch (e) {
      if (e instanceof Error && e.message.includes('NOSCRIPT')) { this.sha = undefined; return this.run(key, now, member) }
      throw e
    }
  }

  async check(key: string, at = this.now()): Promise<RateLimitResult> {
    // process id + counter + random: unique even if two calls in the same process land on the same millisecond
    const member = `${at}:${process.pid}:${counter++}:${Math.random().toString(36).slice(2, 8)}`
    const [allowed, remaining, retryAfterMs] = await this.run(this.prefix + key, at, member)
    return allowed === 1 ? { allowed: true, remaining: Math.max(0, remaining) } : { allowed: false, remaining: 0, retryAfterMs }
  }

  async reset(key: string) { await this.redis.del(this.prefix + key) }
}
