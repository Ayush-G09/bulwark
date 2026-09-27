import type { Redis } from 'ioredis'
import type { RateLimitResult } from '@bulwark/core'

export interface RedisTokenBucketOptions {
  capacity: number
  refillPerSec: number
  /** a prefix so several limiters can share one Redis without colliding on keys */
  keyPrefix?: string
  now?: () => number
}

/**
 * The token bucket algorithm is exactly the in-process one (`@bulwark/core`'s `TokenBucketLimiter`), but the check and
 * the spend must happen as one atomic step in Redis, or two processes can both read "2 tokens left", both decide to
 * allow a request that costs 2, and both spend — over-admitting by however many processes raced. A Lua script run with
 * EVAL is Redis's unit of atomicity: nothing else touches the key while it runs, across every client connected to that
 * Redis, on every process, on every machine. That is the actual correctness guarantee this phase is about; the
 * process-local version cannot make it, because there is no single Redis all the copies of it share.
 *
 * State is one Redis hash per key: {tokens, updatedAt}. Ordinary keys (not Lua) would need read-then-write from the
 * client, which is exactly the race above.
 */
const SCRIPT = `
local key = KEYS[1]
local capacity = tonumber(ARGV[1])
local refillPerSec = tonumber(ARGV[2])
local cost = tonumber(ARGV[3])
local now = tonumber(ARGV[4])

local data = redis.call('HMGET', key, 'tokens', 'updatedAt')
local tokens = tonumber(data[1])
local updatedAt = tonumber(data[2])
if tokens == nil then
  tokens = capacity
  updatedAt = now
elseif now > updatedAt then
  local elapsedSec = (now - updatedAt) / 1000
  tokens = math.min(capacity, tokens + elapsedSec * refillPerSec)
  updatedAt = now
end

local allowed = 0
local retryAfterMs = 0
if tokens >= cost then
  tokens = tokens - cost
  allowed = 1
else
  local short = cost - tokens
  retryAfterMs = math.ceil((short / refillPerSec) * 1000)
end

redis.call('HSET', key, 'tokens', tostring(tokens), 'updatedAt', tostring(updatedAt))
-- expire well after the bucket would naturally refill to full, so an idle key is not kept forever
local ttlSec = math.ceil(capacity / refillPerSec) + 60
redis.call('EXPIRE', key, ttlSec)

return { allowed, tostring(tokens), retryAfterMs }
`

export class RedisTokenBucketLimiter {
  private readonly now: () => number
  private readonly prefix: string
  private sha: string | undefined

  constructor(private readonly redis: Redis, private readonly opts: RedisTokenBucketOptions) {
    if (opts.capacity <= 0) throw new Error('capacity must be positive')
    if (opts.refillPerSec <= 0) throw new Error('refillPerSec must be positive')
    this.now = opts.now ?? Date.now
    this.prefix = opts.keyPrefix ?? 'bulwark:tb:'
  }

  private async run(key: string, cost: number, now: number): Promise<[number, string, number]> {
    try {
      if (!this.sha) this.sha = (await this.redis.script('LOAD', SCRIPT)) as string
      return (await this.redis.evalsha(this.sha, 1, key, this.opts.capacity, this.opts.refillPerSec, cost, now)) as [number, string, number]
    } catch (e) {
      if (e instanceof Error && e.message.includes('NOSCRIPT')) { this.sha = undefined; return this.run(key, cost, now) }
      throw e
    }
  }

  async check(key: string, cost = 1, at = this.now()): Promise<RateLimitResult> {
    if (cost <= 0) throw new Error('cost must be positive')
    const [allowed, remaining, retryAfterMs] = await this.run(this.prefix + key, cost, at)
    return allowed === 1 ? { allowed: true, remaining: Number(remaining) } : { allowed: false, remaining: Number(remaining), retryAfterMs }
  }

  async reset(key: string) { await this.redis.del(this.prefix + key) }
}
