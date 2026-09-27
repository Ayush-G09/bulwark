import { randomUUID } from 'node:crypto'
import type { Redis } from 'ioredis'

export interface RedisJobRecord<T> {
  id: string
  data: T
  attempts: number
  maxAttempts: number
  addedAt: number
  lastError?: string
}

export interface RedisQueueOptions {
  /** all keys for this queue are named `<keyPrefix><suffix>`, so several queues can share one Redis */
  keyPrefix: string
  maxAttempts?: number
  backoffMs?: number
  maxBackoffMs?: number
  /** a job claimed but not finished within this long is assumed to belong to a crashed worker and is put back */
  visibilityTimeoutMs?: number
  now?: () => number
}

/**
 * A job queue shared by many worker processes, or many machines, over one Redis: every job is delivered to exactly one
 * worker at a time, retried with backoff on failure, and never silently lost if a worker crashes mid-job.
 *
 * The core move is `LMOVE <ready> <processing> LEFT RIGHT`, a single atomic Redis command: a job leaves the ready list
 * and enters the processing list as one indivisible step. Two workers calling this at once can never both receive the
 * same job — Redis serialises the command, so only one of them gets it; that is the actual distributed-mutual-exclusion
 * guarantee this phase exists to prove, and it needs no lock of our own, because it is what LMOVE already is.
 *
 * If a worker dies after claiming a job but before finishing it, the job sits in the processing list. `reap()` finds
 * entries older than the visibility timeout and returns them to the ready list (or, past their attempt limit, to the
 * dead-letter list) — this is how the system recovers from a crash without a human intervening. A real deployment
 * calls `reap()` on a timer; the delivered method makes that explicit rather than hiding a background timer in here.
 */
export class RedisQueue<T> {
  private readonly ready: string
  private readonly processing: string
  private readonly meta: string // hash: id -> JSON job record
  private readonly claimedAt: string // hash: id -> ms claimed, so reap() knows what has overstayed
  private readonly delayed: string // zset: id -> runAt, for scheduled retries
  private readonly dead: string
  private readonly now: () => number

  constructor(private readonly redis: Redis, private readonly opts: RedisQueueOptions) {
    const p = opts.keyPrefix
    this.ready = `${p}:ready`
    this.processing = `${p}:processing`
    this.meta = `${p}:meta`
    this.claimedAt = `${p}:claimedAt`
    this.delayed = `${p}:delayed`
    this.dead = `${p}:dead`
    this.now = opts.now ?? Date.now
  }

  async enqueue(data: T, o: { id?: string; maxAttempts?: number } = {}): Promise<string> {
    const id = o.id ?? randomUUID()
    const record: RedisJobRecord<T> = { id, data, attempts: 0, maxAttempts: Math.max(1, o.maxAttempts ?? this.opts.maxAttempts ?? 1), addedAt: this.now() }
    const added = await this.redis.hsetnx(this.meta, id, JSON.stringify(record))
    if (!added) throw new Error(`duplicate job id: ${id}`)
    await this.redis.rpush(this.ready, id)
    return id
  }

  /** Blocks up to `timeoutSec` for a job to become available, claims it atomically, and returns it (or null on timeout). */
  async claim(timeoutSec = 5): Promise<RedisJobRecord<T> | null> {
    const id = await this.redis.blmove(this.ready, this.processing, 'LEFT', 'RIGHT', timeoutSec)
    if (!id) return null
    const raw = await this.redis.hget(this.meta, id)
    if (!raw) { await this.redis.lrem(this.processing, 1, id); return null } // the job was deleted from under us (cancelled)
    const record = JSON.parse(raw) as RedisJobRecord<T>
    record.attempts++
    await this.redis.hset(this.meta, id, JSON.stringify(record))
    await this.redis.hset(this.claimedAt, id, String(this.now()))
    return record
  }

  async complete(id: string) {
    await this.redis.multi().lrem(this.processing, 1, id).hdel(this.meta, id).hdel(this.claimedAt, id).exec()
  }

  /** Call when the handler threw. Schedules a retry with backoff, or moves the job to the dead-letter list if it has
   * used its last attempt. */
  async fail(id: string, error: string): Promise<'retrying' | 'dead'> {
    const raw = await this.redis.hget(this.meta, id)
    if (!raw) { await this.redis.lrem(this.processing, 1, id); return 'dead' } // nothing left to retry
    const record = JSON.parse(raw) as RedisJobRecord<T>
    record.lastError = error
    if (record.attempts < record.maxAttempts) {
      const backoff = this.opts.backoffMs ?? 1000
      const cap = this.opts.maxBackoffMs ?? Infinity
      const wait = Math.min(cap, backoff * 2 ** (record.attempts - 1))
      await this.redis.multi()
        .lrem(this.processing, 1, id)
        .hdel(this.claimedAt, id)
        .hset(this.meta, id, JSON.stringify(record))
        .zadd(this.delayed, this.now() + wait, id)
        .exec()
      return 'retrying'
    }
    await this.redis.multi()
      .lrem(this.processing, 1, id)
      .hdel(this.claimedAt, id)
      .hdel(this.meta, id)
      .rpush(this.dead, JSON.stringify(record))
      .ltrim(this.dead, -1000, -1) // keep the dead-letter list bounded
      .exec()
    return 'dead'
  }

  /** Moves due delayed (retrying) jobs back to the ready list. Call on a timer, or after `fail()`, to actually run retries. */
  async promoteDue(at = this.now()): Promise<number> {
    const due = await this.redis.zrangebyscore(this.delayed, '-inf', at)
    if (due.length === 0) return 0
    const pipe = this.redis.multi()
    for (const id of due) pipe.zrem(this.delayed, id).rpush(this.ready, id)
    await pipe.exec()
    return due.length
  }

  /** Recovers jobs whose worker went silent: past the visibility timeout, put back in `processing`. Call on a timer.
   * Returns how many were reclaimed and how many were given up on (moved to dead-letter, out of attempts). */
  async reap(at = this.now()): Promise<{ reclaimed: number; dead: number }> {
    const timeout = this.opts.visibilityTimeoutMs ?? 30_000
    const ids = await this.redis.lrange(this.processing, 0, -1)
    let reclaimed = 0
    let dead = 0
    for (const id of ids) {
      const claimedAtStr = await this.redis.hget(this.claimedAt, id)
      if (!claimedAtStr || at - Number(claimedAtStr) < timeout) continue
      const outcome = await this.fail(id, 'worker did not finish within the visibility timeout')
      if (outcome === 'retrying') reclaimed++
      else dead++
    }
    return { reclaimed, dead }
  }

  async deadLetters(limit = 100): Promise<RedisJobRecord<T>[]> {
    const raw = await this.redis.lrange(this.dead, -limit, -1)
    return raw.map((s) => JSON.parse(s) as RedisJobRecord<T>)
  }

  async stats() {
    const [readyLen, processingLen, delayedLen, deadLen] = await Promise.all([
      this.redis.llen(this.ready), this.redis.llen(this.processing), this.redis.zcard(this.delayed), this.redis.llen(this.dead),
    ])
    return { ready: readyLen, processing: processingLen, delayed: delayedLen, dead: deadLen }
  }

  /** For tests and local runs only: drops every key this queue owns. */
  async _wipe() {
    await this.redis.del(this.ready, this.processing, this.meta, this.claimedAt, this.delayed, this.dead)
  }
}
