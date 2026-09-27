import { afterAll, describe, expect, it } from 'vitest'
import { RedisQueue } from '../src/redisQueue'
import { runWorker } from '../src/redisWorker'
import { client, redisAvailable } from './redis'

const available = await redisAvailable()
const redis = available ? client() : (null as never)
afterAll(async () => { if (available) await redis.quit() })

const queue = <T>(o: Partial<import('../src/redisQueue').RedisQueueOptions> = {}, now = { t: 0 }) =>
  new RedisQueue<T>(redis, { keyPrefix: `test:${Math.random()}`, now: () => now.t, ...o })

describe.skipIf(!available)('RedisQueue: the basics', () => {
  it('a job enqueued can be claimed, and completing it removes it from processing', async () => {
    const q = queue<string>()
    const id = await q.enqueue('hello')
    const job = await q.claim(1)
    expect(job).toMatchObject({ id, data: 'hello', attempts: 1 })
    expect(await q.stats()).toMatchObject({ ready: 0, processing: 1 })
    await q.complete(id)
    expect(await q.stats()).toMatchObject({ ready: 0, processing: 0 })
  })
  it('claim returns null (not throw, not hang forever) when nothing is ready', async () => {
    const q = queue<string>()
    expect(await q.claim(1)).toBeNull()
  })
  it('jobs are delivered first-in-first-out', async () => {
    const q = queue<string>()
    await q.enqueue('a'); await q.enqueue('b'); await q.enqueue('c')
    expect((await q.claim(1))?.data).toBe('a')
    expect((await q.claim(1))?.data).toBe('b')
    expect((await q.claim(1))?.data).toBe('c')
  })
  it('refuses a duplicate id', async () => {
    const q = queue<string>()
    await q.enqueue('x', { id: 'fixed' })
    await expect(q.enqueue('y', { id: 'fixed' })).rejects.toThrow(/duplicate/)
  })
})

describe.skipIf(!available)('RedisQueue: the actual point of this phase — never delivered twice', () => {
  it('claiming the same queue from many real connections at once never gives the same job to two of them', async () => {
    const keyPrefix = `test:${Math.random()}`
    const q = new RedisQueue<number>(redis, { keyPrefix })
    const n = 40
    for (let i = 0; i < n; i++) await q.enqueue(i)
    const connections = Array.from({ length: 8 }, () => client())
    try {
      const queues = connections.map((c) => new RedisQueue<number>(c, { keyPrefix }))
      const claimed: number[] = []
      await Promise.all(queues.map(async (qq) => {
        for (;;) {
          const job = await qq.claim(1)
          if (!job) return
          claimed.push(job.data)
        }
      }))
      expect(claimed.length).toBe(n)
      expect(new Set(claimed).size).toBe(n) // every job claimed exactly once, none claimed twice, none lost
    } finally { await Promise.all(connections.map((c) => c.quit())) }
  })
})

describe.skipIf(!available)('RedisQueue: retries, backoff and dead-letter', () => {
  it('a failed job is scheduled for retry, then promoteDue makes it claimable again', async () => {
    const now = { t: 0 }
    const q = queue<string>({ maxAttempts: 2, backoffMs: 1000 }, now)
    await q.enqueue('x')
    const job = await q.claim(1)
    const outcome = await q.fail(job!.id, 'boom')
    expect(outcome).toBe('retrying')
    expect(await q.claim(1)).toBeNull() // not due yet
    now.t = 1000
    expect(await q.promoteDue(now.t)).toBe(1)
    const retried = await q.claim(1)
    expect(retried).toMatchObject({ id: job!.id, attempts: 2 })
  })
  it('backoff never exceeds maxBackoffMs', async () => {
    const now = { t: 0 }
    const q = queue<string>({ maxAttempts: 5, backoffMs: 100, maxBackoffMs: 300 }, now)
    const id = await q.enqueue('x')
    for (let attempt = 1; attempt <= 4; attempt++) {
      const job = await q.claim(1)
      expect(job!.attempts).toBe(attempt)
      await q.fail(job!.id, 'x')
      const expectedWait = Math.min(300, 100 * 2 ** (attempt - 1))
      expect(await q.promoteDue(now.t + expectedWait - 1)).toBe(0) // not due a moment early
      now.t += expectedWait
      expect(await q.promoteDue(now.t)).toBe(1)
    }
    void id
  })
  it('a job that has used its last attempt goes to the dead-letter list, not back to retry', async () => {
    const q = queue<string>({ maxAttempts: 1 })
    await q.enqueue('doomed')
    const job = await q.claim(1)
    const outcome = await q.fail(job!.id, 'fatal')
    expect(outcome).toBe('dead')
    expect(await q.claim(1)).toBeNull()
    const dead = await q.deadLetters()
    expect(dead).toHaveLength(1)
    expect(dead[0]).toMatchObject({ data: 'doomed', lastError: 'fatal', attempts: 1 })
  })
  it('a job that succeeds after a retry is not left anywhere', async () => {
    const now = { t: 0 }
    const q = queue<string>({ maxAttempts: 2, backoffMs: 0 }, now)
    await q.enqueue('x')
    const job1 = await q.claim(1)
    await q.fail(job1!.id, 'once')
    await q.promoteDue(now.t)
    const job2 = await q.claim(1)
    await q.complete(job2!.id)
    expect(await q.stats()).toMatchObject({ ready: 0, processing: 0, delayed: 0, dead: 0 })
  })
})

describe.skipIf(!available)('RedisQueue: recovering from a crashed worker', () => {
  it('reap() puts a job claimed but never finished back to ready (through the normal retry backoff), once the visibility timeout has passed', async () => {
    const now = { t: 0 }
    const q = queue<string>({ visibilityTimeoutMs: 5000, maxAttempts: 2, backoffMs: 1000 }, now)
    await q.enqueue('orphaned')
    await q.claim(1) // a "worker" claims it and then never calls complete() or fail() — simulating a crash
    now.t = 4000
    expect(await q.reap(now.t)).toEqual({ reclaimed: 0, dead: 0 }) // not overdue yet
    now.t = 5001
    expect(await q.reap(now.t)).toEqual({ reclaimed: 1, dead: 0 }) // reclaimed: scheduled for retry, like any other failure
    expect(await q.claim(1)).toBeNull() // its retry backoff has not elapsed yet
    now.t += 1000
    expect(await q.promoteDue(now.t)).toBe(1)
    const recovered = await q.claim(1)
    expect(recovered).toMatchObject({ data: 'orphaned', attempts: 2 })
  })
  it('reap() sends a job past its attempt limit to dead-letter instead of retrying it forever', async () => {
    const now = { t: 0 }
    const q = queue<string>({ visibilityTimeoutMs: 100, maxAttempts: 1 }, now)
    await q.enqueue('x')
    await q.claim(1)
    now.t = 101
    expect(await q.reap(now.t)).toEqual({ reclaimed: 0, dead: 1 })
    expect(await q.deadLetters()).toHaveLength(1)
  })
})

describe.skipIf(!available)('runWorker: the loop that ties it together', () => {
  it('processes jobs from the queue until told to stop, and reports what happened', async () => {
    const q = queue<number>()
    for (let i = 0; i < 3; i++) await q.enqueue(i)
    const events: string[] = []
    const ctl = new AbortController()
    const done: number[] = []
    const runs = [runWorker(q, async (n) => { done.push(n) }, ctl.signal, { pollTimeoutSec: 1, onEvent: (e) => events.push(e.type) })]
    while (done.length < 3) await new Promise((r) => setTimeout(r, 10))
    ctl.abort()
    await Promise.all(runs)
    expect(done.sort()).toEqual([0, 1, 2])
    expect(events.filter((e) => e === 'completed')).toHaveLength(3)
  })
  it('a handler that throws reports failed, and the job is not lost', async () => {
    const q = queue<number>({ maxAttempts: 1 })
    await q.enqueue(1)
    const ctl = new AbortController()
    const events: string[] = []
    const run = runWorker(q, async () => { throw new Error('bad') }, ctl.signal, { pollTimeoutSec: 1, onEvent: (e) => events.push(e.type) })
    while (events.length < 1) await new Promise((r) => setTimeout(r, 10))
    ctl.abort()
    await run
    expect(events).toEqual(['claimed', 'dead'])
    expect(await q.deadLetters()).toHaveLength(1)
  })
})
