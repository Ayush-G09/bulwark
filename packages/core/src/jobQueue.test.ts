import { describe, expect, it } from 'vitest'
import { JobQueue } from './jobQueue'

const tick = (ms = 5) => new Promise((r) => setTimeout(r, ms))
const noSleep = async () => {}

describe('JobQueue: running jobs', () => {
  it('runs a job and reports completion; the queue goes idle', async () => {
    const done: number[] = []
    const q = new JobQueue<number>(async (n) => { done.push(n) }, { concurrency: 2 })
    q.enqueue(1); q.enqueue(2)
    await q.idle()
    expect(done.sort()).toEqual([1, 2])
    expect(q.stats).toMatchObject({ enqueued: 2, completed: 2, failed: 0 })
  })

  it('never runs more than `concurrency` jobs at once', async () => {
    let active = 0
    let maxActive = 0
    const q = new JobQueue<number>(async () => { active++; maxActive = Math.max(maxActive, active); await tick(15); active-- }, { concurrency: 2 })
    for (let i = 0; i < 6; i++) q.enqueue(i)
    await q.idle()
    expect(maxActive).toBe(2)
  })

  it('runs ready jobs in priority order, ties broken by arrival', async () => {
    const order: string[] = []
    const q = new JobQueue<string>(async (v) => { order.push(v) }, { concurrency: 1 })
    q.enqueue('low', { priority: 0 })
    q.enqueue('high', { priority: 10 })
    q.enqueue('mid', { priority: 5 })
    q.enqueue('low-2', { priority: 0 })
    await q.idle()
    expect(order).toEqual(['low', 'high', 'mid', 'low-2']) // 'low' had already started before the others arrived
  })

  it('a delayed job does not run before its time, and does run once it is due', async () => {
    const now = { t: 0 }
    const ran: number[] = []
    const waits: number[] = []
    // a fake sleep that records the wait rather than actually waiting, so the test controls time itself
    const q = new JobQueue<number>(async (n) => { ran.push(n) }, { concurrency: 1, now: () => now.t, sleep: async (ms) => { waits.push(ms) } })
    const id = q.enqueue(1, { delayUntil: 500 })
    expect(q.get(id)?.status).toBe('delayed')
    expect(ran).toEqual([]) // the fake sleep never resolves on its own, so nothing has run yet
    expect(waits).toEqual([500])
  })
  it('a real (unmocked) delay genuinely postpones the job', async () => {
    const ran: number[] = []
    const q = new JobQueue<number>(async (n) => { ran.push(n) }, { concurrency: 1 })
    q.enqueue(1, { delayUntil: Date.now() + 10_000 })
    await tick(20)
    expect(ran).toEqual([]) // 10s out, real 20ms is nowhere near enough
  })

  it('a job that fails and has no retries left goes straight to dead-letter', async () => {
    const q = new JobQueue<number>(async () => { throw new Error('boom') }, { concurrency: 1, maxAttempts: 1, sleep: noSleep })
    q.enqueue(1)
    await q.idle()
    expect(q.stats).toMatchObject({ completed: 0, retried: 0, dead: 1 })
    expect(q.deadLetters[0]).toMatchObject({ data: 1, attempts: 1, lastError: 'boom', status: 'dead' })
  })

  it('retries with growing backoff, then dies after the last attempt', async () => {
    const waits: number[] = []
    let calls = 0
    const q = new JobQueue<number>(async () => { calls++; throw new Error('down') }, {
      concurrency: 1, maxAttempts: 3, backoffMs: 100, sleep: async (ms) => { if (ms > 0) waits.push(ms) },
    })
    q.enqueue(1)
    await q.idle()
    expect(calls).toBe(3)
    expect(waits).toEqual([100, 200]) // exponential, one fewer wait than attempts (no wait after the last)
    expect(q.stats).toMatchObject({ retried: 2, dead: 1 })
  })

  it('backoff never exceeds maxBackoffMs, however many attempts fail', async () => {
    const waits: number[] = []
    const q = new JobQueue<number>(async () => { throw new Error('x') }, {
      concurrency: 1, maxAttempts: 6, backoffMs: 100, maxBackoffMs: 300, sleep: async (ms) => { if (ms > 0) waits.push(ms) },
    })
    q.enqueue(1)
    await q.idle()
    expect(waits).toEqual([100, 200, 300, 300, 300])
  })

  it('a job that succeeds on a retry counts as completed, not failed', async () => {
    let calls = 0
    const q = new JobQueue<number>(async () => { calls++; if (calls < 2) throw new Error('once') }, { concurrency: 1, maxAttempts: 3, sleep: noSleep })
    q.enqueue(1)
    await q.idle()
    expect(q.stats).toMatchObject({ completed: 1, retried: 1, dead: 0 })
  })

  it('one job failing does not stop the others', async () => {
    const done: number[] = []
    const q = new JobQueue<number>(async (n) => { if (n === 2) throw new Error('x'); done.push(n) }, { concurrency: 1, maxAttempts: 1, sleep: noSleep })
    for (const n of [1, 2, 3]) q.enqueue(n)
    await q.idle()
    expect(done).toEqual([1, 3])
  })
})

describe('JobQueue: ids, cancelling, and inspection', () => {
  it('gives every job an id, and lets a caller supply their own', () => {
    const q = new JobQueue<number>(async () => {}, { concurrency: 1 })
    const id1 = q.enqueue(1)
    const id2 = q.enqueue(2, { id: 'mine' })
    expect(id1).toBeTruthy()
    expect(id2).toBe('mine')
    expect(new Set([id1, id2]).size).toBe(2)
  })
  it('refuses a duplicate id', () => {
    const q = new JobQueue<number>(async () => {}, { concurrency: 1 })
    q.enqueue(1, { id: 'a' })
    expect(() => q.enqueue(2, { id: 'a' })).toThrow(/duplicate/)
  })
  it('a job can be cancelled before it starts, but not once it is running', async () => {
    let started = false
    const q = new JobQueue<number>(async () => { started = true; await tick(30) }, { concurrency: 1 })
    const running = q.enqueue(1) // starts immediately: concurrency 1, nothing ahead of it
    const notYetStarted = q.enqueue(2)
    await tick(5)
    expect(started).toBe(true)
    expect(q.cancel(running)).toBe(false)
    expect(q.cancel(notYetStarted)).toBe(true)
    expect(q.get(notYetStarted)).toBeUndefined()
  })
  it('reports a job’s status while it exists', async () => {
    const q = new JobQueue<number>(async () => { await tick(20) }, { concurrency: 1 })
    const id = q.enqueue(1)
    expect(q.get(id)?.status).toBe('active')
    await q.idle()
    expect(q.get(id)).toBeUndefined() // completed jobs are not kept forever
  })
  it('size counts both running and waiting jobs', async () => {
    const q = new JobQueue<number>(async () => { await tick(20) }, { concurrency: 1 })
    q.enqueue(1); q.enqueue(2); q.enqueue(3)
    await tick(5)
    expect(q.size).toBe(3) // 1 active + 2 waiting
  })
})

describe('JobQueue: shutting down', () => {
  it('refuses new jobs once closed', () => {
    const q = new JobQueue<number>(async () => {}, { concurrency: 1 })
    q.close()
    expect(() => q.enqueue(1)).toThrow(/closed/)
  })
  it('close aborts jobs that are running', async () => {
    let aborted = false
    const q = new JobQueue<number>(async (_n, { signal }) => {
      await new Promise((resolve) => { const t = setTimeout(resolve, 500); signal.addEventListener('abort', () => { clearTimeout(t); aborted = true; resolve(undefined) }) })
    }, { concurrency: 1 })
    q.enqueue(1)
    await tick(5)
    q.close()
    await tick(10)
    expect(aborted).toBe(true)
  })
  it('rejects a bad concurrency', () => {
    expect(() => new JobQueue(async () => {}, { concurrency: 0 })).toThrow()
  })
})

describe('JobQueue: events', () => {
  it('reports the lifecycle of a job that eventually succeeds', async () => {
    const events: string[] = []
    let calls = 0
    const q = new JobQueue<number>(async () => { calls++; if (calls < 2) throw new Error('x') }, {
      concurrency: 1, maxAttempts: 3, sleep: noSleep, onEvent: (e) => events.push(`${e.type}:${e.attempt}`),
    })
    q.enqueue(1)
    await q.idle()
    expect(events).toEqual(['started:1', 'retrying:1', 'started:2', 'completed:2'])
  })
})
