export type JobStatus = 'waiting' | 'delayed' | 'active' | 'completed' | 'failed' | 'dead'

export interface JobRecord<T> {
  id: string
  data: T
  status: JobStatus
  priority: number
  attempts: number
  maxAttempts: number
  addedAt: number
  /** not runnable before this time (a delayed job, or a failed job waiting to retry) */
  runAt: number
  lastError?: string
}

export interface EnqueueOptions {
  id?: string
  /** higher runs first among jobs that are ready; ties broken by arrival order */
  priority?: number
  /** do not run before this time */
  delayUntil?: number
  maxAttempts?: number
}

export interface JobQueueOptions {
  concurrency: number
  /** attempts before a job is retried at all (1 = no retries) */
  maxAttempts?: number
  backoffMs?: number
  /** the longest a retry will ever wait, however many attempts have failed (backoff would otherwise grow without bound) */
  maxBackoffMs?: number
  now?: () => number
  sleep?: (ms: number, signal: AbortSignal) => Promise<void>
  onEvent?: (e: { type: 'started' | 'completed' | 'failed' | 'retrying' | 'dead'; id: string; attempt: number; error?: string }) => void
}

const realSleep = (ms: number, signal: AbortSignal) => new Promise<void>((resolve) => {
  if (signal.aborted || ms <= 0) return resolve()
  const t = setTimeout(() => { signal.removeEventListener('abort', done); resolve() }, ms)
  const done = () => { clearTimeout(t); resolve() }
  signal.addEventListener('abort', done, { once: true })
})

let counter = 0
const nextId = () => `job_${Date.now().toString(36)}_${(counter++).toString(36)}`

/**
 * An in-process job queue: bounded concurrency, exponential backoff with a cap, and a dead-letter list for jobs that
 * exhaust their attempts. Ready jobs (waiting, and due) run in priority order, ties broken by arrival, so a flood of
 * low-priority jobs cannot starve a high-priority one that becomes ready at the same time. A job's own record is
 * mutated in place and can be inspected at any time — useful for a dashboard.
 *
 * A delayed or retrying job does not occupy a concurrency slot while it waits: it is timed by its own call to the
 * injectable `sleep`, the same function everything else's timing goes through, so a test can fully control (and a
 * consumer can fully swap out) how time passes here — there is no separate hidden real-time timer.
 *
 * This is one process's queue: correctness here is about ordering and retry behaviour, not about coordinating with
 * other processes (that is the distributed layer, built on top of this).
 */
export class JobQueue<T> {
  private readonly jobs = new Map<string, JobRecord<T>>()
  /** ids of jobs that are ready to run right now (waiting, or a delay/backoff that has elapsed) */
  private readonly ready = new Set<string>()
  /** ids currently sleeping until their delay or backoff elapses, with the controller that can cut the wait short */
  private readonly waiting = new Map<string, AbortController>()
  private active = 0
  private closed = false
  private readonly runningSignals = new Map<string, AbortController>()
  readonly stats = { enqueued: 0, completed: 0, failed: 0, retried: 0, dead: 0 }
  readonly deadLetters: JobRecord<T>[] = []
  private idleWaiters: (() => void)[] = []

  constructor(private readonly handler: (data: T, ctx: { attempt: number; job: JobRecord<T>; signal: AbortSignal }) => Promise<void>, private readonly opts: JobQueueOptions) {
    if (opts.concurrency < 1) throw new Error('concurrency must be at least 1')
  }

  private get now() { return this.opts.now ?? Date.now }
  private get sleep() { return this.opts.sleep ?? realSleep }

  enqueue(data: T, o: EnqueueOptions = {}): string {
    if (this.closed) throw new Error('queue is closed')
    const id = o.id ?? nextId()
    if (this.jobs.has(id)) throw new Error(`duplicate job id: ${id}`)
    const at = this.now()
    const runAt = o.delayUntil ?? at
    const job: JobRecord<T> = {
      id, data, status: runAt > at ? 'delayed' : 'waiting', priority: o.priority ?? 0,
      attempts: 0, maxAttempts: Math.max(1, o.maxAttempts ?? this.opts.maxAttempts ?? 1), addedAt: at, runAt,
    }
    this.jobs.set(id, job)
    this.stats.enqueued++
    if (job.status === 'waiting') { this.ready.add(id); this.pump() }
    else this.waitThenReady(job)
    return id
  }

  get(id: string): JobRecord<T> | undefined { return this.jobs.get(id) }

  /** Cancels a job that has not started yet (waiting, or still delayed/backing off). Returns false if it is already
   * running or finished, or does not exist. */
  cancel(id: string): boolean {
    const job = this.jobs.get(id)
    if (!job || job.status === 'active') return false
    this.ready.delete(id)
    this.waiting.get(id)?.abort()
    this.waiting.delete(id)
    this.jobs.delete(id)
    return true
  }

  private async waitThenReady(job: JobRecord<T>) {
    const ctl = new AbortController()
    this.waiting.set(job.id, ctl)
    await this.sleep(Math.max(0, job.runAt - this.now()), ctl.signal)
    this.waiting.delete(job.id)
    if (ctl.signal.aborted || !this.jobs.has(job.id)) return // cancelled, or the queue closed, while waiting
    job.status = 'waiting'
    this.ready.add(job.id)
    this.pump()
  }

  private pump() {
    if (this.closed) return
    while (this.active < this.opts.concurrency && this.ready.size > 0) {
      const candidates = [...this.ready].map((id) => this.jobs.get(id) as JobRecord<T>)
      candidates.sort((a, b) => b.priority - a.priority || a.addedAt - b.addedAt)
      const job = candidates[0]
      this.ready.delete(job.id)
      this.start(job)
    }
    if (this.idle_()) { const w = this.idleWaiters; this.idleWaiters = []; w.forEach((r) => r()) }
  }

  private idle_() { return this.active === 0 && this.ready.size === 0 && this.waiting.size === 0 }

  private start(job: JobRecord<T>) {
    job.status = 'active'
    job.attempts++
    this.active++
    const ctl = new AbortController()
    this.runningSignals.set(job.id, ctl)
    this.opts.onEvent?.({ type: 'started', id: job.id, attempt: job.attempts })
    void this.run(job, ctl.signal).finally(() => {
      this.runningSignals.delete(job.id)
      this.active--
      this.pump()
    })
  }

  private async run(job: JobRecord<T>, signal: AbortSignal) {
    try {
      await this.handler(job.data, { attempt: job.attempts, job, signal })
      job.status = 'completed'
      this.stats.completed++
      this.opts.onEvent?.({ type: 'completed', id: job.id, attempt: job.attempts })
      this.jobs.delete(job.id) // a completed job's memory is freed; its outcome was already reported via the event
    } catch (e) {
      const message = (e as Error).message
      job.lastError = message
      if (job.attempts < job.maxAttempts) {
        const backoff = this.opts.backoffMs ?? 1000
        const cap = this.opts.maxBackoffMs ?? Infinity
        const wait = Math.min(cap, backoff * 2 ** (job.attempts - 1))
        job.status = 'delayed'
        job.runAt = this.now() + wait
        this.stats.retried++
        this.opts.onEvent?.({ type: 'retrying', id: job.id, attempt: job.attempts, error: message })
        void this.waitThenReady(job)
      } else {
        job.status = 'dead'
        this.stats.dead++
        this.deadLetters.push(job)
        if (this.deadLetters.length > 1000) this.deadLetters.shift()
        this.opts.onEvent?.({ type: 'dead', id: job.id, attempt: job.attempts, error: message })
        this.jobs.delete(job.id)
      }
    }
  }

  /** Resolves once nothing is running, ready, or waiting out a delay/backoff. */
  idle(): Promise<void> {
    if (this.idle_()) return Promise.resolve()
    return new Promise((r) => this.idleWaiters.push(r))
  }

  get size() { return this.active + this.ready.size + this.waiting.size }

  /** Stops accepting new jobs, cancels everything still running or waiting out a delay, without waiting for them. */
  close() {
    this.closed = true
    for (const ctl of this.runningSignals.values()) ctl.abort()
    for (const ctl of this.waiting.values()) ctl.abort()
  }
}
