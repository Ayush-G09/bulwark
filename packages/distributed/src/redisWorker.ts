import type { RedisJobRecord, RedisQueue } from './redisQueue'

export interface RunWorkerOptions {
  /** how long each claim() call blocks waiting for a job before checking `signal` again */
  pollTimeoutSec?: number
  onEvent?: (e: { type: 'claimed' | 'completed' | 'failed' | 'dead'; id: string; attempt: number; error?: string }) => void
}

/**
 * Runs one worker loop against a RedisQueue until `signal` aborts: claim a job, run the handler, report success or
 * failure. Several of these, in several processes (or several machines), pointed at the same queue, is the whole
 * point — Redis's LMOVE is what stops any two of them from ever claiming the same job.
 */
export async function runWorker<T>(queue: RedisQueue<T>, handler: (data: T, job: RedisJobRecord<T>) => Promise<void>, signal: AbortSignal, opts: RunWorkerOptions = {}): Promise<void> {
  while (!signal.aborted) {
    const job = await queue.claim(opts.pollTimeoutSec ?? 2)
    if (!job) continue
    opts.onEvent?.({ type: 'claimed', id: job.id, attempt: job.attempts })
    try {
      await handler(job.data, job)
      await queue.complete(job.id)
      opts.onEvent?.({ type: 'completed', id: job.id, attempt: job.attempts })
    } catch (e) {
      const outcome = await queue.fail(job.id, (e as Error).message)
      opts.onEvent?.({ type: outcome === 'dead' ? 'dead' : 'failed', id: job.id, attempt: job.attempts, error: (e as Error).message })
    }
  }
}
