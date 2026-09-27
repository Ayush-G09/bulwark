/**
 * The job payloads this server knows how to run. Running arbitrary code from a job body would be a remote code
 * execution hole, so this is a small, fixed set of demo handlers, enough to prove the queue really executes work
 * (not just accepts it) and to give Phase 4's dashboard something real to show moving through retries.
 */
export interface JobPayload {
  type: 'sleep' | 'flaky' | 'fail'
  /** 'sleep': how long to take (ms). 'flaky': how long each attempt takes (ms). */
  ms?: number
  /** 'flaky': succeeds once attempts reaches this number; a fresh job starts at attempt 1 on its first try */
  succeedOnAttempt?: number
  /** 'fail': the message every attempt fails with, so it reliably ends up in the dead-letter list */
  message?: string
}

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms))

/** Runs one job. `attempt` is the queue's own attempt count (1 on the first try), so 'flaky' can decide from that
 * alone whether this is the attempt that should finally succeed. */
export async function runJob(payload: JobPayload, attempt: number): Promise<void> {
  if (payload.type === 'sleep') { await sleep(Math.max(0, payload.ms ?? 100)); return }
  if (payload.type === 'flaky') {
    await sleep(Math.max(0, payload.ms ?? 50))
    if (attempt < (payload.succeedOnAttempt ?? 2)) throw new Error(`flaky job failed on attempt ${attempt}`)
    return
  }
  if (payload.type === 'fail') throw new Error(payload.message ?? 'this job always fails')
  throw new Error(`unknown job type: ${(payload as { type: string }).type}`)
}
