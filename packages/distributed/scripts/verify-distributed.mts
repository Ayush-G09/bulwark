// Proves the distributed limiter and queue are correct under REAL concurrency: not several connections in one Node
// process (which the tests already cover), but several separate OS processes, started with child_process.fork, each
// with its own event loop, hammering the same real Redis at once. This is the same kind of proof Depth's
// verify-real.mts and Redline's evaluation harness use elsewhere in this portfolio: measure against reality, not a
// simulation of it.
//
//   npm run infra:up      # a real Redis on :6390, once
//   npx tsx packages/distributed/scripts/verify-distributed.mts
import { fork } from 'node:child_process'
import { fileURLToPath } from 'node:url'
import Redis from 'ioredis'
import { RedisQueue } from '../src/redisQueue'
import { REDIS_URL } from '../test/redis'

const WORKER = fileURLToPath(new URL('./worker-child.mts', import.meta.url))

function runChild(mode: string, args: Record<string, unknown>): Promise<any> {
  return new Promise((resolve, reject) => {
    const child = fork(WORKER, [], { execArgv: ['--import', 'tsx'], stdio: ['ignore', 'ignore', 'inherit', 'ipc'] })
    child.send({ mode, args })
    child.on('message', (msg: any) => { if (msg?.type === 'result') resolve(msg.value) })
    child.on('exit', (code) => { if (code !== 0) reject(new Error(`child exited with code ${code}`)) })
    child.on('error', reject)
  })
}

let failures = 0
function report(name: string, ok: boolean, detail: string) {
  console.log(`${ok ? 'PASS' : 'FAIL'}: ${name}${detail ? ` — ${detail}` : ''}`)
  if (!ok) failures++
}

// ---------------------------------------------------------------------------------------------------------------
// 1. Token bucket: N real processes race for a bucket of known capacity. Total admitted must equal capacity exactly.
// ---------------------------------------------------------------------------------------------------------------
async function verifyTokenBucket() {
  const prefix = `verify:tb:${Date.now()}:`
  const capacity = 50
  const processes = 6
  const perProcess = 40 // processes * perProcess = 240 requests contending for 50 tokens
  console.log(`\ntoken bucket: ${processes} real processes x ${perProcess} requests each, racing for a bucket of ${capacity}`)
  const results: number[] = await Promise.all(
    Array.from({ length: processes }, () => runChild('token-bucket', { prefix, capacity, refillPerSec: 0.0001, requests: perProcess })),
  )
  const totalAdmitted = results.reduce((a: number, b: number) => a + b, 0)
  report('token bucket admits exactly its capacity, never more, under real cross-process concurrency', totalAdmitted === capacity, `admitted ${totalAdmitted}, capacity ${capacity}`)
}

// ---------------------------------------------------------------------------------------------------------------
// 2. Sliding window: same shape, same proof.
// ---------------------------------------------------------------------------------------------------------------
async function verifySlidingWindow() {
  const prefix = `verify:sw:${Date.now()}:`
  const limit = 50
  const processes = 6
  const perProcess = 40
  console.log(`\nsliding window: ${processes} real processes x ${perProcess} requests each, racing for a limit of ${limit}`)
  const results: number[] = await Promise.all(
    Array.from({ length: processes }, () => runChild('sliding-window', { prefix, limit, windowMs: 60_000, requests: perProcess })),
  )
  const totalAdmitted = results.reduce((a: number, b: number) => a + b, 0)
  report('sliding window admits exactly its limit, never more, under real cross-process concurrency', totalAdmitted === limit, `admitted ${totalAdmitted}, limit ${limit}`)
}

// ---------------------------------------------------------------------------------------------------------------
// 3. Job queue: N real worker processes drain a queue of M jobs. Every job must be claimed exactly once: none
//    delivered twice (mutual exclusion), none lost.
// ---------------------------------------------------------------------------------------------------------------
async function verifyQueueExactlyOnce() {
  const prefix = `verify:q1:${Date.now()}`
  const jobCount = 300
  const workerCount = 8
  console.log(`\njob queue: ${jobCount} jobs, ${workerCount} real worker processes draining them concurrently`)
  const redis = new Redis(REDIS_URL)
  const q = new RedisQueue<number>(redis, { keyPrefix: prefix })
  for (let i = 0; i < jobCount; i++) await q.enqueue(i)
  const results: number[][] = await Promise.all(
    Array.from({ length: workerCount }, () => runChild('drain-queue', { prefix })),
  )
  await redis.quit()
  const all = results.flat()
  const unique = new Set(all)
  report('every job is claimed by exactly one worker: none delivered twice', unique.size === all.length, `${all.length} claims, ${unique.size} unique jobs`)
  report('every job is eventually claimed: none silently lost', unique.size === jobCount, `expected ${jobCount}, got ${unique.size}`)
}

// ---------------------------------------------------------------------------------------------------------------
// 4. Crash recovery: a real process claims jobs and is killed mid-work, without ever calling complete() or fail().
//    reap(), run from a second process, must recover every one of them — proving a crash cannot lose work.
// ---------------------------------------------------------------------------------------------------------------
async function verifyCrashRecovery() {
  const prefix = `verify:q2:${Date.now()}`
  const jobCount = 20
  console.log(`\ncrash recovery: a real worker process is killed mid-job for all ${jobCount} jobs; reap() must recover every one`)
  const redis = new Redis(REDIS_URL)
  const q = new RedisQueue<number>(redis, { keyPrefix: prefix, visibilityTimeoutMs: 300, maxAttempts: 2, backoffMs: 0 })
  for (let i = 0; i < jobCount; i++) await q.enqueue(i)

  const child = fork(WORKER, [], { execArgv: ['--import', 'tsx'], stdio: ['ignore', 'ignore', 'inherit', 'ipc'] })
  const claimedByCrashed = new Promise<number>((resolve) => {
    child.on('message', (msg: any) => { if (msg?.type === 'result') resolve(msg.value) })
  })
  child.send({ mode: 'claim-then-die', args: { prefix, count: jobCount } })
  const gotClaimed = await claimedByCrashed
  child.kill('SIGKILL') // no complete(), no fail(): exactly what a real crash looks like
  await new Promise((r) => setTimeout(r, 150))

  await new Promise((r) => setTimeout(r, 350)) // past the visibility timeout
  let reclaimed = 0
  for (let i = 0; i < 5; i++) { const r = await q.reap(); reclaimed += r.reclaimed }
  await q.promoteDue()
  let recoveredCount = 0
  for (;;) { const job = await q.claim(1); if (!job) break; recoveredCount++; await q.complete(job.id) }
  await redis.quit()
  report('the crashed process actually claimed jobs before dying (the test is meaningful)', gotClaimed === jobCount, `claimed ${gotClaimed} of ${jobCount}`)
  report('every job the crashed worker held is recovered and completable, none stuck forever', recoveredCount === jobCount, `recovered ${recoveredCount} of ${jobCount}`)
}

await verifyTokenBucket()
await verifySlidingWindow()
await verifyQueueExactlyOnce()
await verifyCrashRecovery()

console.log(`\n${failures === 0 ? 'ALL PASSED' : `${failures} CHECK(S) FAILED`}`)
process.exit(failures === 0 ? 0 : 1)
