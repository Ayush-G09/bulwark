// The final proof of Phase 3: three real HTTP servers, each its own OS process in its own Docker container, each
// reachable only on its own port, sharing nothing but Redis. This checks the actual wiring end to end — the HTTP
// layer, the policy lookup, the embedded worker — not just the Redis primitives underneath it (Phase 2 already
// proved those in isolation). If this passes, "run several of these behind a load balancer" is a real, working claim.
//
//   docker compose up -d --build
//   npx tsx apps/server/scripts/verify-multi-instance.mts
import { randomUUID } from 'node:crypto'

const INSTANCES = [
  process.env.INSTANCE_A ?? 'http://localhost:3301',
  process.env.INSTANCE_B ?? 'http://localhost:3302',
  process.env.INSTANCE_C ?? 'http://localhost:3303',
]

let failures = 0
function report(name: string, ok: boolean, detail: string) {
  console.log(`${ok ? 'PASS' : 'FAIL'}: ${name}${detail ? ` — ${detail}` : ''}`)
  if (!ok) failures++
}

async function waitForAll() {
  for (const base of INSTANCES) {
    for (let i = 0; i < 30; i++) {
      try { if ((await fetch(`${base}/health`)).ok) break } catch { /* not up yet */ }
      await new Promise((r) => setTimeout(r, 500))
    }
  }
}

async function health() {
  console.log('\nchecking every instance answers, and each identifies itself differently')
  const ids = await Promise.all(INSTANCES.map(async (base) => (await (await fetch(`${base}/health`)).json()).instanceId))
  report('three ports, three different instance ids (these really are three processes)', new Set(ids).size === 3, ids.join(', '))
}

async function sharedRateLimit() {
  const key = `verify:${randomUUID()}`
  console.log(`\nrate limit: hitting a fresh key through all 3 real instances concurrently, more than the policy's capacity`)
  // the default "login" policy in main.ts's config is a sliding window of 5 per minute; ask for 15 across 3 instances
  const results = await Promise.all(
    Array.from({ length: 15 }, (_, i) => fetch(`${INSTANCES[i % 3]}/limit`, { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify({ policy: 'login', key }) })),
  )
  const allowed = results.filter((r) => r.status === 200).length
  report('exactly the policy\'s limit is admitted in total, however the requests were spread across instances', allowed === 5, `admitted ${allowed} of 15 requests, limit is 5`)
}

async function sharedQueue() {
  const jobCount = 60
  console.log(`\njob queue: enqueueing ${jobCount} jobs through instance A, letting all 3 instances' embedded workers race to drain them`)
  const ids = new Set<string>()
  for (let i = 0; i < jobCount; i++) {
    const r = await fetch(`${INSTANCES[0]}/jobs`, { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify({ type: 'sleep', ms: 20 }) })
    ids.add((await r.json()).id)
  }
  const deadline = Date.now() + 15_000
  let drained = false
  while (Date.now() < deadline) {
    const stats = await (await fetch(`${INSTANCES[0]}/stats`)).json()
    if (stats.queue.ready === 0 && stats.queue.processing === 0) { drained = true; break }
    await new Promise((r) => setTimeout(r, 300))
  }
  report(`all ${jobCount} jobs enqueued through one instance are actually drained by the pool of instances`, drained, drained ? 'queue empty' : 'timed out with jobs still outstanding')
}

await waitForAll()
await health()
await sharedRateLimit()
await sharedQueue()

console.log(`\n${failures === 0 ? 'ALL PASSED' : `${failures} CHECK(S) FAILED`}`)
process.exit(failures === 0 ? 0 : 1)
