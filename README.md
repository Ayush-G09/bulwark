# Bulwark

A rate limiter and job queue that hold up under real concurrent, distributed load — with a live dashboard showing
requests being throttled and jobs moving through retries in real time.

**Phase 4 of 5**: core correctness, distributed correctness (Redis), a real HTTP service running as several Docker
containers, and now a live dashboard showing every instance's activity in real time, not just whichever one is serving
the page. Next: deploy.

## Run it

```bash
npm install
npm test          # 46 tests
```

## What is here

- **`TokenBucketLimiter`**: a token bucket per key. Tokens refill continuously (computed lazily from elapsed time, no
  background timer), a request is all-or-nothing, and a clock that jumps backward can never refund tokens. Verified
  with a property test against a naive "add continuously, cap at capacity" reference model over 50 randomised trials.
- **`SlidingWindowLog`**: the exact sliding window (every timestamp kept, O(requests in the window) per key). Simple
  and correct by construction; used as ground truth for the counter below.
- **`SlidingWindowCounter`**: the O(1)-per-key approximation, blending the previous fixed window's count into the
  current one so two full-limit bursts either side of a window boundary cannot both get through. A property test
  checks it never allows meaningfully more traffic than the exact log over randomised timing, and a direct test proves
  the blend actually decays through the window rather than just at its edges.
- **`JobQueue`**: bounded concurrency, priority (ties broken by arrival), delayed jobs, exponential backoff with a cap,
  and a dead-letter list. A delayed or retrying job does not hold a concurrency slot while it waits — it is timed by
  its own call to the same injectable `sleep` everything else's timing goes through, so tests control time exactly
  with no real waiting (the whole suite runs in well under a second).

## How it is verified

- Every module has a property test against an independent reference (a naive simulation for the token bucket, the
  exact log for the sliding-window counter) over many randomised trials, not just hand-picked examples.
- Mutation testing: six deliberately broken versions of the trust-critical logic (in the limiters and the queue) were
  each run against the test suite and each one failed a test. One survived on the first pass (a missing check that a
  sliding window's blend actually decays mid-window, not just at the very edges) and a test was added for it.

## Phase 2: correct across many processes (Redis)

**Phase 2 of 5.** The same algorithms, made correct when shared by many processes (or machines) instead of one, using
Redis as the one thing they all actually share.

```bash
npm run infra:up              # a real Redis, on :6390, via Docker
npm test                       # 76 tests (46 in-process + 30 against real Redis)
npm run verify:distributed     # the real proof: several separate OS processes, not just several connections
```

- **`RedisTokenBucketLimiter` / `RedisSlidingWindowLimiter`**: the same algorithms as Phase 1, but the read-decide-write
  step runs as one Lua script (`EVAL`), which Redis executes as a single atomic unit across every client connected to
  it — the actual fix for the race a naive "GET, decide, SET" would have.
- **`RedisQueue`**: a job queue shared by many workers. Claiming a job is one atomic `LMOVE` from a ready list to a
  processing list — Redis's own primitive is the mutual-exclusion guarantee, not a lock built on top of it. A worker
  that crashes mid-job is recovered by `reap()`, which finds jobs claimed past a visibility timeout and requeues them
  through the normal retry/backoff path (or dead-letters them, past their attempt limit) — nothing is lost, and nothing
  needs a human to notice.
- **The real proof** (`verify-distributed.mts`): forks several genuine, separate Node processes — not just separate
  connections in one process — and has them race for a rate limiter's capacity, or drain a shared job queue, or crash
  mid-job. It checks the actual counts: exactly `capacity` requests admitted however many processes raced for it,
  every job claimed by exactly one worker, every crashed worker's jobs recovered. Current result: all four checks pass
  (50/50 admitted exactly on both limiters across 6 real processes; 300/300 jobs claimed exactly once across 8 real
  workers; 20/20 crashed jobs recovered).

## Phase 3: a real service, running as several instances

**Phase 3 of 5.** The distributed limiter and queue from Phase 2, behind a real HTTP API, actually run as several
Docker containers sharing one Redis — not described as capable of it, run that way and checked.

```bash
npm run docker:up               # builds the server image, starts Redis + 3 independent server containers
npm run verify:multi-instance   # the real proof: hits all 3 real containers over HTTP
npm test                        # 90 tests (unchanged from Phase 2, plus 14 for the HTTP surface)
npm run docker:down
```

### The API

- `POST /limit` `{ policy, key, cost? }` → `200` with `{ allowed: true, remaining }`, or `429` with `retryAfterMs`.
  Policies are named in config (`api`: token bucket, `login`: sliding window, by default) — see `POLICIES_JSON`.
- `POST /jobs` `{ type: "sleep" | "flaky" | "fail", ... }` → `202 { id }`. Demo job types only: running arbitrary code
  from a request body would be a remote-code-execution hole, so the set of things a job can do is fixed and small,
  enough to prove the queue really executes work (and retries, and dead-letters it) rather than just accepting it.
- `GET /stats`, `GET /jobs/dead`, `GET /health` (the last also reports which instance answered — see `INSTANCE_ID`).

### What actually changed to make this correct

- **A real, easy-to-hit distributed-systems bug, and its fix.** Claiming a job blocks its Redis connection (`BLMOVE`)
  until one arrives. The first version shared one connection between the embedded worker(s) and the HTTP-facing rate
  limiter checks; a request could land on a connection that was mid-block for up to two seconds, turning a
  millisecond check into one that occasionally took six. Every worker now gets its own dedicated connection
  (`redis.duplicate()`), so a wait on one can never hold up anything else. Caught by timing an actual request, not by
  reading the code — the fix is one line, finding it needed a profiler's instinct.
- **A test-isolation bug that looked like a queue bug.** The job queue's Redis key prefix was hardcoded, so every
  test run (and every debug script) shared the same Redis keys with no cleanup between them — leftover "processing"
  and "dead" entries from earlier runs made later assertions fail for reasons that had nothing to do with the code
  under test. The prefix is now configurable (`QUEUE_PREFIX`), and tests each use their own.
- **`verify-multi-instance.mts`**: waits for three real containers to come up, confirms they really are three separate
  processes (different instance ids), fires 15 concurrent requests at a 5-per-window rate limit spread across all
  three, and drains 60 real jobs enqueued through one instance using the whole pool's embedded workers. All three
  checks pass against the actual Docker stack.

## Phase 4: the live dashboard

**Phase 4 of 5.** A dashboard showing rate limits being throttled and jobs moving through retries as it happens —
across the whole cluster, not just whichever instance happens to be serving the page.

```bash
npm run docker:up
# open http://localhost:3301, :3302 or :3303 — send traffic to any of them, watch it appear on all three
npm test          # 98 tests
```

### How it stays cluster-wide, not per-process

Every instance publishes what it does — a rate-limit check, a job claimed, retried, completed, or dead-lettered — to a
Redis pub/sub channel. Every instance (including the one that published it) is also subscribed, and relays what it
receives to its own connected dashboard viewers over WebSocket. Open the dashboard on instance A and send traffic to
instance B, and A shows it live: proven directly in the browser against the real three-container stack (instance A's
page displayed instance B's rate-limit checks and completed job, having received none of them itself), and covered by
a test that starts two instances, sends a request through only one, and checks the other's dashboard socket receives
it.

Two real bugs, both about **scoping shared state correctly**, the throughline of this whole project:

- **The event channel was a hardcoded name.** Every server instance published to the same fixed Redis channel — fine
  for one deployment, but two unrelated deployments (or, as this project's own test suite found, two unrelated test
  files running in parallel against the same real Redis) would leak into each other's dashboards. It is now derived
  from the same key prefix the job queue already uses, the same scoping principle applied consistently. The bug was
  visibly real (an unrelated instance's events appearing on a dashboard that should never have seen them) but not
  reliably reproducible on demand, since it depended on test-runner parallel timing — so a second, deterministic test
  was added that starts two differently-scoped servers directly and asserts one never sees the other's events. That
  test catches the mutation every time; the original relies on the whole suite running in parallel to catch it at all.
- **A test asserted `remaining: 0` exactly.** With continuous refill, a fraction of a token can trickle back in during
  the few milliseconds between requests — the correct answer is a tiny positive remainder, not exactly zero. Not a bug
  in the limiter; a test that did not account for the same continuous-time reality Phase 1 already designed for.
