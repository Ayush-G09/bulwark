// A real, separate OS process used by verify-distributed.mts. It does one job, told by its parent over IPC, against
// the real Redis — this file is never imported directly, only forked.
import Redis from 'ioredis'
import { RedisTokenBucketLimiter } from '../src/redisTokenBucket'
import { RedisSlidingWindowLimiter } from '../src/redisSlidingWindow'
import { RedisQueue } from '../src/redisQueue'
import { REDIS_URL } from '../test/redis'

process.on('message', async (msg: any) => {
  const redis = new Redis(REDIS_URL)
  const { mode, args } = msg
  let result: unknown

  if (mode === 'token-bucket') {
    const l = new RedisTokenBucketLimiter(redis, { capacity: args.capacity, refillPerSec: args.refillPerSec, keyPrefix: args.prefix })
    let admitted = 0
    for (let i = 0; i < args.requests; i++) if ((await l.check('shared')).allowed) admitted++
    result = admitted
  } else if (mode === 'sliding-window') {
    const l = new RedisSlidingWindowLimiter(redis, { limit: args.limit, windowMs: args.windowMs, keyPrefix: args.prefix })
    let admitted = 0
    for (let i = 0; i < args.requests; i++) if ((await l.check('shared')).allowed) admitted++
    result = admitted
  } else if (mode === 'drain-queue') {
    const q = new RedisQueue<number>(redis, { keyPrefix: args.prefix })
    const claimed: number[] = []
    for (;;) {
      const job = await q.claim(1)
      if (!job) break
      claimed.push(job.data)
      await q.complete(job.id)
    }
    result = claimed
  } else if (mode === 'claim-then-die') {
    const q = new RedisQueue<number>(redis, { keyPrefix: args.prefix })
    let claimed = 0
    while (claimed < args.count) { const job = await q.claim(1); if (job) claimed++ } // deliberately never complete() or fail()
    result = claimed
  }

  process.send?.({ type: 'result', value: result })
  await redis.quit()
})
