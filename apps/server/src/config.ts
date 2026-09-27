export type PolicyConfig =
  | { kind: 'token-bucket'; capacity: number; refillPerSec: number }
  | { kind: 'sliding-window'; limit: number; windowMs: number }

export interface Config {
  port: number
  redisUrl: string
  /** named rate-limit policies, e.g. { api: { kind: 'token-bucket', capacity: 100, refillPerSec: 10 } } */
  policies: Record<string, PolicyConfig>
  queue: { concurrency: number; maxAttempts: number; backoffMs: number; maxBackoffMs: number; visibilityTimeoutMs: number }
  /** the Redis key prefix the job queue uses; several instances of the real service should share the default, but
   * tests (or several unrelated services on one Redis) need their own so they do not see each other's jobs */
  queuePrefix: string
  /** how often this instance sweeps its own state: promotes due retries and reaps overdue claims (ms) */
  reapIntervalMs: number
  /** identifies which instance answered a request, for proving several are really running (defaults to hostname:pid) */
  instanceId: string
}

const DEFAULT_POLICIES: Record<string, PolicyConfig> = {
  api: { kind: 'token-bucket', capacity: 100, refillPerSec: 20 },
  login: { kind: 'sliding-window', limit: 5, windowMs: 60_000 },
}

export function loadConfig(env: NodeJS.ProcessEnv = process.env): Config {
  const int = (k: string, d: number) => {
    const v = env[k]
    if (v === undefined || v === '') return d
    const n = Number(v)
    if (!Number.isInteger(n) || n < 0) throw new Error(`${k} must be a whole number`)
    return n
  }
  let policies = DEFAULT_POLICIES
  if (env.POLICIES_JSON) {
    try { policies = JSON.parse(env.POLICIES_JSON) } catch { throw new Error('POLICIES_JSON is not valid JSON') }
  }
  return {
    port: int('PORT', 3300),
    redisUrl: env.REDIS_URL || 'redis://127.0.0.1:6390',
    policies,
    queue: {
      concurrency: Math.max(1, int('QUEUE_CONCURRENCY', 4)),
      maxAttempts: Math.max(1, int('QUEUE_MAX_ATTEMPTS', 5)),
      backoffMs: int('QUEUE_BACKOFF_MS', 500),
      maxBackoffMs: int('QUEUE_MAX_BACKOFF_MS', 30_000),
      visibilityTimeoutMs: int('QUEUE_VISIBILITY_TIMEOUT_MS', 15_000),
    },
    reapIntervalMs: int('REAP_INTERVAL_MS', 2000),
    queuePrefix: env.QUEUE_PREFIX || 'bulwark:jobs',
    instanceId: env.INSTANCE_ID || `${env.HOSTNAME || 'local'}:${process.pid}`,
  }
}
