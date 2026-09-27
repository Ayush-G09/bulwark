import Redis from 'ioredis'

export const REDIS_URL = process.env.REDIS_URL ?? 'redis://127.0.0.1:6390'

let cached: boolean | undefined
/** Whether a real Redis is reachable right now, so tests can skip cleanly (not fail) when infra:up has not been run. */
export async function redisAvailable(): Promise<boolean> {
  if (cached !== undefined) return cached
  const r = new Redis(REDIS_URL, { lazyConnect: true, retryStrategy: () => null, connectTimeout: 800 })
  try { await r.connect(); await r.ping(); cached = true } catch { cached = false } finally { r.disconnect() }
  return cached
}

export function client(): Redis {
  return new Redis(REDIS_URL)
}
