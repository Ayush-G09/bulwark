import { existsSync } from 'node:fs'
import Redis from 'ioredis'
import { loadConfig } from './config'
import { createServer } from './server'

if (existsSync('.env')) process.loadEnvFile('.env')

let config
try {
  config = loadConfig()
} catch (e) {
  console.error(`Cannot start: ${(e as Error).message}`)
  process.exit(2)
}

const redis = new Redis(config.redisUrl)
redis.on('error', (e) => console.error(`redis error: ${e.message}`))

const server = createServer(config, redis, { log: (m) => console.log(`${new Date().toISOString()} ${m}`) })
const port = await server.listen(config.port)
console.log(`Bulwark [${config.instanceId}] listening on :${port}, policies: ${Object.keys(config.policies).join(', ')}`)

for (const sig of ['SIGINT', 'SIGTERM'] as const) {
  process.on(sig, () => { console.log(`${sig}: shutting down`); void server.close().then(() => { redis.disconnect(); process.exit(0) }) })
}
