import { createServer as createHttp } from 'node:http'
import type { IncomingMessage, Server } from 'node:http'
import { readFile } from 'node:fs/promises'
import { fileURLToPath } from 'node:url'
import { WebSocketServer } from 'ws'
import type { WebSocket } from 'ws'
import type { Redis } from 'ioredis'
import { RedisQueue, RedisSlidingWindowLimiter, RedisTokenBucketLimiter, runWorker } from '@bulwark/distributed'
import type { Config, PolicyConfig } from './config'
import { Dashboard } from './dashboard'
import type { DashboardEvent } from './dashboard'
import { runJob } from './jobs'
import type { JobPayload } from './jobs'

const DASHBOARD_HTML = fileURLToPath(new URL('../public/dashboard.html', import.meta.url))

const MAX_BODY = 1024 * 1024
const NAME = /^[A-Za-z0-9_.:-]{1,200}$/

class TooLarge extends Error {}

function readBody(req: IncomingMessage): Promise<Buffer> {
  return new Promise((resolve, reject) => {
    const parts: Buffer[] = []
    let size = 0
    let tooLarge = false
    req.on('data', (c: Buffer) => {
      if (tooLarge) return
      size += c.length
      if (size > MAX_BODY) { tooLarge = true; reject(new TooLarge()); return } // let the response still be written; do not destroy the socket here
      parts.push(c)
    })
    req.on('end', () => { if (!tooLarge) resolve(Buffer.concat(parts)) })
    req.on('error', reject)
  })
}

interface Limiter { check(key: string, cost?: number): Promise<{ allowed: boolean; remaining: number; retryAfterMs?: number }> }

function buildLimiter(redis: Redis, name: string, policy: PolicyConfig): Limiter {
  const keyPrefix = `bulwark:policy:${name}:`
  if (policy.kind === 'token-bucket') return new RedisTokenBucketLimiter(redis, { capacity: policy.capacity, refillPerSec: policy.refillPerSec, keyPrefix })
  const sw = new RedisSlidingWindowLimiter(redis, { limit: policy.limit, windowMs: policy.windowMs, keyPrefix })
  return { check: (key) => sw.check(key) } // a sliding window has no notion of "cost"; every request counts as one
}

export interface BulwarkServer {
  http: Server
  queue: RedisQueue<JobPayload>
  close(): Promise<void>
  listen(port: number): Promise<number>
}

/**
 * The HTTP front door: rate-limit checks and job submission, backed by the distributed primitives from Phase 2. Every
 * instance of this server that points at the same Redis shares exactly the same limiter and queue state — that is the
 * entire point of Phase 3: it is now safe to run several of these at once (see docker-compose.yml and
 * verify-multi-instance.mts), because correctness lives in Redis, not in any one process's memory.
 */
export function createServer(config: Config, redis: Redis, opts: { log?: (msg: string) => void; dashboardSubFactory?: () => Redis } = {}): BulwarkServer {
  const log = opts.log ?? (() => {})
  const limiters = new Map(Object.entries(config.policies).map(([name, p]) => [name, buildLimiter(redis, name, p)] as const))
  const queue = new RedisQueue<JobPayload>(redis, { keyPrefix: config.queuePrefix, ...config.queue })
  const dashboard = new Dashboard(redis, opts.dashboardSubFactory ?? (() => redis.duplicate()), `${config.queuePrefix}:events`)

  // Claiming a job blocks the Redis connection it runs on (BLMOVE) until one arrives or the poll times out. Sharing
  // that connection with anything else — the HTTP-facing rate limiter checks, or another worker's own claim — would
  // queue those behind however long the block lasts, turning a millisecond request into a multi-second one. Every
  // worker gets its own dedicated connection so its wait can never hold up anything else.
  const workerCtl = new AbortController()
  const workerConnections = Array.from({ length: config.queue.concurrency }, () => redis.duplicate())
  const workers = workerConnections.map((conn) => {
    const workerQueue = new RedisQueue<JobPayload>(conn, { keyPrefix: config.queuePrefix, ...config.queue })
    return runWorker(workerQueue, (payload, job) => runJob(payload, job.attempts), workerCtl.signal, {
      onEvent: (e) => {
        log(`[${config.instanceId}] job ${e.type} ${e.id}${e.error ? `: ${e.error}` : ''}`)
        dashboard.publish({ type: 'job', instanceId: config.instanceId, event: e.type, id: e.id, attempt: e.attempt, error: e.error, at: Date.now() })
      },
    })
  })

  const reapTimer = setInterval(() => {
    void queue.promoteDue().catch((e) => log(`promoteDue error: ${(e as Error).message}`))
    void queue.reap().catch((e) => log(`reap error: ${(e as Error).message}`))
  }, config.reapIntervalMs)

  const http = createHttp(async (req, res) => {
    const send = (status: number, body: unknown) => {
      res.statusCode = status
      res.setHeader('content-type', 'application/json')
      res.setHeader('x-instance-id', config.instanceId)
      res.end(JSON.stringify(body))
    }
    try {
      const url = new URL(req.url ?? '/', 'http://localhost')

      if (req.method === 'GET' && (url.pathname === '/' || url.pathname === '/dashboard')) {
        res.setHeader('content-type', 'text/html; charset=utf-8')
        try { return res.end(await readFile(DASHBOARD_HTML)) } catch { res.statusCode = 500; return res.end('dashboard page missing') }
      }

      if (req.method === 'GET' && url.pathname === '/health') {
        return send(200, { ok: true, instanceId: config.instanceId, uptimeSec: Math.round(process.uptime()) })
      }

      if (req.method === 'GET' && url.pathname === '/stats') {
        return send(200, { instanceId: config.instanceId, policies: Object.keys(config.policies), queue: await queue.stats() })
      }

      if (req.method === 'GET' && url.pathname === '/jobs/dead') {
        const limit = Math.min(500, Number(url.searchParams.get('limit')) || 100)
        return send(200, { dead: await queue.deadLetters(limit) })
      }

      if (req.method === 'POST' && url.pathname === '/limit') {
        let body: any
        try { body = JSON.parse((await readBody(req)).toString('utf8') || '{}') } catch (e) { if (e instanceof TooLarge) throw e; return send(400, { error: 'bad json' }) }
        const policy = String(body.policy ?? '')
        const key = String(body.key ?? '')
        if (!limiters.has(policy)) return send(404, { error: `unknown policy "${policy}"`, policies: [...limiters.keys()] })
        if (!NAME.test(key)) return send(400, { error: 'key must be 1-200 characters of letters, digits, . _ - :' })
        const cost = body.cost === undefined ? 1 : Number(body.cost)
        if (!Number.isFinite(cost) || cost <= 0) return send(400, { error: 'cost must be a positive number' })
        const result = await (limiters.get(policy) as Limiter).check(key, cost)
        dashboard.publish({ type: 'limit', instanceId: config.instanceId, policy, key, allowed: result.allowed, remaining: result.remaining, at: Date.now() })
        return send(result.allowed ? 200 : 429, { policy, key, ...result })
      }

      if (req.method === 'POST' && url.pathname === '/jobs') {
        let body: any
        try { body = JSON.parse((await readBody(req)).toString('utf8') || '{}') } catch (e) { if (e instanceof TooLarge) throw e; return send(400, { error: 'bad json' }) }
        if (!['sleep', 'flaky', 'fail'].includes(body.type)) return send(400, { error: 'type must be one of: sleep, flaky, fail' })
        const id = await queue.enqueue(body as JobPayload, { maxAttempts: body.maxAttempts })
        return send(202, { id })
      }

      return send(404, { error: 'not found' })
    } catch (e) {
      if (e instanceof TooLarge) return send(413, { error: 'body too large' })
      log(`request error: ${(e as Error).message}`)
      if (!res.headersSent) send(500, { error: 'internal error' })
    }
  })

  // ---- live dashboard: WebSocket broadcast, fed by the Redis pub/sub relay above ----
  const wss = new WebSocketServer({ noServer: true, maxPayload: 4096 })
  const sockets = new Set<WebSocket>()
  const sendWs = (ws: WebSocket, e: DashboardEvent) => { try { ws.send(JSON.stringify(e)) } catch { /* client is gone */ } }
  const offDashboardEvent = dashboard.onEvent((e) => { for (const ws of sockets) sendWs(ws, e) })

  http.on('upgrade', (req: IncomingMessage, socket, head) => {
    const url = new URL(req.url ?? '/', 'http://localhost')
    if (url.pathname !== '/dashboard/live') { socket.write('HTTP/1.1 404 Not Found\r\nConnection: close\r\n\r\n'); socket.destroy(); return }
    wss.handleUpgrade(req, socket, head, (ws) => {
      sockets.add(ws)
      ws.on('close', () => sockets.delete(ws))
      ws.on('error', () => sockets.delete(ws))
      ws.on('message', () => { /* the dashboard only listens; it has nothing to send us */ })
    })
  })

  return {
    http, queue,
    // 0.0.0.0, not 'localhost': inside a container, 'localhost' would only accept connections from within the
    // container itself, and the port mapping to the host would never reach it.
    listen: (port) => new Promise((resolve) => http.listen(port, '0.0.0.0', () => resolve((http.address() as { port: number }).port))),
    close: async () => {
      clearInterval(reapTimer)
      workerCtl.abort()
      offDashboardEvent()
      for (const ws of sockets) ws.terminate()
      await dashboard.close()
      await new Promise<void>((r) => { http.close(() => r()); http.closeAllConnections?.() })
      await Promise.race([Promise.all(workers), new Promise((r) => setTimeout(r, 5000))])
      for (const conn of workerConnections) conn.disconnect()
    },
  }
}
