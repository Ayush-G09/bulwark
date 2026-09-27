import type { Redis } from 'ioredis'

export type DashboardEvent =
  | { type: 'limit'; instanceId: string; policy: string; key: string; allowed: boolean; remaining: number; at: number }
  | { type: 'job'; instanceId: string; event: 'claimed' | 'completed' | 'failed' | 'retrying' | 'dead'; id: string; jobType?: string; attempt: number; error?: string; at: number }

/**
 * A live event bus for the dashboard, shared across every server instance of one deployment via Redis pub/sub — not
 * just this process's own activity. `publish()` is called locally (a rate-limit check happened here, a job finished
 * here); every instance subscribed to the same channel receives it and re-broadcasts it to its own connected dashboard
 * viewers. The result: open the dashboard on any one of several instances and see what all of them are doing, which is
 * the actual point of a dashboard for a distributed system — one instance's view of only its own traffic would be
 * misleading.
 *
 * The channel name is not a fixed constant: it is scoped per deployment (in practice, derived from the same key
 * prefix the job queue already uses), the same way the queue and limiter keys are. Without that, two unrelated
 * Bulwark deployments sharing one Redis — or, as this project's own tests found, two unrelated test files running in
 * parallel against the same real Redis — would each see the other's traffic on their dashboard.
 *
 * Subscribing puts a Redis connection into a special mode where it can only (P)SUBSCRIBE/UNSUBSCRIBE — no ordinary
 * command can share it, the same kind of connection-isolation rule Phase 3 already had to learn for blocking commands.
 * So this takes its own dedicated connection, never one also used for anything else.
 */
export class Dashboard {
  private readonly listeners = new Set<(e: DashboardEvent) => void>()
  private readonly sub: Redis
  private readonly channel: string

  constructor(private readonly pub: Redis, subConnectionFactory: () => Redis, channel = 'bulwark:events') {
    this.channel = channel
    this.sub = subConnectionFactory()
    this.sub.subscribe(this.channel).catch(() => { /* reconnection is handled by ioredis; a dropped subscribe retries automatically */ })
    this.sub.on('message', (_channel: string, raw: string) => {
      let e: DashboardEvent
      try { e = JSON.parse(raw) } catch { return } // a malformed message from a future/older version of this service is ignored, not fatal
      for (const fn of this.listeners) fn(e)
    })
  }

  onEvent(fn: (e: DashboardEvent) => void): () => void {
    this.listeners.add(fn)
    return () => this.listeners.delete(fn)
  }

  /** Tells every subscribed instance (including this one, when its own message arrives back) about something that
   * just happened here. Never throws: a dashboard that is briefly unreachable must not affect real traffic. */
  publish(e: DashboardEvent) {
    this.pub.publish(this.channel, JSON.stringify(e)).catch(() => { /* the dashboard is a nicety; losing one event to it is not a real failure */ })
  }

  async close() {
    this.listeners.clear()
    await this.sub.unsubscribe(this.channel).catch(() => {})
    this.sub.disconnect()
  }
}
