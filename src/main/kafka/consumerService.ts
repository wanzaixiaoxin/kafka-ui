import type { Kafka, Consumer as KConsumer } from 'kafkajs'
import { randomUUID } from 'crypto'
import type { ConsumedMessage, ConsumerOptions, FetchMessagesOptions } from '../../shared/types'

export type MsgCallback = (msg: ConsumedMessage) => void

export class ConsumerService {
  /* ================================================================
   *  实时消费者（start / stop）—— 固定 groupId，实例持久化
   *
   *  策略：
   *   - groupId = `live-${topic}`，同一 topic 永远同一组
   *   - stop 只停 fetch loop，不 disconnect；consumer 留在 livePool
   *   - 再次 start 时复用已有 consumer，直接 run
   *   - 首次 GROUP_JOIN ~3s，后续因 group 已存在于 broker，快很多
   * ================================================================ */
  private livePool = new Map<string, {      // key = topic
    consumer: KConsumer
    groupId: string
  }>()
  private liveActive = new Map<string, {    // key = consumerId
    consumer: KConsumer
    partition?: number
    onMsg: MsgCallback
  }>()

  async start(kafka: Kafka, opts: ConsumerOptions, onMsg: MsgCallback): Promise<string> {
    const { topic } = opts
    const groupId = `live-${topic}`
    const consumerId = randomUUID()

    let consumer: KConsumer
    let isNew = false
    const pooled = this.livePool.get(topic)

    if (pooled) {
      /* 复用：先 stop 残留的 fetch loop */
      try { await pooled.consumer.stop() } catch { /**/ }
      consumer = pooled.consumer
      console.log(`[live] 复用 consumer topic=${topic}`)
    } else {
      consumer = kafka.consumer({ groupId })
      await consumer.connect()
      await consumer.subscribe({ topic, fromBeginning: opts.fromBeginning ?? true })
      this.livePool.set(topic, { consumer, groupId })
      isNew = true
      console.log(`[live] 新建 consumer topic=${topic}`)
    }

    this.liveActive.set(consumerId, { consumer, partition: opts.partition, onMsg })

    const tp = opts.partition
    const to = opts.fromOffset ?? (opts.fromBeginning ? '0' : undefined)

    if (tp !== undefined && to !== undefined) {
      /* 先清除旧 handler 防止累积（复用 consumer 时每次 start 都会注册新的） */
      try { (consumer as any).off?.(consumer.events.GROUP_JOIN) } catch { /**/ }
      consumer.on(consumer.events.GROUP_JOIN, async () => {
        console.log(`[live] GROUP_JOIN + seek partition=${tp} offset=${to}`)
        await consumer.seek({ topic, partition: tp, offset: to })
      })
    }

    consumer.run({
      autoCommit: false,
      eachMessage: async ({ topic: _t, partition, message }) => {
        if (tp !== undefined && partition !== tp) return
        const headers: Record<string, string> = {}
        if (message.headers)
          for (const [k, v] of Object.entries(message.headers))
            if (v) headers[k] = typeof v === 'string' ? v : v.toString('utf-8')
        onMsg({
          topic: _t, partition, offset: message.offset,
          key: message.key?.toString('utf-8'),
          value: message.value?.toString('utf-8') ?? '',
          headers: Object.keys(headers).length > 0 ? headers : undefined,
          timestamp: Number(message.timestamp)
        })
      }
    }).catch((err) => {
      console.error('[live] run error:', err.message)
    })

    console.log(`[live] started consumerId=${consumerId.slice(0, 8)} topic=${topic} isNew=${isNew}`)
    return consumerId
  }

  async stop(consumerId: string): Promise<void> {
    const a = this.liveActive.get(consumerId)
    if (!a) return
    try { await a.consumer.stop() } catch { /**/ }
    this.liveActive.delete(consumerId)
    console.log(`[live] stopped consumerId=${consumerId.slice(0, 8)}`)
  }

  async stopAll(): Promise<void> {
    const activeEntries = Array.from(this.liveActive.entries())
    for (const [, a] of activeEntries) {
      try { await a.consumer.stop() } catch { /**/ }
    }
    this.liveActive.clear()
    const poolEntries = Array.from(this.livePool.values())
    for (const p of poolEntries) {
      try { await p.consumer.disconnect() } catch { /**/ }
    }
    this.livePool.clear()
  }

  /* ================================================================
   *  消息浏览：每次独立创建 consumer，用完即毁
   *  GROUP_JOIN 的 ~3s 开销无法避免，但至少稳定、不出错
   * ================================================================ */

  async fetchMessages(kafka: Kafka, opts: FetchMessagesOptions): Promise<ConsumedMessage[]> {
    const { topic, partition, offset, fromBeginning = false, limit = 50 } = opts
    const t0 = Date.now()

    /* ---- 1. admin 水位 ---- */
    const admin = kafka.admin()
    await admin.connect()
    let oo: Array<{ partition: number; high: string; low: string }>
    try {
      oo = (await admin.fetchTopicOffsets(topic)).map((o) => ({
        partition: Number(o.partition), high: o.high, low: o.low
      }))
    } finally { await admin.disconnect().catch(() => {}) }

    /* ---- 2. 计算 ---- */
    const seeks: Array<{ partition: number; offset: string }> = []
    let expected = 0
    for (const o of oo) {
      if (partition !== undefined && o.partition !== partition) continue
      const hi = BigInt(o.high), lo = BigInt(o.low)
      let to: bigint
      if (offset !== undefined)      to = BigInt(offset)
      else if (!fromBeginning)       to = hi - BigInt(limit)
      else                           to = lo
      if (to < lo) to = lo
      const n = hi - to
      if (n > BigInt(0)) { expected += Number(n); seeks.push({ partition: o.partition, offset: to.toString() }) }
    }
    const max = Math.min(limit, expected)
    console.log(`[fetch] A+B expected=${expected} max=${max} t+${Date.now()-t0}ms`)
    if (max <= 0) return []

    /* ---- 3. 创建消费者 ---- */
    const consumer = kafka.consumer({
      groupId: `fc-${randomUUID()}`,
      maxWaitTimeInMs: 50,
    })
    await consumer.connect()
    await consumer.subscribe({ topic, fromBeginning: true })
    console.log(`[fetch] C1 connect+subscribe t+${Date.now()-t0}ms`)

    const msgs: ConsumedMessage[] = []
    let done = false
    let joinDone = false

    consumer.on(consumer.events.GROUP_JOIN, async () => {
      if (joinDone) return
      joinDone = true
      for (const s of seeks)
        try { await consumer.seek({ topic, partition: s.partition, offset: s.offset }) } catch { /**/ }
      console.log(`[fetch] C2 GROUP_JOIN + seek 完成 t+${Date.now()-t0}ms`)
    })

    consumer.run({
      autoCommit: false,
      eachMessage: async ({ topic: t, partition: p, message }) => {
        if (partition !== undefined && p !== partition) return
        if (done) return
        const headers: Record<string, string> = {}
        if (message.headers)
          for (const [k, v] of Object.entries(message.headers))
            if (v) headers[k] = typeof v === 'string' ? v : v.toString('utf-8')
        msgs.push({
          topic: t, partition: p, offset: message.offset,
          key: message.key?.toString('utf-8'),
          value: message.value?.toString('utf-8') ?? '',
          headers: Object.keys(headers).length > 0 ? headers : undefined,
          timestamp: Number(message.timestamp)
        })
        if (msgs.length >= max) { console.log(`[fetch] D 收够 ${msgs.length} 条 t+${Date.now()-t0}ms`); done = true }
      }
    })

    /* 等消息收齐或超时，使用 Promise 代替轮询 */
    await new Promise<void>((resolve) => {
      const timer = setTimeout(() => resolve(), 15000)
      const check = (): void => {
        if (done) {
          clearTimeout(timer)
          resolve()
        } else {
          /* 使用 setImmediate 让出 CPU，比 setInterval 更高效 */
          setTimeout(check, 50)
        }
      }
      setTimeout(check, 50)
    })

    try { await consumer.stop() } catch { /**/ }
    try { await consumer.disconnect() } catch { /**/ }

    console.log(`[fetch] 完成 ← ${msgs.length}/${max} 条, 耗时 ${Date.now()-t0}ms`)
    return msgs
  }
}

export const consumerSvc = new ConsumerService()
