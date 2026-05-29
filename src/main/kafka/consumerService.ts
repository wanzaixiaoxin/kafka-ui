import type { Kafka, Consumer as KConsumer, Admin } from 'kafkajs'
import { randomUUID } from 'crypto'
import type { ConsumedMessage, ConsumerOptions, FetchMessagesOptions } from '../../shared/types'

export type MsgCallback = (msg: ConsumedMessage) => void

export class ConsumerService {
  /* ================================================================
   *  实时消费者 —— 常驻 consumer + 会话控制
   *
   *  策略（与消息浏览相同架构）：
   *   - 按 topic 维护常驻 consumer，consumer.run() 只启动一次
   *   - start：设置会话（onMsg 回调 + 分区过滤），必要时 seek
   *   - stop：清空会话，consumer 继续留在 group 中运行（消息丢弃）
   *   - 再次 start：无需 GROUP_JOIN，直接更新回调 + seek（~瞬间）
   *
   *  消息流转：
   *   eachMessage → 查 liveSessions（可能有多个同时活跃的 session）
   *               → 按 partition 过滤 → 调用对应 onMsg
   * ================================================================ */

  /** 按 topic 缓存的常驻 consumer */
  private liveConsumers = new Map<string, {
    consumer: KConsumer
    groupId: string
  }>()

  /** 活跃的消费会话（一个 topic 可以同时有多个 session） */
  private liveSessions = new Map<string, {    // key = consumerId
    topic: string
    partition?: number
    onMsg: MsgCallback
  }>()

  /** 辅助方法：从 KafkaMessage 提取 ConsumedMessage */
  private static toConsumedMessage(t: string, p: number, message: { offset: string; key?: Buffer | null; value?: Buffer | null; headers?: Record<string, Buffer | string | (Buffer | string)[] | undefined> | null; timestamp?: string | number }): ConsumedMessage {
    const headers: Record<string, string> = {}
    if (message.headers)
      for (const [k, v] of Object.entries(message.headers))
        if (v) headers[k] = Array.isArray(v) ? v.map(b => typeof b === 'string' ? b : b.toString('utf-8')).join(',') : typeof v === 'string' ? v : v.toString('utf-8')
    return {
      topic: t, partition: p, offset: message.offset,
      key: message.key?.toString('utf-8'),
      value: message.value?.toString('utf-8') ?? '',
      headers: Object.keys(headers).length > 0 ? headers : undefined,
      timestamp: Number(message.timestamp)
    }
  }

  /** 确保常驻 consumer 存在并运行 */
  private async ensureLiveConsumer(kafka: Kafka, topic: string, fromBeginning: boolean): Promise<{
    consumer: KConsumer
    isFirst: boolean
  }> {
    const cached = this.liveConsumers.get(topic)
    if (cached) {
      return { consumer: cached.consumer, isFirst: false }
    }

    const groupId = `live-${topic}`
    const consumer = kafka.consumer({ groupId })

    await consumer.connect()
    await consumer.subscribe({ topic, fromBeginning: true })
    this.liveConsumers.set(topic, { consumer, groupId })

    /* 启动常驻 fetch loop —— 只启动一次，永不 stop */
    consumer.run({
      autoCommit: false,
      eachMessage: async ({ topic: t, partition, message }) => {
        const msg = ConsumerService.toConsumedMessage(t, partition, message)
        /* 遍历所有活跃会话，投递消息 */
        this.liveSessions.forEach((session) => {
          if (session.topic !== t) return
          if (session.partition !== undefined && partition !== session.partition) return
          session.onMsg(msg)
        })
      }
    }).catch(() => { /* disconnect 时 reject，忽略 */ })

    console.log(`[live] 新建常驻 consumer topic=${topic}`)
    return { consumer, isFirst: true }
  }

  async start(kafka: Kafka, opts: ConsumerOptions, onMsg: MsgCallback): Promise<string> {
    const { topic } = opts
    const consumerId = randomUUID()
    const t0 = Date.now()

    const { consumer, isFirst } = await this.ensureLiveConsumer(kafka, topic, opts.fromBeginning ?? true)

    /* 注册会话 */
    this.liveSessions.set(consumerId, {
      topic,
      partition: opts.partition,
      onMsg
    })

    /* 首次创建需要等 GROUP_JOIN 再 seek；复用模式直接 seek */
    const seekTarget = opts.fromOffset ?? (opts.fromBeginning ? '0' : undefined)

    if (opts.partition !== undefined && seekTarget !== undefined) {
      if (isFirst) {
        await new Promise<void>((resolve) => {
          consumer.on(consumer.events.GROUP_JOIN, async () => {
            try { await consumer.seek({ topic, partition: opts.partition!, offset: seekTarget }) } catch { /**/ }
            resolve()
          })
        })
      } else {
        try { await consumer.seek({ topic, partition: opts.partition, offset: seekTarget }) } catch { /**/ }
      }
    }

    console.log(`[live] started id=${consumerId.slice(0, 8)} topic=${topic} isNew=${isFirst} t+${Date.now() - t0}ms`)
    return consumerId
  }

  async stop(consumerId: string): Promise<void> {
    const existed = this.liveSessions.delete(consumerId)
    if (existed) {
      console.log(`[live] stopped id=${consumerId.slice(0, 8)} (session removed, consumer stays)`)
    }
  }

  async stopAll(): Promise<void> {
    /* 清空所有会话 */
    this.liveSessions.clear()
    /* 断开所有常驻 live consumer */
    const liveEntries = Array.from(this.liveConsumers.values())
    for (const entry of liveEntries) {
      try { await entry.consumer.disconnect() } catch { /**/ }
    }
    this.liveConsumers.clear()
    /* 清理 fetch consumer */
    const fetchEntries = Array.from(this.fetchState.values())
    for (const f of fetchEntries) {
      try { await f.consumer.disconnect() } catch { /**/ }
    }
    this.fetchState.clear()
  }

  /* ================================================================
   *  消息浏览：常驻 consumer + seek，避免反复 GROUP_JOIN
   *
   *  策略：
   *   - 每个连接维护一个专用 fetch consumer，consumer.run() 只启动一次
   *   - 每次查询：seek 到目标 offset → 等待消息收齐 → 标记 done
   *   - consumer 始终留在 group中，不 stop / 不 disconnect
   *   - 首次 GROUP_JOIN ~3s，后续查询只需 seek + fetch（~50-200ms）
   * ================================================================ */

  private fetchState = new Map<string, {
    consumer: KConsumer
    topic: string
    running: boolean
    session: number      /* 递增会话 ID，用于标识当前查询 */
    collected: ConsumedMessage[]
    resolve?: () => void /* 当前等待的 Promise resolve */
    max: number
    partition?: number
  }>()

  /** 获取或创建 fetch 专用的常驻 consumer */
  private async ensureFetchConsumer(connId: string, kafka: Kafka, topic: string, partition?: number): Promise<{
    state: NonNullable<ConsumerService['fetchState'] extends Map<string, infer V> ? V : never>
    isFirst: boolean
  }> {
    const existing = this.fetchState.get(connId)
    if (existing && existing.topic === topic) {
      return { state: existing, isFirst: false }
    }

    /* 旧 consumer topic 不同或不存在，需要清理/创建 */
    if (existing) {
      try { await existing.consumer.disconnect() } catch { /**/ }
    }

    const consumer = kafka.consumer({
      groupId: `fetch-${connId.slice(0, 8)}`,
      maxWaitTimeInMs: 50,
    })

    const state: NonNullable<ConsumerService['fetchState'] extends Map<string, infer V> ? V : never> = {
      consumer,
      topic,
      running: false,
      session: 0,
      collected: [],
      max: 0,
      partition
    }

    await consumer.connect()
    await consumer.subscribe({ topic, fromBeginning: true })

    /* 启动常驻 fetch loop —— 只启动一次 */
    state.running = true
    consumer.run({
      autoCommit: false,
      eachMessage: async ({ topic: t, partition: p, message }) => {
        const s = this.fetchState.get(connId)
        if (!s || s.session === 0) return            /* 无活跃会话，丢弃 */
        if (s.partition !== undefined && p !== s.partition) return

        const headers: Record<string, string> = {}
        if (message.headers)
          for (const [k, v] of Object.entries(message.headers))
            if (v) headers[k] = typeof v === 'string' ? v : v.toString('utf-8')

        s.collected.push({
          topic: t, partition: p, offset: message.offset,
          key: message.key?.toString('utf-8'),
          value: message.value?.toString('utf-8') ?? '',
          headers: Object.keys(headers).length > 0 ? headers : undefined,
          timestamp: Number(message.timestamp)
        })

        if (s.collected.length >= s.max) {
          const resolve = s.resolve
          s.session = 0        /* 标记会话结束，后续消息直接丢弃 */
          s.resolve = undefined
          resolve?.()           /* 唤醒等待的 fetchMessages */
        }
      }
    }).catch(() => { /* disconnect 时 reject，忽略 */ })

    this.fetchState.set(connId, state)
    return { state, isFirst: true }
  }

  async fetchMessages(kafka: Kafka, opts: FetchMessagesOptions, poolAdmin?: Admin, connId?: string): Promise<ConsumedMessage[]> {
    const { topic, partition, offset, fromBeginning = false, limit = 50 } = opts
    const t0 = Date.now()

    /* ---- 1. admin 水位 ---- */
    let oo: Array<{ partition: number; high: string; low: string }>
    const needCleanup = !poolAdmin
    const admin = poolAdmin ?? kafka.admin()
    if (!poolAdmin) await admin.connect()
    try {
      oo = (await admin.fetchTopicOffsets(topic)).map((o) => ({
        partition: Number(o.partition), high: o.high, low: o.low
      }))
    } finally { if (needCleanup) await admin.disconnect().catch(() => {}) }

    /* ---- 2. 计算 seek 目标 ---- */
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
    console.log(`[fetch] plan expected=${expected} max=${max} t+${Date.now() - t0}ms`)
    if (max <= 0) return []

    /* ---- 3. 获取常驻 consumer ---- */
    const cid = connId ?? `tmp-${randomUUID()}`
    const { state, isFirst } = await this.ensureFetchConsumer(cid, kafka, topic, partition)

    /* ---- 4. 首次启动需要等 GROUP_JOIN，后续直接 seek ---- */
    if (isFirst) {
      await new Promise<void>((resolve) => {
        const evt = state.consumer.events.GROUP_JOIN
        const onJoin = async (): Promise<void> => {
          for (const s of seeks)
            try { await state.consumer.seek({ topic, partition: s.partition, offset: s.offset }) } catch { /**/ }
          resolve()
        }
        state.consumer.on(evt, onJoin)
      })
      console.log(`[fetch] GROUP_JOIN + seek 完成 t+${Date.now() - t0}ms`)
    } else {
      /* 复用：直接 seek */
      for (const s of seeks)
        try { await state.consumer.seek({ topic, partition: s.partition, offset: s.offset }) } catch { /**/ }
      console.log(`[fetch] seek 完成 (复用) t+${Date.now() - t0}ms`)
    }

    /* ---- 5. 开始新会话，等待消息收齐 ---- */
    state.collected = []
    state.max = max
    state.partition = partition
    const sessionId = ++state.session

    const result = await Promise.race([
      /* 等待 eachMessage 回调收齐后 resolve */
      new Promise<ConsumedMessage[]>((resolve) => {
        state.resolve = () => resolve([...state.collected])
      }),
      /* 超时保底 */
      new Promise<ConsumedMessage[]>((resolve) =>
        setTimeout(() => {
          if (state.session === sessionId) {
            state.session = 0
            state.resolve = undefined
          }
          resolve([...state.collected])
        }, 10000)
      )
    ])

    console.log(`[fetch] 完成 ← ${result.length}/${max} 条, 耗时 ${Date.now() - t0}ms`)

    /* 非复用模式（无 connId）用完断开 */
    if (!connId) {
      try { await state.consumer.disconnect() } catch { /**/ }
      this.fetchState.delete(cid)
    }

    return result
  }
}

export const consumerSvc = new ConsumerService()
