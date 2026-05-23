import type { Kafka, Consumer as KConsumer } from 'kafkajs'
import { randomUUID } from 'crypto'
import type { ConsumedMessage, ConsumerOptions, FetchMessagesOptions } from '../../renderer/src/types/kafka'

/** 消息回调类型 */
export type MsgCallback = (msg: ConsumedMessage) => void

/** 活跃消费者记录 */
interface ActiveConsumer {
  consumer: KConsumer
  consumerId: string
}

/* ------------------------------------------------------------------ */
/*  查询请求上下文                                                        */
/* ------------------------------------------------------------------ */
interface FetchRequest {
  targetPartition: number | undefined
  messages: ConsumedMessage[]
  maxToCollect: number
  resolve: (msgs: ConsumedMessage[]) => void
  timer: ReturnType<typeof setTimeout> | null
}

/** 消费者服务 */
export class ConsumerService {
  /* ================================================================
   *  实时消费者（start / stop）
   * ================================================================ */
  private consumers: Map<string, ActiveConsumer> = new Map()

  async start(kafka: Kafka, opts: ConsumerOptions, onMsg: MsgCallback): Promise<string> {
    const consumerId = randomUUID()
    const consumer = kafka.consumer({ groupId: `kafka-client-consumer-${consumerId}` })

    await consumer.connect()
    await consumer.subscribe({ topic: opts.topic, fromBeginning: opts.fromBeginning ?? false })

    this.consumers.set(consumerId, { consumer, consumerId })

    const targetPartition = opts.partition
    const targetOffset = opts.fromOffset ?? (opts.fromBeginning ? '0' : undefined)

    if (targetPartition !== undefined && targetOffset !== undefined) {
      consumer.on(consumer.events.GROUP_JOIN, async () => {
        await consumer.seek({ topic: opts.topic, partition: targetPartition, offset: targetOffset })
      })
    }

    await consumer.run({
      eachMessage: async ({ topic, partition, message }) => {
        if (targetPartition !== undefined && partition !== targetPartition) return
        const headers: Record<string, string> = {}
        if (message.headers) {
          for (const [k, v] of Object.entries(message.headers)) {
            if (v) headers[k] = typeof v === 'string' ? v : v.toString('utf-8')
          }
        }
        onMsg({
          topic, partition,
          offset: message.offset,
          key: message.key?.toString('utf-8'),
          value: message.value?.toString('utf-8') ?? '',
          headers: Object.keys(headers).length > 0 ? headers : undefined,
          timestamp: Number(message.timestamp)
        })
      }
    })

    return consumerId
  }

  async stop(consumerId: string): Promise<void> {
    const entry = this.consumers.get(consumerId)
    if (!entry) return
    try { await entry.consumer.disconnect() } catch { /* 忽略 */ }
    this.consumers.delete(consumerId)
  }

  async stopAll(): Promise<void> {
    const ids = [...this.consumers.keys()]
    await Promise.all(ids.map((id) => this.stop(id)))
  }

  /* ================================================================
   *  消息浏览：常驻消费者（从不 stop，只 seek）
   *
   *  核心思想：
   *    consumer 创建后持续运行，eachMessage 一直挂在后台。
   *    每次查询只做两件事：seek 到目标位置 → 设置请求上下文 → 等消息。
   *    consumer 绝不 stop / disconnect / re-run。
   *
   *  耗时对比：
   *    之前：每次创建 consumer → joinGroup(3~5s) → seek → fetch
   *    现在：首次 joinGroup(3~5s) → 后续查询 < 1s
   * ================================================================ */
  private fc: KConsumer | null = null        // fetch consumer
  private fcKafka: Kafka | null = null
  private fcTopic: string | null = null
  private fcReady = false                     // GROUP_JOIN 已完成

  /** 当前活跃的查询请求（同时只允许一个） */
  private fReq: FetchRequest | null = null

  /** 确保常驻消费者存在并就绪 */
  private async ensureFc(kafka: Kafka, topic: string): Promise<KConsumer> {
    /* 同实例同 topic 直接复用 */
    if (this.fc && this.fcKafka === kafka && this.fcTopic === topic && this.fcReady) {
      return this.fc
    }

    /* 清理旧消费者（连接/topic 变了） */
    await this.destroyFc()

    const consumer = kafka.consumer({
      groupId: `kfc-${randomUUID()}`,
      sessionTimeout: 30000,
      heartbeatInterval: 3000,
      maxWaitTimeInMs: 100,   /* broker 最多等 100ms 就返回（默认 5000ms，导致 seek 延迟） */
    })

    /* 注册一次性的 GROUP_JOIN 处理器 */
    const onJoin = () => {
      this.fcReady = true
    }
    consumer.on(consumer.events.GROUP_JOIN, onJoin)

    await consumer.connect()
    await consumer.subscribe({ topic, fromBeginning: true })

    /* 启动消费 —— 永不 await，永不 stop */
    consumer.run({
      eachMessage: async ({ topic: t, partition: p, message }) => {
        const req = this.fReq
        if (!req) return

        /* 分区过滤 */
        if (req.targetPartition !== undefined && p !== req.targetPartition) return

        /* 已达目标，忽略多余消息 */
        if (req.messages.length >= req.maxToCollect) return

        const headers: Record<string, string> = {}
        if (message.headers) {
          for (const [k, v] of Object.entries(message.headers)) {
            if (v) headers[k] = typeof v === 'string' ? v : v.toString('utf-8')
          }
        }
        req.messages.push({
          topic: t,
          partition: p,
          offset: message.offset,
          key: message.key?.toString('utf-8'),
          value: message.value?.toString('utf-8') ?? '',
          headers: Object.keys(headers).length > 0 ? headers : undefined,
          timestamp: Number(message.timestamp)
        })

        if (req.messages.length >= req.maxToCollect) {
          req.resolve(req.messages)
        }
      }
    })

    this.fc = consumer
    this.fcKafka = kafka
    this.fcTopic = topic

    /* 等待 GROUP_JOIN 完成 */
    await new Promise<void>((resolve) => {
      const t = setInterval(() => {
        if (this.fcReady) { clearInterval(t); resolve() }
      }, 200)
      setTimeout(() => { clearInterval(t); resolve() }, 15000)
    })

    console.log('[fetch] 常驻消费者就绪')
    return consumer
  }

  /** 销毁常驻消费者 */
  private async destroyFc(): Promise<void> {
    /* 取消进行中的查询 */
    if (this.fReq) {
      this.fReq.resolve(this.fReq.messages)
      this.fReq = null
    }
    if (this.fc) {
      try { await this.fc.stop() } catch { /* ok */ }
      try { await this.fc.disconnect() } catch { /* ok */ }
    }
    this.fc = null
    this.fcKafka = null
    this.fcTopic = null
    this.fcReady = false
  }

  /** 连接切换时清理 */
  async resetPool(): Promise<void> {
    await this.destroyFc()
  }

  /**
   * 一次性拉取消息
   *
   * 流程：
   *   1. admin 获取水位 → 算 expectedCount
   *   2. 获取 / 创建常驻消费者（首次 3~5s，后续 0ms）
   *   3. seek 到目标位置
   *   4. 设置请求上下文 → consumer.eachMessage 自动收集
   *   5. 收够 expectedCount → 返回
   */
  async fetchMessages(kafka: Kafka, opts: FetchMessagesOptions): Promise<ConsumedMessage[]> {
    const { topic, partition, offset, fromBeginning = false, limit = 50 } = opts
    const t0 = Date.now()

    /* ---- 1. admin 获取水位 ---- */
    const admin = kafka.admin()
    await admin.connect()
    let offs: Array<{ partition: number; high: string; low: string }>
    try {
      offs = (await admin.fetchTopicOffsets(topic)).map((o) => ({
        partition: Number(o.partition),
        high: o.high,
        low: o.low
      }))
    } finally {
      await admin.disconnect().catch(() => {})
    }

    /* ---- 2. 计算 seek 位置 + 预期数量 ---- */
    const seekOps: Array<{ partition: number; offset: string }> = []
    let expected = 0

    for (const o of offs) {
      if (partition !== undefined && o.partition !== partition) continue
      const high = BigInt(o.high)
      const low = BigInt(o.low)

      let to: bigint
      if (offset !== undefined) to = BigInt(offset)
      else if (!fromBeginning) to = high - BigInt(limit)
      else to = low
      if (to < low) to = low

      const avail = high - to
      if (avail > 0n) {
        const n = Number(avail)
        expected += n
        seekOps.push({ partition: o.partition, offset: to.toString() })
      }
    }

    const maxToCollect = Math.min(limit, expected)
    console.log(`[fetch] topic=${topic} expected=${expected} maxCollect=${maxToCollect} t+${Date.now() - t0}ms`)

    if (maxToCollect <= 0) return []

    /* ---- 3. 获取常驻消费者 + seek ---- */
    const consumer = await this.ensureFc(kafka, topic)

    /* pause → seek → resume：KafkaJS 要求在 running 状态下 seek 前必须先 pause */
    consumer.pause([{ topic }])
    /* 等待当前 fetch 周期结束（maxWaitTimeInMs=100 保证很快） */
    await new Promise((r) => setTimeout(r, 150))
    for (const s of seekOps) {
      await consumer.seek({ topic, partition: s.partition, offset: s.offset })
    }
    consumer.resume([{ topic }])

    /* ---- 4. 设置请求上下文，等待收集 ---- */
    return new Promise<ConsumedMessage[]>((resolve) => {
      // 取消上一个请求（如果有）
      if (this.fReq) {
        clearTimeout(this.fReq.timer ?? undefined)
        this.fReq.resolve(this.fReq.messages)
      }

      const msgs: ConsumedMessage[] = []

      const finish = () => {
        clearTimeout(timer)
        if (this.fReq?.messages === msgs) this.fReq = null
        const elapsed = Date.now() - t0
        console.log(`[fetch] ← ${msgs.length} 条, 耗时 ${elapsed}ms`)
        resolve(msgs)
      }

      const timer = setTimeout(finish, 8000) // 8 秒兜底

      this.fReq = {
        targetPartition: partition,
        messages: msgs,
        maxToCollect,
        resolve: finish,
        timer
      }
    })
  }
}

export const consumerSvc = new ConsumerService()
