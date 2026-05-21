import type { Kafka, Consumer as KConsumer } from 'kafkajs'
import { randomUUID } from 'crypto'
import type { ConsumedMessage, ConsumerOptions } from '../../renderer/src/types/kafka'

/** 消息回调类型 */
export type MsgCallback = (msg: ConsumedMessage) => void

/** 活跃消费者记录 */
interface ActiveConsumer {
  consumer: KConsumer
  consumerId: string
}

/** 消费者服务 - 从 Kafka 消费消息 */
export class ConsumerService {
  /** 活跃消费者映射 */
  private consumers: Map<string, ActiveConsumer> = new Map()

  /**
   * 启动消费者
   * 创建消费者实例并开始消费，每条消息通过回调推送
   * 返回 consumerId 用于后续管理
   */
  async start(kafka: Kafka, opts: ConsumerOptions, onMsg: MsgCallback): Promise<string> {
    const consumerId = randomUUID()
    const groupId = `kafka-client-consumer-${consumerId}`
    const consumer = kafka.consumer({ groupId })

    await consumer.connect()

    /* 指定分区时直接分配，否则订阅整个 topic */
    if (opts.partition !== undefined) {
      await consumer.assign([{ topic: opts.topic, partition: opts.partition }])
    } else {
      await consumer.subscribe({ topic: opts.topic, fromBeginning: opts.fromBeginning ?? false })
    }

    /* 指定 offset 时 seek 到对应位置 */
    if (opts.fromOffset !== undefined && opts.partition !== undefined) {
      await consumer.seek({
        topic: opts.topic,
        partition: opts.partition,
        offset: opts.fromOffset
      })
    } else if (opts.fromBeginning && opts.partition !== undefined) {
      await consumer.seek({
        topic: opts.topic,
        partition: opts.partition,
        offset: '0'
      })
    }

    /* 保存消费者实例 */
    this.consumers.set(consumerId, { consumer, consumerId })

    /* 开始消费，每条消息通过回调推送 */
    await consumer.run({
      eachMessage: async ({ topic, partition, message }) => {
        /* 解析 headers */
        const headers: Record<string, string> = {}
        if (message.headers) {
          for (const [k, v] of Object.entries(message.headers)) {
            if (v) {
              headers[k] = typeof v === 'string' ? v : v.toString('utf-8')
            }
          }
        }
        const consumed: ConsumedMessage = {
          topic,
          partition,
          offset: message.offset,
          key: message.key ? message.key.toString('utf-8') : undefined,
          value: message.value ? message.value.toString('utf-8') : '',
          headers: Object.keys(headers).length > 0 ? headers : undefined,
          timestamp: Number(message.timestamp)
        }
        onMsg(consumed)
      }
    })

    return consumerId
  }

  /** 停止指定消费者 */
  async stop(consumerId: string): Promise<void> {
    const entry = this.consumers.get(consumerId)
    if (!entry) return
    try {
      await entry.consumer.disconnect()
    } catch { /* 忽略断开错误 */ }
    this.consumers.delete(consumerId)
  }

  /** 停止所有消费者 */
  async stopAll(): Promise<void> {
    const ids = [...this.consumers.keys()]
    await Promise.all(ids.map((id) => this.stop(id)))
  }
}

export const consumerSvc = new ConsumerService()
