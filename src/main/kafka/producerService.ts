import type { Kafka } from 'kafkajs'
import type { KafkaMessage, SendResult } from '../../renderer/src/types/kafka'

/** 生产者服务 - 发送消息到 Kafka */
export class ProducerService {
  /** 发送单条消息，返回发送结果 */
  async send(kafka: Kafka, msg: KafkaMessage): Promise<SendResult> {
    const producer = kafka.producer()
    try {
      await producer.connect()
      /* 转换 headers 格式为 KafkaJS 所需格式 */
      const headers: Record<string, string> = {}
      if (msg.headers) {
        for (const [k, v] of Object.entries(msg.headers)) {
          headers[k] = v
        }
      }
      const result = await producer.send({
        topic: msg.topic,
        messages: [
          {
            key: msg.key || null,
            value: msg.value,
            partition: msg.partition,
            headers: Object.keys(headers).length > 0 ? headers : undefined
          }
        ]
      })
      /* 取第一条记录的元数据作为结果 */
      const meta = result[0]
      return {
        topic: meta.topicName,
        partition: meta.partition,
        offset: meta.offset,
        timestamp: Number(meta.timestamp)
      }
    } finally {
      try { await producer.disconnect() } catch { /* 忽略断开错误 */ }
    }
  }
}

export const producerSvc = new ProducerService()
