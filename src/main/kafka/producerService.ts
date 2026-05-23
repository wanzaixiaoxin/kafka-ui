import type { Producer } from 'kafkajs'
import type { KafkaMessage, SendResult } from '../../shared/types'

/** 生产者服务 - 发送消息到 Kafka */
export class ProducerService {
  /** 通过池化的 Producer 发送消息 */
  async send(producer: Producer, msg: KafkaMessage): Promise<SendResult> {
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
    const meta = result[0]
    return {
      topic: meta.topicName,
      partition: meta.partition,
      offset: meta.offset ?? '0',
      timestamp: Number(meta.timestamp)
    }
  }
}

export const producerSvc = new ProducerService()
