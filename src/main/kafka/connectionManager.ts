import { Kafka, logLevel } from 'kafkajs'
import type { KafkaConnection, ConnectionTestResult } from '../../renderer/src/types/kafka'

/** Kafka 连接管理器，维护所有 Kafka 客户端实例 */
class ConnectionManager {
  private clients: Map<string, Kafka> = new Map()

  /** 根据连接配置创建 Kafka 实例 */
  createKafkaInstance(config: KafkaConnection): Kafka {
    const opts: Record<string, unknown> = {
      brokers: config.brokers,
      clientId: config.clientId,
      logLevel: logLevel.WARN
    }
    if (config.ssl) {
      opts.ssl = true
    }
    if (config.sasl) {
      opts.sasl = {
        mechanism: config.sasl.mechanism,
        username: config.sasl.username,
        password: config.sasl.password
      }
    }
    return new Kafka(opts as ConstructorParameters<typeof Kafka>[0])
  }

  /** 测试连接，获取集群元数据 */
  async testConnection(config: KafkaConnection): Promise<ConnectionTestResult> {
    const kafka = this.createKafkaInstance(config)
    const admin = kafka.admin()
    try {
      await admin.connect()
      const meta = await admin.fetchTopicMetadata()
      const clusterInfo = await admin.describeCluster()
      await admin.disconnect()
      return {
        success: true,
        brokers: clusterInfo.brokers.map((b) => ({
          nodeId: b.nodeId,
          host: b.host,
          port: b.port
        })),
        controllerId: clusterInfo.controllerId,
        clusterId: clusterInfo.clusterId
      }
    } catch (err: unknown) {
      try { await admin.disconnect() } catch { /* 忽略断开错误 */ }
      const msg = err instanceof Error ? err.message : String(err)
      return { success: false, error: msg }
    }
  }

  /** 获取当前激活的 Kafka 实例（无则创建） */
  getActiveKafka(config: KafkaConnection): Kafka {
    let client = this.clients.get(config.id)
    if (!client) {
      client = this.createKafkaInstance(config)
      this.clients.set(config.id, client)
    }
    return client
  }

  /** 获取已有客户端 */
  get(connId: string): Kafka | undefined {
    return this.clients.get(connId)
  }

  /** 断开并移除指定 Kafka 实例 */
  async disconnect(connId: string): Promise<void> {
    this.clients.delete(connId)
  }

  /** 关闭所有客户端 */
  async closeAll(): Promise<void> {
    this.clients.clear()
  }
}

export const connMgr = new ConnectionManager()
