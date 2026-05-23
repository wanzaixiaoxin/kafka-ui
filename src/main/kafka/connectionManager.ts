import { Kafka, logLevel, Admin, Producer } from 'kafkajs'
import type { KafkaConnection, ConnectionTestResult } from '../../shared/types'

/** Kafka 连接管理器，维护所有 Kafka 客户端实例及其子资源 */
class ConnectionManager {
  private clients: Map<string, Kafka> = new Map()
  private admins: Map<string, Admin> = new Map()
  private producers: Map<string, Producer> = new Map()

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
    return new Kafka(opts as unknown as ConstructorParameters<typeof Kafka>[0])
  }

  /** 测试连接，获取集群元数据 */
  async testConnection(config: KafkaConnection): Promise<ConnectionTestResult> {
    const kafka = this.createKafkaInstance(config)
    const admin = kafka.admin()
    try {
      await admin.connect()
      await admin.fetchTopicMetadata()
      const clusterInfo = await admin.describeCluster()
      await admin.disconnect()
      return {
        success: true,
        brokers: clusterInfo.brokers.map((b) => ({
          nodeId: b.nodeId,
          host: b.host,
          port: b.port
        })),
        controllerId: clusterInfo.controller ?? undefined,
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

  /** 获取池化的 Admin 客户端 */
  async getAdmin(connId: string): Promise<Admin | null> {
    const kafka = this.clients.get(connId)
    if (!kafka) return null
    let admin = this.admins.get(connId)
    if (!admin) {
      admin = kafka.admin()
      await admin.connect()
      this.admins.set(connId, admin)
    }
    return admin
  }

  /** 获取池化的 Producer */
  async getProducer(connId: string): Promise<Producer | null> {
    const kafka = this.clients.get(connId)
    if (!kafka) return null
    let producer = this.producers.get(connId)
    if (!producer) {
      producer = kafka.producer()
      await producer.connect()
      this.producers.set(connId, producer)
    }
    return producer
  }

  /** 断开并移除指定 Kafka 实例及其子资源 */
  async disconnect(connId: string): Promise<void> {
    const admin = this.admins.get(connId)
    if (admin) {
      this.admins.delete(connId)
      try { await admin.disconnect() } catch { /* 忽略 */ }
    }
    const producer = this.producers.get(connId)
    if (producer) {
      this.producers.delete(connId)
      try { await producer.disconnect() } catch { /* 忽略 */ }
    }
    this.clients.delete(connId)
  }

  /** 关闭所有客户端及其子资源 */
  async closeAll(): Promise<void> {
    const disconnects: Promise<void>[] = []
    const adminEntries = Array.from(this.admins.entries())
    for (const [id, admin] of adminEntries) {
      this.admins.delete(id)
      disconnects.push(admin.disconnect().catch(() => {}))
    }
    const producerEntries = Array.from(this.producers.entries())
    for (const [id, producer] of producerEntries) {
      this.producers.delete(id)
      disconnects.push(producer.disconnect().catch(() => {}))
    }
    await Promise.all(disconnects)
    this.clients.clear()
  }
}

export const connMgr = new ConnectionManager()
