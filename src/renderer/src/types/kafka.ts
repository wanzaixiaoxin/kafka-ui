/** Kafka 连接配置 */
export interface KafkaConnection {
  id: string
  name: string
  brokers: string[]
  /** 客户端 ID */
  clientId: string
  /** 是否启用 SSL */
  ssl: boolean
  /** SASL 认证配置 */
  sasl?: {
    mechanism: 'plain' | 'scram-sha-256' | 'scram-sha-512'
    username: string
    password: string
  }
  /** 连接描述 */
  description?: string
  /** 创建时间戳 */
  createdAt: number
  /** 更新时间戳 */
  updatedAt: number
}

/** 连接测试结果 */
export interface ConnectionTestResult {
  success: boolean
  brokers?: Array<{ nodeId: number; host: string; port: number }>
  controllerId?: number
  clusterId?: string
  error?: string
}

/** Kafka 消息（生产用） */
export interface KafkaMessage {
  topic: string
  key?: string
  value: string
  partition?: number
  headers?: Record<string, string>
  timestamp?: number
}

/** 发送结果 */
export interface SendResult {
  topic: string
  partition: number
  offset: string
  timestamp: number
}

/** 已消费的消息 */
export interface ConsumedMessage {
  topic: string
  partition: number
  offset: string
  key?: string
  value: string
  headers?: Record<string, string>
  timestamp: number
}

/** 消费者启动选项 */
export interface ConsumerOptions {
  topic: string
  partition?: number
  fromBeginning?: boolean
  fromOffset?: string
}

/** Topic 基本信息 */
export type TopicInfo = {
  topic: string
  partitions: number
  replicas: number
  isInternal: boolean
}

/** 分区信息 */
export type PartitionInfo = {
  partitionId: number
  leader: number
  replicas: number[]
  isr: number[]
}

/** Topic 详情 */
export type TopicDetail = {
  topic: string
  partitions: PartitionInfo[]
}

/** 分区 Offset 信息 */
export type PartitionOffset = {
  partition: number
  earliestOffset: string
  latestOffset: string
}

/** 消费者组基本信息 */
export type ConsumerGroupInfo = {
  groupId: string
  state: string
  members: number
  protocol: string
}

/** 消费者组详情 */
export type ConsumerGroupDetail = {
  groupId: string
  state: string
  protocol: string
  members: Array<{
    clientId: string
    memberId: string
    host: string
  }>
  offsets: Array<{
    topic: string
    partition: number
    currentOffset: string
    logEndOffset: string
    lag: string
  }>
}

/** 连接状态 */
export type ConnectionStatus = 'disconnected' | 'connecting' | 'connected' | 'error'
