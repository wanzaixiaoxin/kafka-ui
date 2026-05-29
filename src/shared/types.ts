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

/** 拉取消息选项（一次性批量拉取） */
export interface FetchMessagesOptions {
  topic: string
  partition?: number
  /** 起始 offset，不传则根据 fromBeginning 决定 */
  offset?: string
  /** 是否从最早开始，默认 false（从最新开始） */
  fromBeginning?: boolean
  /** 最大拉取条数 */
  limit: number
}

/** 连接状态 */
export type ConnectionStatus = 'disconnected' | 'connecting' | 'connected' | 'error'

/** 日志级别 */
export type LogLevel = 'debug' | 'info' | 'warn' | 'error' | 'fatal'

/** 日志来源 */
export type LogSource = 'main' | 'renderer' | 'preload'

/** 单条日志记录 */
export interface LogEntry {
  /** 唯一 ID */
  id: string
  /** 日志级别 */
  level: LogLevel
  /** 时间戳 */
  timestamp: number
  /** 日志来源进程 */
  source: LogSource
  /** 日志消息 */
  message: string
  /** 附加数据（JSON 字符串） */
  data?: string
  /** 错误堆栈（仅 error/fatal 级别） */
  stack?: string
  /** 文件/模块来源 */
  origin?: string
}

/** 创建 Topic 选项 */
export interface CreateTopicOptions {
  topic: string
  numPartitions: number
  replicationFactor: number
}

/** 创建 Topic 结果 */
export interface CreateTopicResult {
  success: boolean
  error?: string
}

/** 应用设置 */
export interface AppSettings {
  maxMessages: number
  autoRefreshInterval: number
  theme: 'light' | 'dark' | 'system'
}

/** 应用设置默认值 */
export const DEFAULT_SETTINGS: AppSettings = {
  maxMessages: 500,
  autoRefreshInterval: 0,
  theme: 'light'
}
