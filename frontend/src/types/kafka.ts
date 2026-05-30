// ============================================================
//  Kafka Client 类型定义 — 前端与 Rust 后端的 JSON 契约
// ============================================================

/** Kafka 连接配置 */
export interface KafkaConnection {
  id: string
  name: string
  brokers: string[]
  clientId: string
  ssl: boolean
  sasl?: {
    mechanism: 'plain' | 'scram-sha-256' | 'scram-sha-512'
    username: string
    password: string
  }
  description?: string
  createdAt: number
  updatedAt: number
}

export interface ConnectionTestResult {
  success: boolean
  brokers?: Array<{ nodeId: number; host: string; port: number }>
  controllerId?: number
  clusterId?: string
  error?: string
}

export interface KafkaMessage {
  topic: string
  key?: string
  value: string
  partition?: number
  headers?: Record<string, string>
}

export interface SendResult {
  topic: string
  partition: number
  offset: string
  timestamp: number
}

export interface ConsumedMessage {
  topic: string
  partition: number
  offset: string
  key?: string
  value: string
  headers?: Record<string, string>
  timestamp: number
}

export interface ConsumerOptions {
  topic: string
  partition?: number
  fromBeginning?: boolean
  fromOffset?: string
}

export interface FetchMessagesOptions {
  topic: string
  partition?: number
  offset?: string
  fromBeginning?: boolean
  limit: number
}

export type TopicInfo = {
  topic: string
  partitions: number
  replicas: number
  isInternal: boolean
}

export type PartitionInfo = {
  partitionId: number
  leader: number
  replicas: number[]
  isr: number[]
}

export type TopicDetail = {
  topic: string
  partitions: PartitionInfo[]
}

export type PartitionOffset = {
  partition: number
  earliestOffset: string
  latestOffset: string
}

export type ConsumerGroupInfo = {
  groupId: string
  state: string
  members: number
  protocol: string
}

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

export type ConnectionStatus = 'disconnected' | 'connecting' | 'connected' | 'error'

export type LogLevel = 'debug' | 'info' | 'warn' | 'error' | 'fatal'

export type LogSource = 'main' | 'renderer'

export interface LogEntry {
  id: string
  level: LogLevel
  timestamp: number
  source: LogSource
  message: string
  data?: string
  stack?: string
  origin?: string
}

export interface CreateTopicOptions {
  topic: string
  numPartitions: number
  replicationFactor: number
}

export interface CreateTopicResult {
  success: boolean
  error?: string
}

export interface AppSettings {
  maxMessages: number
  autoRefreshInterval: number
  theme: 'light' | 'dark' | 'system'
}

export const DEFAULT_SETTINGS: AppSettings = {
  maxMessages: 500,
  autoRefreshInterval: 0,
  theme: 'light'
}
