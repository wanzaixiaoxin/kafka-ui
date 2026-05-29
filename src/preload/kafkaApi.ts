import { ipcRenderer } from 'electron'
import type {
  KafkaConnection,
  ConnectionTestResult,
  TopicInfo,
  TopicDetail,
  PartitionOffset,
  KafkaMessage,
  SendResult,
  ConsumedMessage,
  ConsumerOptions,
  FetchMessagesOptions,
  ConsumerGroupInfo,
  ConsumerGroupDetail,
  LogEntry,
  CreateTopicOptions,
  CreateTopicResult
} from '../shared/types'

/** 连接管理 API */
export const connectionApi = {
  list: (): Promise<KafkaConnection[]> =>
    ipcRenderer.invoke('kafka:connection:list'),

  save: (conn: Partial<KafkaConnection> & { name: string; brokers: string[] }): Promise<KafkaConnection> =>
    ipcRenderer.invoke('kafka:connection:save', conn),

  remove: (id: string): Promise<{ success: boolean; error?: string }> =>
    ipcRenderer.invoke('kafka:connection:remove', id),

  test: (conn: KafkaConnection): Promise<ConnectionTestResult> =>
    ipcRenderer.invoke('kafka:connection:test', conn),

  use: (id: string): Promise<{ success: boolean; error?: string }> =>
    ipcRenderer.invoke('kafka:connection:use', id),

  /** 获取当前激活连接 ID */
  activeId: (): Promise<string | null> =>
    ipcRenderer.invoke('kafka:connection:activeId'),

  /** 监听连接变更事件 */
  onChanged: (callback: (id: string) => void): (() => void) => {
    const handler = (_event: unknown, id: string): void => callback(id)
    ipcRenderer.on('kafka:connection:changed', handler)
    return () => {
      ipcRenderer.removeListener('kafka:connection:changed', handler)
    }
  }
}

/** Preload 层 Kafka API - 桥接渲染进程与主进程 */
export const kafkaApi = {
  /* ---- 连接管理 ---- */
  connections: connectionApi,

  /* ---- Topic 操作 ---- */
  topics: {
    list: (showInternal?: boolean): Promise<TopicInfo[] | { error: string }> =>
      ipcRenderer.invoke('kafka:topic:list', showInternal),

    describe: (topic: string): Promise<TopicDetail | { error: string }> =>
      ipcRenderer.invoke('kafka:topic:describe', topic),

    offsets: (topic: string): Promise<PartitionOffset[] | { error: string }> =>
      ipcRenderer.invoke('kafka:topic:offsets', topic),

    messages: (opts: FetchMessagesOptions): Promise<ConsumedMessage[] | { error: string }> =>
      ipcRenderer.invoke('kafka:topic:messages', opts),

    create: (opts: CreateTopicOptions): Promise<CreateTopicResult> =>
      ipcRenderer.invoke('kafka:topic:create', opts)
  },

  /* ---- 生产者 ---- */
  producer: {
    send: (msg: KafkaMessage): Promise<SendResult | { error: string }> =>
      ipcRenderer.invoke('kafka:producer:send', msg)
  },

  /* ---- 消费者 ---- */
  consumer: {
    start: (opts: ConsumerOptions): Promise<{ consumerId: string } | { error: string }> =>
      ipcRenderer.invoke('kafka:consumer:start', opts),

    stop: (consumerId: string): Promise<{ success: boolean; error?: string }> =>
      ipcRenderer.invoke('kafka:consumer:stop', consumerId),

    stopAll: (): Promise<{ success: boolean; error?: string }> =>
      ipcRenderer.invoke('kafka:consumer:stopAll'),

    onMessage: (callback: (msg: ConsumedMessage) => void): (() => void) => {
      const handler = (_event: unknown, msg: unknown): void => callback(msg as ConsumedMessage)
      ipcRenderer.on('kafka:consumer:message', handler)
      return () => {
        ipcRenderer.removeListener('kafka:consumer:message', handler)
      }
    }
  },

  /* ---- 消费者组 ---- */
  groups: {
    list: (): Promise<ConsumerGroupInfo[] | { error: string }> =>
      ipcRenderer.invoke('kafka:group:list'),

    describe: (groupId: string): Promise<ConsumerGroupDetail | { error: string }> =>
      ipcRenderer.invoke('kafka:group:describe', groupId)
  },

  /* ---- 日志 ---- */
  log: {
    getAll: (): Promise<LogEntry[]> =>
      ipcRenderer.invoke('log:getAll') as Promise<LogEntry[]>,

    clear: (): Promise<void> =>
      ipcRenderer.invoke('log:clear'),

    onEntry: (callback: (entry: LogEntry) => void): (() => void) => {
      const handler = (_event: unknown, entry: unknown): void => callback(entry as LogEntry)
      ipcRenderer.on('log:entry', handler)
      return () => {
        ipcRenderer.removeListener('log:entry', handler)
      }
    },

    send: (entry: LogEntry): void => {
      ipcRenderer.send('log:renderer', entry)
    }
  },

  /* ---- 设置 ---- */
  settings: {
    get: (): Promise<{ maxMessages: number; autoRefreshInterval: number; theme: string }> =>
      ipcRenderer.invoke('settings:get'),

    update: (s: Record<string, unknown>): Promise<{ maxMessages: number; autoRefreshInterval: number; theme: string }> =>
      ipcRenderer.invoke('settings:update', s)
  }
}
