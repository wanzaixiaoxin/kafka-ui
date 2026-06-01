import { invoke } from '@tauri-apps/api/core'
import { listen, emit } from '@tauri-apps/api/event'
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
  CreateTopicResult,
  ExportOptions,
  ImportOptions,
  ImportExportProgress
} from '../types/kafka'

/** 渲染进程 Kafka API 客户端 — 通过 Tauri invoke/event 调用 Rust 后端 */
export const kafkaApiClient = {
  /* ---- 连接管理 ---- */
  connections: {
    list: (): Promise<KafkaConnection[]> =>
      invoke('connection_list'),

    save: (conn: Partial<KafkaConnection> & { name: string; brokers: string[] }): Promise<KafkaConnection> =>
      invoke('connection_save', { conn }),

    remove: (id: string): Promise<{ success: boolean; error?: string }> =>
      invoke('connection_remove', { id }),

    test: (conn: KafkaConnection): Promise<ConnectionTestResult> =>
      invoke('connection_test', { conn }),

    use: (id: string): Promise<{ success: boolean; error?: string }> =>
      invoke('connection_use', { id }),

    /** 获取当前激活连接 ID */
    activeId: (): Promise<string | null> =>
      invoke('connection_active_id'),

    /** 监听连接变更事件 */
    onChanged: (callback: (id: string) => void): (() => void) => {
      let cancelled = false
      let unlisten: (() => void) | null = null
      listen<string>('kafka:connection:changed', (event) => {
        if (!cancelled) callback(event.payload)
      }).then((fn) => {
        if (cancelled) fn()
        else unlisten = fn
      })
      return () => {
        cancelled = true
        unlisten?.()
      }
    }
  },

  /* ---- Topic 操作 ---- */
  topics: {
    list: (showInternal?: boolean): Promise<TopicInfo[] | { error: string }> =>
      invoke('topic_list', { showInternal }),

    describe: (topic: string): Promise<TopicDetail | { error: string }> =>
      invoke('topic_describe', { topic }),

    offsets: (topic: string): Promise<PartitionOffset[] | { error: string }> =>
      invoke('topic_offsets', { topic }),

    messages: (opts: FetchMessagesOptions): Promise<ConsumedMessage[] | { error: string }> =>
      invoke('topic_messages', { opts }),

    create: (opts: CreateTopicOptions): Promise<CreateTopicResult> =>
      invoke('topic_create', { opts })
  },

  /* ---- 生产者 ---- */
  producer: {
    send: (msg: KafkaMessage): Promise<SendResult | { error: string }> =>
      invoke('producer_send', { msg })
  },

  /* ---- 消费者 ---- */
  consumer: {
    start: (opts: ConsumerOptions): Promise<{ consumerId: string } | { error: string }> =>
      invoke('consumer_start', { opts }),

    stop: (consumerId: string): Promise<{ success: boolean; error?: string }> =>
      invoke('consumer_stop', { consumerId }),

    stopAll: (): Promise<{ success: boolean; error?: string }> =>
      invoke('consumer_stop_all'),

    onMessage: (callback: (msg: ConsumedMessage) => void): (() => void) => {
      let cancelled = false
      let unlisten: (() => void) | null = null
      listen<ConsumedMessage>('kafka:consumer:message', (event) => {
        if (!cancelled) callback(event.payload)
      }).then((fn) => {
        if (cancelled) fn()
        else unlisten = fn
      })
      return () => {
        cancelled = true
        unlisten?.()
      }
    }
  },

  /* ---- 消费者组 ---- */
  groups: {
    list: (): Promise<ConsumerGroupInfo[] | { error: string }> =>
      invoke('group_list'),

    describe: (groupId: string): Promise<ConsumerGroupDetail | { error: string }> =>
      invoke('group_describe', { groupId })
  },

  /* ---- 日志 ---- */
  log: {
    getAll: (): Promise<LogEntry[]> =>
      invoke('log_get_all') as Promise<LogEntry[]>,

    clear: (): Promise<void> =>
      invoke('log_clear'),

    onEntry: (callback: (entry: LogEntry) => void): (() => void) => {
      let cancelled = false
      let unlisten: (() => void) | null = null
      listen<LogEntry>('log:entry', (event) => {
        if (!cancelled) callback(event.payload)
      }).then((fn) => {
        if (cancelled) fn()
        else unlisten = fn
      })
      return () => {
        cancelled = true
        unlisten?.()
      }
    },

    send: (entry: LogEntry): void => {
      emit('log:renderer', entry)
    }
  },

  /* ---- 导入导出 ---- */
  importExport: {
    exportStart: (opts: ExportOptions): Promise<void> =>
      invoke('export_start', { opts }),

    importStart: (opts: ImportOptions): Promise<void> =>
      invoke('import_start', { opts }),

    exportCancel: (): Promise<void> =>
      invoke('export_cancel'),

    importCancel: (): Promise<void> =>
      invoke('import_cancel'),

    onExportProgress: (callback: (progress: ImportExportProgress) => void): (() => void) => {
      let cancelled = false
      let unlisten: (() => void) | null = null
      listen<ImportExportProgress>('kafka:export:progress', (event) => {
        if (!cancelled) callback(event.payload)
      }).then((fn) => {
        if (cancelled) fn()
        else unlisten = fn
      })
      return () => {
        cancelled = true
        unlisten?.()
      }
    },

    onImportProgress: (callback: (progress: ImportExportProgress) => void): (() => void) => {
      let cancelled = false
      let unlisten: (() => void) | null = null
      listen<ImportExportProgress>('kafka:import:progress', (event) => {
        if (!cancelled) callback(event.payload)
      }).then((fn) => {
        if (cancelled) fn()
        else unlisten = fn
      })
      return () => {
        cancelled = true
        unlisten?.()
      }
    }
  }
}
