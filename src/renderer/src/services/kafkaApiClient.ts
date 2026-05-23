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
  LogEntry
} from '../types/kafka'

/** 渲染进程 Kafka API 客户端 - 通过 preload 暴露的 api 调用主进程 */
export const kafkaApiClient = {
  /* ---- 连接管理 ---- */
  connections: {
    list: (): Promise<KafkaConnection[]> => window.api.connections.list(),

    save: (conn: Partial<KafkaConnection> & { name: string; brokers: string[] }): Promise<KafkaConnection> =>
      window.api.connections.save(conn),

    remove: (id: string): Promise<{ success: boolean; error?: string }> =>
      window.api.connections.remove(id),

    test: (conn: KafkaConnection): Promise<ConnectionTestResult> =>
      window.api.connections.test(conn),

    use: (id: string): Promise<{ success: boolean; error?: string }> =>
      window.api.connections.use(id),

    /** 获取当前激活连接 ID */
    activeId: (): Promise<string | null> =>
      window.api.connections.activeId()
  },

  /* ---- Topic 操作 ---- */
  topics: {
    list: (showInternal?: boolean): Promise<TopicInfo[] | { error: string }> =>
      window.api.topics.list(showInternal),

    describe: (topic: string): Promise<TopicDetail | { error: string }> =>
      window.api.topics.describe(topic),

    offsets: (topic: string): Promise<PartitionOffset[] | { error: string }> =>
      window.api.topics.offsets(topic),

    messages: (opts: FetchMessagesOptions): Promise<ConsumedMessage[] | { error: string }> =>
      window.api.topics.messages(opts)
  },

  /* ---- 生产者 ---- */
  producer: {
    send: (msg: KafkaMessage): Promise<SendResult | { error: string }> =>
      window.api.producer.send(msg)
  },

  /* ---- 消费者 ---- */
  consumer: {
    start: (opts: ConsumerOptions): Promise<{ consumerId: string } | { error: string }> =>
      window.api.consumer.start(opts),

    stop: (consumerId: string): Promise<{ success: boolean; error?: string }> =>
      window.api.consumer.stop(consumerId),

    stopAll: (): Promise<{ success: boolean; error?: string }> =>
      window.api.consumer.stopAll(),

    onMessage: (callback: (msg: ConsumedMessage) => void): (() => void) =>
      window.api.consumer.onMessage(callback)
  },

  /* ---- 消费者组 ---- */
  groups: {
    list: (): Promise<ConsumerGroupInfo[] | { error: string }> =>
      window.api.groups.list(),

    describe: (groupId: string): Promise<ConsumerGroupDetail | { error: string }> =>
      window.api.groups.describe(groupId)
  },

  /* ---- 日志 ---- */
  log: {
    getAll: (): Promise<LogEntry[]> =>
      window.api.log.getAll(),

    clear: (): Promise<void> =>
      window.api.log.clear(),

    onEntry: (callback: (entry: LogEntry) => void): (() => void) =>
      window.api.log.onEntry(callback),

    send: (entry: LogEntry): void =>
      window.api.log.send(entry)
  }
}
