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
    /** 获取所有连接列表 */
    list: (): Promise<KafkaConnection[]> => window.api.connections.list(),

    /** 保存连接 */
    save: (conn: Partial<KafkaConnection> & { name: string; brokers: string[] }): Promise<KafkaConnection> =>
      window.api.connections.save(conn),

    /** 删除连接 */
    remove: (id: string): Promise<{ success: boolean; error?: string }> =>
      window.api.connections.remove(id),

    /** 测试连接 */
    test: (conn: KafkaConnection): Promise<ConnectionTestResult> =>
      window.api.connections.test(conn),

    /** 切换激活连接 */
    use: (id: string): Promise<{ success: boolean; error?: string }> =>
      window.api.connections.use(id)
  },

  /* ---- 连接操作 ---- */
  /** 连接到 Kafka */
  connect: (connId: string) => window.api.connect(connId),

  /** 断开连接 */
  disconnect: (connId: string) => window.api.disconnect(connId),

  /* ---- Topic 操作 ---- */
  topics: {
    /** 获取 Topic 列表 */
    list: (showInternal?: boolean): Promise<TopicInfo[] | { error: string }> =>
      window.api.topics.list(showInternal) as Promise<TopicInfo[] | { error: string }>,

    /** 获取 Topic 详情 */
    describe: (topic: string): Promise<TopicDetail | { error: string }> =>
      window.api.topics.describe(topic) as Promise<TopicDetail | { error: string }>,

    /** 获取 Topic Offset */
    offsets: (topic: string): Promise<PartitionOffset[] | { error: string }> =>
      window.api.topics.offsets(topic) as Promise<PartitionOffset[] | { error: string }>,

    /** 拉取 Topic 消息 */
    messages: (opts: FetchMessagesOptions): Promise<ConsumedMessage[] | { error: string }> =>
      window.api.topics.messages(opts) as Promise<ConsumedMessage[] | { error: string }>
  },

  /* ---- 旧版 Topic 操作（保留兼容） ---- */
  /** 获取 Topic 列表 */
  listTopics: (connId: string) => window.api.listTopics(connId),

  /** 获取 Topic 详情 */
  getTopicDetail: (connId: string, topic: string) => window.api.getTopicDetail(connId, topic),

  /** 创建 Topic */
  createTopic: (connId: string, config: unknown) => window.api.createTopic(connId, config),

  /** 删除 Topic */
  deleteTopic: (connId: string, topic: string) => window.api.deleteTopic(connId, topic),

  /* ---- 生产者 ---- */
  /** 发送消息（旧版） */
  produce: (connId: string, msg: unknown) => window.api.produce(connId, msg),

  /** 发送消息 */
  producer: {
    send: (msg: KafkaMessage): Promise<SendResult | { error: string }> =>
      window.api.producer.send(msg) as Promise<SendResult | { error: string }>
  },

  /* ---- 消费者 ---- */
  /** 开始消费（旧版） */
  consume: (connId: string, config: unknown) => window.api.consume(connId, config),

  /** 停止消费（旧版） */
  stopConsume: (connId: string) => window.api.stopConsume(connId),

  /** 消费者 API */
  consumer: {
    /** 启动消费者 */
    start: (opts: ConsumerOptions): Promise<{ consumerId: string } | { error: string }> =>
      window.api.consumer.start(opts) as Promise<{ consumerId: string } | { error: string }>,

    /** 停止消费者 */
    stop: (consumerId: string): Promise<{ success: boolean; error?: string }> =>
      window.api.consumer.stop(consumerId) as Promise<{ success: boolean; error?: string }>,

    /** 停止所有消费者 */
    stopAll: (): Promise<{ success: boolean; error?: string }> =>
      window.api.consumer.stopAll() as Promise<{ success: boolean; error?: string }>,

    /** 监听消费消息 */
    onMessage: (callback: (msg: ConsumedMessage) => void): (() => void) =>
      window.api.consumer.onMessage(callback as (msg: unknown) => void) as unknown as () => void
  },

  /* ---- 消费者组 ---- */

  /** 消费者组 API */
  groups: {
    /** 获取消费者组列表 */
    list: (): Promise<ConsumerGroupInfo[] | { error: string }> =>
      window.api.groups.list() as Promise<ConsumerGroupInfo[] | { error: string }>,

    /** 获取消费者组详情 */
    describe: (groupId: string): Promise<ConsumerGroupDetail | { error: string }> =>
      window.api.groups.describe(groupId) as Promise<ConsumerGroupDetail | { error: string }>
  },

  /** 获取消费者组列表 */
  listGroups: (connId: string) => window.api.listGroups(connId),

  /** 获取消费者组详情 */
  getGroupDetail: (connId: string, groupId: string) =>
    window.api.getGroupDetail(connId, groupId),

  /* ---- 日志 ---- */
  log: {
    /** 获取所有缓冲日志 */
    getAll: (): Promise<LogEntry[]> =>
      window.api.log.getAll() as Promise<LogEntry[]>,

    /** 清空日志 */
    clear: (): Promise<void> =>
      window.api.log.clear(),

    /** 监听实时日志推送 */
    onEntry: (callback: (entry: LogEntry) => void): (() => void) =>
      window.api.log.onEntry(callback as (entry: unknown) => void) as unknown as () => void,

    /** 渲染进程发送日志到主进程 */
    send: (entry: LogEntry): void =>
      window.api.log.send(entry)
  }
}
