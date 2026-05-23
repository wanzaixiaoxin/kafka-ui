import { ipcRenderer } from 'electron'
import type { KafkaConnection, ConnectionTestResult } from '../renderer/src/types/kafka'

/** 连接管理 API */
export const connectionApi = {
  /** 获取所有连接列表 */
  list: (): Promise<KafkaConnection[]> =>
    ipcRenderer.invoke('kafka:connection:list'),

  /** 保存连接 */
  save: (conn: Partial<KafkaConnection> & { name: string; brokers: string[] }): Promise<KafkaConnection> =>
    ipcRenderer.invoke('kafka:connection:save', conn),

  /** 删除连接 */
  remove: (id: string): Promise<{ success: boolean; error?: string }> =>
    ipcRenderer.invoke('kafka:connection:remove', id),

  /** 测试连接 */
  test: (conn: KafkaConnection): Promise<ConnectionTestResult> =>
    ipcRenderer.invoke('kafka:connection:test', conn),

  /** 切换激活连接 */
  use: (id: string): Promise<{ success: boolean; error?: string }> =>
    ipcRenderer.invoke('kafka:connection:use', id)
}

/** Preload 层 Kafka API - 桥接渲染进程与主进程 */
export const kafkaApi = {
  /* ---- 连接管理 ---- */
  connections: connectionApi,

  /** 连接到 Kafka */
  connect: (connId: string): Promise<{ success: boolean }> =>
    ipcRenderer.invoke('kafka:connect', connId),

  /** 断开连接 */
  disconnect: (connId: string): Promise<{ success: boolean }> =>
    ipcRenderer.invoke('kafka:disconnect', connId),

  /* ---- Topic 操作 ---- */
  topics: {
    /** 获取 Topic 列表 */
    list: (showInternal?: boolean): Promise<unknown> =>
      ipcRenderer.invoke('kafka:topic:list', showInternal),

    /** 获取 Topic 详情 */
    describe: (topic: string): Promise<unknown> =>
      ipcRenderer.invoke('kafka:topic:describe', topic),

    /** 获取 Topic Offset */
    offsets: (topic: string): Promise<unknown> =>
      ipcRenderer.invoke('kafka:topic:offsets', topic),

    /** 拉取 Topic 消息（一次性批量拉取） */
    messages: (opts: unknown): Promise<unknown> =>
      ipcRenderer.invoke('kafka:topic:messages', opts)
  },

  /* ---- 旧版 Topic 操作（保留兼容） ---- */
  listTopics: (connId: string): Promise<unknown[]> =>
    ipcRenderer.invoke('kafka:listTopics', connId),

  getTopicDetail: (connId: string, topic: string): Promise<unknown | null> =>
    ipcRenderer.invoke('kafka:getTopicDetail', connId, topic),

  createTopic: (connId: string, config: unknown): Promise<{ success: boolean }> =>
    ipcRenderer.invoke('kafka:createTopic', connId, config),

  deleteTopic: (connId: string, topic: string): Promise<{ success: boolean }> =>
    ipcRenderer.invoke('kafka:deleteTopic', connId, topic),

  /* ---- 生产者 ---- */
  produce: (connId: string, msg: unknown): Promise<{ success: boolean }> =>
    ipcRenderer.invoke('kafka:produce', connId, msg),

  /** 发送消息 */
  producer: {
    send: (msg: unknown): Promise<unknown> =>
      ipcRenderer.invoke('kafka:producer:send', msg)
  },

  /* ---- 消费者 ---- */
  consume: (connId: string, config: unknown): Promise<unknown[]> =>
    ipcRenderer.invoke('kafka:consume', connId, config),

  stopConsume: (connId: string): Promise<{ success: boolean }> =>
    ipcRenderer.invoke('kafka:stopConsume', connId),

  /** 消费者 API */
  consumer: {
    /** 启动消费者 */
    start: (opts: unknown): Promise<unknown> =>
      ipcRenderer.invoke('kafka:consumer:start', opts),

    /** 停止消费者 */
    stop: (consumerId: string): Promise<unknown> =>
      ipcRenderer.invoke('kafka:consumer:stop', consumerId),

    /** 停止所有消费者 */
    stopAll: (): Promise<unknown> =>
      ipcRenderer.invoke('kafka:consumer:stopAll'),

    /** 监听消费消息事件 */
    onMessage: (callback: (msg: unknown) => void): (() => void) => {
      const handler = (_event: unknown, msg: unknown): void => callback(msg)
      ipcRenderer.on('kafka:consumer:message', handler)
      return () => {
        ipcRenderer.removeListener('kafka:consumer:message', handler)
      }
    }
  },

  /* ---- 消费者组 ---- */

  /** 消费者组 API */
  groups: {
    /** 获取消费者组列表 */
    list: (): Promise<unknown> =>
      ipcRenderer.invoke('kafka:group:list'),

    /** 获取消费者组详情 */
    describe: (groupId: string): Promise<unknown> =>
      ipcRenderer.invoke('kafka:group:describe', groupId)
  },

  listGroups: (connId: string): Promise<unknown[]> =>
    ipcRenderer.invoke('kafka:listGroups', connId),

  getGroupDetail: (connId: string, groupId: string): Promise<unknown | null> =>
    ipcRenderer.invoke('kafka:getGroupDetail', connId, groupId),

  /* ---- 日志 ---- */
  log: {
    getAll: (): Promise<unknown> =>
      ipcRenderer.invoke('log:getAll'),

    clear: (): Promise<void> =>
      ipcRenderer.invoke('log:clear'),

    onEntry: (callback: (entry: unknown) => void): (() => void) => {
      const handler = (_event: unknown, entry: unknown): void => callback(entry)
      ipcRenderer.on('log:entry', handler)
      return () => {
        ipcRenderer.removeListener('log:entry', handler)
      }
    },

    send: (entry: unknown): void => {
      ipcRenderer.send('log:renderer', entry)
    }
  }
}
