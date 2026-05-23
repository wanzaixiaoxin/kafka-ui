import { ipcMain } from 'electron'
import * as store from '../store/connectionStore'
import { connMgr } from '../kafka/connectionManager'
import { listTopics, describeTopic, getTopicOffsets } from '../kafka/topicService'
import { producerSvc } from '../kafka/producerService'
import { consumerSvc } from '../kafka/consumerService'
import { listGroups, describeGroup } from '../kafka/groupService'
import type { KafkaConnection, ConnectionTestResult } from '../../renderer/src/types/kafka'

/** 注册所有 Kafka 相关的 IPC 处理器 */
export function registerKafkaHandlers(): void {
  /* ---- 连接管理 ---- */

  /** 获取所有连接列表 */
  ipcMain.handle('kafka:connection:list', (): KafkaConnection[] => {
    return store.list()
  })

  /** 保存连接（新增或更新） */
  ipcMain.handle('kafka:connection:save', (_e, conn: Partial<KafkaConnection> & { name: string; brokers: string[] }): KafkaConnection => {
    return store.save(conn)
  })

  /** 删除连接 */
  ipcMain.handle('kafka:connection:remove', (_e, id: string): { success: boolean; error?: string } => {
    /* 不允许删除当前激活连接 */
    if (store.getActive() === id) {
      return { success: false, error: '无法删除当前正在使用的连接' }
    }
    connMgr.disconnect(id)
    const ok = store.remove(id)
    return { success: ok }
  })

  /** 测试连接 */
  ipcMain.handle('kafka:connection:test', async (_e, conn: KafkaConnection): Promise<ConnectionTestResult> => {
    return await connMgr.testConnection(conn)
  })

  /** 切换激活连接 */
  ipcMain.handle('kafka:connection:use', (_e, id: string): { success: boolean; error?: string } => {
    const found = store.list().find((c) => c.id === id)
    if (!found) {
      return { success: false, error: '连接不存在' }
    }
    store.setActive(id)
    /* 预创建 Kafka 实例 */
    connMgr.getActiveKafka(found)
    /* 切换连接时停止所有消费者 + 清理查询池 */
    consumerSvc.stopAll().catch(() => { /* 忽略 */ })
    consumerSvc.resetPool().catch(() => { /* 忽略 */ })
    return { success: true }
  })

  /* ---- 通用 Kafka 操作 ---- */

  ipcMain.handle('kafka:connect', async (_e, _connId: string) => {
    return { success: true }
  })

  ipcMain.handle('kafka:disconnect', async (_e, _connId: string) => {
    return { success: true }
  })

  /* ---- Topic 操作 ---- */

  /** 获取 Topic 列表，可选是否显示内部 Topic */
  ipcMain.handle('kafka:topic:list', async (_e, showInternal?: boolean) => {
    const activeId = store.getActive()
    if (!activeId) {
      return { error: '没有激活的连接，请先选择一个连接' }
    }
    const kafka = connMgr.get(activeId)
    if (!kafka) {
      return { error: 'Kafka 客户端未就绪，请重新选择连接' }
    }
    try {
      const topics = await listTopics(kafka)
      /* 根据 showInternal 参数过滤内部 Topic */
      if (!showInternal) {
        return topics.filter((t) => !t.isInternal)
      }
      return topics
    } catch (err: unknown) {
      const msg = err instanceof Error ? err.message : String(err)
      return { error: msg }
    }
  })

  /** 获取 Topic 详情 */
  ipcMain.handle('kafka:topic:describe', async (_e, topic: string) => {
    const activeId = store.getActive()
    if (!activeId) {
      return { error: '没有激活的连接，请先选择一个连接' }
    }
    const kafka = connMgr.get(activeId)
    if (!kafka) {
      return { error: 'Kafka 客户端未就绪，请重新选择连接' }
    }
    try {
      const detail = await describeTopic(kafka, topic)
      return detail
    } catch (err: unknown) {
      const msg = err instanceof Error ? err.message : String(err)
      return { error: msg }
    }
  })

  /** 获取 Topic Offset 信息 */
  ipcMain.handle('kafka:topic:offsets', async (_e, topic: string) => {
    const activeId = store.getActive()
    if (!activeId) {
      return { error: '没有激活的连接，请先选择一个连接' }
    }
    const kafka = connMgr.get(activeId)
    if (!kafka) {
      return { error: 'Kafka 客户端未就绪，请重新选择连接' }
    }
    try {
      const offsets = await getTopicOffsets(kafka, topic)
      return offsets
    } catch (err: unknown) {
      const msg = err instanceof Error ? err.message : String(err)
      return { error: msg }
    }
  })

  /** 拉取 Topic 消息（一次性批量拉取） */
  ipcMain.handle('kafka:topic:messages', async (_e, opts) => {
    const activeId = store.getActive()
    if (!activeId) {
      return { error: '没有激活的连接，请先选择一个连接' }
    }
    const kafka = connMgr.get(activeId)
    if (!kafka) {
      return { error: 'Kafka 客户端未就绪，请重新选择连接' }
    }
    try {
      const messages = await consumerSvc.fetchMessages(kafka, opts)
      return messages
    } catch (err: unknown) {
      const msg = err instanceof Error ? err.message : String(err)
      return { error: msg }
    }
  })

  /* ---- 旧版 Topic 操作（保留兼容） ---- */
  ipcMain.handle('kafka:listTopics', async (_e, _connId: string) => {
    return []
  })

  ipcMain.handle('kafka:getTopicDetail', async (_e, _connId: string, _topic: string) => {
    return null
  })

  ipcMain.handle('kafka:createTopic', async (_e, _connId: string, _config: unknown) => {
    return { success: true }
  })

  ipcMain.handle('kafka:deleteTopic', async (_e, _connId: string, _topic: string) => {
    return { success: true }
  })

  /* ---- 生产者 ---- */
  ipcMain.handle('kafka:produce', async (_e, _connId: string, _msg: unknown) => {
    return { success: true }
  })

  /** 发送消息 */
  ipcMain.handle('kafka:producer:send', async (event, msg) => {
    const activeId = store.getActive()
    if (!activeId) {
      return { error: '没有激活的连接，请先选择一个连接' }
    }
    const kafka = connMgr.get(activeId)
    if (!kafka) {
      return { error: 'Kafka 客户端未就绪，请重新选择连接' }
    }
    try {
      const result = await producerSvc.send(kafka, msg)
      return result
    } catch (err: unknown) {
      const msg = err instanceof Error ? err.message : String(err)
      return { error: msg }
    }
  })

  /* ---- 消费者 ---- */
  ipcMain.handle('kafka:consume', async (_e, _connId: string, _config: unknown) => {
    return []
  })

  ipcMain.handle('kafka:stopConsume', async (_e, _connId: string) => {
    return { success: true }
  })

  /** 启动消费者 */
  ipcMain.handle('kafka:consumer:start', async (event, opts) => {
    const activeId = store.getActive()
    if (!activeId) {
      return { error: '没有激活的连接，请先选择一个连接' }
    }
    const kafka = connMgr.get(activeId)
    if (!kafka) {
      return { error: 'Kafka 客户端未就绪，请重新选择连接' }
    }
    try {
      const consumerId = await consumerSvc.start(kafka, opts, (msg) => {
        /* 通过 IPC 事件推送消息到渲染进程 */
        event.sender.send('kafka:consumer:message', msg)
      })
      return { consumerId }
    } catch (err: unknown) {
      const errMsg = err instanceof Error ? err.message : String(err)
      return { error: errMsg }
    }
  })

  /** 停止消费者 */
  ipcMain.handle('kafka:consumer:stop', async (_e, consumerId: string) => {
    try {
      await consumerSvc.stop(consumerId)
      return { success: true }
    } catch (err: unknown) {
      const msg = err instanceof Error ? err.message : String(err)
      return { success: false, error: msg }
    }
  })

  /** 停止所有消费者 */
  ipcMain.handle('kafka:consumer:stopAll', async () => {
    try {
      await consumerSvc.stopAll()
      return { success: true }
    } catch (err: unknown) {
      const msg = err instanceof Error ? err.message : String(err)
      return { success: false, error: msg }
    }
  })

  /* ---- 消费者组 ---- */

  /** 获取消费者组列表 */
  ipcMain.handle('kafka:group:list', async () => {
    const activeId = store.getActive()
    if (!activeId) {
      return { error: '没有激活的连接，请先选择一个连接' }
    }
    const kafka = connMgr.get(activeId)
    if (!kafka) {
      return { error: 'Kafka 客户端未就绪，请重新选择连接' }
    }
    try {
      const groups = await listGroups(kafka)
      return groups
    } catch (err: unknown) {
      const msg = err instanceof Error ? err.message : String(err)
      return { error: msg }
    }
  })

  /** 获取消费者组详情 */
  ipcMain.handle('kafka:group:describe', async (_e, groupId: string) => {
    const activeId = store.getActive()
    if (!activeId) {
      return { error: '没有激活的连接，请先选择一个连接' }
    }
    const kafka = connMgr.get(activeId)
    if (!kafka) {
      return { error: 'Kafka 客户端未就绪，请重新选择连接' }
    }
    try {
      const detail = await describeGroup(kafka, groupId)
      return detail
    } catch (err: unknown) {
      const msg = err instanceof Error ? err.message : String(err)
      return { error: msg }
    }
  })

  ipcMain.handle('kafka:listGroups', async (_e, _connId: string) => {
    return []
  })

  ipcMain.handle('kafka:getGroupDetail', async (_e, _connId: string, _groupId: string) => {
    return null
  })
}
