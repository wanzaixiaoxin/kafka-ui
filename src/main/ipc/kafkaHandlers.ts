import { ipcMain, BrowserWindow } from 'electron'
import * as store from '../store/connectionStore'
import { connMgr } from '../kafka/connectionManager'
import { listTopics, describeTopic, getTopicOffsets } from '../kafka/topicService'
import { producerSvc } from '../kafka/producerService'
import { consumerSvc } from '../kafka/consumerService'
import { listGroups, describeGroup } from '../kafka/groupService'
import type { Admin, Kafka, Producer } from 'kafkajs'
import type { KafkaConnection, ConnectionTestResult } from '../../shared/types'

/** 获取当前激活连接 ID，无则返回错误 */
function requireActiveId(): string | { error: string } {
  const activeId = store.getActive()
  if (!activeId) {
    return { error: '没有激活的连接，请先选择一个连接' }
  }
  return activeId
}

/** 将错误转换为 ErrorResult */
function toErrorResult(err: unknown): { error: string } {
  const msg = err instanceof Error ? err.message : String(err)
  return { error: msg }
}

/**
 * 确保 Kafka 客户端存在（自动从存储恢复）。
 * 应用重启后 activeConnectionId 持久化了，但 connMgr 的内存 Map 为空，
 * 需要从存储中读取连接配置并重建 Kafka 客户端实例。
 */
function ensureKafkaClient(connId: string): Kafka | null {
  let kafka = connMgr.get(connId)
  if (kafka) return kafka
  /* 内存中不存在，从存储中查找连接配置并重建 */
  const found = store.list().find((c) => c.id === connId)
  if (!found) return null
  console.log(`[auto-reconnect] 为连接 ${found.name}(${connId.slice(0, 8)}) 重建 Kafka 客户端`)
  return connMgr.getActiveKafka(found)
}

/** 注册所有 Kafka 相关的 IPC 处理器 */
export function registerKafkaHandlers(): void {
  /* ---- 连接管理 ---- */

  ipcMain.handle('kafka:connection:list', (): KafkaConnection[] => {
    return store.list()
  })

  ipcMain.handle('kafka:connection:save', (_e, conn: Partial<KafkaConnection> & { name: string; brokers: string[] }): KafkaConnection => {
    return store.save(conn)
  })

  ipcMain.handle('kafka:connection:remove', async (_e, id: string): Promise<{ success: boolean; error?: string }> => {
    if (store.getActive() === id) {
      return { success: false, error: '无法删除当前正在使用的连接' }
    }
    await connMgr.disconnect(id)
    const ok = store.remove(id)
    return { success: ok }
  })

  ipcMain.handle('kafka:connection:test', async (_e, conn: KafkaConnection): Promise<ConnectionTestResult> => {
    return await connMgr.testConnection(conn)
  })

  ipcMain.handle('kafka:connection:use', (_e, id: string): { success: boolean; error?: string } => {
    const found = store.list().find((c) => c.id === id)
    if (!found) {
      return { success: false, error: '连接不存在' }
    }
    store.setActive(id)
    connMgr.getActiveKafka(found)
    consumerSvc.stopAll().catch(() => { /* 忽略 */ })
    /* 广播连接变更事件到所有渲染进程 */
    BrowserWindow.getAllWindows().forEach((w) => {
      if (!w.isDestroyed()) {
        w.webContents.send('kafka:connection:changed', id)
      }
    })
    return { success: true }
  })

  ipcMain.handle('kafka:connection:activeId', (): string | null => {
    return store.getActive()
  })

  /* ---- Topic 操作（使用池化 Admin） ---- */

  ipcMain.handle('kafka:topic:list', async (_e, showInternal?: boolean) => {
    const activeId = requireActiveId()
    if (typeof activeId === 'object') return activeId
    try {
      ensureKafkaClient(activeId)
      const admin = await connMgr.getAdmin(activeId)
      if (!admin) return { error: 'Kafka 客户端未就绪，请重新选择连接' }
      const topics = await listTopics(admin)
      if (!showInternal) {
        return topics.filter((t) => !t.isInternal)
      }
      return topics
    } catch (err: unknown) {
      return toErrorResult(err)
    }
  })

  ipcMain.handle('kafka:topic:describe', async (_e, topic: string) => {
    const activeId = requireActiveId()
    if (typeof activeId === 'object') return activeId
    try {
      ensureKafkaClient(activeId)
      const admin = await connMgr.getAdmin(activeId) as Admin
      return await describeTopic(admin, topic)
    } catch (err: unknown) {
      return toErrorResult(err)
    }
  })

  ipcMain.handle('kafka:topic:offsets', async (_e, topic: string) => {
    const activeId = requireActiveId()
    if (typeof activeId === 'object') return activeId
    try {
      ensureKafkaClient(activeId)
      const admin = await connMgr.getAdmin(activeId) as Admin
      return await getTopicOffsets(admin, topic)
    } catch (err: unknown) {
      return toErrorResult(err)
    }
  })

  ipcMain.handle('kafka:topic:messages', async (_e, opts) => {
    const activeId = requireActiveId()
    if (typeof activeId === 'object') return activeId
    const kafka = ensureKafkaClient(activeId)
    if (!kafka) return { error: 'Kafka 客户端未就绪，请重新选择连接' }
    try {
      const admin = await connMgr.getAdmin(activeId) as Admin
      return await consumerSvc.fetchMessages(kafka, opts, admin, activeId)
    } catch (err: unknown) {
      return toErrorResult(err)
    }
  })

  /* ---- 生产者（使用池化 Producer） ---- */

  ipcMain.handle('kafka:producer:send', async (_e, msg) => {
    const activeId = requireActiveId()
    if (typeof activeId === 'object') return activeId
    try {
      ensureKafkaClient(activeId)
      const producer = await connMgr.getProducer(activeId) as Producer
      return await producerSvc.send(producer, msg)
    } catch (err: unknown) {
      return toErrorResult(err)
    }
  })

  /* ---- 消费者 ---- */

  ipcMain.handle('kafka:consumer:start', async (event, opts) => {
    const activeId = requireActiveId()
    if (typeof activeId === 'object') return activeId
    const kafka = ensureKafkaClient(activeId)
    if (!kafka) return { error: 'Kafka 客户端未就绪，请重新选择连接' }
    try {
      const consumerId = await consumerSvc.start(kafka, opts, (msg) => {
        event.sender.send('kafka:consumer:message', msg)
      })
      return { consumerId }
    } catch (err: unknown) {
      return toErrorResult(err)
    }
  })

  ipcMain.handle('kafka:consumer:stop', async (_e, consumerId: string) => {
    try {
      await consumerSvc.stop(consumerId)
      return { success: true }
    } catch (err: unknown) {
      return { success: false, error: err instanceof Error ? err.message : String(err) }
    }
  })

  ipcMain.handle('kafka:consumer:stopAll', async () => {
    try {
      await consumerSvc.stopAll()
      return { success: true }
    } catch (err: unknown) {
      return { success: false, error: err instanceof Error ? err.message : String(err) }
    }
  })

  /* ---- 消费者组（使用池化 Admin） ---- */

  ipcMain.handle('kafka:group:list', async () => {
    const activeId = requireActiveId()
    if (typeof activeId === 'object') return activeId
    try {
      ensureKafkaClient(activeId)
      const admin = await connMgr.getAdmin(activeId) as Admin
      return await listGroups(admin)
    } catch (err: unknown) {
      return toErrorResult(err)
    }
  })

  ipcMain.handle('kafka:group:describe', async (_e, groupId: string) => {
    const activeId = requireActiveId()
    if (typeof activeId === 'object') return activeId
    try {
      ensureKafkaClient(activeId)
      const admin = await connMgr.getAdmin(activeId) as Admin
      return await describeGroup(admin, groupId)
    } catch (err: unknown) {
      return toErrorResult(err)
    }
  })

  /* ---- 设置 ---- */

  ipcMain.handle('settings:get', () => {
    return store.getSettings()
  })

  ipcMain.handle('settings:update', (_e, s: Partial<store.AppSettings>) => {
    return store.updateSettings(s)
  })
}
