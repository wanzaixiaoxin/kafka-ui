import { randomUUID } from 'crypto'
import Store from 'electron-store'
import type { KafkaConnection } from '../../shared/types'

/** 应用设置类型 */
export interface AppSettings {
  maxMessages: number
  autoRefreshInterval: number
  theme: 'light' | 'dark' | 'system'
}

/** 存储数据结构 */
interface StoreSchema {
  connections: KafkaConnection[]
  activeConnectionId: string | null
  settings: AppSettings
}

const DEFAULT_SETTINGS: AppSettings = {
  maxMessages: 500,
  autoRefreshInterval: 0,
  theme: 'light'
}

/** 连接持久化存储 */
const store = new Store<StoreSchema>({
  defaults: {
    connections: [],
    activeConnectionId: null,
    settings: DEFAULT_SETTINGS
  }
})

/** 获取所有连接 */
export function list(): KafkaConnection[] {
  return store.get('connections', [])
}

/** 保存连接（新增或更新） */
export function save(conn: Partial<KafkaConnection> & { name: string; brokers: string[] }): KafkaConnection {
  const conns = list()
  const now = Date.now()

  if (conn.id) {
    const idx = conns.findIndex((c) => c.id === conn.id)
    if (idx >= 0) {
      conns[idx] = { ...conns[idx], ...conn, updatedAt: now }
      store.set('connections', conns)
      return conns[idx]
    }
  }

  const newConn: KafkaConnection = {
    id: randomUUID(),
    name: conn.name,
    brokers: conn.brokers,
    clientId: conn.clientId || 'kafka-client',
    ssl: conn.ssl ?? false,
    sasl: conn.sasl,
    description: conn.description,
    createdAt: now,
    updatedAt: now
  }
  conns.push(newConn)
  store.set('connections', conns)
  return newConn
}

/** 删除连接 */
export function remove(id: string): boolean {
  const conns = list().filter((c) => c.id !== id)
  if (conns.length === list().length) return false
  store.set('connections', conns)
  if (store.get('activeConnectionId') === id) {
    store.set('activeConnectionId', null)
  }
  return true
}

/** 获取当前激活的连接 ID */
export function getActive(): string | null {
  return store.get('activeConnectionId', null)
}

/** 设置当前激活的连接 ID */
export function setActive(id: string | null): void {
  store.set('activeConnectionId', id)
}

/** 获取设置 */
export function getSettings(): AppSettings {
  return store.get('settings')
}

/** 更新设置 */
export function updateSettings(s: Partial<AppSettings>): AppSettings {
  const cur = getSettings()
  const next = { ...cur, ...s }
  store.set('settings', next)
  return next
}
