import { randomUUID } from 'crypto'
import { safeStorage } from 'electron'
import Store from 'electron-store'
import type { KafkaConnection, AppSettings } from '../../shared/types'
import { DEFAULT_SETTINGS } from '../../shared/types'

export type { AppSettings } from '../../shared/types'

/** 存储数据结构 */
interface StoreSchema {
  connections: KafkaConnection[]
  activeConnectionId: string | null
  settings: AppSettings
}

/** 连接持久化存储 */
const store = new Store<StoreSchema>({
  defaults: {
    connections: [],
    activeConnectionId: null,
    settings: DEFAULT_SETTINGS
  }
})

/** 加密密码（返回 base64 编码的加密字符串） */
function encryptPassword(password: string): string {
  if (!password) return ''
  return safeStorage.encryptString(password).toString('base64')
}

/** 解密密码 */
function decryptPassword(encrypted: string): string {
  if (!encrypted) return ''
  try {
    return safeStorage.decryptString(Buffer.from(encrypted, 'base64'))
  } catch {
    /* 如果解密失败（如旧数据未加密），返回原始值 */
    return encrypted
  }
}

/** 对连接列表中的密码进行解密（读取时使用） */
function decryptConnPasswords(conns: KafkaConnection[]): KafkaConnection[] {
  return conns.map((c) => {
    if (!c.sasl?.password) return c
    return {
      ...c,
      sasl: {
        ...c.sasl,
        password: decryptPassword(c.sasl.password)
      }
    }
  })
}

/** 获取所有连接（自动解密密码） */
export function list(): KafkaConnection[] {
  const raw = store.get('connections', [])
  return decryptConnPasswords(raw)
}

/** 保存连接（自动加密密码） */
export function save(conn: Partial<KafkaConnection> & { name: string; brokers: string[] }): KafkaConnection {
  const conns = store.get('connections', [])
  const now = Date.now()

  /* 加密密码 */
  const encryptedSasl = conn.sasl
    ? { ...conn.sasl, password: encryptPassword(conn.sasl.password) }
    : undefined

  if (conn.id) {
    const idx = conns.findIndex((c) => c.id === conn.id)
    if (idx >= 0) {
      conns[idx] = {
        ...conns[idx],
        ...conn,
        sasl: encryptedSasl ?? conns[idx].sasl,
        updatedAt: now
      }
      store.set('connections', conns)
      return decryptConnPasswords([conns[idx]])[0]
    }
  }

  const newConn: KafkaConnection = {
    id: randomUUID(),
    name: conn.name,
    brokers: conn.brokers,
    clientId: conn.clientId || 'kafka-client',
    ssl: conn.ssl ?? false,
    sasl: encryptedSasl,
    description: conn.description,
    createdAt: now,
    updatedAt: now
  }
  conns.push(newConn)
  store.set('connections', conns)
  return decryptConnPasswords([newConn])[0]
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
