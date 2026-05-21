/** 设置项类型定义 */
export interface AppSettings {
  /** 消息最大保留数量 */
  maxMessages: number
  /** 自动刷新间隔（秒），0 表示禁用 */
  autoRefreshInterval: number
  /** 主题模式 */
  theme: 'light' | 'dark' | 'system'
}

/** 默认设置 */
const DEFAULTS: AppSettings = {
  maxMessages: 500,
  autoRefreshInterval: 0,
  theme: 'light'
}

const STORAGE_KEY = 'kafka-client-settings'

/** 读取设置 */
export function loadSettings(): AppSettings {
  try {
    const raw = localStorage.getItem(STORAGE_KEY)
    if (!raw) return { ...DEFAULTS }
    const parsed = JSON.parse(raw)
    return { ...DEFAULTS, ...parsed }
  } catch {
    return { ...DEFAULTS }
  }
}

/** 保存设置 */
export function saveSettings(s: Partial<AppSettings>): AppSettings {
  const cur = loadSettings()
  const next = { ...cur, ...s }
  localStorage.setItem(STORAGE_KEY, JSON.stringify(next))
  return next
}

/** 获取单个设置值 */
export function getSetting<K extends keyof AppSettings>(key: K): AppSettings[K] {
  return loadSettings()[key]
}
