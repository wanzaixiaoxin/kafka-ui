/** 应用设置类型（与主进程 connectionStore 保持一致） */
export interface AppSettings {
  maxMessages: number
  autoRefreshInterval: number
  theme: 'light' | 'dark' | 'system'
}

/** 读取设置 - 通过 IPC 从主进程 electron-store 获取 */
export async function loadSettings(): Promise<AppSettings> {
  try {
    return await window.api.settings.get() as AppSettings
  } catch {
    return { maxMessages: 500, autoRefreshInterval: 0, theme: 'light' }
  }
}

/** 保存设置 */
export async function saveSettings(s: Partial<AppSettings>): Promise<AppSettings> {
  return await window.api.settings.update(s) as AppSettings
}

/** 获取单个设置值（同步缓存版本，用于不支持 async 的场景） */
let _cachedSettings: AppSettings | null = null

export function setCachedSettings(s: AppSettings): void {
  _cachedSettings = s
}

export function getCachedSettings(): AppSettings | null {
  return _cachedSettings
}
