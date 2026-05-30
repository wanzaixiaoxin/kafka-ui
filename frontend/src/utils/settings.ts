import { invoke } from '@tauri-apps/api/core'
import type { AppSettings } from '../types/kafka'
import { DEFAULT_SETTINGS } from '../types/kafka'

export type { AppSettings } from '../types/kafka'
export { DEFAULT_SETTINGS } from '../types/kafka'

/** 读取设置 - 通过 Tauri invoke 从 Rust 后端获取 */
export async function loadSettings(): Promise<AppSettings> {
  try {
    return await invoke('settings_get') as AppSettings
  } catch {
    return { ...DEFAULT_SETTINGS }
  }
}

/** 保存设置 */
export async function saveSettings(s: Partial<AppSettings>): Promise<AppSettings> {
  return await invoke('settings_update', { settings: s }) as AppSettings
}

/** 获取单个设置值（同步缓存版本，用于不支持 async 的场景） */
let _cachedSettings: AppSettings | null = null

export function setCachedSettings(s: AppSettings): void {
  _cachedSettings = s
}

export function getCachedSettings(): AppSettings | null {
  return _cachedSettings
}
