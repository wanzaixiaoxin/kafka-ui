/**
 * 主进程日志服务
 * - 收集主进程所有日志（console 拦截 + 未捕获异常）
 * - 通过 IPC 实时推送到渲染进程
 * - 本地内存环形缓冲区保留最近 5000 条
 */
import { BrowserWindow } from 'electron'
import { randomUUID } from 'crypto'
import type { LogLevel, LogSource, LogEntry } from '../../shared/types'

const MAX_LOG_ENTRIES = 5000

/** 日志服务单例 */
class LogService {
  private buffer: LogEntry[] = []
  private win: BrowserWindow | null = null

  /** 设置主窗口引用，用于推送日志 */
  setWindow(win: BrowserWindow): void {
    this.win = win
  }

  /** 记录一条日志 */
  log(level: LogLevel, source: LogSource, message: string, data?: unknown, stack?: string, origin?: string): void {
    const entry: LogEntry = {
      id: randomUUID(),
      level,
      timestamp: Date.now(),
      source,
      message,
      data: data !== undefined ? (typeof data === 'string' ? data : JSON.stringify(data, null, 2)) : undefined,
      stack,
      origin
    }

    /* 写入缓冲区 */
    this.buffer.push(entry)
    if (this.buffer.length > MAX_LOG_ENTRIES) {
      this.buffer = this.buffer.slice(-MAX_LOG_ENTRIES)
    }

    /* 实时推送到渲染进程 */
    this.pushToRenderer(entry)
  }

  /** 获取所有缓冲日志 */
  getLogs(): LogEntry[] {
    return [...this.buffer]
  }

  /** 清空日志 */
  clear(): void {
    this.buffer = []
  }

  /** 推送单条日志到渲染进程 */
  private pushToRenderer(entry: LogEntry): void {
    try {
      if (this.win && !this.win.isDestroyed()) {
        this.win.webContents.send('log:entry', entry)
      }
    } catch {
      /* 忽略推送错误 */
    }
  }
}

export const logService = new LogService()
