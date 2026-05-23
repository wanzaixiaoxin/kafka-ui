/**
 * 日志 IPC 处理器和拦截器
 * - 拦截主进程 console 方法
 * - 捕获未处理异常和 Promise 拒绝
 * - 注册 IPC 通道供渲染进程查询/清除日志
 */
import { ipcMain } from 'electron'
import { logService } from '../logging/logService'
import type { LogEntry } from '../../shared/types'

/** 保存原始 console 方法 */
const origConsole = {
  log: console.log,
  warn: console.warn,
  error: console.error,
  info: console.info,
  debug: console.debug
}

/** 安装主进程 console 拦截器 */
function interceptConsole(): void {
  const fmt = (args: unknown[]): string =>
    args.map((a) => (typeof a === 'string' ? a : typeof a === 'object' ? JSON.stringify(a, null, 2) : String(a))).join(' ')

  console.log = (...args: unknown[]) => {
    origConsole.log(...args)
    logService.log('info', 'main', fmt(args))
  }
  console.info = (...args: unknown[]) => {
    origConsole.info(...args)
    logService.log('info', 'main', fmt(args))
  }
  console.warn = (...args: unknown[]) => {
    origConsole.warn(...args)
    logService.log('warn', 'main', fmt(args))
  }
  console.error = (...args: unknown[]) => {
    origConsole.error(...args)
    /* 尝试从参数中提取 Error 对象的堆栈 */
    let stack: string | undefined
    for (const arg of args) {
      if (arg instanceof Error) {
        stack = arg.stack
        break
      }
    }
    logService.log('error', 'main', fmt(args), undefined, stack)
  }
  console.debug = (...args: unknown[]) => {
    origConsole.debug(...args)
    logService.log('debug', 'main', fmt(args))
  }
}

/** 安装全局异常捕获 */
function interceptExceptions(): void {
  process.on('uncaughtException', (err: Error) => {
    origConsole.error('[Uncaught Exception]', err)
    logService.log('fatal', 'main', `Uncaught Exception: ${err.message}`, undefined, err.stack)
  })

  process.on('unhandledRejection', (reason: unknown) => {
    const msg = reason instanceof Error ? reason.message : String(reason)
    const stack = reason instanceof Error ? reason.stack : undefined
    origConsole.error('[Unhandled Rejection]', reason)
    logService.log('error', 'main', `Unhandled Rejection: ${msg}`, undefined, stack)
  })
}

/** 注册日志相关 IPC 通道 */
export function registerLogHandlers(): void {
  /* 安装拦截器 */
  interceptConsole()
  interceptExceptions()

  /* 获取所有缓冲日志 */
  ipcMain.handle('log:getAll', (): LogEntry[] => {
    return logService.getLogs()
  })

  /* 清空日志 */
  ipcMain.handle('log:clear', (): void => {
    logService.clear()
  })

  /* 渲染进程转发日志到主进程（用于统一展示） */
  ipcMain.on('log:renderer', (_event, entry: LogEntry) => {
    logService.log(entry.level, 'renderer', entry.message, entry.data, entry.stack, entry.origin)
  })
}
