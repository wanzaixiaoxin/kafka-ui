import { emit } from '@tauri-apps/api/event'
import type { LogLevel } from '../types/kafka'

/**
 * 拦截渲染进程 console 方法
 * 将日志转发到 Rust 后端统一收集
 * 作为 side-effect 模块，import 即生效，确保只执行一次
 */

const orig = {
  log: console.log,
  warn: console.warn,
  error: console.error,
  info: console.info
}

const send = (level: LogLevel, args: unknown[]): void => {
  try {
    const msg = args
      .map((a) =>
        typeof a === 'string'
          ? a
          : a instanceof Error
            ? `${a.message}\n${a.stack || ''}`
            : typeof a === 'object'
              ? JSON.stringify(a, null, 2)
              : String(a)
      )
      .join(' ')

    let stack: string | undefined
    for (const a of args) {
      if (a instanceof Error) {
        stack = a.stack
        break
      }
    }

    emit('log:renderer', {
      id: crypto.randomUUID(),
      level,
      timestamp: Date.now(),
      source: 'renderer' as const,
      message: msg,
      stack
    })
  } catch {
    /* 忽略 */
  }
}

console.log = (...args) => { orig.log(...args); send('info', args) }
console.info = (...args) => { orig.info(...args); send('info', args) }
console.warn = (...args) => { orig.warn(...args); send('warn', args) }
console.error = (...args) => { orig.error(...args); send('error', args) }
console.debug = (...args) => { orig.debug?.(...args); send('debug', args) }

window.addEventListener('error', (event) => {
  send('error', [event.error || event.message])
})
window.addEventListener('unhandledrejection', (event) => {
  send('error', [event.reason])
})
