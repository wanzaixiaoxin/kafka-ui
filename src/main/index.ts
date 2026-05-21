import { app, shell, BrowserWindow } from 'electron'
import { join } from 'path'
import { electronApp, optimizer, is } from '@electron-toolkit/utils'
import { registerKafkaHandlers } from './ipc/kafkaHandlers'
import Store from 'electron-store'

/** 窗口状态存储 */
interface WindowState {
  x?: number
  y?: number
  width: number
  height: number
  isMaximized: boolean
}

const windowStore = new Store<{ windowState: WindowState }>({
  name: 'window-state',
  defaults: {
    windowState: {
      width: 1200,
      height: 800,
      isMaximized: false
    }
  }
})

let mainWindow: BrowserWindow | null = null

/** 保存窗口状态 */
function saveWindowState(): void {
  if (!mainWindow) return
  const bounds = mainWindow.getBounds()
  const isMaximized = mainWindow.isMaximized()
  windowStore.set('windowState', {
    x: bounds.x,
    y: bounds.y,
    width: bounds.width,
    height: bounds.height,
    isMaximized
  })
}

/** 创建主窗口 */
function createWindow(): void {
  const saved = windowStore.get('windowState')

  mainWindow = new BrowserWindow({
    title: 'Kafka Client',
    width: saved.width,
    height: saved.height,
    minWidth: 1024,
    minHeight: 680,
    x: saved.x,
    y: saved.y,
    show: false,
    autoHideMenuBar: true,
    webPreferences: {
      preload: join(__dirname, '../preload/index.js'),
      sandbox: false
    }
  })

  /* 恢复最大化状态 */
  if (saved.isMaximized) {
    mainWindow.maximize()
  }

  mainWindow.on('ready-to-show', () => {
    mainWindow?.show()
  })

  /* 窗口关闭时保存状态 */
  mainWindow.on('close', () => {
    saveWindowState()
  })

  mainWindow.webContents.setWindowOpenHandler((details) => {
    shell.openExternal(details.url)
    return { action: 'deny' }
  })

  if (is.dev && process.env['ELECTRON_RENDERER_URL']) {
    mainWindow.loadURL(process.env['ELECTRON_RENDERER_URL'])
  } else {
    mainWindow.loadFile(join(__dirname, '../renderer/index.html'))
  }
}

/** 应用关闭前停止所有消费者 */
async function stopAllConsumers(): Promise<void> {
  try {
    const { consumerSvc } = await import('./kafka/consumerService')
    await consumerSvc.stopAll()
  } catch {
    /* 忽略停止消费者时的错误 */
  }
}

app.whenReady().then(() => {
  electronApp.setAppUserModelId('com.kafka-client.app')

  app.on('browser-window-created', (_, window) => {
    optimizer.watchWindowShortcuts(window)
  })

  /* 注册 Kafka IPC 处理器 */
  registerKafkaHandlers()

  createWindow()

  app.on('activate', () => {
    if (BrowserWindow.getAllWindows().length === 0) createWindow()
  })
})

/* 应用关闭前停止所有消费者 */
app.on('before-quit', async () => {
  await stopAllConsumers()
})

/* 所有窗口关闭时退出应用（Windows 平台） */
app.on('window-all-closed', () => {
  app.quit()
})
