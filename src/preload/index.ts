import { contextBridge, ipcRenderer } from 'electron'
import { electronAPI } from '@electron-toolkit/preload'
import { kafkaApi } from './kafkaApi'

/** 暴露 API 到渲染进程 */
if (process.contextIsolated) {
  try {
    contextBridge.exposeInMainWorld('electron', electronAPI)
    contextBridge.exposeInMainWorld('api', kafkaApi)
  } catch (error) {
    console.error(error)
  }
} else {
  // @ts-ignore fallback
  window.electron = electronAPI
  // @ts-ignore fallback
  window.api = kafkaApi
}
