import { contextBridge } from 'electron'
import { electronAPI } from '@electron-toolkit/preload'
import { kafkaApi } from './kafkaApi'

if (process.contextIsolated) {
  try {
    contextBridge.exposeInMainWorld('electron', electronAPI)
    contextBridge.exposeInMainWorld('api', kafkaApi)
  } catch (error) {
    console.error(error)
  }
} else {
  console.error('contextIsolation is disabled, refusing to expose APIs')
}
