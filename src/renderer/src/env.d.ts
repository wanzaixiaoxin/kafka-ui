/// <reference types="vite/client" />

interface Window {
  electron: unknown
  api: typeof import('../../preload/kafkaApi')['kafkaApi']
}
