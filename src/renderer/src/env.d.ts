/// <reference types="vite/client" />

interface Window {
  electron: unknown
  api: typeof import('./services/kafkaApiClient')['kafkaApiClient']
}
