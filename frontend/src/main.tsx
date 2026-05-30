import React from 'react'
import ReactDOM from 'react-dom/client'
import App from './App'

/** 在模块顶层拦截 console，确保只执行一次 */
import './utils/consoleInterceptor'

ReactDOM.createRoot(document.getElementById('root')!).render(
  <React.StrictMode>
    <App />
  </React.StrictMode>
)
