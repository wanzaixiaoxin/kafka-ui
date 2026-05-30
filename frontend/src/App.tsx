import { useState, useEffect } from 'react'
import { HashRouter, Routes, Route } from 'react-router-dom'
import { ConfigProvider, theme } from 'antd'
import zhCN from 'antd/locale/zh_CN'
import ErrorBoundary from './components/ErrorBoundary'
import AppLayout from './components/AppLayout'
import Connections from './pages/Connections'
import Topics from './pages/Topics'
import TopicDetail from './pages/TopicDetail'
import Producer from './pages/Producer'
import Consumer from './pages/Consumer'
import ConsumerGroups from './pages/ConsumerGroups'
import Settings from './pages/Settings'
import { loadSettings, setCachedSettings, DEFAULT_SETTINGS, type AppSettings } from './utils/settings'

/** 根据设置获取 antd 主题算法 */
function getThemeAlgorithm(settingsTheme: string) {
  switch (settingsTheme) {
    case 'dark':
      return theme.darkAlgorithm
    case 'system':
      return window.matchMedia('(prefers-color-scheme: dark)').matches
        ? theme.darkAlgorithm
        : theme.defaultAlgorithm
    default:
      return theme.defaultAlgorithm
  }
}

/** 应用根组件 - 配置路由、主题和错误边界 */
export default function App(): JSX.Element {
  const [settings, setSettings] = useState<AppSettings>(DEFAULT_SETTINGS)

  useEffect(() => {
    loadSettings().then((s) => {
      setSettings(s)
      setCachedSettings(s)
    })
  }, [])

  const algorithm = getThemeAlgorithm(settings.theme)

  return (
    <ErrorBoundary>
      <ConfigProvider
        locale={zhCN}
        theme={{
          algorithm,
          token: {
            borderRadius: 4,
            fontSize: 13
          }
        }}
      >
        <HashRouter>
          <Routes>
            <Route path="/" element={<AppLayout />}>
              <Route index element={<Connections />} />
              <Route path="connections" element={<Connections />} />
              <Route path="topics" element={<Topics />} />
              <Route path="topics/:topicName" element={<TopicDetail />} />
              <Route path="producer" element={<Producer />} />
              <Route path="consumer" element={<Consumer />} />
              <Route path="groups" element={<ConsumerGroups />} />
              <Route path="settings" element={<Settings />} />
            </Route>
          </Routes>
        </HashRouter>
      </ConfigProvider>
    </ErrorBoundary>
  )
}
