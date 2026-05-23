import { useState, useEffect, useCallback, useRef } from 'react'
import { Outlet, useNavigate, useLocation } from 'react-router-dom'
import { Layout, Menu, Button, Space, Breadcrumb, Badge } from 'antd'
import {
  ApiOutlined,
  UnorderedListOutlined,
  SendOutlined,
  InboxOutlined,
  TeamOutlined,
  SettingOutlined,
  ReloadOutlined,
  HomeOutlined,
  BugOutlined
} from '@ant-design/icons'
import type { MenuProps } from 'antd'
import ConnectionSelector from './ConnectionSelector'
import DevToolsPanel from './DevToolsPanel'
import { kafkaApiClient } from '../services/kafkaApiClient'
import type { KafkaConnection, LogLevel } from '../types/kafka'

const { Header, Sider, Content } = Layout

/** 导航菜单项 */
const menuItems: MenuProps['items'] = [
  { key: '/connections', icon: <ApiOutlined />, label: '连接管理' },
  { key: '/topics', icon: <UnorderedListOutlined />, label: 'Topic 管理' },
  { key: '/producer', icon: <SendOutlined />, label: '消息生产' },
  { key: '/consumer', icon: <InboxOutlined />, label: '消息消费' },
  { key: '/groups', icon: <TeamOutlined />, label: '消费者组' },
  { key: '/settings', icon: <SettingOutlined />, label: '设置' }
]

/** 路由到面包屑名称的映射 */
const breadcrumbMap: Record<string, string> = {
  '/connections': '连接管理',
  '/topics': 'Topic 管理',
  '/producer': '消息生产',
  '/consumer': '消息消费',
  '/groups': '消费者组',
  '/settings': '设置'
}

/**
 * 拦截渲染进程 console 方法
 * 将日志转发到主进程统一收集
 */
function interceptRendererConsole(): void {
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

      kafkaApiClient.log.send({
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

  /* 捕获渲染进程未处理异常 */
  window.addEventListener('error', (event) => {
    send('error', [event.error || event.message])
  })
  window.addEventListener('unhandledrejection', (event) => {
    send('error', [event.reason])
  })
}

/** 应用布局组件 */
export default function AppLayout(): JSX.Element {
  const navigate = useNavigate()
  const location = useLocation()
  const [collapsed, setCollapsed] = useState(false)
  const [activeConn, setActiveConn] = useState<KafkaConnection | null>(null)
  const [devToolsOpen, setDevToolsOpen] = useState(false)
  const [devToolsHeight, setDevToolsHeight] = useState(300)
  const [errorCount, setErrorCount] = useState(0)
  const resizeRef = useRef<{ startY: number; startH: number } | null>(null)

  /** 初始化渲染进程日志拦截（只执行一次） */
  useEffect(() => {
    interceptRendererConsole()
  }, [])

  /** 监听实时日志统计 error 数量 */
  useEffect(() => {
    const unsub = kafkaApiClient.log.onEntry((entry) => {
      if (entry.level === 'error' || entry.level === 'fatal') {
        setErrorCount((c) => c + 1)
      }
    })
    return () => { unsub() }
  }, [])

  /** F12 快捷键切换 DevTools */
  useEffect(() => {
    const handler = (e: KeyboardEvent): void => {
      if (e.key === 'F12') {
        e.preventDefault()
        setDevToolsOpen((prev) => !prev)
      }
    }
    window.addEventListener('keydown', handler)
    return () => window.removeEventListener('keydown', handler)
  }, [])

  /** 拖拽调整面板高度 */
  const onDragStart = (e: React.MouseEvent): void => {
    e.preventDefault()
    resizeRef.current = { startY: e.clientY, startH: devToolsHeight }
    const onMouseMove = (ev: MouseEvent): void => {
      if (!resizeRef.current) return
      const delta = resizeRef.current.startY - ev.clientY
      const newH = Math.max(150, Math.min(600, resizeRef.current.startH + delta))
      setDevToolsHeight(newH)
    }
    const onMouseUp = (): void => {
      resizeRef.current = null
      window.removeEventListener('mousemove', onMouseMove)
      window.removeEventListener('mouseup', onMouseUp)
    }
    window.addEventListener('mousemove', onMouseMove)
    window.addEventListener('mouseup', onMouseUp)
  }

  /** 加载当前激活连接 */
  const loadActive = useCallback(async (): Promise<void> => {
    try {
      const list = await kafkaApiClient.connections.list()
      if (list.length > 0) {
        setActiveConn(list[0])
      }
    } catch {
      /* 忽略 */
    }
  }, [])

  useEffect(() => {
    loadActive()
  }, [loadActive])

  /** 获取当前选中的菜单项 */
  const getSelectedKey = (): string => {
    const path = location.pathname
    if (path.startsWith('/topics')) return '/topics'
    return path || '/connections'
  }

  /** 生成面包屑 */
  const getBreadcrumbs = (): { title: string; path?: string }[] => {
    const path = location.pathname
    const crumbs: { title: string; path?: string }[] = [
      { title: <HomeOutlined />, path: '/connections' }
    ]
    if (path.startsWith('/topics/')) {
      crumbs.push({ title: 'Topic 管理', path: '/topics' })
      crumbs.push({ title: decodeURIComponent(path.split('/topics/')[1] || '') })
    } else if (breadcrumbMap[path]) {
      crumbs.push({ title: breadcrumbMap[path] })
    }
    return crumbs
  }

  /** 菜单点击导航 */
  const onMenuClick: MenuProps['onClick'] = ({ key }) => {
    navigate(key)
  }

  /** 刷新当前页面 */
  const onRefresh = (): void => {
    navigate(0)
  }

  return (
    <Layout style={{ height: '100vh', display: 'flex', flexDirection: 'column' }}>
      {/* 顶部标题栏 */}
      <Header
        style={{
          display: 'flex',
          alignItems: 'center',
          justifyContent: 'space-between',
          padding: '0 16px',
          background: '#001529',
          height: 48,
          lineHeight: '48px',
          flexShrink: 0
        }}
      >
        <Space>
          <span style={{ color: '#fff', fontWeight: 600, fontSize: 15 }}>
            Kafka Client
          </span>
          {activeConn && (
            <Space size={4} style={{ marginLeft: 8 }}>
              <Badge status="success" />
              <span style={{ color: 'rgba(255,255,255,0.85)', fontSize: 12 }}>
                {activeConn.name}
              </span>
            </Space>
          )}
          <ConnectionSelector />
        </Space>
        <Space>
          {/* DevTools 切换按钮 */}
          <Button
            type="text"
            size="small"
            icon={
              <Badge count={errorCount} size="small" offset={[2, -2]} color="#ff4d4f">
                <BugOutlined style={{ color: devToolsOpen ? '#1890ff' : '#fff' }} />
              </Badge>
            }
            onClick={() => { setDevToolsOpen(!devToolsOpen); setErrorCount(0) }}
            title="开发者工具 (F12)"
          />
          <Button
            type="text"
            icon={<ReloadOutlined style={{ color: '#fff' }} />}
            onClick={onRefresh}
            size="small"
          />
        </Space>
      </Header>

      {/* 中间主内容区域 */}
      <Layout style={{ flex: 1, overflow: 'hidden' }}>
        <Sider
          collapsible
          collapsed={collapsed}
          onCollapse={setCollapsed}
          width={160}
          collapsedWidth={48}
          theme="light"
          style={{ borderRight: '1px solid #f0f0f0' }}
        >
          <Menu
            mode="inline"
            selectedKeys={[getSelectedKey()]}
            items={menuItems}
            onClick={onMenuClick}
            style={{ borderRight: 0, fontSize: 13 }}
          />
        </Sider>
        <Content
          style={{
            padding: 16,
            background: '#f5f5f5',
            overflow: 'auto',
            flex: 1
          }}
        >
          <Breadcrumb
            style={{ marginBottom: 12 }}
            items={getBreadcrumbs().map((crumb) => ({
              title: crumb.path ? (
                <a onClick={() => navigate(crumb.path!)}>{crumb.title}</a>
              ) : (
                crumb.title
              )
            }))}
          />
          <Outlet />
        </Content>
      </Layout>

      {/* DevTools 面板（底部可拖拽） */}
      {devToolsOpen && (
        <div style={{ flexShrink: 0, display: 'flex', flexDirection: 'column' }}>
          {/* 拖拽分隔条 */}
          <div
            onMouseDown={onDragStart}
            style={{
              height: 4,
              background: '#3c3c3c',
              cursor: 'ns-resize',
              flexShrink: 0,
              transition: 'background 0.15s'
            }}
            onMouseOver={(e) => (e.currentTarget.style.background = '#007acc')}
            onMouseOut={(e) => (e.currentTarget.style.background = '#3c3c3c')}
          />
          <div style={{ height: devToolsHeight, overflow: 'hidden' }}>
            <DevToolsPanel visible={devToolsOpen} />
          </div>
        </div>
      )}
    </Layout>
  )
}
