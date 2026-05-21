import { useState, useEffect, useCallback } from 'react'
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
  HomeOutlined
} from '@ant-design/icons'
import type { MenuProps } from 'antd'
import ConnectionSelector from './ConnectionSelector'
import { kafkaApiClient } from '../services/kafkaApiClient'
import type { KafkaConnection } from '../types/kafka'

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

/** 应用布局组件 - 包含顶部标题栏、侧边导航和内容区域 */
export default function AppLayout(): JSX.Element {
  const navigate = useNavigate()
  const location = useLocation()
  const [collapsed, setCollapsed] = useState(false)
  const [activeConn, setActiveConn] = useState<KafkaConnection | null>(null)

  /** 加载当前激活连接 */
  const loadActive = useCallback(async (): Promise<void> => {
    try {
      const list = await kafkaApiClient.connections.list()
      /* 通过判断激活连接：加载所有连接，找到当前使用的 */
      if (list.length > 0) {
        /* 默认使用第一个连接作为活跃连接（简化逻辑） */
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
    <Layout style={{ height: '100vh' }}>
      <Header
        style={{
          display: 'flex',
          alignItems: 'center',
          justifyContent: 'space-between',
          padding: '0 16px',
          background: '#001529',
          height: 48,
          lineHeight: '48px'
        }}
      >
        <Space>
          <span style={{ color: '#fff', fontWeight: 600, fontSize: 15 }}>
            Kafka Client
          </span>
          {/* 连接状态指示器 */}
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
        <Button
          type="text"
          icon={<ReloadOutlined style={{ color: '#fff' }} />}
          onClick={onRefresh}
          size="small"
        />
      </Header>
      <Layout>
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
            overflow: 'auto'
          }}
        >
          {/* 面包屑导航 */}
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
    </Layout>
  )
}
