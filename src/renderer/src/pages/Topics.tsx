import { useState, useEffect, useCallback } from 'react'
import { useNavigate } from 'react-router-dom'
import { Card, Table, Input, Checkbox, Button, Tag, Space, Typography, Alert, Empty, Spin, message } from 'antd'
import { SearchOutlined, ReloadOutlined, UnorderedListOutlined, WarningOutlined } from '@ant-design/icons'
import type { ColumnsType } from 'antd/es/table'
import { kafkaApiClient } from '../services/kafkaApiClient'
import { useActiveConnection } from '../hooks/useActiveConnection'
import type { TopicInfo } from '../types/kafka'

const { Title } = Typography

/** Topic 列表页面 */
export default function Topics(): JSX.Element {
  const navigate = useNavigate()
  const [topics, setTopics] = useState<TopicInfo[]>([])
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [search, setSearch] = useState('')
  const [showInternal, setShowInternal] = useState(false)
  const { hasConn } = useActiveConnection()

  /** 加载 Topic 列表 */
  const load = useCallback(async (): Promise<void> => {
    setLoading(true)
    setError(null)
    try {
      if (!hasConn) {
        setTopics([])
        return
      }
      const res = await kafkaApiClient.topics.list(showInternal)
      if (res && 'error' in res) {
        setError(res.error)
        setTopics([])
        message.error(res.error)
      } else {
        setTopics(res as TopicInfo[])
      }
    } catch (err: unknown) {
      const msg = err instanceof Error ? err.message : '加载 Topic 列表失败'
      setError(msg)
      setTopics([])
      message.error(msg)
    } finally {
      setLoading(false)
    }
  }, [showInternal, hasConn])

  /** 挂载时及 showInternal 变化时加载 */
  useEffect(() => {
    load()
  }, [load])

  /** 按名称过滤 */
  const filtered = topics.filter((t) =>
    t.topic.toLowerCase().includes(search.toLowerCase())
  )

  /** 表格列定义 */
  const columns: ColumnsType<TopicInfo> = [
    {
      title: 'Topic',
      dataIndex: 'topic',
      key: 'topic',
      ellipsis: true,
      sorter: (a, b) => a.topic.localeCompare(b.topic)
    },
    {
      title: '分区数',
      dataIndex: 'partitions',
      key: 'partitions',
      width: 90,
      sorter: (a, b) => a.partitions - b.partitions
    },
    {
      title: '副本数',
      dataIndex: 'replicas',
      key: 'replicas',
      width: 90,
      sorter: (a, b) => a.replicas - b.replicas
    },
    {
      title: '类型',
      dataIndex: 'isInternal',
      key: 'isInternal',
      width: 100,
      render: (internal: boolean) => (
        <Tag color={internal ? 'red' : 'green'}>
          {internal ? '内部' : '普通'}
        </Tag>
      )
    },
    {
      title: '操作',
      key: 'actions',
      width: 100,
      render: (_: unknown, record: TopicInfo) => (
        <Button
          type="link"
          size="small"
          onClick={() => navigate(`/topics/${encodeURIComponent(record.topic)}`)}
        >
          详情
        </Button>
      )
    }
  ]

  return (
    <div>
      <Title level={4} style={{ marginTop: 0, marginBottom: 16 }}>
        <UnorderedListOutlined style={{ marginRight: 8 }} />
        Topic 列表
      </Title>

      {/* 无连接警告 */}
      {!hasConn && (
        <Alert
          message="未选择连接"
          description="请先在顶部选择一个 Kafka 连接，或前往连接管理页面创建连接。"
          type="warning"
          icon={<WarningOutlined />}
          showIcon
          style={{ marginBottom: 16 }}
          action={
            <Button size="small" onClick={() => navigate('/connections')}>
              前往连接管理
            </Button>
          }
        />
      )}

      {/* 错误提示（带重试按钮） */}
      {error && (
        <Alert
          message={error}
          type="error"
          showIcon
          style={{ marginBottom: 16 }}
          action={
            <Button size="small" onClick={load}>
              重试
            </Button>
          }
        />
      )}

      <Card size="small">
        {/* 控制栏 */}
        <Space style={{ marginBottom: 12 }} wrap>
          <Input
            placeholder="搜索 Topic 名称"
            prefix={<SearchOutlined />}
            value={search}
            onChange={(e) => setSearch(e.target.value)}
            style={{ width: 240 }}
            allowClear
          />
          <Checkbox
            checked={showInternal}
            onChange={(e) => setShowInternal(e.target.checked)}
          >
            显示内部 Topic
          </Checkbox>
          <Button
            icon={<ReloadOutlined />}
            onClick={load}
            loading={loading}
          >
            刷新
          </Button>
        </Space>

        {/* Topic 表格 */}
        <Spin spinning={loading}>
          {filtered.length === 0 && !loading && !error ? (
            <Empty description="暂无 Topic 数据" />
          ) : (
            <Table
              rowKey="topic"
              dataSource={filtered}
              columns={columns}
              size="small"
              pagination={{ pageSize: 20, showSizeChanger: true, showTotal: (t) => `共 ${t} 个` }}
            />
          )}
        </Spin>
      </Card>
    </div>
  )
}
