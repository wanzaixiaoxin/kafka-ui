import { useState, useEffect, useCallback } from 'react'
import { useNavigate } from 'react-router-dom'
import { Card, Table, Input, Checkbox, Button, Tag, Space, Typography, Alert, Empty, Spin, message, Modal, Form, InputNumber } from 'antd'
import { SearchOutlined, ReloadOutlined, UnorderedListOutlined, WarningOutlined, PlusOutlined } from '@ant-design/icons'
import type { ColumnsType } from 'antd/es/table'
import { kafkaApiClient } from '../services/kafkaApiClient'
import { useActiveConnection } from '../hooks/useActiveConnection'
import type { TopicInfo, CreateTopicOptions } from '../types/kafka'

const { Title } = Typography

/** Topic 列表页面 */
export default function Topics(): JSX.Element {
  const navigate = useNavigate()
  const [topics, setTopics] = useState<TopicInfo[]>([])
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [search, setSearch] = useState('')
  const [showInternal, setShowInternal] = useState(false)
  const [createModalVisible, setCreateModalVisible] = useState(false)
  const [createLoading, setCreateLoading] = useState(false)
  const [form] = Form.useForm()
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

  /** 创建 Topic */
  const handleCreate = useCallback(async (): Promise<void> => {
    try {
      const values = await form.validateFields()
      setCreateLoading(true)
      const opts: CreateTopicOptions = {
        topic: values.topic,
        numPartitions: values.numPartitions,
        replicationFactor: values.replicationFactor
      }
      const res = await kafkaApiClient.topics.create(opts)
      if ('error' in res && res.error) {
        message.error(res.error)
      } else {
        message.success('Topic 创建成功')
        setCreateModalVisible(false)
        form.resetFields()
        load()
      }
    } catch (err: unknown) {
      if (err instanceof Error) {
        message.error(err.message)
      }
    } finally {
      setCreateLoading(false)
    }
  }, [form, load])

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
    <>
      <Card
        title={<><UnorderedListOutlined style={{ marginRight: 8 }} />Topic 列表</>}
        size="small"
      >
      {/* 无连接警告 */}
      {!hasConn && (
        <Alert
          message="未选择连接"
          description="请先在顶部选择一个 Kafka 连接，或前往连接管理页面创建连接。"
          type="warning"
          icon={<WarningOutlined />}
          showIcon
          style={{ marginBottom: 12 }}
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
          style={{ marginBottom: 12 }}
          action={
            <Button size="small" onClick={load}>
              重试
            </Button>
          }
        />
      )}
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
          <Button
            type="primary"
            icon={<PlusOutlined />}
            onClick={() => setCreateModalVisible(true)}
            disabled={!hasConn}
          >
            新建 Topic
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

      {/* 创建 Topic 弹窗 */}
      <Modal
        title="新建 Topic"
        open={createModalVisible}
        onOk={handleCreate}
        onCancel={() => { setCreateModalVisible(false); form.resetFields() }}
        confirmLoading={createLoading}
        okText="创建"
        cancelText="取消"
      >
        <Form form={form} layout="vertical">
          <Form.Item
            name="topic"
            label="Topic 名称"
            rules={[
              { required: true, message: '请输入 Topic 名称' },
              { pattern: /^[a-zA-Z0-9._-]+$/, message: '只允许字母、数字、点、下划线和连字符' }
            ]}
          >
            <Input placeholder="例如: my-topic" />
          </Form.Item>
          <Form.Item
            name="numPartitions"
            label="分区数"
            initialValue={3}
            rules={[{ required: true, message: '请输入分区数' }]}
          >
            <InputNumber min={1} max={1000} style={{ width: '100%' }} />
          </Form.Item>
          <Form.Item
            name="replicationFactor"
            label="副本因子"
            initialValue={1}
            rules={[{ required: true, message: '请输入副本因子' }]}
          >
            <InputNumber min={1} max={10} style={{ width: '100%' }} />
          </Form.Item>
        </Form>
      </Modal>
    </>
  )
}
