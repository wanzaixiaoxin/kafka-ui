import { useState, useEffect, useCallback } from 'react'
import {
  Card, Table, Button, Modal, Form, Input, Select, Switch, Space,
  Popconfirm, message, Tag, Badge, Typography
} from 'antd'
import {
  PlusOutlined, ApiOutlined, EditOutlined, DeleteOutlined,
  CheckCircleOutlined, CloseCircleOutlined, ThunderboltOutlined
} from '@ant-design/icons'
import type { ColumnsType } from 'antd/es/table'
import { kafkaApiClient } from '../services/kafkaApiClient'
import type { KafkaConnection, ConnectionTestResult } from '../types/kafka'

const { TextArea } = Input
const { Text } = Typography

/** 表单值类型 */
interface FormValues {
  name: string
  brokers: string
  clientId: string
  ssl: boolean
  saslMechanism: string
  username: string
  password: string
  description: string
}

/** 连接管理页面 */
export default function Connections(): JSX.Element {
  const [conns, setConns] = useState<KafkaConnection[]>([])
  const [activeId, setActiveId] = useState<string | null>(null)
  const [modalOpen, setModalOpen] = useState(false)
  const [editingConn, setEditingConn] = useState<KafkaConnection | null>(null)
  const [testLoading, setTestLoading] = useState<string | null>(null)
  const [testResult, setTestResult] = useState<ConnectionTestResult | null>(null)
  const [testResultOpen, setTestResultOpen] = useState(false)
  const [form] = Form.useForm<FormValues>()

  /** 加载连接列表和激活状态 */
  const load = useCallback(async (): Promise<void> => {
    const [list, id] = await Promise.all([
      kafkaApiClient.connections.list(),
      kafkaApiClient.connections.activeId()
    ])
    setConns(list)
    setActiveId(id)
  }, [])

  useEffect(() => {
    load()
  }, [load])

  /** 打开新建弹窗 */
  const onAdd = (): void => {
    setEditingConn(null)
    form.resetFields()
    form.setFieldsValue({
      clientId: 'kafka-client',
      ssl: false,
      saslMechanism: 'none'
    })
    setModalOpen(true)
  }

  /** 打开编辑弹窗 */
  const onEdit = (conn: KafkaConnection): void => {
    setEditingConn(conn)
    form.setFieldsValue({
      name: conn.name,
      brokers: conn.brokers.join('\n'),
      clientId: conn.clientId,
      ssl: conn.ssl,
      saslMechanism: conn.sasl?.mechanism || 'none',
      username: conn.sasl?.username || '',
      password: conn.sasl?.password || '',
      description: conn.description || ''
    })
    setModalOpen(true)
  }

  /** 提交表单（新建/编辑） */
  const onSubmit = async (): Promise<void> => {
    try {
      const vals = await form.validateFields()
      const brokers = vals.brokers
        .split('\n')
        .map((b) => b.trim())
        .filter(Boolean)

      if (brokers.length === 0) {
        message.error('请至少输入一个 Broker 地址')
        return
      }

      const saslMechanism = vals.saslMechanism
      const payload: Partial<KafkaConnection> & { name: string; brokers: string[] } = {
        name: vals.name,
        brokers,
        clientId: vals.clientId || 'kafka-client',
        ssl: vals.ssl,
        description: vals.description || undefined,
        ...(editingConn ? { id: editingConn.id } : {})
      }

      if (saslMechanism !== 'none' && vals.username) {
        payload.sasl = {
          mechanism: saslMechanism as 'plain' | 'scram-sha-256' | 'scram-sha-512',
          username: vals.username,
          password: vals.password
        }
      }

      await kafkaApiClient.connections.save(payload)
      message.success(editingConn ? '连接已更新' : '连接已创建')
      setModalOpen(false)
      load()
    } catch (err) {
      message.error(err instanceof Error ? err.message : String(err))
    }
  }

  /** 删除连接 */
  const onRemove = async (id: string): Promise<void> => {
    const res = await kafkaApiClient.connections.remove(id)
    if (res.success) {
      message.success('连接已删除')
      load()
    } else {
      message.error(res.error || '删除失败')
    }
  }

  /** 测试连接 */
  const onTest = async (conn: KafkaConnection): Promise<void> => {
    setTestLoading(conn.id)
    const result = await kafkaApiClient.connections.test(conn)
    setTestLoading(null)
    setTestResult(result)
    setTestResultOpen(true)
  }

  /** 切换激活连接 */
  const onUse = async (id: string): Promise<void> => {
    const res = await kafkaApiClient.connections.use(id)
    if (res.success) {
      setActiveId(id)
      message.success('已切换连接')
    } else {
      message.error(res.error || '切换失败')
    }
  }

  /** 表格列定义 */
  const columns: ColumnsType<KafkaConnection> = [
    {
      title: '名称',
      dataIndex: 'name',
      key: 'name',
      width: 140,
      render: (name: string, record: KafkaConnection) => (
        <Space>
          {activeId === record.id ? (
            <Badge status="success" />
          ) : null}
          <Text strong={activeId === record.id}>{name}</Text>
        </Space>
      )
    },
    {
      title: 'Brokers',
      dataIndex: 'brokers',
      key: 'brokers',
      render: (brokers: string[]) => brokers.join(', ')
    },
    {
      title: 'Client ID',
      dataIndex: 'clientId',
      key: 'clientId',
      width: 120
    },
    {
      title: 'SSL',
      dataIndex: 'ssl',
      key: 'ssl',
      width: 60,
      render: (ssl: boolean) => (
        <Tag color={ssl ? 'green' : 'default'}>{ssl ? '是' : '否'}</Tag>
      )
    },
    {
      title: 'SASL',
      key: 'sasl',
      width: 100,
      render: (_: unknown, record: KafkaConnection) => (
        <Tag color={record.sasl ? 'blue' : 'default'}>
          {record.sasl?.mechanism || 'none'}
        </Tag>
      )
    },
    {
      title: '描述',
      dataIndex: 'description',
      key: 'description',
      ellipsis: true,
      render: (desc: string) => desc || '-'
    },
    {
      title: '操作',
      key: 'actions',
      width: 280,
      render: (_: unknown, record: KafkaConnection) => (
        <Space size="small">
          <Button
            type="link"
            size="small"
            icon={<ThunderboltOutlined />}
            loading={testLoading === record.id}
            onClick={() => onTest(record)}
          >
            测试
          </Button>
          {activeId !== record.id ? (
            <Button
              type="link"
              size="small"
              onClick={() => onUse(record.id)}
            >
              使用
            </Button>
          ) : (
            <Tag color="green">当前连接</Tag>
          )}
          <Button
            type="link"
            size="small"
            icon={<EditOutlined />}
            onClick={() => onEdit(record)}
          >
            编辑
          </Button>
          <Popconfirm
            title="确认删除该连接？"
            onConfirm={() => onRemove(record.id)}
            okText="删除"
            cancelText="取消"
          >
            <Button
              type="link"
              size="small"
              danger
              icon={<DeleteOutlined />}
              disabled={activeId === record.id}
            >
              删除
            </Button>
          </Popconfirm>
        </Space>
      )
    }
  ]

  /** 监听 SASL 机制字段 */
  const saslMechanism = Form.useWatch('saslMechanism', form)

  return (
    <>
      <Card
        title={
          <Space>
            <ApiOutlined />
            <span>连接管理</span>
          </Space>
        }
        extra={
          <Button type="primary" icon={<PlusOutlined />} onClick={onAdd}>
            新建连接
          </Button>
        }
      >
        <Table
          rowKey="id"
          dataSource={conns}
          columns={columns}
          size="small"
          pagination={false}
        />
      </Card>

      {/* 新建/编辑连接弹窗 */}
      <Modal
        title={editingConn ? '编辑连接' : '新建连接'}
        open={modalOpen}
        onOk={onSubmit}
        onCancel={() => setModalOpen(false)}
        okText="保存"
        cancelText="取消"
        width={560}
        destroyOnClose
      >
        <Form form={form} layout="vertical" style={{ marginTop: 16 }}>
          <Form.Item
            name="name"
            label="连接名称"
            rules={[{ required: true, message: '请输入连接名称' }]}
          >
            <Input placeholder="例如：本地开发环境" />
          </Form.Item>
          <Form.Item
            name="brokers"
            label="Broker 地址"
            rules={[{ required: true, message: '请输入 Broker 地址' }]}
          >
            <TextArea
              rows={3}
              placeholder="每行一个 Broker 地址，例如：&#10;localhost:9092&#10;localhost:9093"
            />
          </Form.Item>
          <Form.Item name="clientId" label="Client ID">
            <Input placeholder="kafka-client" />
          </Form.Item>
          <Form.Item name="ssl" label="SSL" valuePropName="checked">
            <Switch />
          </Form.Item>
          <Form.Item name="saslMechanism" label="SASL 机制">
            <Select>
              <Select.Option value="none">无</Select.Option>
              <Select.Option value="plain">PLAIN</Select.Option>
              <Select.Option value="scram-sha-256">SCRAM-SHA-256</Select.Option>
              <Select.Option value="scram-sha-512">SCRAM-SHA-512</Select.Option>
            </Select>
          </Form.Item>
          {saslMechanism && saslMechanism !== 'none' && (
            <>
              <Form.Item
                name="username"
                label="用户名"
                rules={[{ required: true, message: '请输入用户名' }]}
              >
                <Input placeholder="SASL 用户名" />
              </Form.Item>
              <Form.Item
                name="password"
                label="密码"
                rules={[{ required: true, message: '请输入密码' }]}
              >
                <Input.Password placeholder="SASL 密码" />
              </Form.Item>
            </>
          )}
          <Form.Item name="description" label="描述">
            <TextArea rows={2} placeholder="连接描述（可选）" />
          </Form.Item>
        </Form>
      </Modal>

      {/* 测试结果弹窗 */}
      <Modal
        title={
          testResult?.success ? (
            <Space>
              <CheckCircleOutlined style={{ color: '#52c41a' }} />
              <span>连接成功</span>
            </Space>
          ) : (
            <Space>
              <CloseCircleOutlined style={{ color: '#ff4d4f' }} />
              <span>连接失败</span>
            </Space>
          )
        }
        open={testResultOpen}
        onCancel={() => setTestResultOpen(false)}
        footer={
          <Button onClick={() => setTestResultOpen(false)}>关闭</Button>
        }
        width={480}
      >
        {testResult?.success ? (
          <div>
            <p><Text strong>集群 ID：</Text>{testResult.clusterId || '-'}</p>
            <p><Text strong>Controller：</Text>Node {testResult.controllerId}</p>
            <p><Text strong>Broker 数量：</Text>{testResult.brokers?.length || 0}</p>
            {testResult.brokers && testResult.brokers.length > 0 && (
              <div>
                <Text strong>Broker 列表：</Text>
                <ul style={{ marginTop: 4, paddingLeft: 20 }}>
                  {testResult.brokers.map((b) => (
                    <li key={b.nodeId}>
                      Node {b.nodeId}: {b.host}:{b.port}
                    </li>
                  ))}
                </ul>
              </div>
            )}
          </div>
        ) : (
          <p style={{ color: '#ff4d4f' }}>{testResult?.error || '未知错误'}</p>
        )}
      </Modal>
    </>
  )
}
