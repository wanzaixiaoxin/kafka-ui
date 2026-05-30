import { useState, useEffect, useCallback } from 'react'
import {
  Card, Form, Input, InputNumber, Button, Select, Alert, Descriptions, Spin, message, AutoComplete
} from 'antd'
import { SendOutlined, WarningOutlined } from '@ant-design/icons'
import { useNavigate } from 'react-router-dom'
import HeaderEditor from '../components/HeaderEditor'
import { kafkaApiClient } from '../services/kafkaApiClient'
import { useActiveConnection } from '../hooks/useActiveConnection'
import type { KafkaMessage, SendResult, TopicInfo } from '../types/kafka'

/** 消息生产页面 */
export default function Producer(): JSX.Element {
  const navigate = useNavigate()
  const [form] = Form.useForm()
  const [loading, setLoading] = useState(false)
  const [topics, setTopics] = useState<TopicInfo[]>([])
  const [result, setResult] = useState<SendResult | null>(null)
  const [error, setError] = useState<string | null>(null)
  const [headers, setHeaders] = useState<Record<string, string>>({})
  const { hasConn } = useActiveConnection()
  const [topicsLoading, setTopicsLoading] = useState(false)

  /** 加载 topic 列表 */
  const loadTopics = useCallback(async (): Promise<void> => {
    if (!hasConn) return
    setTopicsLoading(true)
    try {
      const res = await kafkaApiClient.topics.list()
      if (!Array.isArray(res)) {
        if (res && 'error' in res) {
          message.error(res.error)
        }
        return
      }
      setTopics(res)
    } catch (err: unknown) {
      message.error(err instanceof Error ? err.message : '加载 Topic 列表失败')
    } finally {
      setTopicsLoading(false)
    }
  }, [hasConn])

  useEffect(() => {
    loadTopics()
  }, [loadTopics])

  /** 格式化 JSON */
  const formatJson = (): void => {
    const val = form.getFieldValue('value')
    if (!val) return
    try {
      const obj = JSON.parse(val)
      form.setFieldsValue({ value: JSON.stringify(obj, null, 2) })
      message.success('格式化成功')
    } catch {
      message.error('不是有效的 JSON')
    }
  }

  /** 发送消息 */
  const send = async (vals: KafkaMessage): Promise<void> => {
    setLoading(true)
    setError(null)
    setResult(null)
    try {
      /* 过滤空 headers */
      const cleanHeaders: Record<string, string> = {}
      if (headers) {
        for (const [k, v] of Object.entries(headers)) {
          if (k.trim()) cleanHeaders[k.trim()] = v
        }
      }
      const msg: KafkaMessage = {
        topic: vals.topic,
        key: vals.key || undefined,
        value: vals.value,
        partition: vals.partition,
        headers: Object.keys(cleanHeaders).length > 0 ? cleanHeaders : undefined
      }
      const res = await kafkaApiClient.producer.send(msg)
      if ('error' in res) {
        setError(res.error)
        message.error(res.error)
      } else {
        setResult(res)
        message.success('发送成功')
      }
    } catch (err: unknown) {
      const msg = err instanceof Error ? err.message : String(err)
      setError(msg)
      message.error(msg)
    } finally {
      setLoading(false)
    }
  }

  /** topic 下拉选项 */
  const topicOptions = topics.map((t) => ({ value: t.topic, label: t.topic }))

  return (
    <Card title={<><SendOutlined /> 消息发送</>} size="small">
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

      <Spin spinning={topicsLoading}>
        <Form
          form={form}
          layout="vertical"
          onFinish={send}
          initialValues={{ value: '' }}
        >
          <Form.Item
            label="Topic"
            name="topic"
            rules={[{ required: true, message: '请选择 Topic' }]}
          >
            <AutoComplete
              options={topicOptions}
              placeholder="输入或选择 Topic"
              filterOption={(input, option) =>
                (option?.value as string)?.toLowerCase().includes(input.toLowerCase()) ?? false
              }
            />
          </Form.Item>

          <Form.Item label="Key" name="key">
            <Input placeholder="可选" />
          </Form.Item>

          <Form.Item label="分区（可选）" name="partition">
            <InputNumber min={0} style={{ width: '100%' }} placeholder="不指定则自动选择" />
          </Form.Item>

          <Form.Item label="Headers">
            <HeaderEditor value={headers} onChange={setHeaders} />
          </Form.Item>

          <Form.Item
            label="Value"
            name="value"
            rules={[{ required: true, message: '请输入消息内容' }]}
          >
            <Input.TextArea
              style={{ fontFamily: 'monospace', minHeight: 200 }}
              placeholder="输入消息内容"
            />
          </Form.Item>

          <Form.Item>
            <Button onClick={formatJson} style={{ marginRight: 8 }}>
              格式化 JSON
            </Button>
            <Button type="primary" htmlType="submit" loading={loading} icon={<SendOutlined />} disabled={!hasConn}>
              发送
            </Button>
          </Form.Item>
        </Form>
      </Spin>

      {error && (
        <Alert type="error" message="发送失败" description={error} showIcon style={{ marginTop: 16 }} />
      )}

      {result && (
        <Alert
          type="success"
          message="发送成功"
          showIcon
          style={{ marginTop: 16 }}
          description={
            <Descriptions size="small" column={2}>
              <Descriptions.Item label="Topic">{result.topic}</Descriptions.Item>
              <Descriptions.Item label="分区">{result.partition}</Descriptions.Item>
              <Descriptions.Item label="Offset">{result.offset}</Descriptions.Item>
              <Descriptions.Item label="时间戳">
                {new Date(result.timestamp).toLocaleString('zh-CN')}
              </Descriptions.Item>
            </Descriptions>
          }
        />
      )}
    </Card>
  )
}
