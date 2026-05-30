import { useState, useEffect, useCallback, useRef } from 'react'
import { useNavigate } from 'react-router-dom'
import {
  Card, Input, Select, Radio, InputNumber, Button, Badge, Space, Drawer, Descriptions, Alert, Spin, message
} from 'antd'
import { InboxOutlined, WarningOutlined } from '@ant-design/icons'
import MessageTable from '../components/MessageTable'
import JsonViewer from '../components/JsonViewer'
import { kafkaApiClient } from '../services/kafkaApiClient'
import { getCachedSettings } from '../utils/settings'
import { useActiveConnection } from '../hooks/useActiveConnection'
import type { ConsumedMessage, TopicInfo } from '../types/kafka'

/** 消息消费页面 */
export default function Consumer(): JSX.Element {
  const navigate = useNavigate()
  const [topic, setTopic] = useState('')
  const [topics, setTopics] = useState<TopicInfo[]>([])
  const [partition, setPartition] = useState<number | undefined>(undefined)
  const [partitionCount, setPartitionCount] = useState(0)
  const [fromType, setFromType] = useState<'latest' | 'earliest' | 'offset'>('earliest')
  const [fromOffset, setFromOffset] = useState<number>(0)
  const [running, setRunning] = useState(false)
  const [msgs, setMsgs] = useState<ConsumedMessage[]>([])
  const [consumerId, setConsumerId] = useState<string | null>(null)
  const [selectedMsg, setSelectedMsg] = useState<ConsumedMessage | null>(null)
  const [drawerOpen, setDrawerOpen] = useState(false)
  const { hasConn } = useActiveConnection()
  const [topicsLoading, setTopicsLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const unsubRef = useRef<(() => void) | null>(null)
  const consumerIdRef = useRef<string | null>(null)

  /** 从设置缓存中获取最大消息数量 */
  const maxMsgs = getCachedSettings()?.maxMessages ?? 500

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

  /* 加载 topic 列表（连接变化时重新加载） */
  useEffect(() => {
    loadTopics()
  }, [loadTopics])

  /* 仅在组件卸载时停止消费者，避免 loadTopics 引用变化导致意外 cleanup */
  useEffect(() => {
    return () => {
      unsubRef.current?.()
      const cid = consumerIdRef.current
      if (cid) {
        kafkaApiClient.consumer.stop(cid).catch(() => {})
      }
    }
  }, [])

  /** 当 topic 改变时加载分区数 */
  const onTopicChange = async (val: string): Promise<void> => {
    setTopic(val)
    setPartition(undefined)
    if (!val) {
      setPartitionCount(0)
      return
    }
    try {
      const res = await kafkaApiClient.topics.describe(val)
      if ('error' in res) {
        setPartitionCount(0)
        message.error(res.error)
        return
      }
      setPartitionCount(res.partitions.length)
    } catch (err: unknown) {
      setPartitionCount(0)
      message.error(err instanceof Error ? err.message : '加载分区信息失败')
    }
  }

  /** 开始消费 */
  const startConsume = async (): Promise<void> => {
    if (!topic) {
      message.warning('请先选择 Topic')
      return
    }
    setError(null)
    try {
      /* 注册消息监听 */
      const unsub = kafkaApiClient.consumer.onMessage((msg: ConsumedMessage) => {
        setMsgs((prev) => {
          const next = [...prev, msg]
          /* 超过最大数量时丢弃最旧的 */
          return next.length > maxMsgs ? next.slice(-maxMsgs) : next
        })
      })
      unsubRef.current = unsub

      const opts = {
        topic,
        partition,
        fromBeginning: fromType === 'earliest',
        fromOffset: fromType === 'offset' ? String(fromOffset) : undefined
      }
      const res = await kafkaApiClient.consumer.start(opts)
      if ('error' in res) {
        unsub()
        setError(res.error)
        message.error(res.error)
        return
      }
      setConsumerId(res.consumerId)
      consumerIdRef.current = res.consumerId
      setRunning(true)
      message.success('开始消费')
    } catch (err: unknown) {
      const msg = err instanceof Error ? err.message : String(err)
      setError(msg)
      message.error(msg)
    }
  }

  /** 停止消费 */
  const stopConsume = async (): Promise<void> => {
    try {
      unsubRef.current?.()
      unsubRef.current = null
      if (consumerId) {
        await kafkaApiClient.consumer.stop(consumerId)
      }
      setRunning(false)
      setConsumerId(null)
      consumerIdRef.current = null
      message.info('已停止消费')
    } catch (err: unknown) {
      message.error(err instanceof Error ? err.message : '停止消费失败')
    }
  }

  /** 清空消息 */
  const clearMsgs = (): void => {
    setMsgs([])
  }

  /** 查看消息详情 */
  const viewMsg = (msg: ConsumedMessage): void => {
    setSelectedMsg(msg)
    setDrawerOpen(true)
  }

  /** 分区选项 */
  const partitionOpts = [
    { value: undefined, label: '全部分区' },
    ...Array.from({ length: partitionCount }, (_, i) => ({
      value: i,
      label: `分区 ${i}`
    }))
  ]

  /** topic 下拉选项 */
  const topicOptions = topics.map((t) => ({ value: t.topic, label: t.topic }))

  return (
    <Card title={<><InboxOutlined /> 消息消费</>} size="small">
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

      {/* 错误提示 */}
      {error && (
        <Alert
          message={error}
          type="error"
          showIcon
          closable
          style={{ marginBottom: 16 }}
          onClose={() => setError(null)}
        />
      )}

      {/* 控制栏 */}
      <Space wrap style={{ marginBottom: 16 }}>
        <Spin spinning={topicsLoading} size="small">
          <Select
            showSearch
            style={{ width: 250 }}
            placeholder="选择 Topic"
            options={topicOptions}
            value={topic || undefined}
            onChange={onTopicChange}
            filterOption={(input, option) =>
              (option?.label as string)?.toLowerCase().includes(input.toLowerCase()) ?? false
            }
          />
        </Spin>
        <Select
          style={{ width: 140 }}
          options={partitionOpts}
          value={partition}
          onChange={setPartition}
          placeholder="选择分区"
        />
        <Radio.Group value={fromType} onChange={(e) => setFromType(e.target.value)}>
          <Radio.Button value="latest">Latest</Radio.Button>
          <Radio.Button value="earliest">Earliest</Radio.Button>
          <Radio.Button value="offset">指定 Offset</Radio.Button>
        </Radio.Group>
        {fromType === 'offset' && (
          <InputNumber
            min={0}
            value={fromOffset}
            onChange={(v) => setFromOffset(v ?? 0)}
            placeholder="Offset"
          />
        )}
        <Button type="primary" onClick={startConsume} disabled={running || !hasConn}>
          开始消费
        </Button>
        <Button danger onClick={stopConsume} disabled={!running}>
          停止消费
        </Button>
        <Button onClick={clearMsgs}>清空消息</Button>
      </Space>

      {/* 状态栏 */}
      <div style={{ marginBottom: 16 }}>
        <Space>
          <Badge status={running ? 'processing' : 'default'} />
          <span>{running ? '消费中' : '已停止'}</span>
          <span>| 已消费 {msgs.length} 条消息</span>
          <span style={{ color: '#999', fontSize: 12 }}>
            (最大保留 {maxMsgs} 条)
          </span>
        </Space>
      </div>

      {/* 消息表格 */}
      <MessageTable messages={msgs} onMessageClick={viewMsg} />

      {/* 消息详情抽屉 */}
      <Drawer
        title="消息详情"
        width={600}
        open={drawerOpen}
        onClose={() => setDrawerOpen(false)}
      >
        {selectedMsg && (
          <>
            <Descriptions size="small" column={2} style={{ marginBottom: 16 }}>
              <Descriptions.Item label="Topic">{selectedMsg.topic}</Descriptions.Item>
              <Descriptions.Item label="分区">{selectedMsg.partition}</Descriptions.Item>
              <Descriptions.Item label="Offset">{selectedMsg.offset}</Descriptions.Item>
              <Descriptions.Item label="时间戳">
                {new Date(selectedMsg.timestamp).toLocaleString('zh-CN')}
              </Descriptions.Item>
              <Descriptions.Item label="Key">{selectedMsg.key || '-'}</Descriptions.Item>
            </Descriptions>
            {selectedMsg.headers && Object.keys(selectedMsg.headers).length > 0 && (
              <div style={{ marginBottom: 16 }}>
                <div style={{ fontWeight: 'bold', marginBottom: 4 }}>Headers:</div>
                <JsonViewer data={JSON.stringify(selectedMsg.headers)} />
              </div>
            )}
            <div>
              <div style={{ fontWeight: 'bold', marginBottom: 4 }}>Value:</div>
              <JsonViewer data={selectedMsg.value} />
            </div>
          </>
        )}
      </Drawer>
    </Card>
  )
}
