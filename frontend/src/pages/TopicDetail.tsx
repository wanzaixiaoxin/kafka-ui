import { useState, useEffect, useCallback } from 'react'
import { useParams, useNavigate } from 'react-router-dom'
import {
  Card, Table, Tag, Button, Typography, Alert, Spin, Space,
  Select, InputNumber, Drawer, Descriptions
} from 'antd'
import { ArrowLeftOutlined, SearchOutlined } from '@ant-design/icons'
import type { ColumnsType } from 'antd/es/table'
import MessageTable from '../components/MessageTable'
import JsonViewer from '../components/JsonViewer'
import { kafkaApiClient } from '../services/kafkaApiClient'
import type { TopicDetail, PartitionInfo, PartitionOffset, ConsumedMessage } from '../types/kafka'

const { Title } = Typography

/** Topic 详情页面 */
export default function TopicDetail(): JSX.Element {
  const { topicName } = useParams<{ topicName: string }>()
  const navigate = useNavigate()
  const [detail, setDetail] = useState<TopicDetail | null>(null)
  const [offsets, setOffsets] = useState<PartitionOffset[]>([])
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)

  /* ---- 消息浏览相关状态 ---- */
  const [msgPartition, setMsgPartition] = useState<number | undefined>(undefined)
  const [msgFromType, setMsgFromType] = useState<'latest' | 'earliest'>('latest')
  const [msgOffset, setMsgOffset] = useState<string>('')
  const [msgLimit, setMsgLimit] = useState<number>(50)
  const [messages, setMessages] = useState<ConsumedMessage[]>([])
  const [msgLoading, setMsgLoading] = useState(false)
  const [msgError, setMsgError] = useState<string | null>(null)
  const [selectedMsg, setSelectedMsg] = useState<ConsumedMessage | null>(null)
  const [drawerOpen, setDrawerOpen] = useState(false)

  /** 加载 Topic 详情和 Offset */
  const load = useCallback(async (): Promise<void> => {
    if (!topicName) return
    setLoading(true)
    setError(null)
    try {
      const [detailRes, offsetRes] = await Promise.all([
        kafkaApiClient.topics.describe(topicName),
        kafkaApiClient.topics.offsets(topicName)
      ])

      if (detailRes && 'error' in detailRes) {
        setError(detailRes.error)
        return
      }
      if (offsetRes && 'error' in offsetRes) {
        setError(offsetRes.error)
        return
      }

      setDetail(detailRes as TopicDetail)
      setOffsets(offsetRes as PartitionOffset[])
    } catch (err: unknown) {
      setError(err instanceof Error ? err.message : '加载 Topic 详情失败')
    } finally {
      setLoading(false)
    }
  }, [topicName])

  useEffect(() => {
    load()
  }, [load])

  /** 拉取消息 */
  const fetchMessages = async (): Promise<void> => {
    if (!topicName) return
    setMsgLoading(true)
    setMsgError(null)
    setMessages([]) // 清空旧消息
    try {
      const opts = {
        topic: topicName,
        partition: msgPartition,
        offset: msgFromType === 'latest' ? undefined : (msgOffset || undefined),
        fromBeginning: msgFromType === 'earliest',
        limit: msgLimit
      }
      const res = await kafkaApiClient.topics.messages(opts)
      if (res && 'error' in res) {
        setMsgError(res.error)
        return
      }
      const msgs = res as ConsumedMessage[]
      setMessages(msgs)
      if (msgs.length === 0) {
        setMsgError('未查询到消息。可能原因：\n1. Topic 中无消息\n2. 选择的 Offset 范围内无消息\n3. 请尝试选择"最早消息"或调整 Offset')
      }
    } catch (err: unknown) {
      const errorMsg = err instanceof Error ? err.message : '拉取消息失败'
      setMsgError(errorMsg)
    } finally {
      setMsgLoading(false)
    }
  }

  /** 查看消息详情 */
  const viewMsg = (msg: ConsumedMessage): void => {
    setSelectedMsg(msg)
    setDrawerOpen(true)
  }

  /** 分区 — 合并分区信息 + Offset 范围的统一视图 */
  type MergedPartition = PartitionInfo & {
    earliestOffset?: string
    latestOffset?: string
    messageCount: string
  }

  const mergedPartitions: MergedPartition[] = (detail?.partitions ?? []).map((p) => {
    const o = offsets.find((off) => off.partition === p.partitionId)
    return {
      ...p,
      earliestOffset: o?.earliestOffset,
      latestOffset: o?.latestOffset,
      messageCount: o ? (BigInt(o.latestOffset) - BigInt(o.earliestOffset)).toString() : '-'
    }
  })

  const mergedColumns: ColumnsType<MergedPartition> = [
    {
      title: '分区',
      dataIndex: 'partitionId',
      key: 'partitionId',
      width: 70,
      sorter: (a, b) => a.partitionId - b.partitionId
    },
    {
      title: 'Leader',
      dataIndex: 'leader',
      key: 'leader',
      width: 70
    },
    {
      title: '最早 Offset',
      dataIndex: 'earliestOffset',
      key: 'earliestOffset',
      width: 130,
      render: (v: string | undefined) => v ?? '-'
    },
    {
      title: '最新 Offset',
      dataIndex: 'latestOffset',
      key: 'latestOffset',
      width: 130,
      render: (v: string | undefined) => v ?? '-'
    },
    {
      title: '消息数',
      dataIndex: 'messageCount',
      key: 'messageCount',
      width: 90,
      sorter: (a, b) => Number(BigInt(a.messageCount) - BigInt(b.messageCount)),
      render: (count: string) => (
        <Tag color="orange" style={{ margin: 0 }}>{count}</Tag>
      )
    },
    {
      title: 'ISR',
      dataIndex: 'isr',
      key: 'isr',
      width: 120,
      render: (isr: number[]) => (
        <Space size={2}>
          {isr.map((r) => (
            <Tag key={r} color="green" style={{ fontSize: 11, lineHeight: '18px' }}>{r}</Tag>
          ))}
        </Space>
      )
    },
    {
      title: '副本',
      dataIndex: 'replicas',
      key: 'replicas',
      render: (replicas: number[]) => (
        <Space size={2}>
          {replicas.map((r) => (
            <Tag key={r} color="blue" style={{ fontSize: 11, lineHeight: '18px' }}>{r}</Tag>
          ))}
        </Space>
      )
    }
  ]

  /** 分区选项 */
  const partitionOpts = [
    { value: undefined, label: '全部分区' },
    ...(detail?.partitions ?? []).map((p) => ({
      value: p.partitionId,
      label: `分区 ${p.partitionId}`
    }))
  ]

  return (
    <div>
      {/* 页头 */}
      <Space style={{ marginBottom: 16 }}>
        <Button
          icon={<ArrowLeftOutlined />}
          onClick={() => navigate('/topics')}
        >
          返回
        </Button>
        <Title level={4} style={{ marginTop: 0, marginBottom: 0 }}>
          Topic 详情: {topicName}
        </Title>
      </Space>

      {error && (
        <Alert
          message={error}
          type="error"
          showIcon
          style={{ marginBottom: 16 }}
        />
      )}

      <Spin spinning={loading}>
        {/* 分区信息 & Offset 范围 */}
        <Card title="分区详情" size="small" style={{ marginBottom: 16 }}>
          <Table
            rowKey="partitionId"
            dataSource={mergedPartitions}
            columns={mergedColumns}
            size="small"
            pagination={false}
          />
        </Card>
      </Spin>

      {/* 消息浏览 */}
      <Card
        title={<><SearchOutlined /> 消息浏览</>}
        size="small"
        style={{ marginBottom: 16 }}
      >
        {/* 查询控制栏 */}
        <Space wrap style={{ marginBottom: 16 }}>
          <span>分区:</span>
          <Select
            style={{ width: 140 }}
            options={partitionOpts}
            value={msgPartition}
            onChange={setMsgPartition}
            placeholder="选择分区"
          />
          <span>起始位置:</span>
          <Select
            style={{ width: 130 }}
            value={msgFromType}
            onChange={(v) => { setMsgFromType(v); setMsgOffset('') }}
            options={[
              { value: 'latest', label: '最新消息' },
              { value: 'earliest', label: '最早消息' }
            ]}
          />
          {msgFromType === 'earliest' && (
            <>
              <span>Offset:</span>
              <InputNumber
                style={{ width: 150 }}
                min={0}
                value={msgOffset ? Number(msgOffset) : undefined}
                onChange={(v) => setMsgOffset(v != null ? String(v) : '')}
                placeholder="留空则从最早开始"
              />
            </>
          )}
          <span>条数:</span>
          <InputNumber
            style={{ width: 80 }}
            min={1}
            max={500}
            value={msgLimit}
            onChange={(v) => setMsgLimit(v ?? 50)}
          />
          <Button
            type="primary"
            icon={<SearchOutlined />}
            loading={msgLoading}
            onClick={fetchMessages}
          >
            {msgLoading ? '查询中...' : '查询'}
          </Button>
          {msgLoading && (
            <span style={{ color: '#999', fontSize: 12 }}>
              查询中，请稍候...
            </span>
          )}
        </Space>

        {/* 查询错误提示 */}
        {msgError && (
          <Alert
            message={msgError}
            type="error"
            showIcon
            closable
            style={{ marginBottom: 12 }}
            onClose={() => setMsgError(null)}
          />
        )}

        {/* 消息统计 */}
        {messages.length > 0 && (
          <div style={{ marginBottom: 8, color: '#666', fontSize: 13 }}>
            共查询到 <Tag color="blue">{messages.length}</Tag> 条消息
          </div>
        )}

        {/* 消息表格 */}
        <MessageTable messages={messages} onMessageClick={viewMsg} />

        {/* 无数据提示 */}
        {!msgLoading && messages.length === 0 && !msgError && (
          <div style={{ textAlign: 'center', padding: '24px 0', color: '#999' }}>
            选择查询条件后点击"查询"按钮浏览消息
          </div>
        )}
      </Card>

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
    </div>
  )
}
