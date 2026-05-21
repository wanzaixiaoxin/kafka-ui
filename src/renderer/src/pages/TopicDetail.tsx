import { useState, useEffect, useCallback } from 'react'
import { useParams, useNavigate } from 'react-router-dom'
import { Card, Table, Tag, Button, Typography, Alert, Spin, Space } from 'antd'
import { ArrowLeftOutlined } from '@ant-design/icons'
import type { ColumnsType } from 'antd/es/table'
import { kafkaApiClient } from '../services/kafkaApiClient'
import type { TopicDetail, PartitionInfo, PartitionOffset } from '../types/kafka'

const { Title } = Typography

/** Topic 详情页面 */
export default function TopicDetail(): JSX.Element {
  const { topicName } = useParams<{ topicName: string }>()
  const navigate = useNavigate()
  const [detail, setDetail] = useState<TopicDetail | null>(null)
  const [offsets, setOffsets] = useState<PartitionOffset[]>([])
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)

  /** 加载 Topic 详情和 Offset */
  const load = useCallback(async (): Promise<void> => {
    if (!topicName) return
    setLoading(true)
    setError(null)
    try {
      /* 并行请求详情和 offset */
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

  /** 挂载时加载数据 */
  useEffect(() => {
    load()
  }, [load])

  /** 分区信息列定义 */
  const partitionColumns: ColumnsType<PartitionInfo> = [
    {
      title: '分区 ID',
      dataIndex: 'partitionId',
      key: 'partitionId',
      width: 90,
      sorter: (a, b) => a.partitionId - b.partitionId
    },
    {
      title: 'Leader',
      dataIndex: 'leader',
      key: 'leader',
      width: 80
    },
    {
      title: '副本',
      dataIndex: 'replicas',
      key: 'replicas',
      render: (replicas: number[]) => (
        <Space size={4}>
          {replicas.map((r) => (
            <Tag key={r} color="blue">{r}</Tag>
          ))}
        </Space>
      )
    },
    {
      title: 'ISR',
      dataIndex: 'isr',
      key: 'isr',
      render: (isr: number[]) => (
        <Space size={4}>
          {isr.map((r) => (
            <Tag key={r} color="green">{r}</Tag>
          ))}
        </Space>
      )
    }
  ]

  /** Offset 范围列定义 */
  const offsetColumns: ColumnsType<PartitionOffset & { messageCount: string }> = [
    {
      title: '分区',
      dataIndex: 'partition',
      key: 'partition',
      width: 80,
      sorter: (a, b) => a.partition - b.partition
    },
    {
      title: '最早 Offset',
      dataIndex: 'earliestOffset',
      key: 'earliestOffset',
      width: 140
    },
    {
      title: '最新 Offset',
      dataIndex: 'latestOffset',
      key: 'latestOffset',
      width: 140
    },
    {
      title: '消息数量',
      dataIndex: 'messageCount',
      key: 'messageCount',
      width: 120,
      render: (count: string) => (
        <Tag color="orange">{count}</Tag>
      )
    }
  ]

  /** 计算 messageCount */
  const offsetsWithCount = offsets.map((o) => ({
    ...o,
    messageCount: (BigInt(o.latestOffset) - BigInt(o.earliestOffset)).toString()
  }))

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
        {/* 分区信息 */}
        <Card title="分区信息" size="small" style={{ marginBottom: 16 }}>
          <Table
            rowKey="partitionId"
            dataSource={detail?.partitions ?? []}
            columns={partitionColumns}
            size="small"
            pagination={false}
          />
        </Card>

        {/* Offset 范围 */}
        <Card title="Offset 范围" size="small">
          <Table
            rowKey="partition"
            dataSource={offsetsWithCount}
            columns={offsetColumns}
            size="small"
            pagination={false}
          />
        </Card>
      </Spin>
    </div>
  )
}
