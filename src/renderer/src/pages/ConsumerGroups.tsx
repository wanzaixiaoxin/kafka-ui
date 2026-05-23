import { useCallback, useEffect, useState } from 'react'
import { useNavigate } from 'react-router-dom'
import {
  Button,
  Card,
  Drawer,
  Input,
  Space,
  Spin,
  Table,
  Tag,
  Typography,
  Alert,
  message
} from 'antd'
import {
  ReloadOutlined,
  SearchOutlined,
  TeamOutlined,
  WarningOutlined
} from '@ant-design/icons'
import type { ColumnsType } from 'antd/es/table'
import { kafkaApiClient } from '../services/kafkaApiClient'
import { useActiveConnection } from '../hooks/useActiveConnection'
import type { ConsumerGroupInfo, ConsumerGroupDetail } from '../types/kafka'

/** 根据消费者组状态返回对应颜色 */
function stateColor(state: string): string {
  switch (state) {
    case 'Stable':
      return 'green'
    case 'PreparingRebalance':
      return 'gold'
    case 'CompletingRebalance':
      return 'orange'
    case 'Dead':
      return 'red'
    case 'Empty':
      return 'blue'
    default:
      return 'default'
  }
}

/** 根据 lag 值返回对应颜色 */
function lagColor(lag: string): string {
  const n = Number(lag)
  if (isNaN(n) || n === 0) return 'green'
  if (n > 1000) return 'red'
  if (n > 100) return 'orange'
  return 'green'
}

/** 消费者组页面 */
export default function ConsumerGroups(): JSX.Element {
  const navigate = useNavigate()
  const [groups, setGroups] = useState<ConsumerGroupInfo[]>([])
  const [loading, setLoading] = useState(false)
  const [keyword, setKeyword] = useState('')
  const { hasConn } = useActiveConnection()

  /* 抽屉状态 */
  const [drawerOpen, setDrawerOpen] = useState(false)
  const [detail, setDetail] = useState<ConsumerGroupDetail | null>(null)
  const [detailLoading, setDetailLoading] = useState(false)

  /** 加载消费者组列表 */
  const fetchGroups = useCallback(async () => {
    if (!hasConn) {
      setGroups([])
      return
    }
    setLoading(true)
    try {
      const res = await kafkaApiClient.groups.list()
      if (res && 'error' in res) {
        message.error(res.error)
        setGroups([])
      } else {
        setGroups(res as ConsumerGroupInfo[])
      }
    } catch (err: unknown) {
      message.error(err instanceof Error ? err.message : String(err))
      setGroups([])
    } finally {
      setLoading(false)
    }
  }, [hasConn])

  /** 打开消费者组详情 */
  const openDetail = async (groupId: string) => {
    setDrawerOpen(true)
    setDetailLoading(true)
    setDetail(null)
    try {
      const res = await kafkaApiClient.groups.describe(groupId)
      if (res && 'error' in res) {
        message.error(res.error)
      } else {
        setDetail(res as ConsumerGroupDetail)
      }
    } catch (err: unknown) {
      message.error(err instanceof Error ? err.message : String(err))
    } finally {
      setDetailLoading(false)
    }
  }

  useEffect(() => {
    fetchGroups()
  }, [fetchGroups])

  /* 按关键词过滤 */
  const filtered = keyword
    ? groups.filter((g) =>
        g.groupId.toLowerCase().includes(keyword.toLowerCase())
      )
    : groups

  /* 消费者组列表列定义 */
  const columns: ColumnsType<ConsumerGroupInfo> = [
    {
      title: 'Group ID',
      dataIndex: 'groupId',
      key: 'groupId',
      ellipsis: true
    },
    {
      title: '状态',
      dataIndex: 'state',
      key: 'state',
      width: 160,
      render: (state: string) => (
        <Tag color={stateColor(state)}>{state}</Tag>
      )
    },
    {
      title: '成员数',
      dataIndex: 'members',
      key: 'members',
      width: 100
    },
    {
      title: '协议',
      dataIndex: 'protocol',
      key: 'protocol',
      width: 140,
      ellipsis: true
    },
    {
      title: '操作',
      key: 'action',
      width: 100,
      render: (_, record) => (
        <Button type="link" size="small" onClick={() => openDetail(record.groupId)}>
          详情
        </Button>
      )
    }
  ]

  /* 成员表格列定义 */
  const memberColumns: ColumnsType<ConsumerGroupDetail['members'][0]> = [
    { title: 'Client ID', dataIndex: 'clientId', key: 'clientId', ellipsis: true },
    { title: 'Member ID', dataIndex: 'memberId', key: 'memberId', ellipsis: true },
    { title: 'Host', dataIndex: 'host', key: 'host', ellipsis: true }
  ]

  /* 偏移量表格列定义 */
  const offsetColumns: ColumnsType<ConsumerGroupDetail['offsets'][0]> = [
    { title: 'Topic', dataIndex: 'topic', key: 'topic', ellipsis: true },
    { title: 'Partition', dataIndex: 'partition', key: 'partition', width: 100 },
    { title: 'Current Offset', dataIndex: 'currentOffset', key: 'currentOffset', width: 140 },
    { title: 'Log End Offset', dataIndex: 'logEndOffset', key: 'logEndOffset', width: 140 },
    {
      title: 'Lag',
      dataIndex: 'lag',
      key: 'lag',
      width: 120,
      render: (lag: string) => (
        <span style={{ color: lagColor(lag), fontWeight: 500 }}>{lag}</span>
      )
    }
  ]

  return (
    <>
      <Card
        title={
          <Space>
            <TeamOutlined />
            <span>Consumer Group</span>
          </Space>
        }
        size="small"
        extra={
          <Space>
            <Input
              placeholder="搜索 Group ID"
              prefix={<SearchOutlined />}
              size="small"
              allowClear
              style={{ width: 200 }}
              value={keyword}
              onChange={(e) => setKeyword(e.target.value)}
            />
            <Button
              size="small"
              icon={<ReloadOutlined />}
              onClick={fetchGroups}
              loading={loading}
            >
              刷新
            </Button>
          </Space>
        }
      >
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

        <Table
          rowKey="groupId"
          columns={columns}
          dataSource={filtered}
          loading={loading}
          size="small"
          pagination={{ pageSize: 20, showSizeChanger: false }}
        />
      </Card>

      {/* 消费者组详情抽屉 */}
      <Drawer
        title={`消费者组: ${detail?.groupId ?? ''}`}
        open={drawerOpen}
        onClose={() => setDrawerOpen(false)}
        width={720}
        destroyOnClose
      >
        {detailLoading ? (
          <Spin />
        ) : detail ? (
          <Space direction="vertical" style={{ width: '100%' }} size="large">
            {/* 基本信息 */}
            <Card size="small" title="基本信息">
              <Space size="large">
                <span>
                  状态: <Tag color={stateColor(detail.state)}>{detail.state}</Tag>
                </span>
                <span>协议: {detail.protocol}</span>
                <span>成员数: {detail.members.length}</span>
              </Space>
            </Card>

            {/* 成员列表 */}
            <Card size="small" title="成员列表">
              <Table
                rowKey="memberId"
                columns={memberColumns}
                dataSource={detail.members}
                size="small"
                pagination={false}
              />
            </Card>

            {/* 偏移量信息 */}
            <Card size="small" title="偏移量">
              <Table
                rowKey={(r) => `${r.topic}-${r.partition}`}
                columns={offsetColumns}
                dataSource={detail.offsets}
                size="small"
                pagination={{ pageSize: 10, showSizeChanger: false }}
              />
            </Card>
          </Space>
        ) : (
          <Typography.Text type="secondary">无数据</Typography.Text>
        )}
      </Drawer>
    </>
  )
}
