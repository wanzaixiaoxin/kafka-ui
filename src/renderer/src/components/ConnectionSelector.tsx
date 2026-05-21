import { useState, useEffect, useCallback } from 'react'
import { Select, Badge, Space, Typography, Tooltip, Modal, message } from 'antd'
import { kafkaApiClient } from '../services/kafkaApiClient'
import type { KafkaConnection } from '../types/kafka'

const { Text } = Typography

/** 连接选择器组件 - 用于在顶部选择当前 Kafka 连接，带状态指示和 Tooltip */
export default function ConnectionSelector(): JSX.Element {
  const [conns, setConns] = useState<KafkaConnection[]>([])
  const [activeId, setActiveId] = useState<string | null>(null)

  /** 加载连接列表 */
  const load = useCallback(async (): Promise<void> => {
    try {
      const list = await kafkaApiClient.connections.list()
      setConns(list)
    } catch {
      /* 忽略加载错误 */
    }
  }, [])

  useEffect(() => {
    load()
  }, [load])

  /** 获取当前激活连接信息 */
  const activeConn = conns.find((c) => c.id === activeId)

  /** 切换连接（带确认弹窗） */
  const onChange = async (id: string): Promise<void> => {
    if (activeId && id !== activeId) {
      /* 切换连接时确认 */
      Modal.confirm({
        title: '切换连接',
        content: '切换连接将停止当前所有正在运行的消费者，确认继续？',
        okText: '确认切换',
        cancelText: '取消',
        onOk: async () => {
          try {
            const res = await kafkaApiClient.connections.use(id)
            if (res.success) {
              setActiveId(id)
              message.success('已切换连接')
            } else {
              message.error(res.error || '切换失败')
            }
          } catch (err: unknown) {
            message.error(err instanceof Error ? err.message : '切换连接失败')
          }
        }
      })
    } else {
      /* 首次选择连接，无需确认 */
      try {
        const res = await kafkaApiClient.connections.use(id)
        if (res.success) {
          setActiveId(id)
        } else {
          message.error(res.error || '切换失败')
        }
      } catch (err: unknown) {
        message.error(err instanceof Error ? err.message : '切换连接失败')
      }
    }
  }

  /** 下拉选项 */
  const options = conns.map((c) => ({
    value: c.id,
    label: (
      <Tooltip
        title={`Brokers: ${c.brokers.join(', ')}${c.description ? `\n${c.description}` : ''}`}
        placement="bottom"
      >
        <Space>
          <Badge status={c.id === activeId ? 'success' : 'default'} />
          <span>{c.name}</span>
          <Text type="secondary" style={{ fontSize: 11 }}>
            {c.brokers[0]}
            {c.brokers.length > 1 ? ` +${c.brokers.length - 1}` : ''}
          </Text>
        </Space>
      </Tooltip>
    )
  }))

  return (
    <Space>
      <Badge status={activeId ? 'success' : 'default'} />
      <Select
        placeholder="未选择连接"
        value={activeId || undefined}
        onChange={onChange}
        style={{ width: 220 }}
        options={options}
        notFoundContent="暂无连接"
        allowClear={false}
      />
      {activeConn && (
        <Tooltip title={activeConn.brokers.join(', ')}>
          <Text type="secondary" style={{ color: 'rgba(255,255,255,0.65)', fontSize: 11 }}>
            {activeConn.brokers.join(', ')}
          </Text>
        </Tooltip>
      )}
    </Space>
  )
}
