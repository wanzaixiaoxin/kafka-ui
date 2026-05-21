import { Table, Button } from 'antd'
import type { ColumnsType } from 'antd/es/table'
import type { ConsumedMessage } from '../types/kafka'

interface MessageTableProps {
  /** 消息列表 */
  messages: ConsumedMessage[]
  /** 点击消息回调 */
  onMessageClick?: (msg: ConsumedMessage) => void
}

/** 格式化时间戳为可读日期时间 */
function fmtTime(ts: number): string {
  return new Date(ts).toLocaleString('zh-CN', {
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit'
  })
}

/** 截断字符串 */
function truncate(s: string, max: number): string {
  if (!s) return ''
  return s.length > max ? s.slice(0, max) + '...' : s
}

/** 消息表格组件 - 显示 Kafka 消息列表 */
export default function MessageTable({ messages, onMessageClick }: MessageTableProps): JSX.Element {
  const cols: ColumnsType<ConsumedMessage> = [
    {
      title: '时间戳',
      dataIndex: 'timestamp',
      width: 180,
      render: (v: number) => fmtTime(v)
    },
    {
      title: '分区',
      dataIndex: 'partition',
      width: 60
    },
    {
      title: '偏移量',
      dataIndex: 'offset',
      width: 100
    },
    {
      title: 'Key',
      dataIndex: 'key',
      width: 150,
      ellipsis: true
    },
    {
      title: 'Value',
      dataIndex: 'value',
      ellipsis: true,
      render: (v: string) => {
        /* 尝试解析为 JSON 并格式化 */
        try {
          const obj = JSON.parse(v)
          return truncate(JSON.stringify(obj), 100)
        } catch {
          return truncate(v, 100)
        }
      }
    },
    {
      title: '操作',
      width: 80,
      render: (_v, record) => (
        <Button type="link" size="small" onClick={() => onMessageClick?.(record)}>
          详情
        </Button>
      )
    }
  ]

  return (
    <Table
      dataSource={messages}
      columns={cols}
      rowKey={(r) => `${r.partition}-${r.offset}`}
      size="small"
      pagination={{ pageSize: 50, size: 'small' }}
      scroll={{ y: 'calc(100vh - 400px)' }}
      style={{ width: '100%' }}
    />
  )
}
