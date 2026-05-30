import { Button, Space, Table, Input, Popconfirm } from 'antd'
import { PlusOutlined, DeleteOutlined } from '@ant-design/icons'
import { useCallback } from 'react'

interface HeaderEditorProps {
  /** 当前 headers 值 */
  value?: Record<string, string>
  /** 值变更回调 */
  onChange?: (val: Record<string, string>) => void
}

/** 消息头编辑器组件 - 编辑 Kafka 消息的 Headers，支持受控模式 */
export default function HeaderEditor({ value = {}, onChange }: HeaderEditorProps): JSX.Element {
  /** 将 Record 转为数组便于编辑 */
  const rows = Object.entries(value).map(([k, v]) => ({ key: k, value: v }))

  /** 添加一个 header */
  const add = (): void => {
    const newVal = { ...value, '': '' }
    onChange?.(newVal)
  }

  /** 删除指定 key 的 header */
  const remove = (key: string): void => {
    const newVal = { ...value }
    delete newVal[key]
    onChange?.(newVal)
  }

  /** 更新指定 key 的 header */
  const update = useCallback(
    (oldKey: string, field: 'key' | 'value', val: string): void => {
      const newVal: Record<string, string> = {}
      for (const [k, v] of Object.entries(value)) {
        if (k === oldKey) {
          if (field === 'key') {
            newVal[val] = v
          } else {
            newVal[k] = val
          }
        } else {
          newVal[k] = v
        }
      }
      onChange?.(newVal)
    },
    [value, onChange]
  )

  return (
    <Space direction="vertical" style={{ width: '100%' }}>
      <Button size="small" icon={<PlusOutlined />} onClick={add}>
        添加 Header
      </Button>
      <Table
        dataSource={rows.map((h, i) => ({ ...h, _idx: i }))}
        rowKey="_idx"
        size="small"
        pagination={false}
        columns={[
          {
            title: 'Key',
            dataIndex: 'key',
            render: (v, _r, idx) => (
              <Input
                size="small"
                value={v}
                onChange={(e) => update(rows[idx].key, 'key', e.target.value)}
              />
            )
          },
          {
            title: 'Value',
            dataIndex: 'value',
            render: (v, _r, idx) => (
              <Input
                size="small"
                value={v}
                onChange={(e) => update(rows[idx].key, 'value', e.target.value)}
              />
            )
          },
          {
            title: '',
            width: 40,
            render: (_v, _r, idx) => (
              <Popconfirm title="确认删除?" onConfirm={() => remove(rows[idx].key)}>
                <Button size="small" type="text" danger icon={<DeleteOutlined />} />
              </Popconfirm>
            )
          }
        ]}
      />
    </Space>
  )
}
