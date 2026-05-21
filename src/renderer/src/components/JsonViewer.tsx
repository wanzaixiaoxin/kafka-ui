import { useState } from 'react'
import { Button, Typography, message } from 'antd'

const { Text } = Typography

interface JsonViewerProps {
  /** 原始字符串数据，可能是 JSON */
  data: string
}

/** JSON 查看器组件 - 格式化显示 JSON 数据，支持复制 */
export default function JsonViewer({ data }: JsonViewerProps): JSX.Element {
  const [copied, setCopied] = useState(false)

  /** 尝试解析并格式化 JSON */
  let formatted: string
  let isJson = false
  try {
    const obj = JSON.parse(data)
    formatted = JSON.stringify(obj, null, 2)
    isJson = true
  } catch {
    formatted = data
  }

  /** 复制内容到剪贴板 */
  const copy = async (): Promise<void> => {
    try {
      await navigator.clipboard.writeText(formatted)
      setCopied(true)
      message.success('已复制')
      setTimeout(() => setCopied(false), 2000)
    } catch {
      message.error('复制失败')
    }
  }

  return (
    <div style={{ position: 'relative' }}>
      <Button
        size="small"
        type="text"
        onClick={copy}
        style={{ position: 'absolute', top: 8, right: 8, zIndex: 1 }}
      >
        {copied ? '已复制' : '复制'}
      </Button>
      <pre
        style={{
          maxHeight: 500,
          overflow: 'auto',
          fontSize: 12,
          padding: 12,
          background: '#f5f5f5',
          border: '1px solid #f0f0f0',
          borderRadius: 4,
          margin: 0,
          fontFamily: 'monospace',
          whiteSpace: 'pre-wrap',
          wordBreak: 'break-all'
        }}
      >
        {isJson ? (
          <SyntaxHighlight json={formatted} />
        ) : (
          <Text>{formatted}</Text>
        )}
      </pre>
    </div>
  )
}

/** 简易 JSON 语法高亮 */
function SyntaxHighlight({ json }: { json: string }): JSX.Element {
  /* 使用正则进行基础语法高亮 */
  const highlighted = json.replace(
    /("(?:\\.|[^"\\])*")\s*:/g,
    '<span style="color:#a31515">$1</span>:'
  ).replace(
    /:\s*("(?:\\.|[^"\\])*")/g,
    ': <span style="color:#0b7500">$1</span>'
  ).replace(
    /:\s*(\d+\.?\d*)/g,
    ': <span style="color:#1a1aaa">$1</span>'
  ).replace(
    /:\s*(true|false)/g,
    ': <span style="color:#1a1aaa">$1</span>'
  ).replace(
    /:\s*(null)/g,
    ': <span style="color:#1a1aaa">$1</span>'
  )

  return <span dangerouslySetInnerHTML={{ __html: highlighted }} />
}
