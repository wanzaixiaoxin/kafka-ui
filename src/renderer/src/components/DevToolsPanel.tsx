/**
 * DevToolsPanel - 类似 Chrome DevTools Console 的日志面板
 * 支持：
 * - 实时日志流（主进程 + 渲染进程）
 * - 按级别过滤（info / warn / error / all）
 * - 关键字搜索
 * - 展开错误堆栈
 * - 自动滚动到底部
 * - 清空日志
 */
import { useState, useEffect, useRef, useCallback, useMemo } from 'react'
import { Input, Tag, Button, Space, Tooltip, Empty, Spin } from 'antd'
import {
  ClearOutlined,
  SearchOutlined,
  DownOutlined,
  UpOutlined,
  BugOutlined,
  ExpandOutlined,
  CopyOutlined
} from '@ant-design/icons'
import { kafkaApiClient } from '../services/kafkaApiClient'
import type { LogEntry, LogLevel } from '../types/kafka'

/** 级别对应的颜色和标签 */
const LEVEL_CONFIG: Record<LogLevel, { color: string; bg: string; label: string }> = {
  debug: { color: '#888', bg: '#f5f5f5', label: 'DEBUG' },
  info: { color: '#1890ff', bg: '#e6f7ff', label: 'INFO' },
  warn: { color: '#fa8c16', bg: '#fff7e6', label: 'WARN' },
  error: { color: '#ff4d4f', bg: '#fff2f0', label: 'ERROR' },
  fatal: { color: '#fff', bg: '#cf1322', label: 'FATAL' }
}

/** 级别过滤选项 */
type FilterLevel = 'all' | LogLevel

interface Props {
  /** 面板是否打开 */
  visible: boolean
}

export default function DevToolsPanel({ visible }: Props): JSX.Element | null {
  const [logs, setLogs] = useState<LogEntry[]>([])
  const [filterLevel, setFilterLevel] = useState<FilterLevel>('all')
  const [searchText, setSearchText] = useState('')
  const [autoScroll, setAutoScroll] = useState(true)
  const [expandedIds, setExpandedIds] = useState<Set<string>>(new Set())
  const [loading, setLoading] = useState(false)
  const listRef = useRef<HTMLDivElement>(null)
  const bottomRef = useRef<HTMLDivElement>(null)

  /** 加载历史日志 */
  const loadLogs = useCallback(async () => {
    setLoading(true)
    try {
      const allLogs = await kafkaApiClient.log.getAll()
      setLogs(allLogs)
    } catch { /* 忽略 */ } finally {
      setLoading(false)
    }
  }, [])

  /** 面板打开时加载历史 + 注册实时监听 */
  useEffect(() => {
    if (!visible) return

    loadLogs()

    const unsub = kafkaApiClient.log.onEntry((entry) => {
      setLogs((prev) => {
        const next = [...prev, entry]
        return next.length > 5000 ? next.slice(-5000) : next
      })
    })

    return () => { unsub() }
  }, [visible, loadLogs])

  /** 自动滚动到底部 */
  useEffect(() => {
    if (autoScroll && bottomRef.current) {
      bottomRef.current.scrollIntoView({ behavior: 'smooth' })
    }
  }, [logs, autoScroll])

  /** 手动滚动时判断是否关闭自动滚动 */
  const handleScroll = (): void => {
    if (!listRef.current) return
    const { scrollTop, scrollHeight, clientHeight } = listRef.current
    setAutoScroll(scrollHeight - scrollTop - clientHeight < 50)
  }

  /** 切换展开/折叠 */
  const toggleExpand = (id: string): void => {
    setExpandedIds((prev) => {
      const next = new Set(prev)
      if (next.has(id)) next.delete(id)
      else next.add(id)
      return next
    })
  }

  /** 清空日志 */
  const handleClear = async (): Promise<void> => {
    await kafkaApiClient.log.clear()
    setLogs([])
  }

  /** 复制所有日志到剪贴板 */
  const handleCopy = (): void => {
    const text = filteredLogs
      .map((l) => {
        const time = new Date(l.timestamp).toLocaleString('zh-CN')
        const level = LEVEL_CONFIG[l.level].label
        let line = `[${time}] [${level}] [${l.source}] ${l.message}`
        if (l.stack) line += `\n${l.stack}`
        if (l.data) line += `\n${l.data}`
        return line
      })
      .join('\n')
    navigator.clipboard.writeText(text)
  }

  /** 过滤后的日志（useMemo 避免每次渲染都重新遍历） */
  const filteredLogs = useMemo(() => {
    return logs.filter((l) => {
      if (filterLevel !== 'all' && l.level !== filterLevel) return false
      if (searchText) {
        const s = searchText.toLowerCase()
        return (
          l.message.toLowerCase().includes(s) ||
          (l.stack && l.stack.toLowerCase().includes(s)) ||
          (l.data && l.data.toLowerCase().includes(s))
        )
      }
      return true
    })
  }, [logs, filterLevel, searchText])

  /** 统计各级别数量（useMemo 缓存） */
  const counts = useMemo(() => ({
    all: logs.length,
    error: logs.filter((l) => l.level === 'error' || l.level === 'fatal').length,
    warn: logs.filter((l) => l.level === 'warn').length,
    info: logs.filter((l) => l.level === 'info' || l.level === 'debug').length
  }), [logs])

  /** 快捷跳转到第一个 error */
  const jumpToFirstError = (): void => {
    const idx = filteredLogs.findIndex((l) => l.level === 'error' || l.level === 'fatal')
    if (idx >= 0) {
      const el = document.getElementById(`log-${filteredLogs[idx].id}`)
      el?.scrollIntoView({ behavior: 'smooth', block: 'center' })
      el?.classList.add('log-highlight')
      setTimeout(() => el?.classList.remove('log-highlight'), 2000)
    }
  }

  if (!visible) return null

  return (
    <div
      style={{
        height: '100%',
        display: 'flex',
        flexDirection: 'column',
        background: '#1e1e1e',
        color: '#d4d4d4',
        fontFamily: "'Consolas', 'Monaco', 'Courier New', monospace",
        fontSize: 12
      }}
    >
      {/* 工具栏 */}
      <div
        style={{
          display: 'flex',
          alignItems: 'center',
          padding: '4px 8px',
          background: '#252526',
          borderBottom: '1px solid #3c3c3c',
          gap: 8,
          flexShrink: 0
        }}
      >
        {/* 级别过滤 */}
        <Space size={4}>
          {(
            [
              { key: 'all', label: '全部', count: counts.all },
              { key: 'info', label: 'Info', count: counts.info },
              { key: 'warn', label: 'Warn', count: counts.warn },
              { key: 'error', label: 'Error', count: counts.error }
            ] as const
          ).map(({ key, label, count }) => (
            <Button
              key={key}
              size="small"
              type={filterLevel === key ? 'primary' : 'text'}
              onClick={() => setFilterLevel(key)}
              style={{
                color: filterLevel === key ? '#fff' : '#aaa',
                fontSize: 11,
                height: 24,
                padding: '0 8px'
              }}
            >
              {label}{' '}
              <span style={{ opacity: 0.7, fontSize: 10 }}>
                {count}
              </span>
            </Button>
          ))}
        </Space>

        {/* 搜索框 */}
        <Input
          size="small"
          placeholder="搜索日志..."
          prefix={<SearchOutlined style={{ color: '#666' }} />}
          value={searchText}
          onChange={(e) => setSearchText(e.target.value)}
          style={{ width: 200, background: '#3c3c3c', borderColor: '#555' }}
          allowClear
        />

        {/* 右侧操作按钮 */}
        <div style={{ marginLeft: 'auto' }}>
          <Space size={4}>
            <Tooltip title="跳转到第一个错误">
              <Button
                size="small"
                type="text"
                icon={<BugOutlined />}
                onClick={jumpToFirstError}
                style={{ color: counts.error > 0 ? '#ff4d4f' : '#666' }}
              />
            </Tooltip>
            <Tooltip title="复制全部日志">
              <Button
                size="small"
                type="text"
                icon={<CopyOutlined />}
                onClick={handleCopy}
                style={{ color: '#aaa' }}
              />
            </Tooltip>
            <Tooltip title="清空日志">
              <Button
                size="small"
                type="text"
                icon={<ClearOutlined />}
                onClick={handleClear}
                style={{ color: '#aaa' }}
              />
            </Tooltip>
            <Tooltip title={autoScroll ? '停止自动滚动' : '开启自动滚动'}>
              <Button
                size="small"
                type="text"
                icon={autoScroll ? <DownOutlined /> : <UpOutlined />}
                onClick={() => setAutoScroll(!autoScroll)}
                style={{ color: autoScroll ? '#1890ff' : '#aaa' }}
              />
            </Tooltip>
          </Space>
        </div>
      </div>

      {/* 日志列表 */}
      <div
        ref={listRef}
        onScroll={handleScroll}
        style={{
          flex: 1,
          overflowY: 'auto',
          overflowX: 'hidden',
          padding: '4px 0'
        }}
      >
        {loading && (
          <div style={{ textAlign: 'center', padding: 16 }}>
            <Spin size="small" />
          </div>
        )}

        {!loading && filteredLogs.length === 0 && (
          <Empty
            image={Empty.PRESENTED_IMAGE_SIMPLE}
            description={<span style={{ color: '#666' }}>暂无日志</span>}
            style={{ marginTop: 40 }}
          />
        )}

        {filteredLogs.map((log) => {
          const cfg = LEVEL_CONFIG[log.level]
          const isExpanded = expandedIds.has(log.id)
          const hasDetails = log.stack || log.data
          const time = new Date(log.timestamp).toLocaleTimeString('zh-CN', {
            hour12: false,
            hour: '2-digit',
            minute: '2-digit',
            second: '2-digit'
          })
          const ms = new Date(log.timestamp).getMilliseconds().toString().padStart(3, '0')

          return (
            <div
              key={log.id}
              id={`log-${log.id}`}
              style={{
                padding: '3px 8px 3px 0',
                borderBottom: '1px solid #2d2d2d',
                background: cfg.bg === '#fff2f0' ? '#3a2020' : cfg.bg === '#fff7e6' ? '#3a3020' : 'transparent',
                cursor: hasDetails ? 'pointer' : 'default',
                transition: 'background 0.3s'
              }}
              onClick={hasDetails ? () => toggleExpand(log.id) : undefined}
            >
              {/* 第一行：时间 + 级别 + 来源 + 消息 */}
              <div style={{ display: 'flex', alignItems: 'flex-start' }}>
                <span style={{ color: '#666', flexShrink: 0, width: 90 }}>
                  {time}.{ms}
                </span>
                <Tag
                  color={cfg.color}
                  style={{
                    margin: 0,
                    lineHeight: '18px',
                    fontSize: 10,
                    padding: '0 4px',
                    flexShrink: 0
                  }}
                >
                  {cfg.label}
                </Tag>
                <Tag
                  style={{
                    margin: '0 4px',
                    lineHeight: '18px',
                    fontSize: 10,
                    padding: '0 4px',
                    background: '#333',
                    color: '#999',
                    border: 'none',
                    flexShrink: 0
                  }}
                >
                  {log.source}
                </Tag>
                <span
                  style={{
                    color: cfg.color === '#fff' ? '#ff9999' : cfg.color,
                    wordBreak: 'break-all',
                    flex: 1
                  }}
                >
                  {log.message}
                </span>
                {hasDetails && (
                  <ExpandOutlined
                    style={{
                      color: '#666',
                      marginLeft: 4,
                      flexShrink: 0,
                      transform: isExpanded ? 'rotate(90deg)' : 'none',
                      transition: 'transform 0.15s'
                    }}
                  />
                )}
              </div>

              {/* 展开区域：堆栈 + 附加数据 */}
              {isExpanded && hasDetails && (
                <div style={{ marginLeft: 90, marginTop: 4 }}>
                  {log.data && (
                    <pre
                      style={{
                        margin: '4px 0',
                        padding: 8,
                        background: '#2d2d2d',
                        borderRadius: 4,
                        fontSize: 11,
                        color: '#ccc',
                        whiteSpace: 'pre-wrap',
                        wordBreak: 'break-all',
                        maxHeight: 200,
                        overflow: 'auto'
                      }}
                    >
                      {log.data}
                    </pre>
                  )}
                  {log.stack && (
                    <pre
                      style={{
                        margin: '4px 0',
                        padding: 8,
                        background: '#3a1c1c',
                        borderRadius: 4,
                        fontSize: 11,
                        color: '#ff9999',
                        whiteSpace: 'pre-wrap',
                        wordBreak: 'break-all',
                        maxHeight: 300,
                        overflow: 'auto',
                        borderLeft: '3px solid #ff4d4f'
                      }}
                    >
                      {log.stack}
                    </pre>
                  )}
                </div>
              )}
            </div>
          )
        })}

        <div ref={bottomRef} />
      </div>

      {/* 底部状态栏 */}
      <div
        style={{
          display: 'flex',
          alignItems: 'center',
          padding: '2px 8px',
          background: '#252526',
          borderTop: '1px solid #3c3c3c',
          fontSize: 11,
          color: '#666',
          flexShrink: 0
        }}
      >
        <span>显示 {filteredLogs.length} / {logs.length} 条日志</span>
        <span style={{ marginLeft: 'auto' }}>
          按 <kbd style={{ background: '#3c3c3c', padding: '0 4px', borderRadius: 2 }}>F12</kbd> 切换面板
        </span>
      </div>

      {/* 高亮动画样式 */}
      <style>{`
        .log-highlight {
          animation: logFlash 2s ease-out;
        }
        @keyframes logFlash {
          0% { background: #ff4d4f33 !important; }
          100% { background: transparent !important; }
        }
      `}</style>
    </div>
  )
}
