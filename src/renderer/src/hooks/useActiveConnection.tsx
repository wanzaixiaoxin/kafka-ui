import { useState, useEffect, useCallback } from 'react'
import { kafkaApiClient } from '../services/kafkaApiClient'
import type { KafkaConnection } from '../types/kafka'

/** 返回值类型 */
interface UseActiveConnectionResult {
  /** 是否有可用连接 */
  hasConn: boolean
  /** 连接列表是否正在加载 */
  loading: boolean
  /** 手动重新检查连接 */
  reload: () => Promise<void>
}

/**
 * 通用的"检查激活连接" hook
 * 用于 Topics、Producer、Consumer、ConsumerGroups 等页面，
 * 避免每个页面重复编写连接检查 + 无连接提示逻辑。
 */
export function useActiveConnection(): UseActiveConnectionResult {
  const [hasConn, setHasConn] = useState(true)
  const [loading, setLoading] = useState(false)

  const reload = useCallback(async (): Promise<void> => {
    setLoading(true)
    try {
      const id = await kafkaApiClient.connections.activeId()
      setHasConn(!!id)
    } catch {
      setHasConn(false)
    } finally {
      setLoading(false)
    }
  }, [])

  useEffect(() => {
    reload()
    /* 监听连接变更事件 */
    const unsub = kafkaApiClient.connections.onChanged(() => { reload() })
    return () => { unsub() }
  }, [reload])

  return { hasConn, loading, reload }
}

/**
 * 获取激活连接详情的 hook
 * 用于 AppLayout 等需要显示当前连接信息的组件
 */
export function useActiveConnectionDetail(): {
  activeConn: KafkaConnection | null
  reload: () => Promise<void>
} {
  const [activeConn, setActiveConn] = useState<KafkaConnection | null>(null)

  const reload = useCallback(async (): Promise<void> => {
    try {
      const activeId = await kafkaApiClient.connections.activeId()
      if (activeId) {
        const list = await kafkaApiClient.connections.list()
        const found = list.find((c) => c.id === activeId)
        if (found) setActiveConn(found)
        else setActiveConn(null)
      } else {
        setActiveConn(null)
      }
    } catch {
      /* 忽略 */
    }
  }, [])

  useEffect(() => {
    reload()
    const unsub = kafkaApiClient.connections.onChanged(() => { reload() })
    return () => { unsub() }
  }, [reload])

  return { activeConn, reload }
}
