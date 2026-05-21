import type { Kafka } from 'kafkajs'
import type { TopicInfo, TopicDetail, PartitionOffset } from '../../renderer/src/types/kafka'

/**
 * 获取 Topic 列表
 * 使用 admin 客户端获取 topic 元数据，返回基本信息数组
 */
export async function listTopics(kafka: Kafka): Promise<TopicInfo[]> {
  const admin = kafka.admin()
  try {
    await admin.connect()
    const meta = await admin.fetchTopicMetadata()
    const list: TopicInfo[] = meta.topics.map((t) => {
      /* 计算最大副本因子 */
      const maxRf = t.partitions.reduce((m, p) => Math.max(m, p.replicas.length), 0)
      return {
        topic: t.name,
        partitions: t.partitions.length,
        replicas: maxRf,
        isInternal: false
      }
    })
    return list
  } finally {
    try { await admin.disconnect() } catch { /* 忽略断开错误 */ }
  }
}

/**
 * 获取 Topic 详情
 * 返回各分区的 leader、副本和 ISR 信息
 */
export async function describeTopic(kafka: Kafka, topic: string): Promise<TopicDetail> {
  const admin = kafka.admin()
  try {
    await admin.connect()
    const meta = await admin.fetchTopicMetadata({ topics: [topic] })
    const t = meta.topics[0]
    return {
      topic: t.name,
      partitions: t.partitions.map((p) => ({
        partitionId: p.partitionId,
        leader: p.leader,
        replicas: [...p.replicas],
        isr: [...p.isr]
      }))
    }
  } finally {
    try { await admin.disconnect() } catch { /* 忽略断开错误 */ }
  }
}

/**
 * 获取 Topic 各分区的 Offset 范围
 * 返回最早和最新 offset
 */
export async function getTopicOffsets(kafka: Kafka, topic: string): Promise<PartitionOffset[]> {
  const admin = kafka.admin()
  try {
    await admin.connect()
    /* 获取 topic 的分区列表 */
    const meta = await admin.fetchTopicMetadata({ topics: [topic] })
    const t = meta.topics[0]
    const partitions = t.partitions.map((p) => p.partitionId)

    /* 批量查询 earliest 和 latest offset */
    const [earliest, latest] = await Promise.all([
      admin.fetchTopicOffsetsByTimestamp(topic, 0),
      admin.fetchTopicOffsets(topic)
    ])

    /* 按 partition 排序 */
    const earliestMap = new Map(earliest.map((e) => [e.partition, e.offset]))
    const latestMap = new Map(latest.map((l) => [l.partition, l.offset]))

    return partitions.sort((a, b) => a - b).map((pid) => ({
      partition: pid,
      earliestOffset: earliestMap.get(pid) ?? '0',
      latestOffset: latestMap.get(pid) ?? '0'
    }))
  } finally {
    try { await admin.disconnect() } catch { /* 忽略断开错误 */ }
  }
}
