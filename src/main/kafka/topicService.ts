import type { Admin } from 'kafkajs'
import type { TopicInfo, TopicDetail, PartitionOffset } from '../../shared/types'

/**
 * 获取 Topic 列表
 * 接收已连接的 admin 实例，用完不断开（由调用方池化管理）
 */
export async function listTopics(admin: Admin): Promise<TopicInfo[]> {
  const meta = await admin.fetchTopicMetadata()
  return meta.topics.map((t) => {
    const maxRf = t.partitions.reduce((m, p) => Math.max(m, p.replicas.length), 0)
    return {
      topic: t.name,
      partitions: t.partitions.length,
      replicas: maxRf,
      isInternal: (t as unknown as { internal?: boolean }).internal ?? t.name.startsWith('__')
    }
  })
}

/**
 * 获取 Topic 详情
 */
export async function describeTopic(admin: Admin, topic: string): Promise<TopicDetail> {
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
}

/**
 * 获取 Topic 各分区的 Offset 范围
 */
export async function getTopicOffsets(admin: Admin, topic: string): Promise<PartitionOffset[]> {
  const meta = await admin.fetchTopicMetadata({ topics: [topic] })
  const t = meta.topics[0]
  const partitions = t.partitions.map((p) => p.partitionId)

  const [earliest, latest] = await Promise.all([
    admin.fetchTopicOffsetsByTimestamp(topic, 0),
    admin.fetchTopicOffsets(topic)
  ])

  const earliestMap = new Map(earliest.map((e) => [e.partition, e.offset]))
  const latestMap = new Map(latest.map((l) => [l.partition, l.offset]))

  return partitions.sort((a, b) => a - b).map((pid) => ({
    partition: pid,
    earliestOffset: earliestMap.get(pid) ?? '0',
    latestOffset: latestMap.get(pid) ?? '0'
  }))
}
