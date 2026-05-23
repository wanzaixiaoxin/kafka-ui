import type { Admin } from 'kafkajs'
import type { ConsumerGroupInfo, ConsumerGroupDetail } from '../../shared/types'

/**
 * 获取消费者组列表
 */
export async function listGroups(admin: Admin): Promise<ConsumerGroupInfo[]> {
  const { groups } = await admin.listGroups()

  /* 批量获取所有组的详情以拿到 state 和成员数 */
  const groupIds = groups.map((g) => g.groupId)
  let groupDetails: Map<string, { state: string; members: unknown[]; protocol: string }> = new Map()

  try {
    const details = await admin.describeGroups(groupIds)
    for (const g of details.groups) {
      groupDetails.set(g.groupId, { state: g.state, members: g.members, protocol: g.protocol || '' })
    }
  } catch {
    /* describeGroups 失败时回退到无详情模式 */
  }

  return groups.map((g) => {
    const detail = groupDetails.get(g.groupId)
    return {
      groupId: g.groupId,
      state: detail?.state ?? 'Unknown',
      members: detail?.members?.length ?? 0,
      protocol: detail?.protocol ?? ''
    }
  })
}

/**
 * 获取消费者组详情
 */
export async function describeGroup(
  admin: Admin,
  groupId: string
): Promise<ConsumerGroupDetail> {
  const details = await admin.describeGroups([groupId])
  const desc = details.groups[0]

  const members = desc.members.map((m: { clientId: string; memberId: string; clientHost: string }) => ({
    clientId: m.clientId,
    memberId: m.memberId,
    host: m.clientHost
  }))

  const offsetsResult = await admin.fetchOffsets({ groupId })

  /* fetchOffsets 返回 { topic, partitions: [...] }[]，展平为单条记录 */
  const offsetRecords: Array<{ topic: string; partition: number; offset: string }> = []
  for (const t of offsetsResult) {
    for (const p of t.partitions) {
      offsetRecords.push({ topic: t.topic, partition: p.partition, offset: p.offset })
    }
  }

  const topicSet = new Set(offsetRecords.map((o) => o.topic))
  const endOffsetMap = new Map<string, Map<number, string>>()

  const topicArray = Array.from(topicSet)
  for (const topic of topicArray) {
    try {
      const topicOffsets = await admin.fetchTopicOffsets(topic)
      const m = new Map<number, string>()
      for (const to of topicOffsets) {
        m.set(to.partition, to.offset)
      }
      endOffsetMap.set(topic, m)
    } catch {
      /* 获取 topic offset 失败时跳过 */
    }
  }

  const offsetDetails = offsetRecords.map((o) => {
    const cur = isNaN(Number(o.offset)) ? 0 : Number(o.offset)
    const endMap = endOffsetMap.get(o.topic)
    const end = endMap ? Number(endMap.get(o.partition) ?? '0') : 0
    const lag = end - cur
    return {
      topic: o.topic,
      partition: o.partition,
      currentOffset: String(cur),
      logEndOffset: String(end),
      lag: String(lag >= 0 ? lag : 0)
    }
  })

  return {
    groupId,
    state: desc.state,
    protocol: desc.protocol || '',
    members,
    offsets: offsetDetails
  }
}
