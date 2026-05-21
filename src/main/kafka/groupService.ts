import type { Kafka } from 'kafkajs'
import type { ConsumerGroupInfo, ConsumerGroupDetail } from '../../renderer/src/types/kafka'

/**
 * 获取消费者组列表
 * 先通过 admin.listGroups 获取组基本信息，再逐个 describe 获取成员数和协议
 */
export async function listGroups(kafka: Kafka): Promise<ConsumerGroupInfo[]> {
  const admin = kafka.admin()
  try {
    await admin.connect()
    const { groups } = await admin.listGroups()

    const result: ConsumerGroupInfo[] = []
    for (const g of groups) {
      try {
        const detail = await admin.describeGroup(g.groupId)
        result.push({
          groupId: g.groupId,
          state: g.state,
          members: detail.members.length,
          protocol: detail.protocol || ''
        })
      } catch {
        /* 描述单个组失败时使用基本信息 */
        result.push({
          groupId: g.groupId,
          state: g.state,
          members: 0,
          protocol: ''
        })
      }
    }
    return result
  } finally {
    try { await admin.disconnect() } catch { /* 忽略断开错误 */ }
  }
}

/**
 * 获取消费者组详情
 * 包含组成员信息和消费偏移量（含 lag 计算）
 */
export async function describeGroup(
  kafka: Kafka,
  groupId: string
): Promise<ConsumerGroupDetail> {
  const admin = kafka.admin()
  try {
    await admin.connect()

    /* 获取组基本信息 */
    const desc = await admin.describeGroup(groupId)

    /* 提取成员列表 */
    const members = desc.members.map((m) => ({
      clientId: m.clientId,
      memberId: m.memberId,
      host: m.clientHost
    }))

    /* 获取消费者组的偏移量 */
    const offsets = await admin.fetchOffsets({ groupId })

    /* 按主题收集分区，批量获取各主题的 end offset 以计算 lag */
    const topicSet = new Set(offsets.map((o) => o.topic))
    const endOffsetMap = new Map<string, Map<number, string>>()

    for (const topic of topicSet) {
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

    /* 计算每条 offset 记录的 lag */
    const offsetDetails = offsets.map((o) => {
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
  } finally {
    try { await admin.disconnect() } catch { /* 忽略断开错误 */ }
  }
}
