use std::time::Duration;

use rdkafka::admin::AdminClient;
use rdkafka::client::DefaultClientContext;
use rdkafka::consumer::{BaseConsumer, Consumer, DefaultConsumerContext};
use rdkafka::Offset;
use serde::{Deserialize, Serialize};
use tracing::warn;

use crate::kafka::connection_manager::build_config_for_record;
use crate::store::connection_store::ConnectionRecord;

/// 消费者组基本信息
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConsumerGroupInfo {
    #[serde(rename = "groupId")]
    pub group_id: String,
    pub state: String,
    pub members: usize,
    pub protocol: String,
}

/// 消费者组成员
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GroupMember {
    #[serde(rename = "clientId")]
    pub client_id: String,
    #[serde(rename = "memberId")]
    pub member_id: String,
    pub host: String,
}

/// 消费者组详情
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConsumerGroupDetail {
    #[serde(rename = "groupId")]
    pub group_id: String,
    pub state: String,
    pub protocol: String,
    pub members: Vec<GroupMember>,
    pub offsets: Vec<GroupOffsetDetail>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GroupOffsetDetail {
    pub topic: String,
    pub partition: i32,
    #[serde(rename = "currentOffset")]
    pub current_offset: String,
    #[serde(rename = "logEndOffset")]
    pub log_end_offset: String,
    pub lag: String,
}

const TIMEOUT: Duration = Duration::from_secs(10);
const WATERMARK_TIMEOUT: Duration = Duration::from_secs(5);
const OFFSET_FETCH_TIMEOUT: Duration = Duration::from_secs(10);

/// 列出消费者组
pub fn list_groups(
    admin: &AdminClient<DefaultClientContext>,
) -> Result<Vec<ConsumerGroupInfo>, String> {
    let group_list = admin
        .inner()
        .fetch_group_list(None, TIMEOUT)
        .map_err(|e| format!("Failed to fetch group list: {e}"))?;

    let groups: Vec<ConsumerGroupInfo> = group_list
        .groups()
        .iter()
        .map(|g| ConsumerGroupInfo {
            group_id: g.name().to_string(),
            state: g.state().to_string(),
            members: g.members().len(),
            protocol: g.protocol().to_string(),
        })
        .collect();

    Ok(groups)
}

/// 查看消费者组详情（含 Offset / Lag）
pub fn describe_group(
    admin: &AdminClient<DefaultClientContext>,
    conn: &ConnectionRecord,
    group_id: &str,
) -> Result<ConsumerGroupDetail, String> {
    let group_list = admin
        .inner()
        .fetch_group_list(Some(group_id), TIMEOUT)
        .map_err(|e| format!("Failed to fetch group info: {e}"))?;

    let g = group_list
        .groups()
        .first()
        .ok_or_else(|| format!("Group not found: {group_id}"))?;

    let members: Vec<GroupMember> = g
        .members()
        .iter()
        .map(|m| GroupMember {
            client_id: m.client_id().to_string(),
            member_id: m.id().to_string(),
            host: m.client_host().to_string(),
        })
        .collect();

    // 查询已提交的 Offset + 计算 Lag
    // 注意: 此操作为 best-effort，失败时返回空 offsets 而非阻断整个详情查询
    let offsets = fetch_group_offsets(admin, conn, group_id);

    Ok(ConsumerGroupDetail {
        group_id: g.name().to_string(),
        state: g.state().to_string(),
        protocol: g.protocol().to_string(),
        members,
        offsets,
    })
}

/// 查询消费者组的已提交 Offset 与 Lag。
///
/// 实现方式:
/// 1. 获取集群所有 Topic+Partition 列表
/// 2. 创建临时 BaseConsumer (设置 group.id 但不 join group)
/// 3. assign 所有分区，调用 committed() 发送 OffsetFetch 请求
/// 4. 对每个有已提交 offset 的分区，fetch_watermarks 获取 Log End Offset
/// 5. Lag = Log End Offset - Committed Offset
///
/// 安全性: BaseConsumer + assign 不会触发 group rebalance，
/// committed() 只发送只读的 OffsetFetch 请求。
fn fetch_group_offsets(
    admin: &AdminClient<DefaultClientContext>,
    conn: &ConnectionRecord,
    group_id: &str,
) -> Vec<GroupOffsetDetail> {
    // 1. 获取集群元数据 (所有 Topic + Partition)
    let metadata = match admin.inner().fetch_metadata(None, TIMEOUT) {
        Ok(m) => m,
        Err(e) => {
            warn!("fetch_group_offsets: failed to fetch metadata: {e}");
            return Vec::new();
        }
    };

    // 2. 构建全量 TopicPartitionList
    let mut tpl = rdkafka::TopicPartitionList::new();
    for topic in metadata.topics() {
        for partition in topic.partitions() {
            tpl.add_partition(topic.name(), partition.id());
        }
    }

    // 3. 创建临时 BaseConsumer
    let mut config = match build_config_for_record(conn) {
        Ok(c) => c,
        Err(e) => {
            warn!("fetch_group_offsets: failed to build config: {e}");
            return Vec::new();
        }
    };
    config.set("group.id", group_id);
    config.set("enable.auto.commit", "false");
    config.set("session.timeout.ms", "6000");
    config.set("request.timeout.ms", "5000");

    let consumer: BaseConsumer<DefaultConsumerContext> =
        match config.create_with_context(DefaultConsumerContext) {
            Ok(c) => c,
            Err(e) => {
                warn!("fetch_group_offsets: failed to create consumer: {e}");
                return Vec::new();
            }
        };

    // 4. assign 全部分区 (仅用于查询，不消费消息)
    if let Err(e) = consumer.assign(&tpl) {
        warn!("fetch_group_offsets: assign failed: {e}");
        return Vec::new();
    }

    // 5. 查询已提交 Offset (OffsetFetch 请求)
    let committed = match consumer.committed(OFFSET_FETCH_TIMEOUT) {
        Ok(c) => c,
        Err(e) => {
            warn!("fetch_group_offsets: committed() failed for '{group_id}': {e}");
            return Vec::new();
        }
    };

    // 6. 对每个有已提交 offset 的分区，获取 Log End Offset 并计算 Lag
    let mut offsets = Vec::new();
    for elem in committed.elements() {
        let committed_off = match elem.offset() {
            Offset::Offset(o) if o >= 0 => o,
            _ => continue, // Invalid / Beginning / End / 负值 → 跳过
        };

        let topic_name = elem.topic();
        let partition_id = elem.partition();

        let (_, high) = admin
            .inner()
            .fetch_watermarks(topic_name, partition_id, WATERMARK_TIMEOUT)
            .unwrap_or((0, committed_off));

        let lag = (high - committed_off).max(0);

        offsets.push(GroupOffsetDetail {
            topic: topic_name.to_string(),
            partition: partition_id,
            current_offset: committed_off.to_string(),
            log_end_offset: high.to_string(),
            lag: lag.to_string(),
        });
    }

    offsets.sort_by(|a, b| a.topic.cmp(&b.topic).then(a.partition.cmp(&b.partition)));
    offsets
}
