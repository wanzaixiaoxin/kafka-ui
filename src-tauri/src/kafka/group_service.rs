use std::time::Duration;

use rdkafka::client::DefaultClientContext;
use rdkafka::admin::AdminClient;
use serde::{Deserialize, Serialize};

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

/// 查看消费者组详情
pub fn describe_group(
    admin: &AdminClient<DefaultClientContext>,
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

    // NOTE: rdkafka 0.37 GroupMemberInfo 不暴露 member assignment
    // Offset/lag 需要创建 Consumer 后调用 committed_offsets，暂不实现
    let offsets: Vec<GroupOffsetDetail> = vec![];

    Ok(ConsumerGroupDetail {
        group_id: g.name().to_string(),
        state: g.state().to_string(),
        protocol: g.protocol().to_string(),
        members,
        offsets,
    })
}
