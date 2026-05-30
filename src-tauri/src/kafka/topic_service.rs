use std::time::Duration;

use rdkafka::admin::AdminClient;
use rdkafka::admin::AdminOptions;
use rdkafka::admin::NewTopic;
use rdkafka::admin::TopicReplication;
use rdkafka::client::DefaultClientContext;
use serde::{Deserialize, Serialize};

/// Topic 基本信息
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TopicInfo {
    pub topic: String,
    pub partitions: usize,
    pub replicas: usize,
    #[serde(rename = "isInternal")]
    pub is_internal: bool,
}

/// 分区信息
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartitionInfo {
    #[serde(rename = "partitionId")]
    pub partition_id: i32,
    pub leader: i32,
    pub replicas: Vec<i32>,
    pub isr: Vec<i32>,
}

/// Topic 详情
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TopicDetail {
    pub topic: String,
    pub partitions: Vec<PartitionInfo>,
}

/// 分区 Offset 信息
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartitionOffset {
    pub partition: i32,
    #[serde(rename = "earliestOffset")]
    pub earliest_offset: String,
    #[serde(rename = "latestOffset")]
    pub latest_offset: String,
}

const METADATA_TIMEOUT: Duration = Duration::from_secs(10);

/// 列出所有 Topic
pub fn list_topics(
    admin: &AdminClient<DefaultClientContext>,
    show_internal: bool,
) -> Result<Vec<TopicInfo>, String> {
    let metadata = admin
        .inner()
        .fetch_metadata(None, METADATA_TIMEOUT)
        .map_err(|e| format!("Failed to fetch metadata: {e}"))?;

    let topics: Vec<TopicInfo> = metadata
        .topics()
        .iter()
        .filter(|t| show_internal || !t.name().starts_with("__"))
        .map(|t| {
            let replicas = t
                .partitions()
                .iter()
                .map(|p| p.replicas().len())
                .max()
                .unwrap_or(0);
            TopicInfo {
                topic: t.name().to_string(),
                partitions: t.partitions().len(),
                replicas,
                is_internal: t.name().starts_with("__"),
            }
        })
        .collect();

    Ok(topics)
}

/// 查看 Topic 详情
pub fn describe_topic(
    admin: &AdminClient<DefaultClientContext>,
    topic: &str,
) -> Result<TopicDetail, String> {
    // rdkafka 0.37: fetch_metadata with Option<&str>
    let metadata = admin
        .inner()
        .fetch_metadata(Some(topic), METADATA_TIMEOUT)
        .map_err(|e| format!("Failed to fetch metadata: {e}"))?;

    let t = metadata
        .topics()
        .first()
        .ok_or_else(|| format!("Topic not found: {topic}"))?;

    let partitions: Vec<PartitionInfo> = t
        .partitions()
        .iter()
        .map(|p| PartitionInfo {
            partition_id: p.id(),
            leader: p.leader(),
            replicas: p.replicas().to_vec(),
            isr: p.isr().to_vec(),
        })
        .collect();

    Ok(TopicDetail {
        topic: t.name().to_string(),
        partitions,
    })
}

/// 获取各分区的 Offset 范围
pub fn get_topic_offsets(
    admin: &AdminClient<DefaultClientContext>,
    topic: &str,
) -> Result<Vec<PartitionOffset>, String> {
    let metadata = admin
        .inner()
        .fetch_metadata(Some(topic), METADATA_TIMEOUT)
        .map_err(|e| format!("Failed to fetch metadata: {e}"))?;

    let t = metadata
        .topics()
        .first()
        .ok_or_else(|| format!("Topic not found: {topic}"))?;

    let mut offsets = Vec::new();
    let timeout = Duration::from_secs(5);

    for p in t.partitions() {
        let pid = p.id();

        let (low, high) = admin
            .inner()
            .fetch_watermarks(topic, pid, timeout)
            .map_err(|e| format!("Failed to fetch offsets for partition {pid}: {e}"))?;

        offsets.push(PartitionOffset {
            partition: pid,
            earliest_offset: low.to_string(),
            latest_offset: high.to_string(),
        });
    }

    offsets.sort_by_key(|o| o.partition);
    Ok(offsets)
}

/// 创建 Topic
pub fn create_topic(
    admin: &AdminClient<DefaultClientContext>,
    topic: &str,
    num_partitions: i32,
    replication_factor: i32,
) -> Result<bool, String> {
    let new_topic = NewTopic {
        name: topic,
        num_partitions,
        replication: TopicReplication::Fixed(replication_factor),
        config: vec![],
    };

    let opts = AdminOptions::new().operation_timeout(Some(Duration::from_secs(30)));

    // rdkafka 0.37: create_topics returns Future<Output = KafkaResult<Vec<TopicResult>>>
    // 在 Tauri 命令（async）中通过 .await 获取结果
    futures::executor::block_on(async {
        let results = admin
            .create_topics(&[new_topic], &opts)
            .await
            .map_err(|e| format!("Failed to create topic: {e}"))?;

        // TopicResult 可能是 Ok(name) 或 Err((name, error))
        for result in results {
            if let Err((name, err)) = result {
                return Err(format!("Topic '{name}' creation failed: {err}"));
            }
        }
        Ok(true)
    })
}
