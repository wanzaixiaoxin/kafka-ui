use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::client::DefaultClientContext;
use rdkafka::util::Timeout;
use serde::{Deserialize, Serialize};
use std::time::Duration;

/// 发送到 Kafka 的消息
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct KafkaMessage {
    pub topic: String,
    pub key: Option<String>,
    pub value: String,
    pub partition: Option<i32>,
    pub headers: Option<std::collections::HashMap<String, String>>,
}

/// 发送结果
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SendResult {
    pub topic: String,
    pub partition: i32,
    pub offset: String,
    pub timestamp: i64,
}

/// 发送消息到 Kafka
pub async fn send_message(
    producer: &FutureProducer<DefaultClientContext>,
    msg: &KafkaMessage,
) -> Result<SendResult, String> {
    let timeout = Timeout::After(Duration::from_secs(10));

    let mut record = FutureRecord::to(&msg.topic).payload(&msg.value);

    if let Some(ref key) = msg.key {
        record = record.key(key);
    }

    if let Some(partition) = msg.partition {
        record = record.partition(partition);
    }

    // 构建 headers
    if let Some(ref headers) = msg.headers {
        let mut owned_headers = rdkafka::message::OwnedHeaders::new();
        for (k, v) in headers {
            owned_headers = owned_headers.insert(rdkafka::message::Header {
                key: k,
                value: Some(v),
            });
        }
        record = record.headers(owned_headers);
    }

    let delivery = producer
        .send(record, timeout)
        .await
        .map_err(|e| format!("Send failed: {e:?}"))?;

    Ok(SendResult {
        topic: msg.topic.clone(),
        partition: delivery.0,
        offset: delivery.1.to_string(),
        timestamp: chrono::Utc::now().timestamp_millis(),
    })
}
