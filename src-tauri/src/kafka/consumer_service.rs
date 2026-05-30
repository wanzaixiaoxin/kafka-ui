use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use rdkafka::consumer::{Consumer, DefaultConsumerContext, StreamConsumer};
use rdkafka::client::DefaultClientContext;
use rdkafka::message::{BorrowedMessage, Headers, Message};
use serde::{Deserialize, Serialize};
use tauri::AppHandle;
use tokio::sync::{mpsc, RwLock};
use tracing::{info, warn};
use uuid::Uuid;

use crate::kafka::connection_manager::build_config_for_record;
use crate::store::connection_store::ConnectionRecord;

/// 消费者启动选项
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConsumerOptions {
    pub topic: String,
    pub partition: Option<i32>,
    #[serde(rename = "fromBeginning")]
    pub from_beginning: Option<bool>,
    #[serde(rename = "fromOffset")]
    pub from_offset: Option<String>,
}

/// 消息浏览选项
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FetchMessagesOptions {
    pub topic: String,
    pub partition: Option<i32>,
    pub offset: Option<String>,
    #[serde(rename = "fromBeginning")]
    pub from_beginning: Option<bool>,
    pub limit: Option<u32>,
}

/// 已消费的消息
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConsumedMessage {
    pub topic: String,
    pub partition: i32,
    pub offset: String,
    pub key: Option<String>,
    pub value: String,
    pub headers: Option<HashMap<String, String>>,
    pub timestamp: i64,
}

/// 消费者会话
struct Session {
    topic: String,
    partition: Option<i32>,
    sender: mpsc::UnboundedSender<ConsumedMessage>,
}

/// 常驻消费者
struct LiveConsumer {
    _consumer: Arc<StreamConsumer<DefaultConsumerContext>>,
    cancel_tx: mpsc::Sender<()>,
}

/// 缓存的 fetch consumer（复用避免重复 GROUP_JOIN）
struct FetchConsumerCache {
    consumer: Arc<StreamConsumer<DefaultConsumerContext>>,
    topic: String,
    conn_id: String,
}

/// 消费者服务
pub struct ConsumerService {
    live_consumers: HashMap<String, LiveConsumer>,
    sessions: Arc<RwLock<HashMap<String, Session>>>,
    /// fetch consumer 缓存，按连接复用
    fetch_consumers: HashMap<String, FetchConsumerCache>,
    app_handle: Option<AppHandle>,
}

impl ConsumerService {
    pub fn new() -> Self {
        Self {
            live_consumers: HashMap::new(),
            sessions: Arc::new(RwLock::new(HashMap::new())),
            fetch_consumers: HashMap::new(),
            app_handle: None,
        }
    }

    pub fn set_app_handle(&mut self, handle: AppHandle) {
        self.app_handle = Some(handle);
    }

    /// 启动实时消费
    pub async fn start(
        &mut self,
        conn: &ConnectionRecord,
        opts: &ConsumerOptions,
    ) -> Result<(String, mpsc::UnboundedReceiver<ConsumedMessage>), String> {
        let topic = opts.topic.clone();
        let consumer_id = Uuid::new_v4().to_string();

        if !self.live_consumers.contains_key(&topic) {
            let mut config = build_config_for_record(conn)?;
            config.set("group.id", &format!("live-{}", topic));
            config.set("enable.auto.commit", "false");
            config.set("auto.offset.reset", "earliest");

            let consumer: StreamConsumer<DefaultConsumerContext> = config
                .create_with_context(DefaultConsumerContext)
                .map_err(|e| format!("Failed to create consumer: {e}"))?;

            consumer
                .subscribe(&[topic.as_str()])
                .map_err(|e| format!("Failed to subscribe: {e}"))?;

            let consumer_arc = Arc::new(consumer);
            let consumer_clone = Arc::clone(&consumer_arc);
            let (cancel_tx, mut cancel_rx) = mpsc::channel::<()>(1);

            let sessions_clone = Arc::clone(&self.sessions);
            let t = topic.clone();

            tokio::spawn(async move {
                loop {
                    tokio::select! {
                        _ = cancel_rx.recv() => {
                            info!("Live consumer for topic {} stopped", t);
                            break;
                        }
                        result = consumer_clone.recv() => {
                            match result {
                                Ok(msg) => {
                                    Self::dispatch_message(&sessions_clone, &msg).await;
                                }
                                Err(e) => {
                                    warn!("Consumer recv error: {}", e);
                                }
                            }
                        }
                    }
                }
            });

            self.live_consumers.insert(
                topic.clone(),
                LiveConsumer {
                    _consumer: consumer_arc,
                    cancel_tx,
                },
            );
            info!("Created live consumer for topic {}", topic);
        }

        let (tx, rx) = mpsc::unbounded_channel();
        self.sessions.write().await.insert(
            consumer_id.clone(),
            Session {
                topic: topic.clone(),
                partition: opts.partition,
                sender: tx,
            },
        );

        info!("Started session {} for topic {}", &consumer_id[..8], topic);
        Ok((consumer_id, rx))
    }

    /// 停止会话
    pub async fn stop(&mut self, consumer_id: &str) {
        self.sessions.write().await.remove(consumer_id);
        info!("Stopped session {}", &consumer_id[..8]);
    }

    /// 消息浏览：复用 fetch consumer（避免重复 GROUP_JOIN）
    pub async fn fetch_messages(
        &mut self,
        conn: &ConnectionRecord,
        conn_id: &str,
        admin: &rdkafka::admin::AdminClient<DefaultClientContext>,
        opts: &FetchMessagesOptions,
    ) -> Result<Vec<ConsumedMessage>, String> {
        let topic = opts.topic.clone();
        let limit = opts.limit.unwrap_or(50) as usize;
        let from_beginning = opts.from_beginning.unwrap_or(false);

        // 1. 获取水位（使用池化 admin，无额外连接开销）
        let metadata = admin
            .inner()
            .fetch_metadata(Some(&topic), Duration::from_secs(10))
            .map_err(|e| format!("Metadata error: {e}"))?;

        let topic_meta = metadata
            .topics()
            .first()
            .ok_or_else(|| format!("Topic not found: {topic}"))?;

        let mut seeks: Vec<(i32, i64)> = Vec::new();
        let mut expected = 0i64;

        for p in topic_meta.partitions() {
            let pid = p.id();
            if let Some(part) = opts.partition {
                if pid != part { continue; }
            }
            let (low, high) = admin
                .inner()
                .fetch_watermarks(&topic, pid, Duration::from_secs(5))
                .map_err(|e| format!("Watermark error: {e}"))?;

            let target = if let Some(ref off) = opts.offset {
                off.parse::<i64>().unwrap_or(low)
            } else if !from_beginning {
                (high - limit as i64).max(low)
            } else {
                low
            };
            let n = high - target;
            if n > 0 {
                expected += n;
                seeks.push((pid, target));
            }
        }

        let max = limit.min(expected as usize);
        if max == 0 {
            return Ok(vec![]);
        }

        let t0 = std::time::Instant::now();

        // 2. 获取或创建缓存的 fetch consumer（复用避免 GROUP_JOIN）
        let consumer = if let Some(cached) = self.fetch_consumers.get(conn_id) {
            if cached.topic != topic {
                // topic 变了，需要重新 subscribe
                cached.consumer.unsubscribe();
                cached
                    .consumer
                    .subscribe(&[topic.as_str()])
                    .map_err(|e| format!("Subscribe failed: {e}"))?;
            }
            Arc::clone(&cached.consumer)
        } else {
            let mut config = build_config_for_record(conn)?;
            config
                .set("group.id", &format!("fetch-{}", conn_id))
                .set("enable.auto.commit", "false")
                .set("auto.offset.reset", "earliest");

            let c: StreamConsumer<DefaultConsumerContext> = config
                .create_with_context(DefaultConsumerContext)
                .map_err(|e| format!("Failed to create consumer: {e}"))?;

            c.subscribe(&[topic.as_str()])
                .map_err(|e| format!("Subscribe failed: {e}"))?;

            // 等待首次 GROUP_JOIN
            let join_t0 = std::time::Instant::now();
            loop {
                let assignment = c
                    .assignment()
                    .map_err(|e| format!("Assignment error: {e}"))?;
                if assignment.count() > 0 {
                    break;
                }
                if join_t0.elapsed() > Duration::from_secs(15) {
                    return Err("GROUP_JOIN timeout".to_string());
                }
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
            tracing::info!(
                "[fetch] GROUP_JOIN took {}ms (cached for reuse)",
                join_t0.elapsed().as_millis()
            );

            let consumer = Arc::new(c);
            self.fetch_consumers.insert(
                conn_id.to_string(),
                FetchConsumerCache {
                    consumer: Arc::clone(&consumer),
                    topic: topic.clone(),
                    conn_id: conn_id.to_string(),
                },
            );
            consumer
        };

        // 3. Seek 到目标 offset（复用模式下直接 seek，无需等 GROUP_JOIN）
        for (pid, offset) in &seeks {
            consumer
                .seek(&topic, *pid, rdkafka::Offset::Offset(*offset), Duration::from_secs(5))
                .map_err(|e| format!("Seek failed: {e}"))?;
        }

        // 4. Poll 收集消息
        let mut collected = Vec::new();
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);

        while collected.len() < max && tokio::time::Instant::now() < deadline {
            match tokio::time::timeout(Duration::from_secs(2), consumer.recv()).await {
                Ok(Ok(msg)) => {
                    if let Some(part) = opts.partition {
                        if msg.partition() != part { continue; }
                    }
                    let hdrs = msg.headers().map(|h| {
                        let mut map = HashMap::new();
                        for i in 0..h.count() {
                            let header = h.get(i);
                            map.insert(
                                header.key.to_string(),
                                String::from_utf8_lossy(header.value.unwrap_or(&[])).to_string(),
                            );
                        }
                        map
                    });
                    collected.push(ConsumedMessage {
                        topic: msg.topic().to_string(),
                        partition: msg.partition(),
                        offset: msg.offset().to_string(),
                        key: msg.key().map(|k| String::from_utf8_lossy(k).to_string()),
                        value: String::from_utf8_lossy(msg.payload().unwrap_or(&[])).to_string(),
                        headers: hdrs,
                        timestamp: msg.timestamp().to_millis().unwrap_or(0),
                    });
                }
                Ok(Err(_)) => break,
                Err(_) => break,
            }
        }

        tracing::info!(
            "[fetch] {} msgs from '{}' in {}ms (cached={})",
            collected.len(),
            topic,
            t0.elapsed().as_millis(),
            self.fetch_consumers.contains_key(conn_id)
        );
        Ok(collected)
    }

    /// 停止所有会话和消费者
    pub async fn stop_all(&mut self) {
        for (topic, live) in self.live_consumers.drain() {
            let _ = live.cancel_tx.send(()).await;
            info!("Stopped live consumer for topic {}", topic);
        }
        self.sessions.write().await.clear();
        // 清理 fetch consumer 缓存
        for (conn_id, cached) in self.fetch_consumers.drain() {
            cached.consumer.unsubscribe();
            info!("Stopped fetch consumer for connection {}", conn_id);
        }
    }

    /// 分发消息到匹配的会话
    async fn dispatch_message(
        sessions: &Arc<RwLock<HashMap<String, Session>>>,
        msg: &BorrowedMessage<'_>,
    ) {
        let sessions = sessions.read().await;
        if sessions.is_empty() { return; }

        let topic = msg.topic();
        let partition = msg.partition();

        let consumed = ConsumedMessage {
            topic: topic.to_string(),
            partition,
            offset: msg.offset().to_string(),
            key: msg.key().map(|k| String::from_utf8_lossy(k).to_string()),
            value: String::from_utf8_lossy(msg.payload().unwrap_or(&[])).to_string(),
            headers: msg.headers().map(|h| {
                let mut map = HashMap::new();
                for i in 0..h.count() {
                    let header = h.get(i);
                    map.insert(header.key.to_string(), String::from_utf8_lossy(header.value.unwrap_or(&[])).to_string());
                }
                map
            }),
            timestamp: msg.timestamp().to_millis().unwrap_or(0),
        };

        for session in sessions.values() {
            if session.topic != topic { continue; }
            if let Some(p) = session.partition {
                if p != partition { continue; }
            }
            let _ = session.sender.send(consumed.clone());
        }
    }
}

impl Default for ConsumerService {
    fn default() -> Self {
        Self::new()
    }
}
