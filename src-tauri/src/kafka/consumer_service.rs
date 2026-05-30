use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use rdkafka::consumer::{BaseConsumer, Consumer, DefaultConsumerContext};
use rdkafka::message::{BorrowedMessage, Headers, Message};
use rdkafka::Offset;
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

/// 常驻消费者（实时消费用，需要 GROUP_JOIN）
struct LiveConsumer {
    _consumer: Arc<rdkafka::consumer::StreamConsumer<DefaultConsumerContext>>,
    cancel_tx: mpsc::Sender<()>,
}

/// 缓存的 fetch consumer（BaseConsumer，复用避免重复建连）
struct FetchConsumerCache {
    consumer: Arc<BaseConsumer<DefaultConsumerContext>>,
}

/// 消费者服务
pub struct ConsumerService {
    live_consumers: HashMap<String, LiveConsumer>,
    sessions: Arc<RwLock<HashMap<String, Session>>>,
    /// fetch consumer 缓存，按连接复用（避免重复 TCP 握手 + 元数据请求）
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

    /// 启动实时消费（使用 StreamConsumer + Consumer Group）
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

            let consumer: rdkafka::consumer::StreamConsumer<DefaultConsumerContext> = config
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

    /// 消息浏览：使用 BaseConsumer + assign 直接拉取
    /// 关键优化：完全不经过 GROUP_JOIN，直接 assign 分区 + seek offset
    pub fn fetch_messages(
        &mut self,
        conn: &ConnectionRecord,
        admin: &rdkafka::admin::AdminClient<rdkafka::client::DefaultClientContext>,
        opts: &FetchMessagesOptions,
    ) -> Result<Vec<ConsumedMessage>, String> {
        let topic = opts.topic.clone();
        let limit = opts.limit.unwrap_or(50) as usize;
        let from_beginning = opts.from_beginning.unwrap_or(false);

        info!("[fetch] ===== 开始拉取消息 ===== topic='{}', limit={}, from_beginning={}, partition={:?}, offset={:?}",
              topic, limit, from_beginning, opts.partition, opts.offset);

        // 步骤1: 获取 Topic 元数据
        info!("[fetch] 步骤1: 获取元数据...");
        let metadata = admin
            .inner()
            .fetch_metadata(Some(&topic), Duration::from_secs(10))
            .map_err(|e| {
                let msg = format!("[步骤1失败] 获取元数据失败: {e}");
                info!("{}", msg);
                msg
            })?;

        let topic_meta = metadata
            .topics()
            .first()
            .ok_or_else(|| {
                let msg = format!("[步骤1失败] Topic 不存在: {topic}");
                info!("{}", msg);
                msg
            })?;

        info!("[fetch] 步骤1完成: 找到 {} 个分区", topic_meta.partitions().len());

        // 步骤1.5: 检测 Broker advertised.listeners 地址不匹配
        // 记录地址差异用于诊断。若 broker 广播 localhost 但用户用其他地址连接，
        // 可能是 Docker/WSL 端口映射场景，后续会自动回退到 localhost 重试。
        let mut address_mismatch_warning: Option<String> = None;
        let mut localhost_fallback_bootstrap: Option<String> = None;
        {
            let bootstrap_hosts: Vec<&str> = conn.brokers.iter()
                .flat_map(|b| b.split(','))
                .map(|s| s.trim().split(':').next().unwrap_or(""))
                .collect();

            let bootstrap_is_local = bootstrap_hosts.iter()
                .any(|h| *h == "localhost" || *h == "127.0.0.1" || *h == "0.0.0.0");

            for broker in metadata.brokers() {
                let broker_host = broker.host();
                let broker_port = broker.port();
                let broker_is_local = broker_host == "localhost"
                    || broker_host == "127.0.0.1"
                    || broker_host.starts_with("127.");

                let broker_in_bootstrap = bootstrap_hosts.iter().any(|h| *h == broker_host);

                if broker_is_local && !bootstrap_is_local {
                    // bootstrap 用远程 IP，broker 广播 localhost
                    // Docker/WSL 场景：通过端口映射暴露，用 localhost 可以直接访问
                    let advertised = format!("{}:{}", broker_host, broker_port);
                    let suggested_host = bootstrap_hosts.first().unwrap_or(&"<您的IP>");
                    address_mismatch_warning = Some(format!(
                        "bootstrap={}  advertised={}  (建议: advertised.listeners=PLAINTEXT://{}:{})",
                        conn.brokers.join(", "), advertised, suggested_host, broker_port
                    ));
                    localhost_fallback_bootstrap = Some(format!("localhost:{}", broker_port));
                    warn!("[fetch] Broker 地址可能不匹配: {}，将尝试 localhost 回退", address_mismatch_warning.as_ref().unwrap());
                } else if !bootstrap_is_local && !broker_is_local && !broker_in_bootstrap {
                    let advertised = format!("{}:{}", broker_host, broker_port);
                    address_mismatch_warning = Some(format!(
                        "bootstrap={}  advertised={}  (可能不在同一网络)",
                        conn.brokers.join(", "), advertised
                    ));
                    warn!("[fetch] Broker 地址可能不匹配: {}", address_mismatch_warning.as_ref().unwrap());
                }
            }
        }

        // 步骤2: 获取水位 + 计算起始 offset
        info!("[fetch] 步骤2: 获取各分区水位...");
        let mut assignments: Vec<(i32, i64, i64)> = Vec::new();
        let mut total_expected = 0i64;

        for p in topic_meta.partitions() {
            let pid = p.id();
            if let Some(part) = opts.partition {
                if pid != part { continue; }
            }
            info!("[fetch] 步骤2: 正在获取分区 {} 水位...", pid);
            let (low, high) = admin
                .inner()
                .fetch_watermarks(&topic, pid, Duration::from_secs(10))
                .map_err(|e| {
                    let msg = format!("[步骤2失败] 获取分区 {} 水位失败: {e}\n请确认 Kafka 集群可正常访问，且 Topic '{}' 存在", pid, topic);
                    info!("{}", msg);
                    msg
                })?;

            info!("[fetch] 步骤2: 分区 {} 水位: low={}, high={}", pid, low, high);

            let start = if let Some(ref off) = opts.offset {
                off.parse::<i64>().unwrap_or(low)
            } else if !from_beginning {
                (high - limit as i64).max(low)
            } else {
                low
            };
            let count = high - start;
            if count > 0 {
                total_expected += count;
                assignments.push((pid, start, high));
            }
        }

        info!("[fetch] 步骤2完成: 总共 {} 个分区有消息, 预期 {} 条", assignments.len(), total_expected);

        let max = limit.min(total_expected as usize);
        if max == 0 {
            info!("[fetch] Topic '{}' 当前无消息可读 (max=0)", topic);
            return Ok(vec![]);
        }

        let t0 = std::time::Instant::now();
        let conn_id = &conn.id;

        // 从缓存获取或创建 BaseConsumer（避免每次查询都 TCP 握手）
        let cached_consumer: Option<Arc<BaseConsumer<DefaultConsumerContext>>> =
            self.fetch_consumers.get(conn_id).map(|c| Arc::clone(&c.consumer));

        // 步骤3-5: 获取/创建 BaseConsumer → assign → poll（支持 localhost 回退）
        let mut collected: Vec<ConsumedMessage> = Vec::with_capacity(max);
        let mut last_error: Option<String> = None;
        let mut used_fallback = false;
        let mut consumer_to_cache: Option<Arc<BaseConsumer<DefaultConsumerContext>>> = None;

        // 尝试列表：先用缓存 consumer（如果有），再用原始 bootstrap，最后用 localhost 回退
        let original_bootstrap = conn.brokers.join(",");
        let mut attempts: Vec<(String, Option<Arc<BaseConsumer<DefaultConsumerContext>>>)> = Vec::new();

        if let Some(c) = cached_consumer {
            // 有缓存的 consumer：直接尝试（跳过步骤3 创建）
            attempts.push((original_bootstrap.clone(), Some(c)));
        }
        // 首次创建尝试
        attempts.push((original_bootstrap, None));
        if let Some(ref fb) = localhost_fallback_bootstrap {
            attempts.push((fb.clone(), None));
        }

        for (attempt, (bootstrap, maybe_cached)) in attempts.iter().enumerate() {
            if attempt > 0 && maybe_cached.is_some() {
                continue; // 已经尝试过缓存的 consumer，跳过重复尝试
            }
            let is_cached = maybe_cached.is_some();
            let first_creation = attempt == 0 && !is_cached;

            if attempt > 0 && !is_cached && !first_creation {
                info!("[fetch] 步骤3(重试): 使用 bootstrap={} 重试...", bootstrap);
                used_fallback = true;
                last_error = None;
            }

            // 步骤3: 获取或创建 BaseConsumer
            let consumer: Arc<BaseConsumer<DefaultConsumerContext>> = if let Some(ref c) = maybe_cached {
                info!("[fetch] 步骤3: 复用缓存的 BaseConsumer (bootstrap={})", bootstrap);
                Arc::clone(c)
            } else {
                info!("[fetch] 步骤3: 创建 BaseConsumer (bootstrap={})...", bootstrap);
                let mut config = build_config_for_record(conn)?;
                config.set("bootstrap.servers", bootstrap);
                let group_id = format!("fetch-{}", Uuid::new_v4().to_string().split('-').next().unwrap_or("x"));
                config
                    .set("group.id", &group_id)
                    .set("enable.auto.commit", "false")
                    .set("auto.offset.reset", "earliest")
                    .set("fetch.min.bytes", "1")
                    .set("fetch.wait.max.ms", "500")
                    .set("fetch.message.max.bytes", "1048576")
                    .set("socket.connection.setup.timeout.ms", "10000")
                    .set("reconnect.backoff.ms", "500")
                    .set("reconnect.backoff.max.ms", "5000");

                let c: BaseConsumer<DefaultConsumerContext> = match config.create_with_context(DefaultConsumerContext) {
                    Ok(c) => c,
                    Err(e) => {
                        let msg = format!("[步骤3失败] 创建 BaseConsumer 失败: {e}");
                        info!("{}", msg);
                        last_error = Some(msg);
                        continue;
                    }
                };
                info!("[fetch] 步骤3完成: BaseConsumer 创建成功, group.id={}, 缓存={}", group_id, is_cached);
                Arc::new(c)
            };

            // 步骤4: assign 分区
            info!("[fetch] 步骤4: 手动 assign 分区...");
            let mut all_tpl = rdkafka::TopicPartitionList::new();
            for (pid, start, _high) in &assignments {
                info!("[fetch] 步骤4: assign 分区 {} offset={}", pid, start);
                if let Err(e) = all_tpl.add_partition_offset(&topic, *pid, Offset::Offset(*start)) {
                    let msg = format!("[步骤4失败] add_partition_offset 失败: {e}");
                    info!("{}", msg);
                    last_error = Some(msg);
                    break;
                }
            }
            if last_error.is_some() { continue; }

            if let Err(e) = consumer.assign(&all_tpl) {
                let msg = format!("[步骤4失败] assign 分区失败: {e}");
                info!("{}", msg);
                last_error = Some(msg);
                continue;
            }

            info!("[fetch] 步骤4完成: 已 assign {} 个分区", assignments.len());

            // 步骤5: Poll 收集消息
            let poll_timeout = if attempt > 0 { Duration::from_secs(5) } else { Duration::from_secs(15) };
            info!("[fetch] 步骤5: 开始 poll 消息 (目标 {} 条, 超时 {}s)...", max, poll_timeout.as_secs());
            collected.clear();
            let deadline = std::time::Instant::now() + poll_timeout;
            let mut empty_polls = 0u32;
            const MAX_EMPTY_POLLS: u32 = 10;
            let mut transport_errors = 0u32;
            const MAX_TRANSPORT_ERRORS: u32 = 3;

            while collected.len() < max && std::time::Instant::now() < deadline {
                match consumer.poll(Duration::from_millis(500)) {
                    Some(Ok(msg)) => {
                        empty_polls = 0;
                        transport_errors = 0;
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
                        let val = String::from_utf8_lossy(msg.payload().unwrap_or(&[])).to_string();
                        let val_preview: String = val.chars().take(60).collect();
                        info!("[fetch] 步骤5: 收到消息 offset={}, partition={}, value={}...",
                              msg.offset(), msg.partition(), val_preview);
                        collected.push(ConsumedMessage {
                            topic: msg.topic().to_string(),
                            partition: msg.partition(),
                            offset: msg.offset().to_string(),
                            key: msg.key().map(|k| String::from_utf8_lossy(k).to_string()),
                            value: val,
                            headers: hdrs,
                            timestamp: msg.timestamp().to_millis().unwrap_or(0),
                        });
                    }
                    Some(Err(e)) => {
                        warn!("[fetch] 步骤5: poll 返回错误: {} (已收集 {} 条, transport_errors={})", e, collected.len(), transport_errors);
                        if !collected.is_empty() {
                            break;
                        }
                        last_error = Some(format!("Kafka 返回错误: {e}"));
                        transport_errors += 1;
                        if transport_errors >= MAX_TRANSPORT_ERRORS {
                            info!("[fetch] 步骤5: 连续 {} 次传输错误, 退出", transport_errors);
                            break;
                        }
                        info!("[fetch] 步骤5: 等待 librdkafka 重连 ({}ms)...", transport_errors * 1000);
                        std::thread::sleep(Duration::from_millis(transport_errors as u64 * 1000));
                    }
                    None => {
                        empty_polls += 1;
                        if empty_polls >= MAX_EMPTY_POLLS {
                            info!("[fetch] 步骤5: 连续 {} 次空 poll, 退出", empty_polls);
                            break;
                        }
                    }
                }
            }

            // 如果收集到消息，跳出重试循环并缓存 consumer
            if !collected.is_empty() {
                last_error = None;
                consumer_to_cache = Some(consumer);
                break;
            }

            if last_error.is_none() {
                last_error = Some("消费者无法连接到分区 Leader".to_string());
            }
        }

        // 更新缓存：成功获取到消息的 consumer 缓存起来供下次复用
        if let Some(c) = consumer_to_cache {
            self.fetch_consumers.insert(
                conn_id.to_string(),
                FetchConsumerCache { consumer: c },
            );
            info!("[fetch] consumer 已缓存 (conn_id={})", conn_id);
        }

        let elapsed = t0.elapsed().as_millis();
        let fallback_note = if used_fallback { " (已使用 localhost 回退)" } else { "" };
        info!(
            "[fetch] 步骤5完成: 收集 {} 条消息, 耗时 {}ms{}",
            collected.len(), elapsed, fallback_note
        );

        // 诊断: 水线显示有消息但 poll 一条都没收到 → 连接性问题
        if collected.is_empty() && total_expected > 0 {
            let mut reason = format!(
                "拉取消息失败：Kafka 水线显示 Topic '{}' 有 {} 条消息，但消费者无法读取任何数据。",
                topic, total_expected
            );

            if let Some(ref err) = last_error {
                reason.push_str(&format!("\n\n错误详情: {}", err));
            }

            reason.push_str("\n\n原因：AdminClient 通过您配置的连接地址访问正常，");
            reason.push_str("但消费者必须直连分区 Leader，而 Broker 广播的 advertised.listeners 地址不可达。");

            if let Some(ref warning) = address_mismatch_warning {
                reason.push_str(&format!("\n\n诊断信息: {}", warning));
            }

            if used_fallback {
                reason.push_str("\n\n已自动尝试 localhost 回退，但仍然无法连接。");
                reason.push_str("\n请确认 Kafka 确实在本机运行且端口可访问。");
            }

            reason.push_str("\n\n解决方案（任选其一）：");
            reason.push_str("\n1. 修改 Kafka server.properties：advertised.listeners=PLAINTEXT://<实际IP>:9092，重启 Kafka");
            reason.push_str("\n2. 如果 Kafka 在 Docker/WSL 中运行，确保端口已映射到本机 localhost");

            return Err(reason);
        }

        // 10 秒超时但收集到部分消息 — 部分成功
        if elapsed >= 9000 && collected.len() < max {
            warn!(
                "[fetch] 部分成功: 预期 {} 条, 实际收集 {} 条 (超时)",
                max, collected.len()
            );
        }

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
        let count = self.fetch_consumers.len();
        self.fetch_consumers.clear();
        if count > 0 {
            info!("Cleared {} cached fetch consumers", count);
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
