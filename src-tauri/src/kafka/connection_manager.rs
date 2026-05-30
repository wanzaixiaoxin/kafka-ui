use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use rdkafka::admin::AdminClient;
use rdkafka::client::DefaultClientContext;
use rdkafka::config::ClientConfig;
use rdkafka::producer::FutureProducer;
use serde::{Deserialize, Serialize};
use tracing::{info, warn};

use crate::store::connection_store::ConnectionRecord;

/// 连接测试结果
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ConnectionTestResult {
    pub success: bool,
    pub brokers: Option<Vec<BrokerInfo>>,
    #[serde(rename = "controllerId")]
    pub controller_id: Option<i32>,
    #[serde(rename = "clusterId")]
    pub cluster_id: Option<String>,
    pub error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BrokerInfo {
    #[serde(rename = "nodeId")]
    pub node_id: i32,
    pub host: String,
    pub port: u16,
}

/// Kafka 连接管理器 — 维护 Admin/Producer 池
pub struct ConnectionManager {
    admins: HashMap<String, Arc<AdminClient<DefaultClientContext>>>,
    producers: HashMap<String, Arc<FutureProducer<DefaultClientContext>>>,
}

impl ConnectionManager {
    pub fn new() -> Self {
        Self {
            admins: HashMap::new(),
            producers: HashMap::new(),
        }
    }

    /// 根据连接配置创建 ClientConfig（优化超时参数）
    fn build_config(conn: &ConnectionRecord) -> Result<ClientConfig, String> {
        let brokers = conn.brokers.join(",");
        let mut config = ClientConfig::new();
        config
            .set("bootstrap.servers", &brokers)
            .set("client.id", &conn.client_id)
            .set("log_level", "4") // LOG_WARNING
            // 优化: 缩短 socket 超时，提升响应速度
            .set("socket.timeout.ms", "5000")
            // 优化: 缩短 API 版本协商超时
            .set("api.version.request.timeout.ms", "3000")
            // 优化: 启用 TCP keepalive 防止连接断开
            .set("socket.keepalive.enable", "true")
            // 优化: 启用连接复用（rdkafka 内部连接池）
            .set("enable.sparse.connections", "true")
            // 优化: 元数据缓存策略，减少重复请求
            .set("metadata.max.age.ms", "30000")    // 30 秒后强制刷新元数据
            .set("topic.metadata.refresh.interval.ms", "30000") // 30 秒定期刷新
            .set("topic.metadata.refresh.fast.interval.ms", "5000") // 快速刷新间隔 5 秒
            .set("topic.metadata.refresh.sparse", "true") // 仅刷新请求的 topic
            // 优化: 减少元数据请求超时
            .set("metadata.request.timeout.ms", "3000");

        if conn.ssl || conn.sasl.is_some() {
            return Err(
                "SSL/SASL 需要 OpenSSL 库。请安装 OpenSSL 后重新编译：\n\
                 1. 安装 OpenSSL: winget install ShiningLight.OpenSSL.Light\n\
                 2. 在 Cargo.toml 中将 rdkafka features 改为: [\"ssl\", \"sasl\", \"cmake-build\"]\n\
                 3. 重新运行 cargo build"
                    .to_string(),
            );
        }

        Ok(config)
    }

    /// 测试连接 — 创建临时 Admin 并获取集群元数据
    pub async fn test_connection(
        conn: &ConnectionRecord,
    ) -> Result<ConnectionTestResult, String> {
        let config = Self::build_config(conn)?;
        // 优化: 测试连接使用更短的超时
        let timeout = Duration::from_secs(5);

        let admin: AdminClient<DefaultClientContext> = config
            .create()
            .map_err(|e| format!("Failed to create Kafka client: {e}"))?;

        // 获取集群元数据
        let metadata = admin
            .inner()
            .fetch_metadata(None, timeout)
            .map_err(|e| format!("Connection failed: {e}"))?;

        let brokers = Some(
            metadata
                .brokers()
                .iter()
                .map(|b| BrokerInfo {
                    node_id: b.id(),
                    host: b.host().to_string(),
                    port: b.port() as u16,
                })
                .collect(),
        );

        Ok(ConnectionTestResult {
            success: true,
            brokers,
            controller_id: Some(metadata.orig_broker_id()),
            cluster_id: None, // rdkafka 0.37 未暴露 cluster_id
            error: None,
        })
    }

    /// 获取或创建池化的 AdminClient
    pub fn get_admin(&mut self, conn_id: &str, conn: &ConnectionRecord) -> Result<Arc<AdminClient<DefaultClientContext>>, String> {
        if let Some(admin) = self.admins.get(conn_id) {
            return Ok(Arc::clone(admin));
        }

        let config = Self::build_config(conn)?;
        let admin: AdminClient<DefaultClientContext> = config
            .create()
            .map_err(|e| format!("Failed to create admin client: {e}"))?;

        info!("Created new pooled AdminClient for connection {}", conn.name);

        let admin = Arc::new(admin);
        self.admins.insert(conn_id.to_string(), Arc::clone(&admin));
        Ok(admin)
    }

    /// 预热连接: 提前创建 Admin + Producer，避免首次操作延迟
    pub fn warmup(&mut self, conn_id: &str, conn: &ConnectionRecord) -> Result<(), String> {
        // 预创建 Admin（会触发元数据请求）
        let admin = self.get_admin(conn_id, conn)?;
        // 预热: 触发一次元数据刷新，让 rdkafka 内部缓存就绪
        let _ = admin.inner().fetch_metadata(None, Duration::from_secs(3));
        
        // 预创建 Producer
        let _ = self.get_producer(conn_id, conn)?;
        
        info!("Connection warmed up for {}", conn.name);
        Ok(())
    }

    /// 获取或创建池化的 FutureProducer
    pub fn get_producer(&mut self, conn_id: &str, conn: &ConnectionRecord) -> Result<Arc<FutureProducer<DefaultClientContext>>, String> {
        if let Some(producer) = self.producers.get(conn_id) {
            return Ok(Arc::clone(producer));
        }

        let mut config = Self::build_config(conn)?;
        config.set("acks", "all");
        config.set("message.timeout.ms", "5000");
        // 优化: 启用生产者批处理，提升吞吐
        config.set("batch.num.messages", "100");
        config.set("batch.size", "16384"); // 16KB
        config.set("linger.ms", "5"); // 最多等 5ms 凑批

        let producer: FutureProducer = config
            .create()
            .map_err(|e| format!("Failed to create producer: {e}"))?;

        info!("Created new pooled Producer for connection {}", conn.name);

        let producer = Arc::new(producer);
        self.producers.insert(conn_id.to_string(), Arc::clone(&producer));
        Ok(producer)
    }

    /// 断开并移除指定连接的资源
    pub async fn disconnect(&mut self, conn_id: &str) {
        // rdkafka 的 AdminClient 和 FutureProducer 在 Drop 时自动断开
        self.admins.remove(conn_id);
        self.producers.remove(conn_id);
        info!("Disconnected resources for connection {}", conn_id);
    }

    /// 关闭所有连接
    pub async fn close_all(&mut self) {
        let admin_count = self.admins.len();
        let producer_count = self.producers.len();
        self.admins.clear();
        self.producers.clear();
        warn!(
            "Closed all Kafka connections ({} admins, {} producers)",
            admin_count, producer_count
        );
    }
}

impl Default for ConnectionManager {
    fn default() -> Self {
        Self::new()
    }
}

/// 为特定连接记录构建 ClientConfig（供 consumer 等模块复用，优化超时）
pub fn build_config_for_record(conn: &ConnectionRecord) -> Result<ClientConfig, String> {
    let brokers = conn.brokers.join(",");
    let mut config = ClientConfig::new();
    config
        .set("bootstrap.servers", &brokers)
        .set("client.id", &conn.client_id)
        .set("log_level", "4")
        .set("socket.timeout.ms", "5000")
        .set("api.version.request.timeout.ms", "3000")
        .set("socket.keepalive.enable", "true")
        // 启用稀疏连接：消费者只连接需要的 broker，避免连接全部 broker
        .set("enable.sparse.connections", "true")
        .set("metadata.max.age.ms", "30000")
        .set("topic.metadata.refresh.interval.ms", "30000")
        .set("topic.metadata.refresh.sparse", "true")
        .set("metadata.request.timeout.ms", "3000");

    if conn.ssl || conn.sasl.is_some() {
        return Err(
            "SSL/SASL 需要 OpenSSL 库。请安装后重新编译。".to_string(),
        );
    }
    Ok(config)
}
