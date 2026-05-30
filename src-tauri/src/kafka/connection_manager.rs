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

    /// 根据连接配置创建 ClientConfig
    fn build_config(conn: &ConnectionRecord) -> Result<ClientConfig, String> {
        let brokers = conn.brokers.join(",");
        let mut config = ClientConfig::new();
        config
            .set("bootstrap.servers", &brokers)
            .set("client.id", &conn.client_id)
            .set("log_level", "4") // LOG_WARNING
            .set("socket.timeout.ms", "10000")
            .set("api.version.request.timeout.ms", "5000");

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
        let timeout = Duration::from_secs(10);

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

        // 验证连接（AdminClient 创建即连接）
        info!("Created new pooled AdminClient for connection {}", conn.name);

        let admin = Arc::new(admin);
        self.admins.insert(conn_id.to_string(), Arc::clone(&admin));
        Ok(admin)
    }

    /// 获取或创建池化的 FutureProducer
    pub fn get_producer(&mut self, conn_id: &str, conn: &ConnectionRecord) -> Result<Arc<FutureProducer<DefaultClientContext>>, String> {
        if let Some(producer) = self.producers.get(conn_id) {
            return Ok(Arc::clone(producer));
        }

        let mut config = Self::build_config(conn)?;
        config.set("acks", "all");
        config.set("message.timeout.ms", "5000");

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

/// 为特定连接记录构建 ClientConfig（供 consumer 等模块复用）
pub fn build_config_for_record(conn: &ConnectionRecord) -> Result<ClientConfig, String> {
    let brokers = conn.brokers.join(",");
    let mut config = ClientConfig::new();
    config
        .set("bootstrap.servers", &brokers)
        .set("client.id", &conn.client_id)
        .set("log_level", "4")
        .set("socket.timeout.ms", "10000")
        .set("api.version.request.timeout.ms", "5000");

    if conn.ssl || conn.sasl.is_some() {
        return Err(
            "SSL/SASL 需要 OpenSSL 库。请安装后重新编译。".to_string(),
        );
    }
    Ok(config)
}
