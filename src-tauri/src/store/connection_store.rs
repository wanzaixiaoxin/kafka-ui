use std::collections::HashMap;
use std::path::PathBuf;
use uuid::Uuid;

use super::AppSettings;

/// Kafka 连接配置
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ConnectionRecord {
    #[serde(default)] // 新建时不传，后端生成 UUID
    pub id: String,
    pub name: String,
    pub brokers: Vec<String>,
    #[serde(default = "default_client_id")]
    pub client_id: String,
    #[serde(default)]
    pub ssl: bool,
    #[serde(default)]
    pub sasl: Option<SaslConfig>,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default)] // 新建时不传，后端生成时间戳
    pub created_at: i64,
    #[serde(default)]
    pub updated_at: i64,
}

fn default_client_id() -> String {
    "kafka-client".to_string()
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SaslConfig {
    pub mechanism: String,
    pub username: String,
    pub password: String,
}

/// 持久化数据结构（保存到磁盘的格式）
#[derive(serde::Serialize, serde::Deserialize)]
struct PersistedData {
    connections: Vec<ConnectionRecord>,
    active_connection_id: Option<String>,
    settings: AppSettings,
}

impl Default for PersistedData {
    fn default() -> Self {
        Self {
            connections: vec![],
            active_connection_id: None,
            settings: AppSettings::default(),
        }
    }
}

/// 连接存储 — 内存 + JSON 文件持久化
pub struct ConnectionStore {
    connections: HashMap<String, ConnectionRecord>,
    active_id: Option<String>,
    settings: AppSettings,
    data_dir: Option<PathBuf>,
}

impl ConnectionStore {
    pub fn new() -> Self {
        Self {
            connections: HashMap::new(),
            active_id: None,
            settings: AppSettings::default(),
            data_dir: None,
        }
    }

    /// 设置数据目录并加载持久化数据
    pub fn init(&mut self, data_dir: PathBuf) {
        self.data_dir = Some(data_dir.clone());
        std::fs::create_dir_all(&data_dir).ok();
        self.load();
        tracing::info!("Store initialized at {}", data_dir.display());
    }

    fn file_path(&self) -> Option<PathBuf> {
        self.data_dir.as_ref().map(|d| d.join("data.json"))
    }

    /// 从磁盘加载
    fn load(&mut self) {
        let path = match self.file_path() {
            Some(p) => p,
            None => return,
        };
        let Ok(data) = std::fs::read_to_string(&path) else {
            return;
        };
        let Ok(mut persisted) = serde_json::from_str::<PersistedData>(&data) else {
            return;
        };
        // 解密 SASL 密码（从 ENC1:<base64> 还原为明文，内存中始终保持明文）
        for c in &mut persisted.connections {
            if let Some(ref mut sasl) = c.sasl {
                if !sasl.password.is_empty() && crate::security::is_encrypted(&sasl.password) {
                    match crate::security::decrypt_string(&sasl.password) {
                        Ok(dec) => sasl.password = dec,
                        Err(e) => {
                            tracing::warn!(
                                "Failed to decrypt SASL password for '{}': {} — password reset to empty",
                                c.name,
                                e
                            );
                            sasl.password = String::new();
                        }
                    }
                }
            }
        }
        for c in persisted.connections {
            self.connections.insert(c.id.clone(), c);
        }
        self.active_id = persisted.active_connection_id;
        self.settings = persisted.settings;
        tracing::info!(
            "Loaded {} connections from disk",
            self.connections.len()
        );
    }

    /// 保存到磁盘
    fn save(&self) {
        let path = match self.file_path() {
            Some(p) => p,
            None => return,
        };
        // 克隆连接列表，在序列化前加密 SASL 密码
        // （内存中的 self.connections 始终是明文，仅落盘时加密）
        let connections: Vec<ConnectionRecord> = self
            .connections
            .values()
            .cloned()
            .map(|mut c| {
                if let Some(ref mut sasl) = c.sasl {
                    if !sasl.password.is_empty() && !crate::security::is_encrypted(&sasl.password)
                    {
                        match crate::security::encrypt_string(&sasl.password) {
                            Ok(enc) => sasl.password = enc,
                            Err(e) => tracing::error!(
                                "Failed to encrypt SASL password for '{}': {} — stored as-is",
                                c.name,
                                e
                            ),
                        }
                    }
                }
                c
            })
            .collect();

        let data = PersistedData {
            connections,
            active_connection_id: self.active_id.clone(),
            settings: self.settings.clone(),
        };
        if let Ok(json) = serde_json::to_string_pretty(&data) {
            if let Err(e) = std::fs::write(&path, json) {
                tracing::error!("Failed to write store {}: {}", path.display(), e);
            }
        }
    }

    // ---- 连接管理 ----

    pub fn list(&self) -> Vec<&ConnectionRecord> {
        let mut conns: Vec<_> = self.connections.values().collect();
        conns.sort_by_key(|c| c.created_at);
        conns
    }

    pub fn save_connection(&mut self, mut record: ConnectionRecord) -> ConnectionRecord {
        let now = chrono::Utc::now().timestamp_millis();
        if record.id.is_empty() {
            record.id = Uuid::new_v4().to_string();
            record.created_at = now;
            record.updated_at = now;
        } else {
            record.updated_at = now;
        }
        self.connections.insert(record.id.clone(), record.clone());
        self.save();
        record
    }

    pub fn remove(&mut self, id: &str) -> bool {
        let existed = self.connections.remove(id).is_some();
        if existed {
            if self.active_id.as_deref() == Some(id) {
                self.active_id = None;
            }
            self.save();
        }
        existed
    }

    pub fn get_active(&self) -> Option<&str> {
        self.active_id.as_deref()
    }

    pub fn set_active(&mut self, id: &str) -> bool {
        if self.connections.contains_key(id) {
            self.active_id = Some(id.to_string());
            self.save();
            true
        } else {
            false
        }
    }

    // ---- 设置 ----

    pub fn get_settings(&self) -> AppSettings {
        self.settings.clone()
    }

    pub fn update_settings(&mut self, partial: &serde_json::Value) -> AppSettings {
        if let Ok(s) = serde_json::from_value::<AppSettings>(partial.clone()) {
            if s.max_messages > 0 {
                self.settings.max_messages = s.max_messages;
            }
            if s.auto_refresh_interval > 0 {
                self.settings.auto_refresh_interval = s.auto_refresh_interval;
            }
            if !s.theme.is_empty() {
                self.settings.theme = s.theme;
            }
        }
        self.save();
        self.settings.clone()
    }
}

impl Default for ConnectionStore {
    fn default() -> Self {
        Self::new()
    }
}
