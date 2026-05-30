pub mod connection_store;

use serde::{Deserialize, Serialize};

/// App 设置
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AppSettings {
    pub max_messages: u32,
    pub auto_refresh_interval: u32,
    pub theme: String,
}

impl Default for AppSettings {
    fn default() -> Self {
        Self {
            max_messages: 500,
            auto_refresh_interval: 0,
            theme: "light".to_string(),
        }
    }
}
