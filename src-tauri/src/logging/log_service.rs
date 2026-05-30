use std::collections::VecDeque;

use serde::Serialize;
use tauri::{AppHandle, Emitter};
use uuid::Uuid;

/// 单条日志记录
#[derive(Debug, Clone, Serialize, serde::Deserialize)]
pub struct LogEntry {
    pub id: String,
    pub level: String,
    pub timestamp: i64,
    pub source: String,
    pub message: String,
    pub data: Option<String>,
    pub stack: Option<String>,
    pub origin: Option<String>,
}

const MAX_LOG_ENTRIES: usize = 5000;

/// 主进程日志服务 — 环形缓冲 + 实时推送到前端
pub struct LogService {
    buffer: VecDeque<LogEntry>,
    app_handle: Option<AppHandle>,
}

impl Default for LogService {
    fn default() -> Self {
        Self {
            buffer: VecDeque::with_capacity(MAX_LOG_ENTRIES),
            app_handle: None,
        }
    }
}

impl LogService {
    pub fn set_app_handle(&mut self, handle: AppHandle) {
        self.app_handle = Some(handle);
    }

    /// 记录一条日志
    pub fn log(&mut self, level: &str, source: &str, message: &str, data: Option<&str>, stack: Option<&str>, origin: Option<&str>) {
        let entry = LogEntry {
            id: Uuid::new_v4().to_string(),
            level: level.to_string(),
            timestamp: chrono::Utc::now().timestamp_millis(),
            source: source.to_string(),
            message: message.to_string(),
            data: data.map(|s| s.to_string()),
            stack: stack.map(|s| s.to_string()),
            origin: origin.map(|s| s.to_string()),
        };

        // 写入环形缓冲
        if self.buffer.len() >= MAX_LOG_ENTRIES {
            self.buffer.pop_front();
        }
        self.buffer.push_back(entry.clone());

        // 实时推送到前端
        if let Some(ref handle) = self.app_handle {
            let _ = handle.emit("log:entry", &entry);
        }
    }

    pub fn get_logs(&self) -> Vec<LogEntry> {
        self.buffer.iter().cloned().collect()
    }

    pub fn clear(&mut self) {
        self.buffer.clear();
    }
}
