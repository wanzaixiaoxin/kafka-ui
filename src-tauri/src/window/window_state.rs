use serde::{Deserialize, Serialize};
use std::path::Path;
use tauri::WebviewWindow;

/// 持久化的窗口状态
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WindowStateData {
    pub x: Option<f64>,
    pub y: Option<f64>,
    pub width: f64,
    pub height: f64,
    pub is_maximized: bool,
}

impl Default for WindowStateData {
    fn default() -> Self {
        Self {
            x: None,
            y: None,
            width: 1200.0,
            height: 800.0,
            is_maximized: false,
        }
    }
}

pub struct WindowState;

impl WindowState {
    /// 从磁盘恢复窗口状态
    pub fn restore(window: &WebviewWindow, data_dir: impl AsRef<Path>) {
        let path = data_dir.as_ref().join("window-state.json");
        let state = std::fs::read_to_string(&path)
            .ok()
            .and_then(|s| serde_json::from_str::<WindowStateData>(&s).ok())
            .unwrap_or_default();

        let _ = window.set_size(tauri::PhysicalSize::new(
            state.width as u32,
            state.height as u32,
        ));
        if state.is_maximized {
            let _ = window.maximize();
        }
        if let (Some(x), Some(y)) = (state.x, state.y) {
            let _ = window.set_position(tauri::PhysicalPosition::new(x as i32, y as i32));
        }
    }

    /// 保存当前窗口状态到磁盘
    pub fn save(window: &WebviewWindow, data_dir: impl AsRef<Path>) {
        let state = WindowStateData {
            x: window.outer_position().ok().map(|p| p.x as f64),
            y: window.outer_position().ok().map(|p| p.y as f64),
            width: window.outer_size().ok().map(|s| s.width as f64).unwrap_or(1200.0),
            height: window.outer_size().ok().map(|s| s.height as f64).unwrap_or(800.0),
            is_maximized: window.is_maximized().unwrap_or(false),
        };
        let path = data_dir.as_ref().join("window-state.json");
        if let Ok(json) = serde_json::to_string(&state) {
            std::fs::write(&path, json).ok();
        }
    }
}
