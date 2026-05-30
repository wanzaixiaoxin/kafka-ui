use tauri::State;

use crate::logging::log_service::LogEntry;
use crate::AppState;

// ---- 日志 ----

#[tauri::command]
pub async fn log_get_all(state: State<'_, AppState>) -> Result<Vec<LogEntry>, String> {
    let log_svc = state.log_service.read().await;
    Ok(log_svc.get_logs())
}

#[tauri::command]
pub async fn log_clear(state: State<'_, AppState>) -> Result<(), String> {
    let mut log_svc = state.log_service.write().await;
    log_svc.clear();
    Ok(())
}

/// 接收前端转发过来的日志
#[tauri::command]
pub async fn log_renderer(state: State<'_, AppState>, entry: LogEntry) -> Result<(), String> {
    let mut log_svc = state.log_service.write().await;
    log_svc.log(
        &entry.level,
        "renderer",
        &entry.message,
        entry.data.as_deref(),
        entry.stack.as_deref(),
        entry.origin.as_deref(),
    );
    Ok(())
}
