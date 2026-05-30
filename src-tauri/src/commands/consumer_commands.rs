use tauri::{AppHandle, Emitter, State};

use crate::kafka::consumer_service::ConsumerOptions;
use crate::AppState;

/// 获取活跃连接（clone 数据后释放 store 锁）
fn get_active_conn(state: &AppState) -> Result<crate::store::connection_store::ConnectionRecord, String> {
    let store = state.store.try_read().map_err(|e| format!("Lock: {e}"))?;
    let active_id = store
        .get_active()
        .ok_or("没有激活的连接，请先选择一个连接")?;
    let conn = store
        .list()
        .into_iter()
        .find(|c| c.id == active_id)
        .cloned()
        .ok_or("激活的连接配置不存在")?;
    Ok(conn)
}

// ---- 实时消费 ----

#[tauri::command]
pub async fn consumer_start(
    state: State<'_, AppState>,
    app_handle: AppHandle,
    opts: serde_json::Value,
) -> Result<serde_json::Value, String> {
    let options: ConsumerOptions = serde_json::from_value(opts)
        .map_err(|e| format!("Invalid options: {e}"))?;

    let conn = get_active_conn(&state)?;

    let (consumer_id, mut rx) = {
        let mut svc = state.consumer_svc.write().await;
        svc.start(&conn, &options).await?
    };

    // 后台 forwarding 任务：将 mpsc channel 的消息转为 Tauri 事件
    let handle = app_handle.clone();
    tokio::spawn(async move {
        while let Some(msg) = rx.recv().await {
            let _ = handle.emit("kafka:consumer:message", &msg);
        }
    });

    Ok(serde_json::json!({ "consumerId": consumer_id }))
}

#[tauri::command]
pub async fn consumer_stop(
    state: State<'_, AppState>,
    consumer_id: String,
) -> Result<serde_json::Value, String> {
    let mut svc = state.consumer_svc.write().await;
    svc.stop(&consumer_id).await;
    Ok(serde_json::json!({ "success": true }))
}

#[tauri::command]
pub async fn consumer_stop_all(
    state: State<'_, AppState>,
) -> Result<serde_json::Value, String> {
    let mut svc = state.consumer_svc.write().await;
    svc.stop_all().await;
    Ok(serde_json::json!({ "success": true }))
}
