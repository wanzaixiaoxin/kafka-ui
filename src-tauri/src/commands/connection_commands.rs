use tauri::{AppHandle, Emitter, State};

use crate::kafka::connection_manager::ConnectionManager;
use crate::store::connection_store::ConnectionRecord;
use crate::AppState;

// ---- 连接管理 ----

#[tauri::command]
pub async fn connection_list(state: State<'_, AppState>) -> Result<Vec<ConnectionRecord>, String> {
    let store = state.store.read().await;
    Ok(store.list().into_iter().cloned().collect())
}

#[tauri::command]
pub async fn connection_save(
    state: State<'_, AppState>,
    conn: ConnectionRecord,
) -> Result<ConnectionRecord, String> {
    // 连接配置变更时，清理旧的池化资源（下次使用时会用新配置重建）
    {
        let mut mgr = state.conn_mgr.write().await;
        mgr.disconnect(&conn.id).await;
    }
    let mut store = state.store.write().await;
    Ok(store.save_connection(conn))
}

#[tauri::command]
pub async fn connection_remove(
    state: State<'_, AppState>,
    id: String,
) -> Result<serde_json::Value, String> {
    // 先断开 Kafka 资源
    {
        let mut mgr = state.conn_mgr.write().await;
        mgr.disconnect(&id).await;
    }
    let mut store = state.store.write().await;
    let ok = store.remove(&id);
    Ok(serde_json::json!({ "success": ok }))
}

#[tauri::command]
pub async fn connection_test(
    _state: State<'_, AppState>,
    conn: ConnectionRecord,
) -> Result<crate::kafka::connection_manager::ConnectionTestResult, String> {
    ConnectionManager::test_connection(&conn).await
}

#[tauri::command]
pub async fn connection_use(
    state: State<'_, AppState>,
    app_handle: AppHandle,
    id: String,
) -> Result<serde_json::Value, String> {
    // 查找连接 → 预创建 admin/producer → 设置 active
    let conn = {
        let store = state.store.read().await;
        store.list().into_iter().find(|c| c.id == id).cloned()
    };

    match conn {
        Some(c) => {
            // 优化: 使用 warmup 预热连接（创建 admin + producer + 触发元数据缓存）
            {
                let mut mgr = state.conn_mgr.write().await;
                // 停止旧的 fetch consumers，避免资源泄漏
                state.consumer_svc.write().await.stop_all().await;
                let _ = mgr.warmup(&id, &c);
            }

            let mut store = state.store.write().await;
            store.set_active(&id);

            let _ = app_handle.emit("kafka:connection:changed", &id);
            Ok(serde_json::json!({ "success": true }))
        }
        None => Ok(serde_json::json!({ "success": false, "error": "连接不存在" })),
    }
}

#[tauri::command]
pub async fn connection_active_id(state: State<'_, AppState>) -> Result<Option<String>, String> {
    let store = state.store.read().await;
    Ok(store.get_active().map(|s| s.to_string()))
}
