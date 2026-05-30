use tauri::State;

use crate::kafka::producer_service;
use crate::store::connection_store::ConnectionRecord;
use crate::AppState;

fn get_active_conn(state: &AppState) -> Result<ConnectionRecord, String> {
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

#[tauri::command]
pub async fn producer_send(
    state: State<'_, AppState>,
    msg: serde_json::Value,
) -> Result<producer_service::SendResult, String> {
    let kafka_msg: producer_service::KafkaMessage =
        serde_json::from_value(msg).map_err(|e| format!("Invalid message: {e}"))?;

    let conn = get_active_conn(&state)?;
    let conn_id = conn.id.clone();
    let mut mgr = state.conn_mgr.write().await;
    let producer = mgr.get_producer(&conn_id, &conn)?;

    producer_service::send_message(&producer, &kafka_msg).await
}
