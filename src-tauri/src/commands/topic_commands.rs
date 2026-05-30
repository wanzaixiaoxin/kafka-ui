use tauri::State;

use crate::kafka::topic_service;
use crate::store::connection_store::ConnectionRecord;
use crate::AppState;

// ---- 内部辅助 ----

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

// ---- Topic 操作 ----

#[tauri::command]
pub async fn topic_list(
    state: State<'_, AppState>,
    show_internal: Option<bool>,
) -> Result<Vec<topic_service::TopicInfo>, String> {
    let conn = get_active_conn(&state)?;
    let conn_id = conn.id.clone();
    let mut mgr = state.conn_mgr.write().await;
    let admin = mgr.get_admin(&conn_id, &conn)?;
    topic_service::list_topics(&admin, show_internal.unwrap_or(false))
}

#[tauri::command]
pub async fn topic_describe(
    state: State<'_, AppState>,
    topic: String,
) -> Result<topic_service::TopicDetail, String> {
    let conn = get_active_conn(&state)?;
    let conn_id = conn.id.clone();
    let mut mgr = state.conn_mgr.write().await;
    let admin = mgr.get_admin(&conn_id, &conn)?;
    topic_service::describe_topic(&admin, &topic)
}

#[tauri::command]
pub async fn topic_offsets(
    state: State<'_, AppState>,
    topic: String,
) -> Result<Vec<topic_service::PartitionOffset>, String> {
    let conn = get_active_conn(&state)?;
    let conn_id = conn.id.clone();
    let mut mgr = state.conn_mgr.write().await;
    let admin = mgr.get_admin(&conn_id, &conn)?;
    topic_service::get_topic_offsets(&admin, &topic)
}

#[tauri::command]
pub async fn topic_messages(
    state: State<'_, AppState>,
    opts: serde_json::Value,
) -> Result<serde_json::Value, String> {
    let fetch_opts: crate::kafka::consumer_service::FetchMessagesOptions =
        serde_json::from_value(opts).map_err(|e| format!("Invalid options: {e}"))?;

    let conn = get_active_conn(&state)?;
    let conn_id = conn.id.clone();

    // 使用池化的 AdminClient（复用连接，无额外握手开销）
    let admin = {
        let mut mgr = state.conn_mgr.write().await;
        mgr.get_admin(&conn_id, &conn)?
    };

    let mut svc = state.consumer_svc.write().await;
    let messages = svc.fetch_messages(&conn, &conn_id, &admin, &fetch_opts).await?;

    Ok(serde_json::to_value(messages).map_err(|e| format!("Serialize: {e}"))?)
}

#[tauri::command]
pub async fn topic_create(
    state: State<'_, AppState>,
    opts: serde_json::Value,
) -> Result<serde_json::Value, String> {
    let topic = opts["topic"].as_str().unwrap_or("");
    let num_partitions = opts["numPartitions"].as_i64().unwrap_or(1) as i32;
    let replication_factor = opts["replicationFactor"].as_i64().unwrap_or(1) as i32;

    if topic.is_empty() {
        return Ok(serde_json::json!({"success": false, "error": "Topic 名称不能为空"}));
    }

    let conn = get_active_conn(&state)?;
    let conn_id = conn.id.clone();
    let mut mgr = state.conn_mgr.write().await;
    let admin = mgr.get_admin(&conn_id, &conn)?;

    match topic_service::create_topic(&admin, topic, num_partitions, replication_factor) {
        Ok(_) => Ok(serde_json::json!({"success": true})),
        Err(e) => Ok(serde_json::json!({"success": false, "error": e})),
    }
}
