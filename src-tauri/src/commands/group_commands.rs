use tauri::State;

use crate::kafka::group_service;
use crate::AppState;

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

// ---- Consumer Groups ----

#[tauri::command]
pub async fn group_list(
    state: State<'_, AppState>,
) -> Result<Vec<group_service::ConsumerGroupInfo>, String> {
    let conn = get_active_conn(&state)?;
    let conn_id = conn.id.clone();
    let mut mgr = state.conn_mgr.write().await;
    let admin = mgr.get_admin(&conn_id, &conn)?;
    group_service::list_groups(&admin)
}

#[tauri::command]
pub async fn group_describe(
    state: State<'_, AppState>,
    group_id: String,
) -> Result<group_service::ConsumerGroupDetail, String> {
    let conn = get_active_conn(&state)?;
    let conn_id = conn.id.clone();
    let mut mgr = state.conn_mgr.write().await;
    let admin = mgr.get_admin(&conn_id, &conn)?;
    group_service::describe_group(&admin, &group_id)
}
