use tauri::State;
use crate::store::AppSettings;
use crate::AppState;

// ---- 设置 ----

#[tauri::command]
pub async fn settings_get(state: State<'_, AppState>) -> Result<AppSettings, String> {
    let store = state.store.read().await;
    Ok(store.get_settings())
}

#[tauri::command]
pub async fn settings_update(
    state: State<'_, AppState>,
    settings: serde_json::Value,
) -> Result<AppSettings, String> {
    let mut store = state.store.write().await;
    Ok(store.update_settings(&settings))
}
