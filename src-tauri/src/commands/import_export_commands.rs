use std::sync::atomic::AtomicBool;
use std::sync::Arc;

use tauri::{AppHandle, Emitter, State};

use crate::kafka::import_export_service::{self, ImportExportProgress};
use crate::AppState;

/// 获取当前激活的连接配置
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

// ---- 导出 ----

#[tauri::command]
pub async fn export_start(
    app_handle: AppHandle,
    state: State<'_, AppState>,
    opts: serde_json::Value,
) -> Result<(), String> {
    let export_opts: import_export_service::ExportOptions =
        serde_json::from_value(opts).map_err(|e| format!("参数解析失败: {e}"))?;

    // 校验
    if export_opts.topic.is_empty() {
        return Err("Topic 名称不能为空".into());
    }
    if export_opts.save_path.is_empty() {
        return Err("保存路径不能为空".into());
    }
    if export_opts.max_count == 0 {
        return Err("导出条数必须大于 0".into());
    }

    let conn = get_active_conn(&state)?;
    let conn_id = conn.id.clone();

    // 使用 ConsumerService 的 fetch_messages（复用缓存的 BaseConsumer + 重试 + localhost 回退）
    let fetch_opts = crate::kafka::consumer_service::FetchMessagesOptions {
        topic: export_opts.topic.clone(),
        partition: export_opts.partition,
        offset: export_opts.offset_start.clone(),
        from_beginning: Some(true), // 从最早开始
        limit: Some(export_opts.max_count),
        offset_end: export_opts.offset_end.clone(),
    };

    let admin = {
        let mut mgr = state.conn_mgr.write().await;
        mgr.get_admin(&conn_id, &conn)?
    };

    // fetch_messages 是同步阻塞方法，放入 spawn_blocking 避免阻塞 tokio 线程
    let svc = Arc::clone(&state.consumer_svc);
    let conn_clone = conn.clone();
    let admin_clone = Arc::clone(&admin);
    let messages = tokio::task::spawn_blocking(move || {
        let mut svc = svc.blocking_write();
        svc.fetch_messages(&conn_clone, &admin_clone, &fetch_opts)
    }).await.map_err(|e| format!("Join error: {e}"))??;

    // spawn_blocking 中写入文件（IO 操作）
    let handle = app_handle.clone();
    let format = export_opts.format.clone();
    let save_path = export_opts.save_path.clone();
    let topic = export_opts.topic.clone();

    tokio::task::spawn_blocking(move || {
        match import_export_service::write_export_file(&handle, &messages, &format, &save_path, &topic) {
            Ok(count) => {
                let _ = handle.emit("kafka:export:progress", ImportExportProgress {
                    status: "completed".into(),
                    current: count,
                    total: count,
                    percent: 100.0,
                    error: None,
                    error_count: None,
                    success_count: None,
                });
            }
            Err(e) => {
                let _ = handle.emit("kafka:export:progress", ImportExportProgress {
                    status: "error".into(),
                    current: 0,
                    total: 0,
                    percent: 0.0,
                    error: Some(e),
                    error_count: None,
                    success_count: None,
                });
            }
        }
    });

    Ok(())
}

// ---- 导入 ----

#[tauri::command]
pub async fn import_start(
    app_handle: AppHandle,
    state: State<'_, AppState>,
    opts: serde_json::Value,
) -> Result<(), String> {
    let import_opts: import_export_service::ImportOptions =
        serde_json::from_value(opts).map_err(|e| format!("参数解析失败: {e}"))?;

    // 校验
    if import_opts.topic.is_empty() {
        return Err("Topic 名称不能为空".into());
    }
    if import_opts.file_path.is_empty() {
        return Err("文件路径不能为空".into());
    }
    if !std::path::Path::new(&import_opts.file_path).exists() {
        return Err(format!("文件不存在: {}", import_opts.file_path));
    }

    let conn = get_active_conn(&state)?;
    let conn_id = conn.id.clone();

    // 获取 Producer
    let producer = {
        let mut mgr = state.conn_mgr.write().await;
        mgr.get_producer(&conn_id, &conn)?
    };

    let cancel = Arc::new(AtomicBool::new(false));
    // 注册取消标志，cancel_import 命令可写入
    state.cancel_flags.write().await.insert("import".into(), Arc::clone(&cancel));

    let handle = app_handle.clone();
    let cancel2 = Arc::clone(&cancel);
    tokio::task::spawn_blocking(move || {
        if let Err(e) = import_export_service::import_messages(
            &handle, &conn, producer.as_ref(), &import_opts, cancel2,
        ) {
            let _ = handle.emit("kafka:import:progress", ImportExportProgress {
                status: "error".into(),
                current: 0,
                total: 0,
                percent: 0.0,
                error: Some(e),
                error_count: None,
                success_count: None,
            });
        }
    });

    Ok(())
}

// ---- 取消命令 ----

#[tauri::command]
pub async fn export_cancel(
    state: State<'_, AppState>,
) -> Result<(), String> {
    let flags = state.cancel_flags.read().await;
    if let Some(flag) = flags.get("export") {
        flag.store(true, std::sync::atomic::Ordering::Relaxed);
    }
    Ok(())
}

#[tauri::command]
pub async fn import_cancel(
    state: State<'_, AppState>,
) -> Result<(), String> {
    let flags = state.cancel_flags.read().await;
    if let Some(flag) = flags.get("import") {
        flag.store(true, std::sync::atomic::Ordering::Relaxed);
    }
    Ok(())
}
