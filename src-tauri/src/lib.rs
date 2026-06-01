mod commands;
mod kafka;
mod logging;
mod store;
mod window;

use std::collections::HashMap;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use tauri::Manager;
use tokio::sync::RwLock;

use crate::kafka::connection_manager::ConnectionManager;
use crate::kafka::consumer_service::ConsumerService;
use crate::logging::log_service::LogService;
use crate::store::connection_store::ConnectionStore;
use crate::window::window_state::WindowState;

/// 应用共享状态
pub struct AppState {
    pub store: Arc<RwLock<ConnectionStore>>,
    pub log_service: Arc<RwLock<LogService>>,
    pub conn_mgr: Arc<RwLock<ConnectionManager>>,
    pub consumer_svc: Arc<RwLock<ConsumerService>>,
    /// 导入导出取消标志（key: "import" / "export"）
    pub cancel_flags: Arc<RwLock<HashMap<String, Arc<AtomicBool>>>>,
}

/// 启动 Tauri 应用
pub fn run() {
    // 初始化 tracing 日志
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::DEBUG)
        .with_target(false)
        .init();

    tracing::info!("Kafka Client starting...");

    tauri::Builder::default()
        .plugin(tauri_plugin_dialog::init())
        .plugin(tauri_plugin_store::Builder::default().build())
        .plugin(tauri_plugin_shell::init())
        .manage(AppState {
            store: Arc::new(RwLock::new(ConnectionStore::default())),
            log_service: Arc::new(RwLock::new(LogService::default())),
            conn_mgr: Arc::new(RwLock::new(ConnectionManager::default())),
            consumer_svc: Arc::new(RwLock::new(ConsumerService::default())),
            cancel_flags: Arc::new(RwLock::new(HashMap::new())),
        })
        .setup(|app| {
            // 初始化数据目录并加载持久化数据
            let app_data_dir = app.path().app_data_dir().unwrap();
            let state = app.state::<AppState>();
            {
                let mut store = state.store.blocking_write();
                store.init(app_data_dir);
            }

            // 初始化日志和消费者服务
            let mut log_svc = state.log_service.blocking_write();
            log_svc.set_app_handle(app.handle().clone());
            drop(log_svc);
            let mut consumer_svc = state.consumer_svc.blocking_write();
            consumer_svc.set_app_handle(app.handle().clone());
            drop(consumer_svc);

            // 恢复窗口状态（需要数据目录）
            let window = app.get_webview_window("main").unwrap();
            WindowState::restore(&window, app.path().app_data_dir().unwrap());

            // 窗口就绪后显示
            let win = window.clone();
            let data_dir = app.path().app_data_dir().unwrap();
            window.on_window_event(move |event| {
                if let tauri::WindowEvent::CloseRequested { .. } = event {
                    WindowState::save(&win, &data_dir);
                }
            });

            // 延迟显示窗口（等 UI 渲染完毕）
            let _ = window.show();

            tracing::info!("Kafka Client ready");
            Ok(())
        })
        .invoke_handler(tauri::generate_handler![
            // 连接管理
            commands::connection_commands::connection_list,
            commands::connection_commands::connection_save,
            commands::connection_commands::connection_remove,
            commands::connection_commands::connection_test,
            commands::connection_commands::connection_use,
            commands::connection_commands::connection_active_id,
            // Topic 操作
            commands::topic_commands::topic_list,
            commands::topic_commands::topic_describe,
            commands::topic_commands::topic_offsets,
            commands::topic_commands::topic_messages,
            commands::topic_commands::topic_create,
            // Producer
            commands::producer_commands::producer_send,
            // Consumer
            commands::consumer_commands::consumer_start,
            commands::consumer_commands::consumer_stop,
            commands::consumer_commands::consumer_stop_all,
            // Consumer Groups
            commands::group_commands::group_list,
            commands::group_commands::group_describe,
            // 日志
            commands::log_commands::log_get_all,
            commands::log_commands::log_clear,
            commands::log_commands::log_renderer,
            // 导入导出
            commands::import_export_commands::export_start,
            commands::import_export_commands::import_start,
            commands::import_export_commands::export_cancel,
            commands::import_export_commands::import_cancel,
            // 设置
            commands::settings_commands::settings_get,
            commands::settings_commands::settings_update,
        ])
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}
