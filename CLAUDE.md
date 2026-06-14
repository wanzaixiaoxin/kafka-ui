# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## 项目概述

Kafka Client — 基于 Tauri + React + TypeScript 的轻量级 Kafka 桌面客户端。

- 前端：React + Ant Design + Vite (`frontend/src/`)
- 后端：Rust + rdkafka (`src-tauri/src/`)
- 类型契约：`frontend/src/types/kafka.ts`（前后端共享的 JSON 类型定义）

## 常用命令

```bash
npm run dev             # 开发（Vite + Tauri）
npm run build           # 生产构建（NSIS 安装包 + 前端 dist 拷贝）
npm run build:renderer  # 仅构建前端
npm run preview         # 预览 Vite 构建产物
```

Rust 端：
```bash
cd src-tauri
cargo check           # 快速检查编译
cargo build --release # Release 构建
cargo build           # Debug 构建
cargo test            # 运行单元测试（含 DPAPI 加密测试）
```

## 架构

### 通信路径

```
React 前端                          Rust 后端
  │                                   │
  ├─ invoke('cmd', args) ──────────► #[tauri::command]
  │                                   │
  ├─ listen('event', cb) ◄────────── app_handle.emit('event', data)
  │                                   │
  └─ emit('log:renderer') ─────────► app_handle.listen()
```

### 核心模块（src-tauri/src/）

| 模块 | 职责 |
|------|------|
| `lib.rs` | Tauri Builder 入口，注册 plugins + commands + AppState |
| `commands/*.rs` | IPC 命令（26 个端点），做参数校验后委托给 kafka 服务层 |
| `kafka/connection_manager.rs` | 连接池：Admin/Producer 按 connId 缓存复用 |
| `kafka/topic_service.rs` | Topic 列表/详情/Offset/创建 |
| `kafka/producer_service.rs` | 消息发送 |
| `kafka/consumer_service.rs` | 实时消费 + 消息浏览（fetch consumer 按连接缓存） |
| `kafka/group_service.rs` | 消费者组列表/详情/Offset/Lag |
| `kafka/import_export_service.rs` | 消息批量导出（JSONL/CSV）+ 导入 |
| `store/connection_store.rs` | 连接配置 + 设置，JSON 文件持久化到 `%APPDATA%` |
| `security/dpapi.rs` | SASL 密码 DPAPI 加密（仅 Windows） |
| `logging/log_service.rs` | 环形缓冲日志 + Tauri 事件推送 |
| `window/window_state.rs` | 窗口位置/大小持久化 |

### AppState

```rust
pub struct AppState {
    pub store: Arc<RwLock<ConnectionStore>>,        // 持久化存储
    pub log_service: Arc<RwLock<LogService>>,        // 日志
    pub conn_mgr: Arc<RwLock<ConnectionManager>>,    // Kafka 连接池
    pub consumer_svc: Arc<RwLock<ConsumerService>>,  // 消费者
    pub cancel_flags: Arc<RwLock<HashMap<String, Arc<AtomicBool>>>>, // 导入导出取消
}
```

### 前后端类型映射

前端 `frontend/src/types/kafka.ts` 是类型定义的唯一来源。Rust 端定义了对应的 struct，使用 `#[serde(rename_all = "camelCase")]` 或 `#[serde(rename = "fieldName")]` 匹配前端传来的 camelCase JSON。

新增 IPC 端点需修改：
1. `src-tauri/src/commands/<name>.rs` — 添加 `#[tauri::command]`
2. `src-tauri/src/lib.rs` — 注册到 `generate_handler![]`
3. `frontend/src/services/kafkaApiClient.ts` — 添加 `invoke()` 调用
4. `frontend/src/types/kafka.ts` — 添加对应类型

### 消息浏览性能优化

`fetch_messages` 复用 fetch consumer（按连接缓存）：
- 首次调用：创建 BaseConsumer → assign 分区 → poll（较慢）
- 后续调用：复用缓存的 consumer（跳过 TCP 握手 + 元数据请求）
- 支持 Docker/WSL 场景的 advertised.listeners 不匹配时自动 localhost 回退

### 消费者组 Offset/Lag

`group_service::fetch_group_offsets` 通过创建临时 BaseConsumer（设置 group.id 但不 join group）+ assign 全部分区 + `committed()` 发送 OffsetFetch 请求来获取已提交 offset，再结合 `fetch_watermarks` 计算 Lag。此操作为 best-effort，失败时返回空 offsets 列表。

### 凭据安全

SASL 密码使用 Windows DPAPI (`CryptProtectData` / `CryptUnprotectData`) 加密后存储在 `data.json` 中：
- 内存中始终是明文（rdkafka 需要明文密码）
- 仅落盘时加密（`connection_store::save` 中调用 `security::encrypt_string`）
- 加密后的密文以 `ENC1:<base64>` 格式存储
- 加密绑定到当前 Windows 用户账户（同一用户才能解密）

### 错误处理

- Rust 命令返回 `Result<T, String>`，`Err(String)` 会被 Tauri 转为异常抛给前端
- 部分命令使用 `Ok(json!({"success": false, "error": ...}))` 模式（注意两种模式的区别）
- 前端 `catch` 块捕获后通过 `message.error()` 展示
- 所有 Kafka 操作需先通过 `get_active_conn()` 检查激活连接

## 构建配置

| 文件 | 用途 |
|------|------|
| `vite.config.ts` | Vite 前端构建（root: frontend, outDir: ../dist/renderer） |
| `src-tauri/tauri.conf.json` | Tauri 窗口/打包配置 |
| `src-tauri/Cargo.toml` | Rust 依赖 |
| `src-tauri/capabilities/default.json` | Tauri v2 权限声明 |
| `tsconfig.json` | 引用 `tsconfig.web.json` |
| `tsconfig.web.json` | 前端 TypeScript 配置 |

## 已知限制

- SSL/SASL 需要 OpenSSL 库后重新编译：默认 Cargo.toml 中 rdkafka 未启用 `ssl`/`sasl` features，运行时若连接配置了 SSL/SASL 会返回编译提示错误
- `connection_test` 不返回 `cluster_id`（rdkafka 0.37 Metadata API 限制）
- 消息 Value/Key 使用 `String::from_utf8_lossy` 转换，二进制消息（Avro/Protobuf）会损坏
- 消费者组 Offset/Lag 查询需要 assign 全部分区，大型集群可能较慢
- 跨平台：当前仅打包 Windows NSIS，macOS/Linux 需要 fallback 加密实现
