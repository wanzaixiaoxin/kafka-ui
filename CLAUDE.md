# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## 项目概述

Kafka Client — 基于 Tauri + React + TypeScript 的轻量级 Kafka 桌面客户端。

- 前端：React + Ant Design + Vite (`src/renderer/`)
- 后端：Rust + rdkafka (`src-tauri/`)
- 共享类型：`src/shared/types.ts`

## 常用命令

```bash
npm run dev           # 开发（Vite + Tauri）
npm run build         # 生产构建（生成 NSIS 安装包）
npm run build:renderer # 仅构建前端
npm run preview       # 预览 Vite 构建产物
```

Rust 端：
```bash
cd src-tauri
cargo check           # 快速检查编译
cargo build --release # Release 构建
cargo build           # Debug 构建
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
  └─ emit('event', data) ──────────► app_handle.listen()
```

### 核心模块（src-tauri/src/）

| 模块 | 职责 |
|------|------|
| `lib.rs` | Tauri Builder 入口，注册 plugins + commands + AppState |
| `commands/*.rs` | 21 个 IPC 命令，每个命令做参数校验后委托给 kafka 服务层 |
| `kafka/connection_manager.rs` | 连接池：Admin/Producer 按 connId 缓存复用 |
| `kafka/topic_service.rs` | Topic 列表/详情/Offset/创建 |
| `kafka/producer_service.rs` | 消息发送 |
| `kafka/consumer_service.rs` | 实时消费 + 消息浏览（fetch consumer 按连接缓存） |
| `kafka/group_service.rs` | 消费者组列表/详情 |
| `store/connection_store.rs` | 连接配置 + 设置，JSON 文件持久化到 `%APPDATA%` |
| `logging/log_service.rs` | 环形缓冲日志 + Tauri 事件推送 |
| `window/window_state.rs` | 窗口位置/大小持久化 |

### AppState

```rust
pub struct AppState {
    pub store: Arc<RwLock<ConnectionStore>>,     // 持久化存储
    pub log_service: Arc<RwLock<LogService>>,     // 日志
    pub conn_mgr: Arc<RwLock<ConnectionManager>>, // Kafka 连接池
    pub consumer_svc: Arc<RwLock<ConsumerService>>, // 消费者
}
```

### 前后端类型映射

前端 `src/shared/types.ts` 是类型定义的唯一来源。Rust 端定义了对应的 struct，使用 `#[serde(rename_all = "camelCase")]` 匹配前端传来的 camelCase JSON。

新增 IPC 端点需修改：
1. `src-tauri/src/commands/<name>.rs` — 添加 `#[tauri::command]`
2. `src-tauri/src/lib.rs` — 注册到 `generate_handler![]`
3. `src/renderer/src/services/kafkaApiClient.ts` — 添加 `invoke()` 调用

### 消息浏览性能优化

`fetch_messages` 复用 fetch consumer（按连接缓存）：
- 首次调用：创建 StreamConsumer → subscribe → GROUP_JOIN（~3s）
- 后续调用：直接 seek → poll（~50-200ms）

### 错误处理

- Rust 命令返回 `Result<T, String>`，`Err(String)` 会被 Tauri 转为异常抛给前端
- 前端 `catch` 块捕获后通过 `message.error()` 展示
- 所有 Kafka 操作需先通过 `get_active_conn()` 检查激活连接

## 构建配置

| 文件 | 用途 |
|------|------|
| `vite.config.ts` | Vite 前端构建（root: src/renderer, outDir: out/renderer） |
| `src-tauri/tauri.conf.json` | Tauri 窗口/打包配置 |
| `src-tauri/Cargo.toml` | Rust 依赖 |
| `src-tauri/capabilities/default.json` | Tauri v2 权限声明 |
| `tsconfig.json` | 引用 `tsconfig.web.json` |
| `tsconfig.web.json` | 前端 TypeScript 配置 |

## 已知限制

- SSL/SASL 需安装 OpenSSL 后启用 rdkafka 的 `ssl` `sasl` features
- 消费者组 Offset/Lag 暂不显示（rdkafka 0.37 GroupMemberInfo 不暴露 assignment）
- `connection_test` 不返回 `cluster_id`（rdkafka 0.37 Metadata API 限制）
