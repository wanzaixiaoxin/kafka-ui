# Kafka Client

轻量级 Kafka 桌面客户端，基于 Tauri + React + TypeScript 构建，用于日常开发、测试和排查 Kafka 消息问题。

## 功能

- **连接管理** - 管理多套 Kafka 连接配置，支持 SSL/SASL 认证
- **Topic 浏览** - 查看 Topic 列表、分区详情、副本和 ISR 信息
- **消息发送** - 向指定 Topic 发送消息，支持 Key、Value、Headers
- **消息消费** - 从 Topic 实时消费消息，支持 Earliest/Latest/指定 Offset
- **Consumer Group** - 查看消费者组列表、消费滞后（Lag）情况
- **JSON 格式化** - 自动识别并格式化 JSON 消息内容

## 技术栈

| 技术 | 用途 |
|------|------|
| Tauri | 桌面应用框架 |
| Rust | 后端（Kafka 通信、存储） |
| React | UI 构建 |
| TypeScript | 类型安全 |
| Vite | 前端构建工具 |
| rdkafka | Kafka 通信（基于 librdkafka） |
| Ant Design | UI 组件库 |

## 项目结构

```
frontend/                   # React 前端
  index.html
  src/
    App.tsx                 # 根组件，路由配置
    pages/                  # 页面组件
    components/             # 公共组件
    services/               # API 客户端 (Tauri invoke)
    hooks/                  # React hooks
    types/                  # 类型定义 (与 Rust 后端的 JSON 契约)
    utils/                  # 工具函数

src-tauri/                  # Rust 后端 (Tauri)
  src/
    main.rs                 # 入口
    lib.rs                  # Tauri Builder
    commands/               # IPC 命令（26 个端点）
    kafka/                  # Kafka 服务层
      connection_manager.rs # 连接池管理
      consumer_service.rs   # 消费服务
      producer_service.rs   # 生产服务
      topic_service.rs      # Topic 操作
      group_service.rs      # 消费者组查询（含 Offset/Lag）
      import_export_service.rs # 消息导入导出
    security/               # 凭据加密（Windows DPAPI）
    store/                  # 持久化存储
    logging/                # 日志服务
    window/                 # 窗口状态管理
  tauri.conf.json           # Tauri 配置
  Cargo.toml                # Rust 依赖
```

## 架构

```
frontend/ (React)           src-tauri/ (Rust)
    │                           │
    ├─ invoke('cmd', args) ───► #[tauri::command]
    │                           │ rdkafka → Kafka
    ├─ listen('event') ◄─────── emit('event', data)
    │                           │ JSON 文件持久化
    └─ emit('log:renderer') ──► LogService → 前端展示
```

- React 只负责界面展示和用户交互
- Kafka 连接逻辑全部在 Rust 后端
- 前端通过 `invoke()` 调用 Rust 命令，通过 `listen()` 接收事件
- 渲染进程无法直接访问 Kafka

## 开发

### 前置条件

- Node.js >= 18
- Rust 工具链（rustup, cargo）
- CMake（编译 librdkafka）
- （可选）OpenSSL 开发库（启用 SSL/SASL 需要）

### 命令

```bash
# 安装依赖
npm install

# 开发模式
npm run dev

# 构建
npm run build

# 仅构建前端
npm run build:renderer

# 仅构建 Rust
cd src-tauri && cargo build --release
```

## 启用 SSL/SASL

1. 安装 OpenSSL 开发库
2. 修改 `src-tauri/Cargo.toml`：
   ```toml
   rdkafka = { version = "0.37", features = ["ssl", "sasl", "cmake-build"] }
   ```
3. 重新构建

## License

MIT
