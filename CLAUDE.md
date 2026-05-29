# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## 项目概述

Kafka Client — 基于 Electron + React + TypeScript 的轻量级 Kafka 桌面客户端。使用 electron-vite 构建，electron-builder 打包 Windows NSIS 安装包。

## 常用命令

```bash
npm install          # 安装依赖（postinstall 会自动执行 electron-builder install-app-deps）
npm run dev          # 开发模式运行
npm run build        # 仅构建（输出到 out/）
npm run preview      # 预览构建产物
npm run dist         # 打包 Windows NSIS 安装包（无签名）
npm run pack         # 打包到目录（不生成安装包，用于调试）
```

## 架构

### 进程模型与通信路径

```
渲染进程 (React) → window.api.xxx (contextBridge 暴露)
  → preload/kafkaApi.ts → ipcRenderer.invoke('kafka:xxx:yyy')
  → 主进程 kafkaHandlers.ts → ipcMain.handle('kafka:xxx:yyy')
  → Kafka 服务层 → KafkaJS → Kafka Cluster
```

- **渲染进程**只能通过 `window.api` 访问功能，禁止直接访问 Node.js / KafkaJS
- **Preload** 层只做安全的 API 暴露，使用 `contextBridge.exposeInMainWorld`
- **主进程**包含所有 Kafka 连接逻辑和业务服务

### 三层 TypeScript 配置

| 配置文件 | 包含范围 |
|----------|---------|
| `tsconfig.node.json` | `src/main/**`, `src/preload/**`, `src/shared/**` |
| `tsconfig.web.json` | `src/renderer/src/**`, `src/shared/**`，含 `@renderer/*` 路径别名 |
| `tsconfig.json` | 顶层引用，仅引用上述两个 project reference |

`electron.vite.config.ts` 中主进程和 preload 使用 `externalizeDepsPlugin()`（node_modules 不打包），渲染进程使用 `@vitejs/plugin-react`。

### 共享类型 (`src/shared/types.ts`)

所有跨进程共用的类型定义在此文件中，包括 `KafkaConnection`, `ConsumedMessage`, `ConsumerOptions`, `FetchMessagesOptions`, `TopicInfo`, `PartitionOffset`, `ConsumerGroupDetail`, `LogEntry`, `AppSettings` 等。渲染进程通过 `src/renderer/src/types/kafka.ts` re-export 这些类型（不重复定义）。

### 存储层 (`src/main/store/connectionStore.ts`)

- 使用 `electron-store` 持久化连接配置、激活连接 ID、应用设置
- SASL 密码使用 `safeStorage.encryptString` 加密存储（base64），读取时自动解密
- 导出函数：`list()`, `save()`, `remove()`, `getActive()`, `setActive()`, `getSettings()`, `updateSettings()`

### 连接管理器 (`src/main/kafka/connectionManager.ts`)

`ConnectionManager` 单例，维护三个 Map：
- `clients: Map<connId, Kafka>` — Kafka 客户端实例
- `admins: Map<connId, Admin>` — 池化的 Admin 实例（lazy init，reuse）
- `producers: Map<connId, Producer>` — 池化的 Producer 实例（lazy init，reuse）

关键方法：
- `testConnection(config)` — 创建临时 Kafka 实例测试连接后立即断开
- `getActiveKafka(config)` — 获取或创建 Kafka 实例
- `getAdmin(connId)` — 获取池化的 Admin（已连接，调用方不断开）
- `getProducer(connId)` — 获取池化的 Producer（已连接，调用方不断开）
- `disconnect(connId)` — 断开并清理指定连接的所有资源
- `closeAll()` — 并发的优雅关闭所有资源

### 消费服务 (`src/main/kafka/consumerService.ts`)

`ConsumerService` 单例，采用**常驻 consumer + 会话控制**架构，避免反复 `GROUP_JOIN`（~3s）：

**实时消费**（`start`/`stop`）：
- 按 topic 维护一个常驻 consumer（`liveConsumers` Map），`consumer.run()` 只启动一次
- `start()` 创建 session（consumerId → {topic, partition, onMsg}），必要时 seek
- `stop()` 仅删除 session，consumer 继续留在 group 中运行
- 再次 `start()` 时无需 GROUP_JOIN，直接更新回调 + seek

**消息浏览**（`fetchMessages`）：
- 按连接维护一个专用 fetch consumer（`fetchState` Map），`consumer.run()` 只启动一次
- 每次查询：seek 到目标 offset → 等待消息收齐 → 标记 done
- 首次 GROUP_JOIN 后，后续查询只需 seek + fetch（~50-200ms）
- 10 秒超时保底

### IPC 处理器 (`src/main/ipc/kafkaHandlers.ts`)

核心模式：
- `requireActiveId()` — 检查当前激活连接，无连接返回 `{ error: string }`
- `ensureKafkaClient(connId)` — 应用重启后自动从存储恢复 Kafka 客户端（内存 Map 为空但持久化的 activeId 仍存在）
- 每个 handler 使用 try/catch 包裹，错误通过 `toErrorResult()` 转为 `{ error: string }` 格式
- 切换连接时广播 `kafka:connection:changed` 事件到所有窗口
- Topic/Producer/Group 操作使用池化的 Admin/Producer（通过 `connMgr.getAdmin`/`getProducer`）

### 日志系统 (`src/main/logging/`)

- `LogService` 单例：环形缓冲区（最多 5000 条），通过 IPC `log:entry` 频道实时推送到渲染进程
- `logHandlers.ts`：拦截主进程 `console` 方法 + `uncaughtException` + `unhandledRejection`，注册 `log:getAll`、`log:clear`、`log:renderer` IPC 通道
- 渲染进程在 `AppLayout` 中监听 `log:entry` 事件，错误计数显示在 DevTools 按钮徽标上

### 渲染进程路由结构

使用 `HashRouter`（适配 Electron 的 file:// 协议）：

```
/                     → AppLayout（布局容器）
  /                     → Connections（连接管理，index 路由）
  /connections           → Connections
  /topics                → Topics（Topic 列表）
  /topics/:topicName     → TopicDetail（分区详情、消息浏览）
  /producer              → Producer（消息发送）
  /consumer              → Consumer（实时消息消费）
  /groups                → ConsumerGroups（消费者组列表+详情）
  /settings              → Settings（应用设置）
```

### 渲染进程关键模式

- **API 调用链**：页面组件 → `kafkaApiClient`（`src/renderer/src/services/kafkaApiClient.ts`）→ `window.api.xxx`
- **无连接检查**：`useActiveConnection()` hook 统一处理"是否已选择激活连接"的状态和监听
- **错误处理**：IPC 返回 `{ error: string }` 或正常数据，页面组件需做 is-object-with-error 判断
- **事件监听**：`kafkaApiClient.consumer.onMessage()` 和 `kafkaApiClient.log.onEntry()` 返回取消监听函数
- **设置缓存**：`utils/settings.ts` 提供同步缓存版本（`setCachedSettings`/`getCachedSettings`）用于不支持 async 的场景
- **主题**：支持 light/dark/system，通过 `getThemeAlgorithm()` 选择 antd 主题算法

### 窗口管理 (`src/main/index.ts`)

- 窗口状态（位置、大小、最大化）通过 `electron-store` 持久化到 `window-state.json`
- 使用 `titleBarStyle: 'hidden'` + `titleBarOverlay` 实现自定义标题栏
- 关闭窗口时先保存状态 → 停止所有消费者 → 关闭所有连接 → 再关闭
- macOS 上 `window-all-closed` 时不退出应用

## 新增功能清单

在新增 IPC 通道时，需要修改五个文件（按顺序）：
1. `src/shared/types.ts` — 添加类型定义
2. `src/preload/kafkaApi.ts` — 暴露 `ipcRenderer.invoke` 封装
3. `src/main/ipc/kafkaHandlers.ts` — 注册 `ipcMain.handle` 处理器
4. `src/renderer/src/services/kafkaApiClient.ts` — 通过 `window.api.xxx` 桥接
5. 页面组件 — 调用 `kafkaApiClient.xxx()` 并处理 error 返回值
