# Kafka Client

一个轻量级的 Kafka 桌面客户端，基于 Electron + React + TypeScript 构建，用于日常开发、测试和排查 Kafka 消息问题。

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
| Electron | 桌面应用框架 |
| React | UI 构建 |
| TypeScript | 类型安全 |
| Vite (electron-vite) | 构建工具 |
| KafkaJS | Kafka 通信 |
| Ant Design | UI 组件库 |
| electron-store | 本地配置持久化 |

## 项目结构

```
src/
  main/                    # Electron 主进程
    index.ts               # 入口，窗口管理
    ipc/kafkaHandlers.ts   # IPC 处理器注册
    kafka/                 # Kafka 服务层
      connectionManager.ts # 连接管理
      topicService.ts      # Topic 操作
      producerService.ts   # 消息生产
      consumerService.ts   # 消息消费
      groupService.ts      # 消费者组查询
    store/
      connectionStore.ts   # 连接配置持久化

  preload/                 # 预加载脚本
    index.ts               # contextBridge 暴露 API
    kafkaApi.ts            # IPC 桥接层

  renderer/                # 渲染进程（React）
    src/
      App.tsx              # 根组件，路由配置
      pages/               # 页面组件
        Connections.tsx     # 连接管理
        Topics.tsx          # Topic 列表
        TopicDetail.tsx     # Topic 详情
        Producer.tsx        # 消息发送
        Consumer.tsx        # 消息消费
        ConsumerGroups.tsx  # 消费者组
        Settings.tsx        # 设置
      components/          # 公共组件
        AppLayout.tsx       # 应用布局
        ConnectionSelector.tsx
        MessageTable.tsx
        JsonViewer.tsx
        HeaderEditor.tsx
        ErrorBoundary.tsx
      services/            # API 客户端
        kafkaApiClient.ts
      types/               # 类型定义
        kafka.ts
      utils/               # 工具函数
        settings.ts
```

## 开发

```bash
# 安装依赖
npm install

# 开发模式
npm run dev

# 构建
npm run build

# 打包 Windows 安装包
npm run dist
```

## 架构

```
React Renderer
    │  IPC 调用
    ▼
Electron Preload (contextBridge)
    │  安全暴露 API
    ▼
Electron Main Process
    │  调用服务层
    ▼
Kafka Service Layer
    │  KafkaJS
    ▼
Kafka Cluster
```

设计原则：
- React 只负责界面展示和用户交互
- Kafka 连接逻辑全部在 Electron 主进程
- Preload 层只暴露有限、安全的 API
- 渲染进程不直接访问 Node.js 或 KafkaJS

## License

MIT
