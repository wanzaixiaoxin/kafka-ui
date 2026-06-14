# Kafka Client 产品说明文档

> 文档版本：v1.0  
> 更新日期：2026-05-27  
> 产品名称：Kafka Client  
> 应用版本：1.0.0  

---

## 目录

1. [产品概述](#1-产品概述)
2. [目标用户](#2-目标用户)
3. [产品定位与竞品对比](#3-产品定位与竞品对比)
4. [功能矩阵](#4-功能矩阵)
5. [功能模块详述](#5-功能模块详述)
6. [数据模型](#6-数据模型)
7. [用户交互流程](#7-用户交互流程)
8. [UI 设计规范](#8-ui-设计规范)
9. [技术架构概览](#9-技术架构概览)
10. [非功能特性](#10-非功能特性)
11. [已知限制与后续规划](#11-已知限制与后续规划)

---

## 1. 产品概述

### 1.1 产品简介

Kafka Client 是一款**轻量级 Kafka 桌面客户端**，基于 Tauri + React + TypeScript 技术栈构建。产品定位为**日常开发、测试和排查 Kafka 消息问题的辅助工具**，帮助开发者和运维人员以图形化界面便捷地操作 Kafka 集群，而无需使用命令行工具或编写代码。

### 1.2 核心价值

- **降低 Kafka 操作门槛**：图形化操作界面，无需记忆 CLI 命令
- **提升日常开发效率**：快速浏览 Topic、发送/消费消息、查看消费者组状态
- **多集群管理**：支持配置和管理多套 Kafka 集群连接
- **安全认证支持**：支持 SSL 加密传输和 SASL 多种认证机制

### 1.3 产品边界

| 包含 | 不包含 |
|------|--------|
| Kafka 集群连接管理 | Topic 创建/删除/修改 |
| Topic 元数据浏览 | 分区扩容/副本重分配 |
| 消息发送（生产） | 集群性能监控（JMX） |
| 消息消费（实时流/批量拉取） | 消息过滤/路由/转换 |
| 消费者组状态查看 | Schema Registry 集成 |
| JSON 消息格式化浏览 | Kafka Connect / ksqlDB 管理 |
| 应用运行日志查看 | ACL 权限管理 |

---

## 2. 目标用户

| 用户角色 | 典型场景 | 核心需求 |
|---------|---------|---------|
| **后端开发者** | 日常开发中查看 Topic 中的消息内容，验证消息生产/消费逻辑 | 快速浏览消息、发送测试消息、查看消费者组消费进度 |
| **大数据工程师** | 排查数据管道问题，验证数据格式和内容 | 消息消费、JSON 格式化查看、Offset 定位 |
| **SRE / 运维人员** | 巡检 Kafka 集群状态，查看消费者组 Lag 情况 | 连接测试、消费者组详情查看、Lag 监控 |
| **测试工程师** | 构造测试消息，验证消息处理的正确性 | 向特定 Topic 发送消息、消费并验证消息内容 |

---

## 3. 产品定位与竞品对比

### 3.1 定位

**轻量级、开箱即用的 Kafka 桌面 GUI 工具**，聚焦于「查消息、看元数据、管连接」三大核心场景。与同类产品相比，强调以下差异化：

- **Tauri 桌面应用**：原生桌面体验，无需浏览器，安装包体积小（Rust 后端）
- **React + TypeScript 前端**：类型安全，易于维护和扩展
- **安全架构**：Rust 进程隔离，SASL 密码通过 Windows DPAPI 加密存储

### 3.2 竞品对比

| 维度 | Kafka Client（本项目） | Kafka Tool（Offset Explorer） | Kafdrop（Web） | AKHQ（Web） |
|------|----------------------|------------------------------|---------------|-------------|
| 形态 | 桌面客户端 | 桌面客户端 | Web 应用 | Web 应用 |
| 技术栈 | Tauri + React + Rust | Java Swing | Spring Boot + React | Spring Boot + Angular |
| Kafka 库 | rdkafka (librdkafka) | 原生 Java | Kafka Java Client | Kafka Java Client |
| 跨平台 | Windows（可扩展） | Windows/macOS/Linux | 跨平台（浏览器） | 跨平台（浏览器） |
| SSL/SASL 支持 | 支持 | 支持 | 支持 | 支持 |
| Topic CRUD | 不支持 | 支持 | 支持 | 支持 |
| Schema Registry | 不支持 | 支持 | 支持 | 支持 |
| 实时消费 | 支持 | 支持 | 支持 | 支持 |
| 中文界面 | 完全支持 | 仅英文 | 仅英文 | 仅英文 |
| License | MIT（开源免费） | 商业软件（试用版有限制） | Apache 2.0 | Apache 2.0 |

---

## 4. 功能矩阵

| 编号 | 功能模块 | 子功能 | 优先级 | 当前状态 |
|------|---------|-------|--------|---------|
| F-01 | 连接管理 | 新建 Kafka 连接（名称、Broker 地址、Client ID 等） | P0 | ✅ 已实现 |
| F-02 | 连接管理 | 编辑连接配置 | P0 | ✅ 已实现 |
| F-03 | 连接管理 | 删除连接 | P0 | ✅ 已实现 |
| F-04 | 连接管理 | 测试连接连通性 | P0 | ✅ 已实现 |
| F-05 | 连接管理 | 切换当前激活的连接 | P0 | ✅ 已实现 |
| F-06 | 连接管理 | SSL 加密传输支持 | P0 | ⚠️ 需重新编译（默认未启用 OpenSSL feature） |
| F-07 | 连接管理 | SASL 认证支持（PLAIN / SCRAM-SHA-256 / SCRAM-SHA-512） | P0 | ⚠️ 需重新编译（默认未启用 SASL feature） |
| F-08 | 连接管理 | 密码加密持久化存储（Windows DPAPI） | P0 | ✅ 已实现 |
| F-09 | Topic 管理 | 查看 Topic 列表 | P0 | ✅ 已实现 |
| F-10 | Topic 管理 | Topic 名称搜索/过滤 | P0 | ✅ 已实现 |
| F-11 | Topic 管理 | 显示/隐藏内部 Topic（__consumer_offsets 等） | P0 | ✅ 已实现 |
| F-12 | Topic 管理 | 查看 Topic 分区详情（Leader、副本、ISR） | P0 | ✅ 已实现 |
| F-13 | Topic 管理 | 查看 Offset 范围（Earliest / Latest） | P0 | ✅ 已实现 |
| F-14 | Topic 管理 | 批量拉取浏览消息（按分区/Offset/条数查询） | P0 | ✅ 已实现 |
| F-15 | Topic 管理 | 表格排序（按 Topic 名/分区数/副本数） | P2 | ✅ 已实现 |
| F-16 | 消息生产 | 选择目标 Topic（支持 AutoComplete 搜索） | P0 | ✅ 已实现 |
| F-17 | 消息生产 | 填写消息 Key | P1 | ✅ 已实现 |
| F-18 | 消息生产 | 填写消息 Value | P0 | ✅ 已实现 |
| F-19 | 消息生产 | 指定分区 | P1 | ✅ 已实现 |
| F-20 | 消息生产 | 添加消息 Headers | P1 | ✅ 已实现 |
| F-21 | 消息生产 | JSON 格式化辅助 | P1 | ✅ 已实现 |
| F-22 | 消息生产 | 发送结果展示（Topic、分区、Offset） | P0 | ✅ 已实现 |
| F-23 | 消息消费 | 选择 Topic 消费 | P0 | ✅ 已实现 |
| F-24 | 消息消费 | 指定分区消费（或全部分区） | P1 | ✅ 已实现 |
| F-25 | 消息消费 | 消费起始位置（Earliest / Latest / 指定 Offset） | P0 | ✅ 已实现 |
| F-26 | 消息消费 | 实时消息推送（事件驱动） | P0 | ✅ 已实现 |
| F-27 | 消息消费 | 停止/清空消费 | P0 | ✅ 已实现 |
| F-28 | 消息消费 | 消息数量限制（最大保留条数可配置） | P1 | ✅ 已实现 |
| F-29 | 消息消费 | 消息详情查看（抽屉面板） | P0 | ✅ 已实现 |
| F-30 | 消息通用 | JSON 自动格式化与语法高亮 | P0 | ✅ 已实现 |
| F-31 | 消息通用 | 消息头部（Headers）查看 | P1 | ✅ 已实现 |
| F-32 | 消费者组 | 查看消费者组列表 | P0 | ✅ 已实现 |
| F-33 | 消费者组 | 按 Group ID 搜索过滤 | P1 | ✅ 已实现 |
| F-34 | 消费者组 | 查看消费者组详情（状态、成员、协议） | P0 | ✅ 已实现 |
| F-35 | 消费者组 | 查看分区 Offset 与 Lag | P0 | ✅ 已实现 |
| F-36 | 消费者组 | Lag 值分级颜色标识 | P2 | ✅ 已实现 |
| F-37 | 应用设置 | 主题切换（浅色/深色/跟随系统） | P1 | ✅ 已实现 |
| F-38 | 应用设置 | 消息最大保留数量配置 | P1 | ✅ 已实现 |
| F-39 | 应用设置 | 自动刷新间隔配置 | P2 | ✅ 已实现 |
| F-40 | 开发者工具 | 应用日志面板（F12 快捷键） | P2 | ✅ 已实现 |
| F-41 | 开发者工具 | 日志级别实时统计（Error 计数） | P2 | ✅ 已实现 |
| F-42 | 生命周期 | 窗口位置/大小/最大化状态持久化 | P2 | ✅ 已实现 |
| F-43 | 生命周期 | 应用退出时自动停止所有消费者 | P2 | ✅ 已实现 |
| F-44 | 生命周期 | 应用重启后自动重建 Kafka 客户端连接 | P2 | ✅ 已实现 |
| F-45 | Topic 管理 | 创建 Topic（指定分区数/副本因子） | P1 | ✅ 已实现 |
| F-46 | 数据导入导出 | 消息批量导出（JSONL / CSV 格式） | P1 | ✅ 已实现 |
| F-47 | 数据导入导出 | 消息批量导入（JSONL / CSV 格式） | P1 | ✅ 已实现 |
| F-48 | 数据导入导出 | 导入/导出进度推送与取消 | P1 | ✅ 已实现 |

---

## 5. 功能模块详述

### 5.1 连接管理模块

#### 5.1.1 功能入口

- 侧边栏导航：`连接管理` 菜单项
- 默认首页：应用启动后默认展示连接管理页面

#### 5.1.2 连接列表

以表格形式展示所有已保存的 Kafka 连接配置。表格包含以下列：

| 列名 | 说明 | 交互 |
|------|------|------|
| 名称 | 连接名称（粗体标记当前激活连接） | 激活连接旁显示绿色 `Badge` |
| Brokers | Broker 地址列表（逗号分隔） | 只读展示 |
| Client ID | 客户端标识符 | 只读展示 |
| SSL | 是否启用 SSL | 绿色 `Tag` 表示已启用 |
| SASL | SASL 机制类型（none/plain/scram-sha-256/scram-sha-512） | 蓝色 `Tag` 展示机制类型 |
| 描述 | 连接备注信息（可空，显示 "-"） | 溢出省略 |
| 操作 | 操作按钮组 | 见下方说明 |

**操作按钮说明：**

| 按钮 | 条件 | 功能 |
|------|------|------|
| 测试 | 始终可见 | 测试与该 Kafka 集群的连接连通性 |
| 使用 | 当前连接非激活 | 将该连接设为激活连接，控制切换连接时停止所有正在运行的消费者 |
| 当前连接 | 当前连接已激活 | 绿色 Tag 标记状态，不可点击 |
| 编辑 | 始终可见 | 打开编辑弹窗修改连接配置 |
| 删除 | 当前连接非激活 | 确认后删除，激活中的连接不可删除 |

#### 5.1.3 连接表单（新建/编辑）

通过 Modal 弹窗展示，表单字段如下：

| 字段 | 类型 | 必填 | 默认值 | 校验规则 |
|------|------|------|--------|---------|
| 连接名称 | Input | 是 | 空 | 非空校验 |
| Broker 地址 | TextArea（多行） | 是 | 空 | 至少一个有效地址 |
| Client ID | Input | 否 | kafka-client | - |
| SSL | Switch | 否 | 关闭 | - |
| SASL 机制 | Select | 否 | 无 | 可选值：无/PLAIN/SCRAM-SHA-256/SCRAM-SHA-512 |
| 用户名 | Input | 条件必填 | 空 | 选择 SASL 时必填 |
| 密码 | Input.Password | 条件必填 | 空 | 选择 SASL 时必填 |
| 描述 | TextArea | 否 | 空 | - |

**交互细节：**
- SASL 用户名/密码字段在 SASL 机制选择为"无"时自动隐藏
- Brokers 地址每行一个，提交时自动按换行分割并 trim
- 密码通过 Windows DPAPI 加密后持久化（`ENC1:<base64>` 格式）

#### 5.1.4 连接测试

测试结果以 Modal 弹窗展示：

**成功时：**
- 标题：绿色 `CheckCircleOutlined` 图标 + "连接成功"
- 展示内容：集群 ID、Controller 节点 ID、Broker 数量、Broker 列表（Node ID: host:port）

**失败时：**
- 标题：红色 `CloseCircleOutlined` 图标 + "连接失败"
- 展示内容：错误消息文本（红色）

#### 5.1.5 连接切换

切换连接时的处理流程：

1. 更新 JSON 存储中的 `activeConnectionId`
2. 调用 `connMgr.getActiveKafka()` 预热 Kafka 客户端实例
3. 调用 `consumerSvc.stopAll()` 停止所有正在运行的消费者
4. 通过 Tauri `emit("kafka:connection:changed")` 广播事件到前端
5. 各页面监听该事件，自动刷新数据

### 5.2 Topic 管理模块

#### 5.2.1 Topic 列表

以表格形式展示当前连接下的 Topic，支持以下交互：

| 功能 | 实现方式 |
|------|---------|
| **名称搜索** | 前端实时过滤，不区分大小写 |
| **显示内部 Topic** | Checkbox 控制是否显示 `__consumer_offsets` 等内部 Topic |
| **刷新** | 重新从 Kafka 集群获取 Topic 列表 |
| **排序** | 按 Topic 名称、分区数、副本数列头排序 |
| **分页** | 每页 20 条，支持切换每页条数 |
| **查看详情** | 点击"详情"按钮导航至 `/topics/:topicName` 详情页 |

**无连接状态：**
- 页面顶部显示 `Alert` 警告条："未选择连接"
- 警告条附带"前往连接管理"快捷按钮
- Topic 表格显示空数据

**错误状态：**
- 页面顶部显示错误 `Alert` 提示
- 错误提示右侧附带"重试"按钮

#### 5.2.2 Topic 详情页

详情页包含三大信息块：

**1. 分区信息表格**

| 列 | 说明 |
|----|------|
| 分区 ID | 分区编号，可排序 |
| Leader | 当前 Leader Broker ID |
| 副本 | 副本列表，以蓝色 `Tag` 展示 |
| ISR | ISR 列表，以绿色 `Tag` 展示 |

**2. Offset 范围表格**

| 列 | 说明 |
|----|------|
| 分区 | 分区编号 |
| 最早 Offset | Earliest Offset |
| 最新 Offset | Latest Offset |
| 消息数量 | 通过 `BigInt(latestOffset) - BigInt(earliestOffset)` 计算，橙色 `Tag` 展示 |

**3. 消息浏览区域**

查询控件：

| 控件 | 类型 | 说明 |
|------|------|------|
| 分区选择 | Select | 可选全部分区或指定分区 |
| 起始位置 | Select | 最新消息 / 最早消息 |
| Offset | InputNumber | 选择"最早消息"后可指定起始 Offset |
| 条数 | InputNumber | 1~500 条，默认 50 |
| 查询按钮 | Button | 触发批量拉取 |

**交互细节：**

- 查询结果通过 `MessageTable` 组件展示
- 点击任意消息行可打开抽屉面板查看消息详情
- 消息详情包含：Topic、分区、Offset、时间戳、Key、Headers、Value
- 消息 Value 通过 `JsonViewer` 自动检测并格式化 JSON

### 5.3 消息生产模块

#### 5.3.1 功能流程

1. 选择目标 Topic（AutoComplete 组件，支持搜索过滤）
2. 可选填写消息 Key
3. 可选指定分区（不指定则由 Kafka 自动选择）
4. 可选添加 Headers（通过 `HeaderEditor` 组件）
5. 填写消息内容 Value（Monospace 字体文本框）
6. 可选点击"格式化 JSON"自动格式化
7. 点击"发送"按钮提交

#### 5.3.2 表单字段

| 字段 | 类型 | 必填 | 默认值 |
|------|------|------|--------|
| Topic | AutoComplete | 是 | 空 |
| Key | Input | 否 | 空 |
| 分区 | InputNumber | 否 | 空（不指定） |
| Headers | HeaderEditor | 否 | 空 |
| Value | TextArea | 是 | 空 |

#### 5.3.3 发送结果展示

发送成功后，以 `Alert` 组件展示发送结果，包含：

- Topic
- 分区（partition）
- Offset
- 时间戳（本地格式）

#### 5.3.4 JSON 格式化

- 点击"格式化 JSON"按钮触发
- 尝试 `JSON.parse()` 后 `JSON.stringify(obj, null, 2)` 美化
- 非 JSON 内容提示"不是有效的 JSON"
- 仅美化 Value 字段，不影响表单其他字段

### 5.4 消息消费模块

#### 5.4.1 功能流程

1. 选择 Topic（下拉 Select，支持搜索）
2. 可选指定分区（默认全部分区）
3. 选择消费起始位置（Earliest / Latest / 指定 Offset）
4. 点击"开始消费"启动实时消费
5. 消息实时推送到表格展示
6. 可随时点击"停止消费"终止
7. 可点击"清空消息"清除已消费消息

#### 5.4.2 消费控制

| 控件 | 说明 |
|------|------|
| 开始消费 | 启动消费者，开始消费前必须先注册消息监听（避免竞态条件） |
| 停止消费 | 取消消息监听，停止对应消费者 |
| 清空消息 | 清空当前页面已消费的消息列表（不影响消费者运行） |

#### 5.4.3 状态展示

| 元素 | 说明 |
|------|------|
| 状态 Badge | 绿色闪烁 = 消费中；灰色 = 已停止 |
| 统计文字 | "已消费 X 条消息 (最大保留 Y 条)" |

#### 5.4.4 消息缓冲区管理

- 使用 `useState` 维护消息数组
- 新消息到达时追加到数组末尾
- 超过 `maxMsgs`（默认 500）时自动丢弃最旧消息
- `maxMsgs` 值从 `settings` 缓存中读取，在设置页面可配置（100~10000）

#### 5.4.5 组件卸载清理

- 页面关闭（组件卸载）时自动取消消息监听
- 自动调用 `consumer.stop(consumerId)` 停止消费者
- 通过 `useRef` 持有消费者 ID 和取消订阅函数，避免闭包捕获过时值

### 5.5 消费者组模块

#### 5.5.1 列表视图

以表格展示当前连接的所有消费者组：

| 列 | 说明 |
|----|------|
| Group ID | 消费者组 ID |
| 状态 | 着色 Tag（Stable=绿、PreparingRebalance=金、CompletingRebalance=橙、Dead=红、Empty=蓝） |
| 成员数 | 当前组成员数量 |
| 协议 | 分区分配协议（如 range、roundrobin、cooperative-sticky 等） |
| 操作 | "详情"按钮，打开抽屉面板 |

**搜索过滤：** 按 Group ID 关键词实时过滤，不区分大小写。

#### 5.5.2 详情抽屉

左侧打开宽 720px 的抽屉面板，包含三块内容：

**1. 基本信息卡片**

- 状态（带颜色 Tag）
- 协议
- 成员数

**2. 成员列表表格**

| 列 | 说明 |
|----|------|
| Client ID | 消费者客户端 ID |
| Member ID | 成员唯一标识 |
| Host | 消费者所在主机地址 |

**3. Offset 信息表格**

| 列 | 说明 |
|----|------|
| Topic | 分区所属 Topic |
| Partition | 分区编号 |
| Current Offset | 当前消费进度 |
| Log End Offset | 日志末尾 Offset |
| Lag | 消费滞后量（按值着色：0=绿、<100=绿、<1000=橙、>1000=红） |

### 5.6 设置模块

#### 5.6.1 应用信息

展示区：应用名称 + 版本号 + 简要描述。

#### 5.6.2 偏好设置

| 设置项 | 类型 | 范围 | 默认值 | 说明 |
|--------|------|------|--------|------|
| 消息最大保留数量 | InputNumber | 100~10000，步长 100 | 500 | 消费页面保留的最大消息条数 |
| 自动刷新间隔（秒） | InputNumber | 0~3600，步长 5 | 0 | 0 表示禁用 |
| 主题 | Select | 浅色/深色/跟随系统 | 浅色 | 即时生效 |

**当前设置摘要：** 设置表单下方展示当前生效设置摘要。

### 5.7 开发者工具模块（DevTools）

#### 5.7.1 入口

- 点击顶部 Header 右侧的 Bug 图标按钮
- 快捷键 `F12`

#### 5.7.2 功能

- 展示应用内所有日志（Rust 后端 + React 前端转发）
- 日志条目实时推送
- Error/Fatal 日志实时统计计数显示在按钮 Badge 上
- 面板高度可拖拽调整（150px~600px）
- 点击 Bug 图标自动重置错误计数

#### 5.7.3 日志级别

| 级别 | 说明 |
|------|------|
| debug | 调试信息（灰色） |
| info | 普通信息（蓝色） |
| warn | 警告（黄色） |
| error | 错误（红色） |
| fatal | 致命错误（深红色） |

#### 5.7.4 日志服务

- Rust 后端维护一个 In-Memory 环形缓冲区，最多保留 5000 条日志
- 前端通过 `consoleInterceptor.ts` 将日志通过 Tauri event 转发到后端统一管理
- 应用关闭时日志不持久化

---

## 6. 数据模型

### 6.1 KafkaConnection（连接配置）

```typescript
interface KafkaConnection {
  id: string              // UUID，唯一标识
  name: string            // 连接名称
  brokers: string[]       // Broker 地址数组 ["host:port", ...]
  clientId: string        // Kafka 客户端 ID
  ssl: boolean            // 是否启用 SSL
  sasl?: {                // SASL 认证配置（可选）
    mechanism: 'plain' | 'scram-sha-256' | 'scram-sha-512'
    username: string
    password: string      // 存储时通过 DPAPI 加密
  }
  description?: string    // 连接描述
  createdAt: number       // 创建时间戳
  updatedAt: number       // 更新时间戳
}
```

### 6.2 KafkaMessage（消息定义）

```typescript
interface KafkaMessage {
  topic: string                          // 目标 Topic
  key?: string                           // 消息 Key
  value: string                          // 消息 Value
  partition?: number                     // 指定分区
  headers?: Record<string, string>       // 消息头
  timestamp?: number                     // 时间戳
}

interface SendResult {
  topic: string          // 发送到的 Topic
  partition: number      // 写入的分区
  offset: string         // 写入的 Offset
  timestamp: number      // 写入时间戳
}

interface ConsumedMessage {
  topic: string          // 来源 Topic
  partition: number      // 来源分区
  offset: string         // Offset
  key?: string           // 消息 Key
  value: string          // 消息 Value
  headers?: Record<string, string>  // 消息头
  timestamp: number      // 时间戳
}
```

### 6.3 TopicMeta（Topic 元数据）

```typescript
type TopicInfo = {
  topic: string          // Topic 名称
  partitions: number     // 分区数
  replicas: number       // 副本因子
  isInternal: boolean    // 是否为内部 Topic
}

type PartitionInfo = {
  partitionId: number    // 分区 ID
  leader: number         // Leader Broker ID
  replicas: number[]     // 副本 Broker ID 列表
  isr: number[]          // ISR Broker ID 列表
}

type PartitionOffset = {
  partition: number       // 分区编号
  earliestOffset: string  // 最早 Offset
  latestOffset: string    // 最新 Offset
}
```

### 6.4 ConsumerGroup（消费者组）

```typescript
type ConsumerGroupInfo = {
  groupId: string          // 组 ID
  state: string            // 组状态
  members: number          // 成员数
  protocol: string         // 分配协议
}

type ConsumerGroupDetail = {
  groupId: string
  state: string
  protocol: string
  members: Array<{         // 成员列表
    clientId: string
    memberId: string
    host: string
  }>
  offsets: Array<{         // 分区偏移量
    topic: string
    partition: number
    currentOffset: string  // 当前消费进度
    logEndOffset: string   // Log 末尾 Offset
    lag: string            // 消费滞后
  }>
}
```

### 6.5 LogEntry（日志记录）

```typescript
interface LogEntry {
  id: string               // 唯一 ID
  level: 'debug' | 'info' | 'warn' | 'error' | 'fatal'
  timestamp: number        // 时间戳
  source: 'main' | 'renderer'  // 来源进程
  message: string          // 日志消息
  data?: string            // 附加数据（JSON 字符串）
  stack?: string           // 错误堆栈
  origin?: string          // 文件/模块来源
}
```

### 6.6 AppSettings（应用设置）

```typescript
interface AppSettings {
  maxMessages: number            // 消息最大保留数量（默认 500）
  autoRefreshInterval: number    // 自动刷新间隔秒数（默认 0=禁用）
  theme: 'light' | 'dark' | 'system'  // 主题（默认 light）
}
```

---

## 7. 用户交互流程

### 7.1 首次使用流程

```
启动应用
  → 进入"连接管理"页面（空列表）
  → 点击"新建连接"
  → 填写连接配置（名称、Brokers、SSL/SASL 可选）
  → 可选"测试连接"验证连通性
  → 保存连接
  → 点击"使用"激活连接
  → 侧边栏导航至其他功能页面
```

### 7.2 消息生产流程

```
进入"消息生产"页面
  → 选择 Topic（输入搜索或下拉选择）
  → 可选填写 Key / 分区 / Headers
  → 填写 Value
  → 可选"格式化 JSON"
  → 点击"发送"
  → 查看发送结果（Topic、分区、Offset、时间戳）
```

### 7.3 实时消费流程

```
进入"消息消费"页面
  → 选择 Topic
  → 可选指定分区
  → 选择起始位置（Earliest / Latest / 指定 Offset）
  → 点击"开始消费"
  → 消息实时推送到表格
  → 点击任意行查看消息详情
  → 点击"停止消费"结束
```

### 7.4 Topic 详情浏览流程

```
进入"Topic 管理"页面
  → 查看 Topic 列表
  → 搜索过滤目标 Topic
  → 点击"详情"
  → 查看分区信息、Offset 范围
  → 在"消息浏览"区域设置查询条件
  → 点击"查询"拉取消息
  → 点击消息行查看详情
```

### 7.5 消费者组检查流程

```
进入"消费者组"页面
  → 查看消费者组列表
  → 搜索过滤目标 Group
  → 点击"详情"
  → 在抽屉中查看成员列表和分区 Offset/Lag
  → 根据 Lag 颜色判断消费进度是否健康
```

### 7.6 连接切换流程（跨页面影响）

```
在任意页面切换顶部连接选择器
  → 所有正在运行的消费者自动停止
  → 各功能页面检测到连接变更后
  → Topic 列表、消费者组列表等数据自动刷新
  → 消费页面重置消费状态（因旧消费者已被停止）
```

### 7.7 异常流程

| 场景 | 表现 |
|------|------|
| 未选择连接时访问功能页面 | 页面顶部显示黄色警告 Alert，含快捷导航按钮 |
| API 调用失败 | 红色 Alert 展示错误信息，部分页面提供重试按钮 |
| 连接测试失败 | 弹窗展示具体错误信息 |
| 消费者连接断开 | 通过 consumer error 事件捕获并展示在 DevTools 日志中 |
| 应用意外关闭 | 窗口 close 事件中异步停止所有消费者和 Kafka 连接 |

---

## 8. UI 设计规范

### 8.1 布局结构

```
┌─────────────────────────────────────────────────┐
│  Header (48px)                                   │
│  [Kafka Client] [● 当前连接名] [连接选择器] [🪲][🔄] │
├──────────┬──────────────────────────────────────┤
│          │  Breadcrumb                           │
│  Sider   │  ──────────────────────────────────   │
│  (160px) │  Content (padding: 16px, overflow)    │
│          │                                        │
│  Menu    │  <Outlet /> 页面内容                   │
│          │                                        │
│          │                                        │
├──────────┴──────────────────────────────────────┤
│  DevTools Panel (可选，150~600px 可拖拽)         │
└─────────────────────────────────────────────────┘
```

### 8.2 布局参数

| 区域 | 参数 |
|------|------|
| Header | 高度 48px，背景色 `#001529` |
| Sider | 宽度 160px（折叠后 48px），浅色主题 |
| Content | padding 16px，背景色 `#f5f5f5` |
| 全局 | 字号 13px，圆角 4px |

### 8.3 主题系统

支持三种主题模式：

- **浅色模式**（默认）：`theme.defaultAlgorithm`
- **深色模式**：`theme.darkAlgorithm`
- **跟随系统**：通过 `window.matchMedia('(prefers-color-scheme: dark)')` 检测

主题通过 Ant Design `ConfigProvider` 全局注入，设置保存后即时生效。

### 8.4 组件使用规范

所有页面统一使用以下组件约定：

| 元素 | 使用组件 |
|------|---------|
| 页面容器 | `Card`（size="small"） |
| 数据表格 | `Table`（size="small"） |
| 状态标签 | `Tag`（根据语义着色） |
| 状态圆点 | `Badge` |
| 弹窗 | `Modal` |
| 详情面板 | `Drawer`（宽 600~720px） |
| 行内编辑表格 | 使用 `HeaderEditor` 封装 |
| 消息表格 | 使用 `MessageTable` 封装 |
| JSON 展示 | 使用 `JsonViewer` 封装 |
| 操作反馈 | `message` 全局 API（toast） |
| 空状态 | `Empty` 组件 |
| 加载中 | `Spin` 组件 |
| 表单 | `Form` + `Form.Item` |

### 8.5 中文本地化

- 全界面使用中文，包括按钮文本、提示信息、表单标签等
- Ant Design 使用 `zhCN` locale
- 时间戳使用 `toLocaleString('zh-CN')` 格式化

---

## 9. 技术架构概览

### 9.1 双层架构（Tauri）

```
┌─────────────────────────────────────────────┐
│           React 前端 (WebView)               │
│  (Pages / Components / Hooks / API Client)  │
│                     ↕ Tauri IPC             │
│              invoke() / emit() / listen()   │
├─────────────────────────────────────────────┤
│              Rust 后端进程                    │
│  ┌─────────────┐  ┌──────────────────────┐  │
│  │ Kafka 服务层  │  │ 日志系统 / 存储系统   │  │
│  │ connMgr     │  │ logService           │  │
│  │ topicSvc    │  │ connectionStore      │  │
│  │ producerSvc │  │ (JSON 文件)           │  │
│  │ consumerSvc │  │ security (DPAPI)     │  │
│  │ groupSvc    │  └──────────────────────┘  │
│  │ importExport│                            │
│  └──────┬──────┘                            │
│         ↕ rdkafka (librdkafka)              │
├─────────────────────────────────────────────┤
│              Kafka Cluster                   │
└─────────────────────────────────────────────┘
```

### 9.2 安全设计

- **进程隔离**：前端 WebView 无 Rust / 文件系统直接访问权限
- **Tauri IPC**：通过 `invoke()` / `emit()` 受控通信
- **密码加密**：使用 Windows DPAPI (`CryptProtectData` / `CryptUnprotectData`) 加密 SASL 密码
  - 密文以 `ENC1:<base64>` 格式存储在 JSON 文件中
  - 加密绑定到当前 Windows 用户账户
  - 仅内存中持有明文（rdkafka 需要）

### 9.3 IPC 通信通道

共 26 个 Tauri command，覆盖所有 Kafka 操作：

| 命名空间 | 命令数 | 说明 |
|---------|--------|------|
| connection_* | 6 | 连接 CRUD、测试、激活 |
| topic_* | 5 | 列表、详情、Offset、消息拉取、创建 |
| producer_* | 1 | 消息发送 |
| consumer_* | 3 | 启动/停止/全停 |
| group_* | 2 | 列表、详情（含 Offset/Lag） |
| log_* | 3 | 获取日志、清空、渲染进程日志转发 |
| export/import_* | 4 | 导出启动/取消、导入启动/取消 |
| settings_* | 2 | 获取设置、更新设置 |

### 9.4 数据持久化

| 数据 | 存储方式 | 位置 |
|------|---------|------|
| 连接配置 | JSON 文件（serde 序列化） | `%APPDATA%/kafka-client/data.json` |
| 应用设置 | JSON 文件（与连接配置同文件） | `%APPDATA%/kafka-client/data.json` |
| SASL 密码 | DPAPI 加密后存于 JSON 文件 | `ENC1:<base64>` 格式 |
| 窗口状态 | JSON 文件 | `%APPDATA%/kafka-client/` |
| 运行日志 | 内存环形缓冲区（5000 条上限） | 不持久化 |

---

## 10. 非功能特性

### 10.1 性能

| 指标 | 说明 |
|------|------|
| 安装包大小 | ~94MB（Windows NSIS 安装包） |
| 窗口最小尺寸 | 1024 x 680px |
| 最大消息保留条数 | 可配置 100~10000 条（默认 500） |
| Topic 列表分页 | 每页 20 条，支持切换 |
| Kafka 客户端复用 | Admin/Producer 按连接 ID 池化复用 |

### 10.2 安全性

- SASL 密码通过 Windows DPAPI 加密后存储（绑定用户账户）
- SASL 密码仅在内存和 IPC 传输中以明文存在
- 前端 WebView 无 Rust 或文件系统直接访问能力
- SSL/SASL 连接需要重新编译 rdkafka（默认未启用 `ssl`/`sasl` feature）

### 10.3 可靠性

| 场景 | 处理方式 |
|------|---------|
| 应用重启 | 自动从持久化存储恢复连接配置并重建 Kafka 客户端 |
| 窗口关闭 | 保存窗口位置/大小状态 |
| 应用退出 | 先停止所有消费者，再断开所有 Kafka 连接，最后关闭窗口 |
| API 调用失败 | 返回 `Err(String)` 由 Tauri 转为异常，或 `Ok(json!({"success":false,"error":...}))` |
| IPC 参数错误 | serde 反序列化失败返回错误 |

### 10.4 可维护性

- 全 TypeScript 类型安全
- 前后端类型契约单一来源：`frontend/src/types/kafka.ts`
- Rust 端各业务逻辑分散在各独立 Service 文件中
- Tauri command 注册集中在 `lib.rs` 的 `generate_handler![]`
- SASL 密码加解密集中在 `security/` 模块

---

## 11. 已知限制与后续规划

### 11.1 已知限制

| 限制 | 说明 |
|------|------|
| 仅支持 Windows | tauri.conf.json 当前仅配置了 NSIS Windows 目标 |
| SSL/SASL 需重新编译 | 默认 rdkafka 未启用 `ssl`/`sasl` feature，需安装 OpenSSL 后修改 Cargo.toml 重新构建 |
| 无 Topic 删除/修改 | 不支持删除 Topic、修改分区数或副本因子 |
| 无 Schema Registry | 无法查看 Avro/Protobuf 序列化的消息（二进制消息会因 `String::from_utf8_lossy` 损坏） |
| 无消息搜索/过滤 | 不支持按内容搜索或过滤消息 |
| 无多 Tab 支持 | 同一时刻只能消费一个 Topic |
| 无连接状态自动恢复 | 连接断开后不会自动重连 |
| 日志不持久化 | DevTools 面板日志在应用重启后丢失 |
| 消费者组 Offset/Lag 查询较慢 | 需 assign 全部分区查询 committed offset，大型集群可能耗时较长 |
| macOS/Linux 加密未实现 | 非 Windows 平台 security 模块为占位实现（密码明文存储） |

### 11.2 后续规划建议

| 优先级 | 功能 | 说明 |
|--------|------|------|
| P0 | Topic CRUD | 创建/删除/清空 Topic，修改分区数 |
| P1 | 跨平台支持 | 添加 macOS (.dmg) 和 Linux (.AppImage) 打包目标 |
| P1 | 连接状态监控 | 连接断开时自动重连或状态提示 |
| P2 | Schema Registry 集成 | 支持 Avro/Protobuf 消息的反序列化展示 |
| P2 | 多 Tab 消费 | 同时消费多个 Topic/分区 |
| P2 | 消息搜索 | 按内容搜索已消费消息 |
| P2 | 数据导出 | 导出消息为 JSON/CSV 格式 |
| P3 | 消息重新发送 | 选中已消费消息重新发送到 Topic |
| P3 | 性能监控 | 展示集群 Broker 指标、分区 Leader 分布 |
| P3 | 主题系统增强 | 自定义主题色，更多主题风格 |
