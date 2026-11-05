# dsh-dingtalk · 通过钉钉使用 DeepSeek Harness

一个零外部依赖的 [DeepSeek Harness](https://github.com/deepseek-ai/deepseek-harness) Host 插件：把 Harness 接到钉钉 **Stream 模式机器人**上，在群里 `@机器人` 或单聊里直接发消息，就能让 Harness 的 agent 处理并回复。

- **无需公网 IP / 内网穿透**：插件作为客户端，向钉钉网关发起一条 WebSocket 长连接。
- **每个钉钉会话对应一个 agent 会话**：按 `conversationId` 建/复用会话，支持多轮上下文，重启后也能接续同一段对话。
- **零第三方依赖**：WebSocket 用 Node 内建 `WebSocket`，HTTP 用内建 `fetch`，协议与官方 `dingtalk-stream` SDK（v2.1.x）一致。
- 同一会话内的消息**串行排队**处理，避免并发打断 agent 轮次。

## 目录

```
dsh-dingtalk/
├── package.json        # bundle 身份 + dsh.bundle.patch 声明 + 展示元数据
├── cordis.patch.yml    # 往插件树插入一行（密钥留空，见下文）
├── index.js            # 全部实现：钉钉 Stream 协议 + agent 会话桥接
├── locale/{zh,en}.json # 插件管理页标题/描述
└── icon.svg
```

## 前置条件

1. 一台能运行 DeepSeek Harness（Desktop 版）的机器，且 Harness 已登录可用的模型账号。
2. 一个钉钉企业内部应用，开通 **Stream 模式机器人**（见下）。

## 一、在钉钉侧创建 Stream 模式机器人

1. 打开 [钉钉开放平台](https://open-dev.dingtalk.com/)，用企业管理员账号登录。
2. 「应用开发 → 企业内部开发 → 创建应用」，填一个名称（如「Harness」）。
3. 记下应用的 **AppKey（即 `clientId`）** 与 **AppSecret（即 `clientSecret`）**。
4. 在应用里进入「应用能力 → 添加应用能力 → 机器人」，完善机器人信息，选择 **Stream 模式（机器人连接模式）**，然后发布。
5. 把机器人**加进某个群**（或与它发起单聊）。

## 二、安装插件

插件以 **bundle** 形式安装到当前 Harness profile（会把本仓库作为本地包 link 进去）：

- **方式 A（推荐，在 Harness 里）**：克隆本仓库后，在 Harness 的「插件」侧栏点「添加插件」，填入本仓库的**绝对路径**；或在对话里让 Harness 调用 `plugin_manager` 的 `install_bundle`，`target` 填绝对路径。
- **方式 B（CLI，非 desktop profile）**：`dsh plugin --profile <name> add /绝对路径/dsh-dingtalk-plugin`。

安装成功后，插件行 `dsh-dingtalk` 会以 `fiberPhase: active` 出现在插件列表里（此时密钥为空，插件处于「已安装但未连接」状态）。

## 三、配置密钥

> ⚠️ **不要在仓库的 `cordis.patch.yml` 里填真实密钥**（那是 bundle 的默认层，会被提交）。
> 真实密钥请写到 **profile 的覆盖层**，即 Harness 的 profile 目录 `~/.dsh/profiles/<profile>/cordis.patch.yml`。

在该文件里按 `id: dsh-dingtalk` 覆盖这一行，填入你的 AppKey / AppSecret（其余字段沿用默认即可）：

```yaml
- id: dsh-dingtalk
  config:
    clientId: '你的AppKey'
    clientSecret: '你的AppSecret'
    agentPreset: standard
    workspacePath: '/Users/you/dingtalk-workspace'   # 可选：agent 会话工作目录（绝对路径）
```

保存后 HMR 会串行重载；或停用再启用该插件使其重连。看到 `apply() started, enabled=true, clientId=SET` 即表示密钥已生效、正在连接钉钉。

## 四、使用

- **群聊**：`@机器人 帮我看看当前工作区`；
- **单聊**：直接发文本。

机器人收到消息后，会为该会话创建/复用 agent，处理完成后把回复（群聊中 @ 提问者）发回钉钉。

## 配置项

| 字段 | 默认 | 说明 |
| --- | --- | --- |
| `clientId` | `''` | 钉钉 AppKey；留空则不连接 |
| `clientSecret` | `''` | 钉钉 AppSecret；留空则不连接 |
| `agentPreset` | `standard` | 每个钉钉会话使用的 agent preset |
| `workspacePath` | cwd | agent 会话工作目录（绝对路径） |
| `provider` / `model` | 沿用默认 | 可选，覆盖默认模型 |
| `replyMention` | `true` | 群聊回复时 @ 提问者 |
| `maxReplyLength` | `20000` | 单条回复最大字符数（超出截断） |
| `conversations` | `[]` | 仅响应的会话 `conversationId` 白名单（空 = 全部） |
| `debug` | `false` | 打印连接/收发日志（排障时改 `true`） |

## 排障

- 插件把诊断日志写到 `~/.dsh/dsh-dingtalk-debug.log`（`apply()` 启动标记、连接/重连、会话恢复、agent 轮次错误链都会记录）。连不上或没回复时先看这里。
- 已安装包改代码后，Harness 会缓存模块，需**完全退出并重开 Harness** 才会加载新的 `index.js`（仅改 config 可由 HMR 生效）。
- 若某段对话的持久化会话损坏（日志会报 `SessionPersistenceCorruptionError`），删掉 `~/.dsh/sessions/` 下对应 `dingtalk-*` 目录与 `~/.dsh/storages/session_projcache/sessions/` 下同名 json 即可；插件会为下一条消息新建会话。

## 已知限制

- 只处理文本消息；图片、文件等暂不处理。
- agent 轮次中若发起 `ask_user_question` / 审批，需要回 Harness 界面回答；钉钉侧会提示去界面处理。
- 回复走钉钉 `sessionWebhook`（有有效期），单轮若超过有效期可能回复失败（会记录日志并自动重试连接）。
- 群聊里机器人只响应被 `@` 的消息。

## License

MIT
