/**
 * dsh-dingtalk — 把 DeepSeek Harness 接到钉钉（Stream 模式机器人）。
 *
 * 无需公网 IP：插件作为客户端向钉钉网关发起一条 WebSocket 长连接，
 * 接收机器人消息（单聊与群聊 @机器人），为每个钉钉会话维护一个 agent 会话，
 * 把回复通过 sessionWebhook 发回钉钉。
 *
 * 零外部依赖：WebSocket 用 Node 内建全局 WebSocket，HTTP 用内建 fetch。
 * 协议细节与官方 dingtalk-stream SDK（v2.1.x）保持一致：
 *   - POST /v1.0/gateway/connections/open 换取 endpoint + ticket
 *   - 连接 `${endpoint}?ticket=${ticket}`
 *   - CALLBACK topic `/v1.0/im/bot/messages/get` 的 data 即机器人消息 JSON 字符串
 *   - 回复：POST 到消息里的 sessionWebhook，头带 x-acs-dingtalk-access-token
 *
 * @module @local/dsh-dingtalk
 */

import { randomUUID } from 'node:crypto'
import { appendFileSync, mkdirSync } from 'node:fs'
import { isAbsolute, resolve } from 'node:path'

/** Loader 行名。 */
export const name = 'dsh-dingtalk'

/** 必需服务：agents 提供创建/复用 agent 会话的工厂。 */
export const inject = ['agents']

const GATEWAY_URL = 'https://api.dingtalk.com/v1.0/gateway/connections/open'
const GET_TOKEN_URL = 'https://oapi.dingtalk.com/gettoken'
const TOPIC_ROBOT = '/v1.0/im/bot/messages/get'
// 调试日志写到 $DSH_HOME（app 会话也写在这里，确保宿主机可读；/tmp 可能被容器隔离，Documents 可能有 TCC 限制）。
const DEBUG_LOG = `${process.env.DSH_HOME ?? `${process.env.HOME}/.dsh`}/dsh-dingtalk-debug.log`

const DEFAULT_MAX_REPLY = 20000

/** 把错误连同 cause 链展开成可读文本（用于真机调试）。 */
function describeError(error) {
  const seen = new Set()
  const lines = []
  let cur = error
  let depth = 0
  while (cur != null && depth < 8) {
    if (typeof cur !== 'object') {
      lines.push(`${'  '.repeat(depth)}${String(cur).slice(0, 600)}`)
      break
    }
    if (seen.has(cur)) { lines.push(`${'  '.repeat(depth)}(circular)`); break }
    seen.add(cur)
    const name = cur.name ?? 'Error'
    const code = cur.code ? `[${cur.code}]` : ''
    const msg = cur.message ?? JSON.stringify(cur)
    lines.push(`${'  '.repeat(depth)}${name}${code}: ${String(msg).slice(0, 600)}`)
    cur = cur.cause
    depth += 1
  }
  return lines.join('\n')
}

function debugLog(message) {
  try {
    appendFileSync(DEBUG_LOG, `[${new Date().toISOString()}] ${message}\n`, 'utf8')
  } catch { /* 忽略写日志失败 */ }
}

/** 校验一行 config。DSH 的 patch 是「整体替换」而非深合并，未知 key 直接报错。 */
function resolveConfig(raw) {
  const value = raw ?? {}
  if (typeof value !== 'object' || Array.isArray(value)) {
    throw new TypeError(`${name}: config must be an object`)
  }
  const known = new Set([
    'clientId', 'clientSecret', 'agentPreset', 'workspacePath',
    'provider', 'model', 'replyMention', 'maxReplyLength', 'conversations', 'debug',
  ])
  for (const key of Object.keys(value)) {
    if (!known.has(key)) throw new TypeError(`${name}: unknown config key ${JSON.stringify(key)}`)
  }

  const str = (v, fallback) => (typeof v === 'string' ? v : fallback)
  const clientId = str(value.clientId, '')
  const clientSecret = str(value.clientSecret, '')
  const agentPreset = str(value.agentPreset, 'standard')
  const provider = str(value.provider, '')
  const model = str(value.model, '')

  let workspacePath = typeof value.workspacePath === 'string' && value.workspacePath.trim() !== ''
    ? value.workspacePath.trim()
    : process.cwd()
  if (!isAbsolute(workspacePath)) throw new TypeError(`${name}: config.workspacePath must be an absolute path`)
  workspacePath = resolve(workspacePath)

  const replyMention = value.replyMention ?? true
  if (typeof replyMention !== 'boolean') throw new TypeError(`${name}: config.replyMention must be a boolean`)

  const maxReplyLength = value.maxReplyLength ?? DEFAULT_MAX_REPLY
  if (!Number.isInteger(maxReplyLength) || maxReplyLength < 1) {
    throw new TypeError(`${name}: config.maxReplyLength must be a positive integer`)
  }

  const conversations = value.conversations ?? []
  if (!Array.isArray(conversations) || conversations.some((id) => typeof id !== 'string')) {
    throw new TypeError(`${name}: config.conversations must be an array of strings`)
  }

  const debug = value.debug ?? false
  if (typeof debug !== 'boolean') throw new TypeError(`${name}: config.debug must be a boolean`)

  return Object.freeze({
    clientId, clientSecret, agentPreset, workspacePath,
    provider, model, replyMention, maxReplyLength,
    conversations: conversations.filter((id) => id.trim() !== ''),
    debug,
    // 只有 AppKey/AppSecret 都非空时才连接钉钉
    enabled: clientId.trim() !== '' && clientSecret.trim() !== '',
  })
}

/** 去掉群聊里开头的 @机器人 提及，得到真实文本。 */
function stripMention(raw) {
  return String(raw ?? '').replace(/^\s*(?:@[^\s]+\s*)+/, '').trim()
}

function truncate(text, max) {
  if (text.length <= max) return text
  return `${text.slice(0, max)}\n…（回复过长已截断）`
}

/** 从 followup 之后新增的会话事件里收集助手文本回复。 */
function collectAssistantText(session, beforeSeq) {
  const parts = []
  let blocked = false
  let turnError = null
  for (const event of session.snapshotEvents(beforeSeq)) {
    if (event.type === 'turn/end') {
      const reason = event.data?.reason
      if (reason?.kind === 'blocked') blocked = true
      if (reason?.kind === 'error' && reason?.error) turnError = reason.error
    }
    if (event.type !== 'assistant/message') continue
    const blocks = event.data?.message?.content ?? []
    for (const block of blocks) {
      if (block.type === 'text' && typeof block.text === 'string' && block.text.trim() !== '') {
        parts.push(block.text)
      }
    }
  }
  const text = parts.join('\n\n').trim()
  if (text !== '') return text
  if (turnError) {
    return `⚠️ 模型调用失败：${turnError.code ? `[${turnError.code}] ` : ''}${turnError.message ?? String(turnError)}`
  }
  return blocked
    ? '（智能体正在等待你的回答或审批，请在 Harness 界面处理。）'
    : '（本次没有生成文本回复。）'
}

export function apply(ctx, rawConfig) {
  const config = resolveConfig(rawConfig)
  debugLog(`apply() started, enabled=${config.enabled}, clientId=${config.clientId ? 'SET' : 'EMPTY'}, pid=${process.pid}`)

  const logger = ctx.logger ?? console
  const log = config.debug
    ? (message) => logger.info(`[${name}] ${message}`)
    : () => {}
  const warn = (message) => logger.warn(`[${name}] ${message}`)

  // conversationId -> { handle, running, queue }
  const sessions = new Map()
  // 首次创建会话的去重：并发首条消息只创建一次
  const inflight = new Map()
  let ws = null
  let reconnectTimer = null
  let stopped = false

  // 连接/收发状态，供 /api/dsh-dingtalk/status 查询（真机调试用）。
  const state = {
    enabled: config.enabled,
    connected: false,
    registered: false,
    lastConnectedAt: null,
    lastError: null,
    lastTurnError: null,
    received: 0,
    replied: 0,
  }

  // 捕获本插件创建的 agent 的轮次错误（含 cause 链），写进文件便于排查。
  ctx.on('agent/error', (payload) => {
    const agentId = String(payload?.agent?.id ?? '')
    if (!agentId.startsWith('dingtalk-')) return
    const detail = describeError(payload?.error)
    state.lastTurnError = detail
    debugLog(`agent/error turn=${payload?.turn} step=${payload?.step} agent=${agentId}\n${detail}`)
  })

  // ── 钉钉 OpenAPI 辅助 ────────────────────────────────────────────────

  async function getAccessToken() {
    const url = `${GET_TOKEN_URL}?appkey=${encodeURIComponent(config.clientId)}&appsecret=${encodeURIComponent(config.clientSecret)}`
    const res = await fetch(url)
    const data = await res.json()
    if (!data.access_token) throw new Error(`gettoken failed: ${JSON.stringify(data)}`)
    return data.access_token
  }

  async function getEndpoint() {
    const res = await fetch(GATEWAY_URL, {
      method: 'POST',
      headers: { 'content-type': 'application/json', accept: 'application/json' },
      body: JSON.stringify({
        clientId: config.clientId,
        clientSecret: config.clientSecret,
        ua: 'dsh-dingtalk',
        subscriptions: [
          { type: 'EVENT', topic: '*' },
          { type: 'CALLBACK', topic: TOPIC_ROBOT },
        ],
      }),
    })
    const data = await res.json()
    if (!data.endpoint || !data.ticket) {
      throw new Error(`gateway/connections/open failed: ${JSON.stringify(data)}`)
    }
    return data
  }

  async function replyTo(robotMessage, text) {
    const webhook = robotMessage?.sessionWebhook
    if (!webhook) return
    const body = {
      msgtype: 'text',
      text: { content: truncate(text, config.maxReplyLength) },
    }
    if (config.replyMention && robotMessage.senderStaffId) {
      body.at = { atUserIds: [robotMessage.senderStaffId], isAtAll: false }
    }
    const token = await getAccessToken()
    const res = await fetch(webhook, {
      method: 'POST',
      headers: {
        'content-type': 'application/json',
        'x-acs-dingtalk-access-token': token,
      },
      body: JSON.stringify(body),
    })
    log(`reply ${robotMessage.conversationId}: HTTP ${res.status}`)
    state.replied += 1
  }

  // ── agent 会话桥接 ───────────────────────────────────────────────────

  function stableSessionId(conversationId) {
    return `dingtalk-${String(conversationId).replace(/[^a-zA-Z0-9_-]/g, '_')}`
  }

  async function openSession(conversationId) {
    const sessionId = stableSessionId(conversationId)

    // 可选：解析并挂载 agent preset（给 agent 装上工具）。
    const presetService = ctx.get('agentPresets')
    const preset = presetService ? await presetService.resolve(config.agentPreset) : undefined

    // 默认模型：优先 config 覆盖，否则沿用 Harness 当前默认模型。
    const agentOptions = {}
    if (config.provider !== '' && config.model !== '') {
      agentOptions.provider = config.provider
      agentOptions.model = config.model
    } else {
      const defaultModel = ctx.get('agentDefaultModel')
      const selection = defaultModel?.currentSelection?.()
      if (selection?.provider && selection?.model) {
        agentOptions.provider = selection.provider
        agentOptions.model = selection.model
        if (selection.reasoningEffort) agentOptions.reasoningEffort = selection.reasoningEffort
      }
    }

    const setup = async (agentCtx) => {
      if (presetService && preset) {
        await presetService.mount(agentCtx, preset.id)
      }
    }

    // 1) 本次运行中已创建且仍在实时注册表 → 直接复用（避免重复 create）。
    const live = ctx.agents.get(sessionId)
    if (live) {
      log(`reuse live agent session ${sessionId} for conversation ${conversationId}`)
      return { agent: live, dispose: async () => {} }
    }

    // 2) 先尝试恢复已持久化的会话：插件重载/Harness 重启后仍接上同一对话。
    try {
      const handle = await ctx.agents.resume({ resumeSessionId: sessionId, agentOptions, setup })
      log(`resumed agent session ${sessionId} for conversation ${conversationId}`)
      return handle
    } catch (resumeError) {
      debugLog(`resume ${sessionId} failed (will create): ${describeError(resumeError)}`)
    }

    // 3) 新建。若同 id 已被损坏/占用的残留会话占据，退避到带随机后缀的新 id，避免「already exists」卡死。
    const createHandle = async (id) => ctx.agents.create({
      sessionId: id,
      meta: {
        cwd: config.workspacePath,
        ...(preset?.id ? { agentPreset: preset.id } : {}),
      },
      agentOptions,
      setup,
    })

    try {
      const handle = await createHandle(sessionId)
      log(`created agent session ${sessionId} for conversation ${conversationId}`)
      return handle
    } catch (createError) {
      const freshId = `${sessionId}-${randomUUID().slice(0, 8)}`
      debugLog(`create ${sessionId} failed, retry fresh id ${freshId}: ${describeError(createError)}`)
      const handle = await createHandle(freshId)
      log(`created agent session ${freshId} (fallback) for conversation ${conversationId}`)
      return handle
    }
  }

  async function runTurn(entry, text, robotMessage) {
    const { handle } = entry
    const agent = handle.agent
    const beforeSeq = agent.session.seq
    try {
      // 构造带 id 的完整用户消息（等价于 dsh-llm 的 createUserMessage，但避免跨包 import 的解析风险）。
      agent.followup({
        role: 'user',
        id: randomUUID(),
        content: [{ type: 'text', text }],
        source: { kind: 'user' },
      })
    } catch (error) {
      warn(`followup failed for ${robotMessage.conversationId}: ${error?.message ?? error}`)
      await replyTo(robotMessage, `⚠️ 消息未能提交：${error?.message ?? error}`)
      return
    }

    await agent.whenIdle()
    const replyText = collectAssistantText(agent.session, beforeSeq)
    try {
      await replyTo(robotMessage, replyText)
    } catch (error) {
      warn(`reply failed for ${robotMessage.conversationId}: ${error?.message ?? error}`)
    }
  }

  /** 每个会话串行处理消息：排队，避免并发轮次互相打断。 */
  async function enqueue(entry, text, robotMessage) {
    entry.queue.push({ text, robotMessage })
    if (entry.running) return
    entry.running = true
    try {
      while (entry.queue.length > 0 && !stopped) {
        const job = entry.queue.shift()
        await runTurn(entry, job.text, job.robotMessage)
      }
    } catch (error) {
      warn(`session loop error for ${robotMessage.conversationId}: ${error?.message ?? error}`)
    } finally {
      entry.running = false
    }
  }

  async function getOrCreateEntry(conversationId) {
    const existing = sessions.get(conversationId)
    if (existing) return existing
    let promise = inflight.get(conversationId)
    if (!promise) {
      promise = openSession(conversationId)
        .then((handle) => {
          const entry = { handle, running: false, queue: [] }
          sessions.set(conversationId, entry)
          return entry
        })
        .finally(() => inflight.delete(conversationId))
      inflight.set(conversationId, promise)
    }
    return promise
  }

  async function handleRobotMessage(robotMessage) {
    const conversationId = robotMessage?.conversationId
    if (!conversationId) return
    if (config.conversations.length > 0 && !config.conversations.includes(conversationId)) return

    if (robotMessage.msgtype !== 'text') {
      log(`ignore non-text message type=${robotMessage.msgtype}`)
      return
    }
    const text = stripMention(robotMessage.text?.content)
    if (text === '') return
    state.received += 1

    try {
      const entry = await getOrCreateEntry(conversationId)
      await enqueue(entry, text, robotMessage)
    } catch (error) {
      warn(`handle message failed for ${conversationId}: ${error?.message ?? error}`)
      try {
        await replyTo(robotMessage, `⚠️ 处理消息时出错：${error?.message ?? error}`)
      } catch { /* 忽略回复失败 */ }
    }
  }

  // ── Stream 连接 ──────────────────────────────────────────────────────

  function ack(messageId) {
    if (!ws || ws.readyState !== 1) return
    ws.send(JSON.stringify({
      code: 200,
      headers: { contentType: 'application/json', messageId },
      message: 'OK',
      data: JSON.stringify({ response: {} }),
    }))
  }

  function onFrame(rawText) {
    let frame
    try {
      frame = JSON.parse(rawText)
    } catch {
      return
    }
    const topic = frame.headers?.topic

    switch (frame.type) {
      case 'SYSTEM': {
        if (topic === 'REGISTERED') {
          state.registered = true
          log('registered with DingTalk gateway')
        } else if (topic === 'CONNECTED') {
          log('gateway CONNECTED')
        } else if (topic === 'ping') {
          ws?.send(JSON.stringify({ code: 200, headers: frame.headers, message: 'OK', data: frame.data }))
        }
        break
      }
      case 'EVENT': {
        // 业务事件：立即 ACK，避免服务端 60s 重试。
        ws?.send(JSON.stringify({
          code: 200,
          headers: { contentType: 'application/json', messageId: frame.headers?.messageId },
          message: 'OK',
          data: JSON.stringify({ status: 'SUCCESS' }),
        }))
        break
      }
      case 'CALLBACK': {
        if (topic !== TOPIC_ROBOT) return
        ack(frame.headers?.messageId)
        let robotMessage
        try {
          robotMessage = JSON.parse(frame.data)
        } catch (error) {
          warn(`bad robot message JSON: ${error?.message ?? error}`)
          return
        }
        log(`robot message from ${robotMessage?.senderNick ?? '?'} in ${robotMessage?.conversationId}`)
        void handleRobotMessage(robotMessage).catch((error) => warn(`unhandled: ${error?.message ?? error}`))
        break
      }
      default:
        break
    }
  }

  function scheduleReconnect() {
    if (stopped) return
    clearTimeout(reconnectTimer)
    reconnectTimer = setTimeout(() => { void connect() }, 5000)
  }

  async function connect() {
    if (stopped || !config.enabled) return
    try {
      const { endpoint, ticket } = await getEndpoint()
      ws = new WebSocket(`${endpoint}?ticket=${encodeURIComponent(ticket)}`)
      ws.onopen = () => {
        state.connected = true
        state.registered = false
        state.lastConnectedAt = Date.now()
        log('connected to DingTalk gateway')
      }
      ws.onmessage = (event) => onFrame(String(event.data))
      ws.onclose = () => {
        state.connected = false
        state.registered = false
        log('connection closed')
        scheduleReconnect()
      }
      ws.onerror = (event) => {
        const msg = event?.message ?? 'websocket error'
        state.lastError = msg
        warn(`websocket error: ${msg}`)
      }
    } catch (error) {
      state.lastError = error?.message ?? String(error)
      warn(`connect failed: ${error?.message ?? error}`)
      scheduleReconnect()
    }
  }

  function start() {
    mkdirSync(config.workspacePath, { recursive: true })
    if (!config.enabled) {
      warn('未配置 clientId/clientSecret，插件保持安装但不会连接钉钉。请在 cordis.patch.yml 中填入后重载。')
      return
    }
    void connect()
  }

  // 全部生命周期注册进 effect：插件禁用/热重载时自动回收。
  ctx.effect(() => {
    start()
    return () => {
      stopped = true
      clearTimeout(reconnectTimer)
      try { ws?.close() } catch { /* 忽略 */ }
      // 会话由 agents 工厂随本 fiber 一并回收；这里只清空引用并尽力关闭。
      const pending = [...sessions.values()]
      sessions.clear()
      for (const entry of pending) {
        entry.handle?.dispose?.().catch(() => {})
      }
    }
  })

  // 可选：暴露一个只读状态端点，便于真机调试时从浏览器/curl 查询连接状态。
  const webServer = ctx.get('webServer')
  if (webServer) {
    ctx.effect(() => webServer.register({
      kind: 'exact',
      path: '/api/dsh-dingtalk/status',
      handler: (_req, res) => {
        res.statusCode = 200
        res.setHeader('content-type', 'application/json; charset=utf-8')
        res.setHeader('cache-control', 'no-store')
        res.end(JSON.stringify({
          ...state,
          conversations: sessions.size,
          workspacePath: config.workspacePath,
          agentPreset: config.agentPreset,
        }))
      },
    }))
  }
}
