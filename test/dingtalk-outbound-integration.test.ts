import { fileURLToPath } from 'node:url'
import type { EventEnvelope, HarnessMeta } from '@agnes/protocol'
import { describe, expect, it, vi } from 'vitest'
import { FakeDingtalkGateway } from '../src/adapters/dingtalk/gateway-fake.js'
import { createDingtalkAdapter } from '../src/adapters/dingtalk/index.js'
import { createRealGateway } from '../src/adapters/dingtalk/gateway-real.js'
import type { RunnerConfig } from '@agnes/channels'
import { createRunner } from '@agnes/channels'
import { createFakeClient } from '@agnes/channels/testkit'

const manifestPath = fileURLToPath(new URL('../src/adapters/dingtalk/channel.json', import.meta.url))
const config: RunnerConfig = {
  channel: 'dingtalk',
  connect: { kind: 'unix', path: '/tmp/agnes.sock' },
  tenant: 'tenant',
  agent: 'agent',
  credentialsFile: '/dev/null',
  allowFrom: [],
  requireMention: true,
  ackReaction: 'off',
  workspace: '/workspace',
  outbound: { costLine: true },
  directory: { sync: false },
  healthz: { enabled: false, port: 9877 },
}

const meta: HarnessMeta = {
  promptTurnId: 'turn-1',
  eventSequence: 2,
  generation: 1,
  lane: 'main',
  phase: 'event',
}

function event(seq: number): EventEnvelope & { _meta: HarnessMeta } {
  return {
    seq,
    ts: '2026-09-12T00:00:00.000Z',
    id: `event-${seq}`,
    type: 'assistant/message',
    data: {},
    actor: { id: 'assistant', org: 'local', role: 'agent', deptPath: [], attrs: {} },
    origin: 'model',
    trust: 'untrusted',
    _meta: { ...meta, eventSequence: seq },
  } as EventEnvelope & { _meta: HarnessMeta }
}

describe('DingTalk runner outbound integration', () => {
  it('replies to a bound DM using only app credentials and the robot identity from Stream', async () => {
    let message: ((input: { headers: { messageId: string }; data: string }) => Promise<void>) | undefined
    const requests: { url: string; body: Record<string, unknown> }[] = []
    const gateway = createRealGateway({
      fetchImpl: vi.fn(async (input, init) => {
        const url = String(input)
        requests.push({ url, body: JSON.parse(String(init?.body)) })
        if (url.endsWith('/oauth2/accessToken')) return Response.json({ accessToken: 'synthetic-token', expireIn: 7200 })
        if (url.endsWith('/robot/oToMessages/batchSend')) return Response.json({ processQueryKey: 'reply-1' })
        throw new Error('Unexpected API request')
      }) as typeof fetch,
      streamFactory: () => ({
        registerCallbackListener(topic, callback) { if (topic.endsWith('/messages/get')) message = callback },
        async connect() {}, async disconnect() {},
      }),
    })
    const adapter = await createDingtalkAdapter(manifestPath, { gateway })
    const client = createFakeClient()
    const sessionId = 'existing-web-session'
    const key = 'agnes:tenant:agent:dingtalk:dm:conversation-1'
    await client.session.new({ cwd: config.workspace, sessionKey: sessionId })
    client.setTimeline(sessionId, { sessionId, upto: 5, generation: 1, opState: null, turns: [], nodes: [{ kind: 'assistant', id: 'old', seq: 2, text: 'private history' }] })
    const runner = createRunner({
      adapter, client, cfg: config, secrets: { clientId: 'app-key', clientSecret: 'app-secret' },
      log: { info() {}, warn() {}, error() {} },
      bindings: { resolve: (route) => route === key ? { sessionId, afterSeq: 5 } : undefined, observe() {} },
    })
    try {
      await runner.start()
      await runner.activate(key, { chatId: 'conversation-1' })
      await message?.({ headers: { messageId: 'incoming-1' }, data: JSON.stringify({
        msgId: 'incoming-1', conversationId: 'conversation-1', conversationType: '1',
        senderStaffId: 'staff-1', robotCode: 'robot-from-stream', msgtype: 'text',
        text: { content: '你好' }, createAt: Date.now(),
      }) })
      await vi.waitFor(() => expect(client.calls.some(call => call.method === 'followUp')).toBe(true))
      client.setTimeline(sessionId, { sessionId, upto: 6, generation: 1, opState: null, turns: [], nodes: [
        { kind: 'assistant', id: 'old', seq: 2, text: 'private history' },
        { kind: 'assistant', id: 'reply', seq: 6, text: '你好，我是 AGH。', streaming: false },
      ] })
      client.pushEvent(sessionId, event(6))
      await vi.waitFor(() => expect(requests.filter(request => request.url.endsWith('/robot/oToMessages/batchSend'))).toHaveLength(1))
      const sent = requests.find(request => request.url.endsWith('/robot/oToMessages/batchSend'))!.body
      expect(sent).toMatchObject({ robotCode: 'robot-from-stream', userIds: ['staff-1'], msgKey: 'sampleMarkdown' })
      expect(JSON.parse(String(sent.msgParam))).toEqual({ title: 'Agnes Harness', text: '你好，我是 AGH。' })
      expect(requests.some(request => request.url.includes('/card/'))).toBe(false)
      expect(client.calls.filter(call => call.method === 'session.new')).toHaveLength(1)
    } finally { await runner.stop() }
  })
  it.each([undefined, 'test-template'])('catches up after reconnect without resending on gap recovery (template=%s)', async (cardTemplateId) => {
    const gateway = new FakeDingtalkGateway()
    const originalStart = gateway.start.bind(gateway)
    let starts = 0
    let releaseReconnect!: () => void
    const reconnectGate = new Promise<void>((resolve) => {
      releaseReconnect = resolve
    })
    gateway.start = vi.fn(async (handlers, signal) => {
      starts++
      if (starts === 2) await reconnectGate
      return originalStart(handlers, signal)
    })
    const adapter = await createDingtalkAdapter(manifestPath, {
      gateway,
      cardTemplateId,
      backoff: { baseMs: 1, maxMs: 2 },
      rate: { perChatPerMin: 6_000 },
    })
    const client = createFakeClient()
    const runner = createRunner({
      adapter,
      cfg: config,
      secrets: { clientId: 'app-key', clientSecret: 'app-secret' },
      client,
      log: { info() {}, warn() {}, error() {} },
    })
    const key = 'agnes:tenant:agent:dingtalk:group:conversation-1'
    await runner.start()
    gateway.emitMessage({
      msgId: 'message-1',
      conversationId: 'conversation-1',
      conversationType: '2',
      senderStaffId: 'staff-1',
      msgtype: 'text',
      text: { content: '@bot hello' },
      isInAtList: true,
      createAt: 1_700_000_000_000,
      robotCode: 'robot-1',
    })
    await vi.waitFor(() => expect(client.calls.some((call) => call.method === 'followUp')).toBe(true))

    gateway.emitDisconnect(new Error('offline'))
    await vi.waitFor(() => expect(starts).toBe(2))
    client.setTimeline(key, {
      sessionId: key,
      upto: 2,
      generation: 1,
      opState: null,
      turns: [],
      nodes: [{ kind: 'assistant', id: 'assistant-1', seq: 2, text: 'done' }],
    })
    client.pushEvent(key, event(2))
    await new Promise((resolve) => setTimeout(resolve, 20))
    expect(gateway.sent).toHaveLength(0)

    releaseReconnect()
    await vi.waitFor(() => expect(gateway.sent).toHaveLength(1))
    expect(gateway.sent[0]).toMatchObject({ kind: cardTemplateId ? 'card' : 'markdown', target: { conversationId: 'conversation-1' } })

    const projections = client.calls.filter((call) => call.method === 'projectUI').length
    client.emit('gap', { sessionId: key, earliestSeq: 1 })
    await vi.waitFor(() =>
      expect(client.calls.filter((call) => call.method === 'projectUI').length).toBeGreaterThan(projections),
    )
    expect(gateway.sent).toHaveLength(1)
    client.setTimeline(key, {
      sessionId: key, upto: 3, generation: 1, opState: null, turns: [],
      nodes: [{ kind: 'assistant', id: 'assistant-1', seq: 3, text: 'done, revised' }],
    })
    client.pushEvent(key, event(3))
    await vi.waitFor(() => expect(gateway.sent).toHaveLength(2))
    expect(gateway.sent[1]?.kind).toBe(cardTemplateId ? 'cardUpdate' : 'markdown')
    await runner.stop()
  })
})
