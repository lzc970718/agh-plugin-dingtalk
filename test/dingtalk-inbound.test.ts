import { ChannelError } from '../src/errors.js'
import { fileURLToPath } from 'node:url'
import { describe, expect, it, vi } from 'vitest'
import type { ChannelEvent, MessageEvent } from '../src/adapter.js'
import {
  attachmentFailureText,
  attachmentsOf,
  MAX_ATTACHMENT_BYTES,
  toEvent,
} from '../src/adapters/dingtalk/events.js'
import type { RawRobotMessage } from '../src/adapters/dingtalk/gateway.js'
import { FakeDingtalkGateway } from '../src/adapters/dingtalk/gateway-fake.js'
import { createDingtalkAdapter } from '../src/adapters/dingtalk/index.js'

const manifestPath = fileURLToPath(new URL('../src/adapters/dingtalk/channel.json', import.meta.url))

type TestRaw = RawRobotMessage & {
  senderId?: string
  content?: RawRobotMessage['content']
}

function raw(override: Partial<TestRaw> = {}): TestRaw {
  return {
    msgId: 'message-1',
    conversationId: 'conversation-1',
    conversationType: '2',
    senderStaffId: 'staff-1',
    senderNick: '李四',
    senderCorpId: 'corp-secret',
    senderId: 'union-1',
    msgtype: 'text',
    text: { content: '@助理 统计本月销售' },
    isInAtList: true,
    createAt: 1_700_000_000_000,
    robotCode: 'robot-1',
    ...override,
  }
}

describe('DingTalk inbound event mapping', () => {
  it('maps identity, group text and stable delivery identifiers without widening raw credentials', () => {
    const event = toEvent(raw(), 'bot-1', [])

    expect(event).toMatchObject({
      kind: 'message',
      eventId: 'message-1',
      messageId: 'message-1',
      accountId: 'robot-1',
      at: '2023-11-14T22:13:20.000Z',
      chat: { id: 'conversation-1', type: 'group' },
      text: '@助理 统计本月销售',
      mentions: { bot: true, replyToBot: false, quoteBot: false },
      sender: {
        userId: 'staff-1',
        unionId: 'union-1',
        displayName: '李四',
        raw: {
          staffId: 'staff-1',
          unionId: 'union-1',
          conversationId: 'conversation-1',
          conversationType: '2',
          senderNick: '李四',
          senderCorpId: 'corp-secret',
        },
      },
    })
  })

  it('maps direct messages, rich text and both documented mention signals', () => {
    expect(toEvent(raw({ conversationType: '1', isInAtList: false }), 'bot-1', []).chat.type).toBe('dm')
    expect(
      toEvent(raw({ isInAtList: false, atUsers: [{ dingtalkId: 'bot-1' }] }), 'bot-1', []).mentions.bot,
    ).toBe(true)
    expect(
      toEvent(raw({ isInAtList: false, atUsers: [{ dingtalkId: 'other' }] }), 'bot-1', []).mentions.bot,
    ).toBe(false)
    expect(
      toEvent(
        raw({
          msgtype: 'richText',
          content: { richText: [{ text: '第一段' }, {}, { text: '第二段' }] },
        }),
        'bot-1',
        [],
      ).text,
    ).toBe('第一段第二段')
  })

  it('fails closed for values outside the raw message discriminant unions', () => {
    expect(() =>
      toEvent(raw({ conversationType: '3' as RawRobotMessage['conversationType'] }), 'bot-1', []),
    ).toThrow(/conversationType/)
    expect(() => toEvent(raw({ msgtype: 'unsupported' as RawRobotMessage['msgtype'] }), 'bot-1', [])).toThrow(
      /msgtype/,
    )
  })
})

describe('DingTalk inbound attachments', () => {
  it('downloads supported media with the exact cap and preserves bounded bytes', async () => {
    const download = vi.fn(async () => ({ bytes: new Uint8Array([1, 2, 3]), mime: 'image/png' }))

    await expect(
      attachmentsOf(raw({ msgtype: 'picture', content: { downloadCode: 'small' } }), download),
    ).resolves.toEqual([
      {
        name: 'message-1.png',
        mime: 'image/png',
        bytes: new Uint8Array([1, 2, 3]),
        sizeBytes: 3,
      },
    ])
    expect(download).toHaveBeenCalledWith('small', MAX_ATTACHMENT_BYTES)
  })

  it('refuses over-limit references rather than pretending that a URL is a saved attachment', async () => {
    const gateway = new FakeDingtalkGateway()
    gateway.downloads.set('large', {
      bytes: new Uint8Array(MAX_ATTACHMENT_BYTES + 1),
      mime: 'application/octet-stream',
    })
    gateway.downloads.set('audio', { url: 'https://download.example/audio' })

    await expect(
      attachmentsOf(
        raw({ msgtype: 'file', content: { downloadCode: 'large', fileName: 'report.zip' } }),
        (code, maxBytes) => gateway.download(code, maxBytes),
      ),
    ).rejects.toThrow(/size limit/)
    await expect(
      attachmentsOf(raw({ msgtype: 'audio', content: { downloadCode: 'audio' } }), (code, maxBytes) =>
        gateway.download(code, maxBytes),
      ),
    ).rejects.toThrow(/size limit/)
  })

  it('rejects unsafe URLs and a gateway that violates the byte limit', async () => {
    await expect(
      attachmentsOf(raw({ msgtype: 'file', content: { downloadCode: 'bad' } }), async () => ({
        url: 'file:///etc/passwd',
      })),
    ).rejects.toThrow(/HTTPS/)
    await expect(
      attachmentsOf(raw({ msgtype: 'file', content: { downloadCode: 'bad' } }), async () => ({
        bytes: new Uint8Array(MAX_ATTACHMENT_BYTES + 1),
        mime: 'application/octet-stream',
      })),
    ).rejects.toThrow(/size limit/)
  })

  it('does not call the gateway for text or media without a download code', async () => {
    const download = vi.fn()
    await expect(attachmentsOf(raw(), download)).resolves.toEqual([])
    await expect(attachmentsOf(raw({ msgtype: 'picture', content: {} }), download)).resolves.toEqual([])
    expect(download).not.toHaveBeenCalled()
  })
})

describe('DingtalkAdapter.toSession and credentialOf', () => {
  it('builds a group session, strips a real bot mention and applies the manifest credential whitelist', async () => {
    const adapter = await createDingtalkAdapter(manifestPath, { gateway: new FakeDingtalkGateway() })
    const intent = adapter.toSession(toEvent(raw(), 'bot-1', []), {
      tenant: 'xinwei',
      agent: 'sales',
    })

    expect(intent).toMatchObject({
      sessionKey: 'agnes:xinwei:sales:dingtalk:group:conversation-1',
      eventId: 'message-1',
      messageId: 'message-1',
      kind: 'message',
      content: [{ type: 'text', text: '统计本月销售' }],
      credential: {
        kind: 'channel',
        channel: 'dingtalk',
        accountId: 'robot-1',
        userId: 'staff-1',
        unionId: 'union-1',
        chatId: 'conversation-1',
        chatType: 'group',
        displayName: '李四',
      },
    })
    expect(Object.keys(intent?.credential.raw ?? {}).sort()).toEqual([
      'conversationId',
      'conversationType',
      'senderNick',
      'staffId',
      'unionId',
    ])
    expect(JSON.stringify(intent?.credential)).not.toContain('corp-secret')
  })

  it('keeps thread identity in the session key and does not strip an unverified @ prefix', async () => {
    const adapter = await createDingtalkAdapter(manifestPath, { gateway: new FakeDingtalkGateway() })
    const base = toEvent(raw({ isInAtList: false, atUsers: [] }), 'bot-1', [])
    const thread: MessageEvent = {
      ...base,
      chat: { id: 'conversation-1', type: 'thread', threadId: 'thread-9' },
      text: '@alice keep this',
      mentions: { bot: false, replyToBot: false, quoteBot: false },
    }

    expect(adapter.toSession(thread, { tenant: 't', agent: 'a' })).toMatchObject({
      sessionKey: 'agnes:t:a:dingtalk:group:conversation-1:thread:thread-9',
      content: [{ type: 'text', text: '@alice keep this' }],
      credential: { chatType: 'thread' },
    })
  })

  it('recognizes only the five command names and preserves attachment trust boundaries', async () => {
    const adapter = await createDingtalkAdapter(manifestPath, { gateway: new FakeDingtalkGateway() })
    const bytes = new Uint8Array([1, 2, 3])
    const event = toEvent(raw({ text: { content: '/preset code' } }), 'bot-1', [
      { name: 'local.bin', mime: 'application/octet-stream', bytes },
      { name: 'remote.txt', mime: 'text/plain', url: 'https://download.example/remote' },
      { name: 'unsafe', mime: 'text/plain', url: 'file:///etc/passwd' },
      { name: 'missing', mime: 'text/plain' },
    ])
    const intent = adapter.toSession(event, { tenant: 't', agent: 'a' })

    expect(intent).toMatchObject({
      kind: 'command',
      command: { name: 'preset', args: ['code'] },
    })
    expect(intent?.content).toEqual([
      { type: 'text', text: '/preset code' },
      {
        type: 'file',
        data: Buffer.from(bytes).toString('base64'),
        name: 'local.bin',
        mimeType: 'application/octet-stream',
      },
    ])

    const unknownCommand = adapter.toSession(
      toEvent(raw({ text: { content: '/deploy now' } }), 'bot-1', []),
      {
        tenant: 't',
        agent: 'a',
      },
    )
    expect(unknownCommand).toMatchObject({ kind: 'message' })
    expect(unknownCommand).not.toHaveProperty('command')
  })

  it('returns null for non-message events and an empty message without attachments', async () => {
    const adapter = await createDingtalkAdapter(manifestPath, { gateway: new FakeDingtalkGateway() })
    const message = toEvent(raw({ text: { content: '   ' } }), 'bot-1', [])
    const card: ChannelEvent = {
      kind: 'cardAction',
      eventId: 'card-1',
      accountId: 'robot-1',
      at: message.at,
      chat: message.chat,
      sender: message.sender,
      cardBizId: 'card-1',
      value: 'approve',
    }

    expect(adapter.toSession(message, { tenant: 't', agent: 'a' })).toBeNull()
    expect(adapter.toSession(card, { tenant: 't', agent: 'a' })).toBeNull()
  })
})

describe('DingtalkAdapter inbound delivery', () => {
  it('preserves repeated delivery ids for downstream claim.once deduplication', async () => {
    const gateway = new FakeDingtalkGateway()
    const adapter = await createDingtalkAdapter(manifestPath, { gateway })
    const events: ChannelEvent[] = []
    await adapter.connect({
      credentials: { clientId: 'app-key', clientSecret: 'app-secret' },
      signal: new AbortController().signal,
      onEvent: (event) => events.push(event),
      log: { info() {}, warn() {}, error() {} },
    })

    gateway.emitMessage(raw())
    gateway.emitMessage(raw())
    await vi.waitFor(() => expect(events).toHaveLength(2))

    expect(events.map((event) => event.eventId)).toEqual(['message-1', 'message-1'])
    await adapter.disconnect()
  })

  it('preserves same-chat order across attachment downloads and redacts failures by omission', async () => {
    const gateway = new FakeDingtalkGateway()
    let release: (() => void) | undefined
    gateway.download = vi.fn(async (code) => {
      if (code === 'slow') await new Promise<void>((resolve) => (release = resolve))
      if (code === 'secret-error') throw new Error('app-secret leaked from backend')
      return { bytes: new Uint8Array([1]), mime: 'image/png' }
    })
    const events: MessageEvent[] = []
    const warn = vi.fn()
    const adapter = await createDingtalkAdapter(manifestPath, { gateway })
    await adapter.connect({
      credentials: { clientId: 'app-key', clientSecret: 'app-secret' },
      signal: new AbortController().signal,
      onEvent: (event) => {
        if (event.kind === 'message') events.push(event)
      },
      log: { info() {}, warn, error() {} },
    })

    gateway.emitMessage(raw({ msgId: 'first', msgtype: 'picture', content: { downloadCode: 'slow' } }))
    gateway.emitMessage(raw({ msgId: 'second', text: { content: 'second' } }))
    await vi.waitFor(() => expect(release).toBeTypeOf('function'))
    expect(events).toHaveLength(0)
    release?.()
    await vi.waitFor(() => expect(events.map((event) => event.messageId)).toEqual(['first', 'second']))

    gateway.emitMessage(raw({ msgId: 'third', msgtype: 'file', content: { downloadCode: 'secret-error' } }))
    await vi.waitFor(() => expect(events).toHaveLength(3))
    expect(events[2]?.attachments).toEqual([])
    expect(events[2]?.text).toContain('附件未能同步')
    expect(adapter.toSession(events[2]!, { tenant: 't', agent: 'a' })?.content).toEqual([
      expect.objectContaining({ type: 'text', text: expect.stringContaining('附件未能同步') }),
    ])
    expect(JSON.stringify(events[2])).not.toContain('app-secret')
    expect(JSON.stringify(warn.mock.calls)).not.toContain('app-secret')
    await adapter.disconnect()
  })

  it('drops a stale pending event without blocking the next connection epoch', async () => {
    const gateway = new FakeDingtalkGateway()
    let release: (() => void) | undefined
    gateway.download = vi.fn(async () => {
      await new Promise<void>((resolve) => (release = resolve))
      return { bytes: new Uint8Array([1]), mime: 'image/png' }
    })
    const events: MessageEvent[] = []
    const adapter = await createDingtalkAdapter(manifestPath, { gateway })
    const connect = () =>
      adapter.connect({
        credentials: { clientId: 'app-key', clientSecret: 'app-secret' },
        signal: new AbortController().signal,
        onEvent: (event) => {
          if (event.kind === 'message') events.push(event)
        },
        log: { info() {}, warn() {}, error() {} },
      })
    await connect()
    gateway.emitMessage(raw({ msgId: 'stale', msgtype: 'picture', content: { downloadCode: 'slow' } }))
    await vi.waitFor(() => expect(release).toBeTypeOf('function'))

    await adapter.disconnect()
    await connect()
    gateway.emitMessage(raw({ msgId: 'fresh', text: { content: 'fresh' } }))
    await vi.waitFor(() => expect(events.map((event) => event.messageId)).toEqual(['fresh']))

    release?.()
    await new Promise((resolve) => setTimeout(resolve, 10))
    expect(events.map((event) => event.messageId)).toEqual(['fresh'])
    await adapter.disconnect()
  })

  it('does not deliver an in-flight attachment after the connection signal aborts', async () => {
    const gateway = new FakeDingtalkGateway()
    let release: (() => void) | undefined
    gateway.download = vi.fn(async () => {
      await new Promise<void>((resolve) => (release = resolve))
      return { bytes: new Uint8Array([1]), mime: 'image/png' }
    })
    const events: ChannelEvent[] = []
    const controller = new AbortController()
    const adapter = await createDingtalkAdapter(manifestPath, { gateway })
    await adapter.connect({
      credentials: { clientId: 'app-key', clientSecret: 'app-secret' },
      signal: controller.signal,
      onEvent: (event) => events.push(event),
      log: { info() {}, warn() {}, error() {} },
    })
    gateway.emitMessage(raw({ msgId: 'pending', msgtype: 'picture', content: { downloadCode: 'slow' } }))
    await vi.waitFor(() => expect(release).toBeTypeOf('function'))

    controller.abort()
    release?.()
    await new Promise((resolve) => setTimeout(resolve, 10))

    expect(events).toEqual([])
    expect(gateway.started).toBe(false)
  })
})

describe('native attachment content', () => {
  it('imports picture, rich-text pictures, video and audio with bounded downloads and keeps successful parts on partial failure', async () => {
    const download = vi.fn(async (code: string) => {
      if (code === 'broken') throw new Error('credential-must-not-leak')
      return { bytes: new Uint8Array([1, 2, 3]), mime: 'application/octet-stream' }
    })
    for (const msgtype of ['picture', 'video', 'audio'] as const) {
      const attachments = await attachmentsOf(raw({ msgtype, content: { downloadCode: 'ok' } }), download)
      expect(attachments[0]?.bytes).toEqual(new Uint8Array([1, 2, 3]))
    }
    const failed = vi.fn()
    const attachments = await attachmentsOf(
      raw({
        msgtype: 'richText',
        content: {
          richText: [
            { text: 'before' },
            { type: 'picture', pictureDownloadCode: 'ok' },
            { type: 'picture', downloadCode: 'broken' },
            { text: 'after' },
            { type: 'picture', downloadCode: 'ok' },
          ],
        },
      }),
      download,
      failed,
    )
    expect(attachments).toHaveLength(2)
    expect(failed).toHaveBeenCalledWith('message-1-2.png', '附件下载失败（DOWNLOAD_FAILED）')
    const adapter = await createDingtalkAdapter(manifestPath, { gateway: new FakeDingtalkGateway() })
    const png = Buffer.from(
      'iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mP8z8BQDwAEhQGAhKmMIQAAAABJRU5ErkJggg==',
      'base64',
    )
    const event = toEvent(
      raw({ msgtype: 'richText', content: { richText: [{ text: 'before' }, { text: 'after' }] } }),
      'bot-1',
      [
        { name: 'picture.png', mime: 'application/octet-stream', bytes: png },
        { name: 'video.mp4', mime: 'video/mp4', bytes: new Uint8Array([1, 2, 3]) },
      ],
    )
    expect(adapter.toSession(event, { tenant: 't', agent: 'a' })?.content).toEqual([
      { type: 'text', text: 'beforeafter' },
      { type: 'image', data: png.toString('base64'), mimeType: 'image/png' },
      { type: 'file', name: 'video.mp4', mimeType: 'video/mp4', data: 'AQID' },
    ])
    const limited = vi.fn(async () => ({
      bytes: new Uint8Array(MAX_ATTACHMENT_BYTES),
      mime: 'application/octet-stream',
    }))
    const tooMany = await attachmentsOf(
      raw({
        msgtype: 'richText',
        content: {
          richText: [
            { type: 'picture', downloadCode: 'a' },
            { type: 'picture', downloadCode: 'b' },
          ],
        },
      }),
      limited,
      failed,
    )
    expect(tooMany).toHaveLength(1)
    expect(limited).toHaveBeenCalledTimes(1)
    const missing = vi.fn()
    await attachmentsOf(raw({ msgtype: 'picture', content: {} }), download, missing)
    expect(missing).toHaveBeenCalled()
  })
})

describe('Stream serialized content compatibility', () => {
  it.each(['picture', 'file', 'audio', 'video', 'richText'] as const)(
    'downloads object and serialized %s payloads identically',
    async (msgtype) => {
      const content =
        msgtype === 'richText'
          ? { richText: [{ text: '看图' }, { type: 'picture', pictureDownloadCode: 'synthetic-code' }] }
          : { downloadCode: 'synthetic-code', fileName: 'synthetic.bin', recognition: '语音识别' }
      const download = vi.fn(async () => ({
        bytes: new Uint8Array([1, 2, 3]),
        mime: 'application/octet-stream',
      }))
      const object = raw({ msgtype, content })
      const encoded = raw({ msgtype, content: JSON.stringify(content) })
      expect(await attachmentsOf(encoded, download)).toEqual(await attachmentsOf(object, download))
      expect(toEvent(encoded, 'bot', []).text).toEqual(toEvent(object, 'bot', []).text)
      expect(download).toHaveBeenCalledWith('synthetic-code', MAX_ATTACHMENT_BYTES)
    },
  )
  it('preserves actual attachment bytes through serialized callback delivery', async () => {
    const gateway = new FakeDingtalkGateway()
    gateway.downloads.set('picture-code', {
      bytes: new Uint8Array([1, 2, 3]),
      mime: 'application/octet-stream',
    })
    const adapter = await createDingtalkAdapter(manifestPath, { gateway })
    const events: MessageEvent[] = []
    await adapter.connect({
      credentials: { clientId: 'synthetic', clientSecret: 'synthetic' },
      signal: new AbortController().signal,
      onEvent: (event) => {
        if (event.kind === 'message') events.push(event)
      },
      log: { info() {}, warn() {}, error() {} },
    })
    try {
      gateway.emitMessage(
        raw({ msgtype: 'picture', content: JSON.stringify({ pictureDownloadCode: 'picture-code' }) }),
      )
      await vi.waitFor(() => expect(events).toHaveLength(1))
      expect(events[0]?.text).not.toContain('附件未能同步')
      expect(adapter.toSession(events[0]!, { tenant: 't', agent: 'a' })?.content).toEqual([
        { type: 'file', data: 'AQID', name: 'message-1.png', mimeType: 'application/octet-stream' },
      ])
    } finally {
      await adapter.disconnect()
    }
  })
  it('reports precise safe failure categories without reflecting backend secrets or signed URLs', async () => {
    expect(
      attachmentFailureText(new ChannelError('E_CONNECT_FAILED', 'DingTalk API failed with HTTP 403')),
    ).toContain('HTTP 403')
    expect(
      attachmentFailureText(
        new ChannelError('E_CONNECT_FAILED', 'DingTalk attachment download failed with HTTP 302'),
      ),
    ).toContain('HTTP 302')
    expect(
      attachmentFailureText(new ChannelError('E_CONNECT_FAILED', 'DingTalk API request failed')),
    ).toContain('API_NETWORK')
    expect(
      attachmentFailureText(new ChannelError('E_CONNECT_FAILED', 'DingTalk attachment download failed')),
    ).toContain('FILE_NETWORK')
    expect(attachmentFailureText(new Error('secret signed-download-url'))).toBe(
      '附件下载失败（DOWNLOAD_FAILED）',
    )
    const fail = vi.fn(),
      download = vi.fn()
    for (const content of [
      'not-json',
      '[]',
      'null',
      JSON.stringify({ text: 'not-media' }),
      'x'.repeat(256 * 1024 + 1),
    ]) {
      await expect(attachmentsOf(raw({ msgtype: 'picture', content }), download, fail)).resolves.toEqual([])
    }
    expect(download).not.toHaveBeenCalled()
    expect(fail).toHaveBeenCalledWith('message-1.png', expect.stringContaining('DOWNLOAD_CODE_MISSING'))
  })
})
