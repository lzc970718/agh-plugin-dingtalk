import { ChannelError } from '../../errors.js'
import type { Attachment, MessageEvent } from '../../adapter.js'
import type { DingtalkGateway, RawRobotMessage } from './gateway.js'

export const MAX_ATTACHMENT_BYTES = 20 * 1024 * 1024

const MESSAGE_TYPES = new Set<RawRobotMessage['msgtype']>([
  'text',
  'picture',
  'file',
  'audio',
  'video',
  'richText',
])

function requiredString(value: unknown, field: string, maxLength: number): string {
  if (typeof value !== 'string' || value.length === 0 || value.length > maxLength) {
    throw new TypeError(`invalid DingTalk ${field}`)
  }
  return value
}

function optionalString(value: unknown, field: string, maxLength: number): string | undefined {
  if (value === undefined) return undefined
  return requiredString(value, field, maxLength)
}

type RobotContent = Exclude<RawRobotMessage['content'], string | undefined>
/** Stream callbacks may contain either an object or a JSON-encoded content object. */
function contentOf(message: RawRobotMessage): RobotContent {
  let content: unknown = message.content
  if (typeof content === 'string') {
    if (content.length > 256 * 1024) return {}
    try {
      content = JSON.parse(content)
    } catch {
      return {}
    }
  }
  return content && typeof content === 'object' && !Array.isArray(content) ? (content as RobotContent) : {}
}

/** Only fixed messages and numeric HTTP statuses cross into the user-visible failure notice. */
export function attachmentFailureText(error: unknown): string {
  if (error instanceof RangeError) return '附件合计超过 20 MiB（ATTACHMENT_LIMIT）'
  if (error instanceof ChannelError) {
    const http = /DingTalk (API|access token|attachment download) failed with HTTP ([1-5][0-9]{2})$/.exec(
      error.message,
    )
    if (http)
      return (
        (http[1] === 'attachment download' ? '文件下载失败' : '获取下载地址失败') + '（HTTP ' + http[2] + '）'
      )
    if (/robotCode is not set$/.test(error.message)) return '机器人标识缺失（BOT_ID_MISSING）'
    if (/unsafe download URL|download address is unsafe/.test(error.message)) {
      const reasons: Record<string, string> = {
        MISSING_URL: '钉钉接口未返回下载地址',
        URL_TOO_LONG: '下载地址超过 32768 字符',
        URL_FORMAT: '下载地址格式无效',
        HTTP_CUSTOM_PORT: 'HTTP 下载地址使用了非默认端口',
        URL_PROTOCOL: '下载地址不是 HTTP/HTTPS',
        URL_CREDENTIALS: '下载地址含用户凭据',
        LOCAL_HOST: '下载地址指向本机',
        NON_PUBLIC_ADDRESS: '下载域名解析到内网或保留 IP（可能受代理 DNS 影响）',
        DNS_FAILED: '下载域名 DNS 解析失败',
        DNS_EMPTY: '下载域名没有可用 DNS 地址',
      }
      const reason = error.detail?.downloadUrlReason
      return typeof reason === 'string' && Object.hasOwn(reasons, reason)
        ? reasons[reason] + '（' + reason + '）'
        : '下载地址未通过安全校验（UNSAFE_DOWNLOAD_URL）'
    }
    if (/returned invalid JSON$/.test(error.message)) return '下载接口返回格式无效（INVALID_RESPONSE）'
    if (/request failed$/.test(error.message)) return '请求下载接口的网络连接失败（API_NETWORK）'
    if (/attachment download failed$/.test(error.message)) return '文件下载网络连接失败（FILE_NETWORK）'
  }
  return '附件下载失败（DOWNLOAD_FAILED）'
}

function textOf(message: RawRobotMessage): string {
  if (message.msgtype === 'text') {
    return typeof message.text?.content === 'string' ? message.text.content : ''
  }
  const content = contentOf(message)
  if (message.msgtype === 'audio') return typeof content.recognition === 'string' ? content.recognition : ''
  if (message.msgtype !== 'richText') return ''
  const richText = content.richText
  if (!Array.isArray(richText)) return ''
  return richText
    .map((part) =>
      typeof part === 'object' && part !== null && typeof (part as { text?: unknown }).text === 'string'
        ? ((part as { text: string }).text ?? '')
        : '',
    )
    .join('')
}

function checkDiscriminants(message: RawRobotMessage): void {
  if (message.conversationType !== '1' && message.conversationType !== '2') {
    throw new TypeError('invalid DingTalk conversationType')
  }
  if (!MESSAGE_TYPES.has(message.msgtype)) throw new TypeError('invalid DingTalk msgtype')
}

export function isSafeAttachmentUrl(value: string): boolean {
  if (value.length > 4096) return false
  if ([...value].some((character) => (character.codePointAt(0) ?? 0) <= 32)) return false
  try {
    const url = new URL(value)
    return url.protocol === 'https:' && url.username === '' && url.password === ''
  } catch {
    return false
  }
}

function defaultName(message: RawRobotMessage): string {
  if (message.msgtype === 'picture') return `${message.msgId}.png`
  if (message.msgtype === 'audio') return `${message.msgId}.amr`
  if (message.msgtype === 'video') return `${message.msgId}.mp4`
  return message.msgId
}

export function safeAttachmentName(value: unknown, fallback: string): string {
  const candidate = typeof value === 'string' ? value.replaceAll('\\', '/').split('/').at(-1) : undefined
  const clean = (input: string) =>
    [...input]
      .filter((character) => {
        const code = character.codePointAt(0) ?? 0
        return code > 31 && code !== 127
      })
      .join('')
      .trim()
  return [...(clean(candidate ?? '') || clean(fallback) || 'attachment')].slice(0, 256).join('')
}

export function safeAttachmentMime(value: unknown, fallback = 'application/octet-stream'): string {
  return typeof value === 'string' && value.length <= 128 && /^[\w!#$&^_.+-]+\/[\w!#$&^_.+-]+$/.test(value)
    ? value
    : fallback
}

export function toEvent(
  message: RawRobotMessage,
  botUserId: string,
  attachments: Attachment[],
): MessageEvent {
  checkDiscriminants(message)
  const messageId = requiredString(message.msgId, 'msgId', 512)
  const conversationId = requiredString(message.conversationId, 'conversationId', 256)
  const staffId = requiredString(message.senderStaffId, 'senderStaffId', 256)
  const senderNick = optionalString(message.senderNick, 'senderNick', 256)
  const senderCorpId = optionalString(message.senderCorpId, 'senderCorpId', 1024)
  const unionId = optionalString((message as unknown as { senderId?: unknown }).senderId, 'senderId', 256)
  const robotCode = optionalString(message.robotCode, 'robotCode', 128)
  if (!Number.isFinite(message.createAt)) throw new TypeError('invalid DingTalk createAt')
  const at = new Date(message.createAt).toISOString()
  const raw: Record<string, string> = {
    staffId,
    conversationId,
    conversationType: message.conversationType,
  }
  if (unionId !== undefined) raw.unionId = unionId
  if (senderNick !== undefined) raw.senderNick = senderNick
  if (senderCorpId !== undefined) raw.senderCorpId = senderCorpId

  return {
    kind: 'message',
    eventId: messageId,
    messageId,
    accountId: robotCode ?? 'dingtalk',
    at,
    chat: { id: conversationId, type: message.conversationType === '1' ? 'dm' : 'group' },
    sender: {
      userId: staffId,
      ...(unionId === undefined ? {} : { unionId }),
      ...(senderNick === undefined ? {} : { displayName: senderNick }),
      raw,
    },
    text: textOf(message),
    attachments: attachments.map((attachment) => ({ ...attachment })),
    mentions: {
      bot:
        message.isInAtList === true ||
        (botUserId.length > 0 &&
          (message.atUsers ?? []).some((candidate) => candidate.dingtalkId === botUserId)),
      replyToBot: false,
      quoteBot: false,
    },
  }
}

export async function attachmentsOf(
  message: RawRobotMessage,
  download: DingtalkGateway['download'],
  onFailure?: (name: string, reason?: string) => void,
): Promise<Attachment[]> {
  checkDiscriminants(message)
  const content = contentOf(message)
  const parts =
    message.msgtype === 'richText'
      ? (Array.isArray(content.richText) ? content.richText : []).filter(
          (part) => part && typeof part === 'object' && part.type === 'picture',
        )
      : ['picture', 'file', 'audio', 'video'].includes(message.msgtype)
        ? [content]
        : []
  const attachments: Attachment[] = []
  let remaining = MAX_ATTACHMENT_BYTES
  for (const [index, part] of parts.slice(0, 50).entries()) {
    const name = safeAttachmentName(
      'fileName' in part ? part.fileName : undefined,
      message.msgtype === 'richText' ? message.msgId + '-' + (index + 1) + '.png' : defaultName(message),
    )
    const code = part.downloadCode || ('pictureDownloadCode' in part ? part.pictureDownloadCode : undefined)
    try {
      if (typeof code !== 'string' || !code.length) {
        onFailure?.(name, '消息缺少附件下载码（DOWNLOAD_CODE_MISSING）')
        continue
      }
      if (remaining <= 0) throw new RangeError('DingTalk attachment exceeds the size limit')
      const result = await download(code, remaining)
      // A temporary URL is not a persisted attachment; never advertise it as a saved original.
      if ('url' in result) {
        if (!isSafeAttachmentUrl(result.url)) throw new TypeError('DingTalk attachment URL must be HTTPS')
        throw new RangeError('DingTalk attachment exceeds the size limit')
      }
      if (!(result.bytes instanceof Uint8Array) || result.bytes.length > remaining)
        throw new RangeError('DingTalk attachment exceeds the size limit')
      remaining -= result.bytes.length
      const fallback =
        message.msgtype === 'audio'
          ? 'audio/amr'
          : message.msgtype === 'video'
            ? 'video/mp4'
            : 'application/octet-stream'
      attachments.push({
        name,
        mime: safeAttachmentMime(result.mime?.split(';')[0]?.trim(), fallback),
        bytes: new Uint8Array(result.bytes),
        sizeBytes: result.bytes.length,
      })
    } catch (error) {
      if (!onFailure) throw error
      onFailure(name, attachmentFailureText(error))
    }
  }
  if (parts.length > 50) onFailure?.('超过 50 个的附件', '单条消息最多同步 50 个附件（ATTACHMENT_COUNT）')
  return attachments
}
