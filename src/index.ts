import { fileURLToPath } from 'node:url'
import type { PluginContextFacets } from '@agnes/extension-api'
import type { ChannelConfiguration, ChannelProvider } from '@agnes/plugin-runtime/channel-contract'
import manifest from './adapters/dingtalk/channel.json' with { type: 'json' }
import { createDingtalkAdapter } from './adapters/dingtalk/index.js'

export const dingtalkProvider: ChannelProvider = {
  id: 'dingtalk', version: '0.3.8', manifest: manifest as ChannelProvider['manifest'],
  configSchema: {type: 'object', additionalProperties: false, properties: {
    cardTemplateId: {type: 'string', title: '互动卡片模板 ID（留空使用普通消息） / Card template ID (blank for plain messages)'},
  }},
  async create({ config }) {
    const options = config ?? {}
    if (typeof options !== 'object' || Array.isArray(options) ||
        Object.keys(options).some(key => key !== 'cardTemplateId') ||
        (options.cardTemplateId !== undefined && typeof options.cardTemplateId !== 'string'))
      throw new Error('Invalid DingTalk instance config')
    const cardTemplateId = typeof options.cardTemplateId === 'string' ? options.cardTemplateId.trim() : ''
    return createDingtalkAdapter(fileURLToPath(new URL('./adapters/dingtalk/channel.json', import.meta.url)), {
      ...(cardTemplateId ? { cardTemplateId } : {}),
    })
  },
}

export const main = {
  inject: ['profileRuntime', 'services', 'providers'],
  apply(ctx: Pick<PluginContextFacets, 'profileRuntime'>, config: Partial<ChannelConfiguration> = {}) {
    if (!ctx.profileRuntime.registerChannel) throw new Error('DingTalk requires AGH Extension API 5.8 or later')
    ctx.profileRuntime.registerChannel(dingtalkProvider, { instances: config.instances ?? [] })
  },
}
