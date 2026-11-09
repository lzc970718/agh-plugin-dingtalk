import { expect, it, vi } from 'vitest'
import type { JsonValue } from '@agnes/protocol'
import { dingtalkProvider, main } from '../src/index.js'

it('registers only a DingTalk platform with the backend bridge and refuses older hosts', () => {
  const registerChannel = vi.fn()
  main.apply({ profileRuntime: { registerChannel } } as never)
  expect(registerChannel).toHaveBeenCalledWith(dingtalkProvider, { instances: [] })
  expect(() => main.apply({ profileRuntime: {} } as never)).toThrow('5.8')
})
it('accepts optional card templates and rejects malformed or secret option fields', async () => {
  expect(dingtalkProvider.configSchema?.required ?? []).not.toContain('cardTemplateId')
  const configurations: JsonValue[] = [null, {}, { cardTemplateId: '' }, { cardTemplateId: '   ' }]
  for (const config of configurations) {
    const adapter = await dingtalkProvider.create({ config })
    expect(adapter.capabilities()).toMatchObject({ card: false, edit: false })
  }
  const card = await dingtalkProvider.create({ config: { cardTemplateId: ' template ' } })
  expect(card.capabilities()).toMatchObject({ card: true, edit: true })
  await expect(dingtalkProvider.create({ config: { cardTemplateId: 42 } })).rejects.toThrow()
  await expect(dingtalkProvider.create({ config: { cardTemplateId: 'template', clientSecret: 'literal' } })).rejects.toThrow()
})
