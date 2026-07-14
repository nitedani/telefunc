import { afterEach, describe, expect, it, vi } from 'vitest'
import { Broadcast, Channel, config, provideTelefuncContext } from 'telefunc'
import { useInvalidate } from './server.js'
import {
  EXTENSION_NAME,
  __TQ__BROADCAST_KEY_PREFIX,
  __TQ__CHANNEL_KEY,
  __TQ__DATA_KEY,
  type TanstackQueryResult,
} from './shared.js'

function getHook() {
  const extension = config.extensions.find((e) => e.name === EXTENSION_NAME)
  if (!extension?.hooks?.onTelefunctionResult) throw new Error('extension not registered')
  return extension.hooks.onTelefunctionResult
}

function runHook(queryKey: readonly unknown[], result: unknown = 'data'): unknown {
  return getHook()({ result, data: { queryKey } })
}

function expectWrapped(out: unknown): TanstackQueryResult {
  if (out === null || typeof out !== 'object' || !(__TQ__DATA_KEY in out)) {
    throw new Error('expected a channel-wrapped result')
  }
  return out as TanstackQueryResult
}

async function publishDelivered(topLevelKey: string): Promise<unknown> {
  const published = await Broadcast.publish(__TQ__BROADCAST_KEY_PREFIX + topLevelKey, [topLevelKey])
  return published.meta?.delivered
}

function spyOnChannelSend() {
  return vi.spyOn(Channel.prototype as unknown as { send: (data: unknown) => Promise<void> }, 'send')
}

// provideTelefuncContext() (sync mode) clears itself on the next macrotask;
// wait it out so context never leaks across tests.
afterEach(async () => {
  vi.restoreAllMocks()
  await new Promise((resolve) => setTimeout(resolve, 0))
})

describe('useInvalidate()', () => {
  it('throws outside a telefunction call', () => {
    expect(() => useInvalidate()).toThrowError('[useInvalidate()] Cannot access context object')
  })

  it('returns a stable trigger within one telefunction call', () => {
    provideTelefuncContext({})
    const invalidate = useInvalidate()
    expect(invalidate).toBeTypeOf('function')
    expect(useInvalidate()).toBe(invalidate)
  })

  it('marks a non-global query live: channel returned, trigger sends, no Broadcast subscription', async () => {
    provideTelefuncContext({})
    const invalidate = useInvalidate()
    const result = expectWrapped(runHook(['todos'], 'todos'))
    expect(result[__TQ__DATA_KEY]).toBe('todos')

    expect(await publishDelivered('todos')).toBe(0)

    const sendSpy = spyOnChannelSend()
    invalidate()
    expect(sendSpy).toHaveBeenCalledExactlyOnceWith('invalidate')

    result[__TQ__CHANNEL_KEY].abort()
  })

  it('returns the plain result for a non-global query without useInvalidate()', () => {
    expect(runHook(['todos-plain'])).toBe('data')
  })

  it('keeps the Broadcast path for a global query without useInvalidate()', async () => {
    const result = expectWrapped(runHook(['global:fallback']))

    expect(await publishDelivered('global:fallback')).toBe(1)

    result[__TQ__CHANNEL_KEY].abort()
    expect(await publishDelivered('global:fallback')).toBe(0)
  })

  it('gives a global query with useInvalidate() both paths', async () => {
    provideTelefuncContext({})
    const invalidate = useInvalidate()
    const result = expectWrapped(runHook(['global:both']))

    expect(await publishDelivered('global:both')).toBe(1)

    const sendSpy = spyOnChannelSend()
    invalidate()
    expect(sendSpy).toHaveBeenCalledExactlyOnceWith('invalidate')

    result[__TQ__CHANNEL_KEY].abort()
  })

  it('replays a trigger fired before the channel exists', () => {
    provideTelefuncContext({})
    const invalidate = useInvalidate()
    invalidate()

    const sendSpy = spyOnChannelSend()
    const result = expectWrapped(runHook(['pending-local']))
    expect(sendSpy).toHaveBeenCalledExactlyOnceWith('invalidate')

    result[__TQ__CHANNEL_KEY].abort()
  })

  it('disconnects on channel close, later triggers are no-ops', () => {
    provideTelefuncContext({})
    const invalidate = useInvalidate()
    const result = expectWrapped(runHook(['closed-local']))
    result[__TQ__CHANNEL_KEY].abort()

    const sendSpy = spyOnChannelSend()
    expect(() => invalidate()).not.toThrow()
    expect(sendSpy).not.toHaveBeenCalled()
  })
})
