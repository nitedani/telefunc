export { invalidate, useInvalidate }

import { Channel, Broadcast, config, getRawContext } from 'telefunc'
import type { Context, TelefuncServerExtension } from 'telefunc'
import { partialMatchKey } from '@tanstack/query-core'
import {
  __TQ__BROADCAST_KEY_PREFIX,
  __TQ__DATA_KEY,
  __TQ__CHANNEL_KEY,
  EXTENSION_NAME,
  isGlobalKey,
  type TanstackQueryExtensionData,
  type TanstackQueryResult,
} from './shared.js'

function topLevelKey(queryKey: readonly unknown[]): string {
  return __TQ__BROADCAST_KEY_PREFIX + String(queryKey[0] ?? '')
}

// --- Extension ---

const extension = {
  name: EXTENSION_NAME,
  hooks: {
    onTelefunctionResult(ctx) {
      const data = ctx.data as TanstackQueryExtensionData

      if ('invalidates' in data) {
        for (const queryKey of data.invalidates) {
          invalidate(queryKey)
        }
        return ctx.result
      }

      const { queryKey } = data
      const trigger = peekTrigger()
      const isGlobal = isGlobalKey(queryKey)
      if (!isGlobal && !trigger) return ctx.result

      const invalidations = new Channel<never, 'invalidate'>()

      if (isGlobal) {
        const unsub = Broadcast.subscribe<readonly unknown[]>(topLevelKey(queryKey), (invalidatedKey) => {
          if (partialMatchKey(queryKey, invalidatedKey)) {
            invalidations.send('invalidate')
          }
        })
        invalidations.onClose(() => unsub())
      }

      if (trigger) {
        trigger.send = () => {
          invalidations.send('invalidate')
        }
        invalidations.onClose(() => {
          trigger.send = null
        })
        if (trigger.pending) {
          trigger.pending = false
          trigger.send()
        }
      }

      return { [__TQ__DATA_KEY]: ctx.result, [__TQ__CHANNEL_KEY]: invalidations.client } satisfies TanstackQueryResult
    },
  },
} satisfies TelefuncServerExtension
config.extensions.push(extension)

// --- Publishing ---

function invalidate(queryKey: readonly unknown[]) {
  Broadcast.publish(topLevelKey(queryKey), queryKey)
}

// --- useInvalidate() ---

/** Returns a trigger that refetches this query on the calling client, and marks the
 *  query live: the response carries the invalidation channel, so the query key doesn't
 *  need the `global:` prefix. Call it inside the telefunction; the trigger is bound to
 *  the current call, so it stays valid inside event listeners that fire after the
 *  telefunction returned. */
function useInvalidate(): () => void {
  const rawContext = getRawContext()
  if (!rawContext)
    throw new Error('[useInvalidate()] Cannot access context object, see https://telefunc.com/getContext#access')
  return getOrCreateTrigger(rawContext).invalidate
}

/** Connects `useInvalidate()` to the query's channel. The extension hook fills `send`
 *  once the channel exists and clears it on close. A trigger fired in between sets
 *  `pending` and is replayed on connect (invalidation is idempotent, so one flag is
 *  the whole buffer). */
type InvalidateTrigger = {
  invalidate: () => void
  send: (() => void) | null
  pending: boolean
}

const INVALIDATE_TRIGGER: unique symbol = Symbol.for('telefunc.tanstackQuery.invalidateTrigger')

function getOrCreateTrigger(rawContext: Context): InvalidateTrigger {
  let trigger = rawContext[INVALIDATE_TRIGGER] as InvalidateTrigger | undefined
  if (!trigger) {
    const created: InvalidateTrigger = {
      send: null,
      pending: false,
      invalidate: () => {
        if (created.send) {
          created.send()
        } else {
          created.pending = true
        }
      },
    }
    rawContext[INVALIDATE_TRIGGER] = created
    trigger = created
  }
  return trigger
}

function peekTrigger(): InvalidateTrigger | null {
  const rawContext = getRawContext()
  if (!rawContext) return null
  return (rawContext[INVALIDATE_TRIGGER] as InvalidateTrigger | undefined) ?? null
}
