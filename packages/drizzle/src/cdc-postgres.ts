export { startPostgresCdc }
export type { PostgresCdcOptions }

import { assertUsage } from './assert.js'
import { sortedRow, type Row } from './row.js'
import type { TableChange, WriteSet } from './change-router.js'

type PostgresCdcOptions = {
  /** node-postgres ClientConfig for the replication connection. */
  clientConfig: Record<string, unknown>
  /** Raw SQL runner on the user's pool, for one-time publication/slot setup. */
  execute: (statement: string) => Promise<unknown>
  slot: string
  publication: string
  onWriteSet: (writes: WriteSet) => void
}

/** Consume the Postgres WAL through a logical replication slot (pgoutput) and turn
 *  each committed transaction into one WriteSet. Every server instance runs its own
 *  consumer on its own slot: Postgres itself is the multi-server fan-out. */
async function startPostgresCdc(options: PostgresCdcOptions): Promise<{ stop: () => Promise<void> }> {
  const { LogicalReplicationService, PgoutputPlugin } = await import('pg-logical-replication')
  await ensureReplicationSetup(options)

  const service = new LogicalReplicationService(options.clientConfig, {
    acknowledge: { auto: true, timeoutSeconds: 10 },
  })
  const plugin = new PgoutputPlugin({ protoVersion: 1, publicationNames: [options.publication] })

  // Row messages between 'begin' and 'commit' consolidate into one WriteSet,
  // so a transaction invalidates each affected graph once.
  let transaction = new Map<string, TableChange>()
  const changeFor = (table: string): TableChange => {
    let change = transaction.get(table)
    if (!change) {
      change = { table, added: [], removed: [] }
      transaction.set(table, change)
    }
    return change
  }

  service.on('data', (_lsn: string, message: PgoutputMessage) => {
    switch (message.tag) {
      case 'begin':
        transaction = new Map()
        break
      case 'insert':
        changeFor(message.relation.name).added.push(sortedRow(message.new))
        break
      case 'update': {
        const change = changeFor(message.relation.name)
        change.added.push(sortedRow(message.new))
        // The old image needs REPLICA IDENTITY FULL; without it the retraction is
        // impossible and the change degrades to firing every query on the table.
        if (message.old) change.removed.push(sortedRow(message.old))
        else change.lossy = true
        break
      }
      case 'delete': {
        const change = changeFor(message.relation.name)
        // Under the default replica identity only the key columns arrive; the
        // partial row is still a sound retraction input (missing columns match).
        const old = message.old ?? message.key
        if (old) change.removed.push(sortedRow(old))
        else change.lossy = true
        break
      }
      case 'truncate':
        for (const relation of message.relations) {
          changeFor(relation.name).lossy = true
        }
        break
      case 'commit':
        if (transaction.size > 0) options.onWriteSet([...transaction.values()])
        transaction = new Map()
        break
    }
  })
  service.on('error', (error: Error) => {
    console.error(
      `[@telefunc/drizzle] replication error on slot "${options.slot}" (multi-instance deployments need one slot per instance, see cdc.slot):`,
      error.message,
    )
  })

  // subscribe() resolves when the connection drops; loop to resume from the slot's
  // confirmed position. stop() ends the loop.
  let stopped = false
  void (async function consume() {
    while (!stopped) {
      await service.subscribe(plugin, options.slot).catch(() => {})
      if (!stopped) await sleep(1_000)
    }
  })()

  return {
    stop: async () => {
      stopped = true
      await service.stop()
    },
  }
}

/** Publication and slot are created once and persist; the slot pins WAL from its
 *  creation point, so restarts resume without missing events. */
async function ensureReplicationSetup(
  options: Pick<PostgresCdcOptions, 'execute' | 'publication' | 'slot'>,
): Promise<void> {
  const walLevel = await options.execute('SHOW wal_level').then(firstColumn)
  assertUsage(
    walLevel === 'logical',
    `Postgres CDC requires wal_level=logical (currently "${walLevel}"). Start Postgres with \`-c wal_level=logical\`, or pass \`{ cdc: false }\` to use write interception instead.`,
  )
  await options.execute(`CREATE PUBLICATION "${options.publication}" FOR ALL TABLES`).catch(ignoreCode('42710'))
  await options
    .execute(`SELECT pg_create_logical_replication_slot('${options.slot}', 'pgoutput')`)
    .catch(ignoreCode('42710'))
}

function ignoreCode(code: string): (error: unknown) => void {
  return (error) => {
    if ((error as { code?: string })?.code !== code) throw error
  }
}

/** Drivers shape raw results differently: node-postgres `{ rows }`, postgres.js a row array. */
function firstColumn(result: unknown): unknown {
  const rows = (result as { rows?: Row[] })?.rows ?? (Array.isArray(result) ? (result as Row[]) : [])
  const row = rows[0]
  return row ? Object.values(row)[0] : undefined
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms))
}

// The pgoutput message shapes consumed above (subset of pg-logical-replication's
// Pgoutput namespace; declared here so the dependency stays an optional peer).
type PgoutputMessage =
  | { tag: 'begin' }
  | { tag: 'commit' }
  | { tag: 'insert'; relation: { name: string }; new: Row }
  | { tag: 'update'; relation: { name: string }; old: Row | null; key: Row | null; new: Row }
  | { tag: 'delete'; relation: { name: string }; old: Row | null; key: Row | null }
  | { tag: 'truncate'; relations: { name: string }[] }
  | { tag: 'relation' | 'origin' | 'type' | 'message' }
