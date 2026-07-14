export { reactiveDrizzle }
export type { ReactiveDrizzleOptions }

import { sql } from 'drizzle-orm'
import { isAsyncMode } from 'telefunc'
import { assertUsage } from './assert.js'
import { ChangeRouter } from './change-router.js'
import { startPostgresCdc } from './cdc-postgres.js'
import { createQueryTracker, type QueryTracker, type SelectBuilderLike } from './query-tracker.js'
import { createWriteTracker, type WriteKind, type WriteTracker } from './write-tracker.js'

type ReactiveDrizzleOptions = {
  /** Change detection through Postgres logical replication; the default for
   *  node-postgres and postgres.js instances, `false` selects ORM write
   *  interception. Multi-instance deployments need a distinct `slot` each. */
  cdc?: boolean | { slot?: string; publication?: string }
}

type ReactiveBatch = {
  /** Group writes that run outside a telefunction (cron jobs, scripts) into one
   *  change emission. */
  batch: <T>(callback: () => T) => T
}

/** Make a drizzle instance reactive: selects awaited inside a telefunction
 *  subscribe the calling client to invalidation (through @telefunc/tanstack-query),
 *  and database changes — detected via Postgres logical replication or, on other
 *  databases, by intercepting writes on this instance — refetch every subscribed
 *  client whose query result they affect. */
function reactiveDrizzle<TDb extends object>(db: TDb, options: ReactiveDrizzleOptions = {}): TDb & ReactiveBatch {
  assertUsage(
    isAsyncMode(),
    'reactiveDrizzle() needs telefunc async hooks to link queries to their telefunction call. Add `import "telefunc/async_hooks"` to your server entry.',
  )

  const cdc = resolveCdc(db, options)
  const router = new ChangeRouter({ fanout: !cdc })
  const queries = createQueryTracker(router, db as never)
  const writes = cdc ? null : createWriteTracker((writeSet) => router.publish(writeSet))
  if (cdc) {
    void startPostgresCdc({ ...cdc, onWriteSet: (writeSet) => router.ingest(writeSet) }).catch((error) => {
      console.error('[@telefunc/drizzle] CDC startup failed:', error)
    })
  }

  return wrapExecutor(db, queries, writes) as TDb & ReactiveBatch
}

// ── CDC mode resolution ─────────────────────────────────────────────

const ENTITY_KIND = Symbol.for('drizzle:entityKind')

function resolveCdc(db: object, options: ReactiveDrizzleOptions) {
  if (options.cdc === false) return null
  const clientConfig = replicationClientConfig(db)
  if (!clientConfig) {
    assertUsage(
      options.cdc === undefined,
      'CDC needs a node-postgres or postgres.js drizzle instance; this driver uses write interception instead (remove the `cdc` option).',
    )
    return null
  }
  const cdc = typeof options.cdc === 'object' ? options.cdc : {}
  const execute = (db as { execute: (query: unknown) => Promise<unknown> }).execute.bind(db)
  return {
    clientConfig,
    execute: (statement: string) => execute(sql.raw(statement)),
    slot: cdc.slot ?? 'telefunc_reactive',
    publication: cdc.publication ?? 'telefunc_reactive',
  }
}

/** Recover a replication connection config from the driver client. */
function replicationClientConfig(db: object): Record<string, unknown> | null {
  const kind = (db.constructor as unknown as Record<symbol, unknown>)[ENTITY_KIND]
  const client = (db as { $client?: Record<string, Record<string, unknown> | undefined> }).$client
  if (!client) return null
  if (kind === 'NodePgDatabase') {
    // A Pool carries `.options`, a bare Client `.connectionParameters`.
    const config = client.options ?? client.connectionParameters
    if (!config) return null
    if (config.connectionString) return { connectionString: config.connectionString }
    const { host, port, database, user, password } = config
    return { host, port, database, user, password }
  }
  if (kind === 'PostgresJsDatabase') {
    const config = client.options
    if (!config) return null
    return {
      host: (config.host as string[] | undefined)?.[0],
      port: (config.port as number[] | undefined)?.[0],
      database: config.database,
      user: config.user,
      password: config.pass ?? undefined,
    }
  }
  return null
}

// ── db proxy ────────────────────────────────────────────────────────

const SELECT_ROOTS = new Set(['select', 'selectDistinct', 'selectDistinctOn'])
const WRITE_ROOTS: Record<string, WriteKind> = { insert: 'insert', update: 'update', delete: 'delete' }

/** Methods that execute a builder. `values` doubles as a chain method on insert
 *  builders, which are not thenable yet at that point — the thenable check below
 *  tells the two apart. */
const TERMINALS = new Set(['then', 'catch', 'finally', 'execute', 'run', 'all', 'get', 'values'])

/** Wrap a db or transaction object. Select chains report to the query tracker on
 *  execution; write chains (ORM mode) run through write capture; transactions
 *  recurse so `tx` behaves like the wrapped db. */
function wrapExecutor<T extends object>(executor: T, queries: QueryTracker, writes: WriteTracker | null): T {
  return new Proxy(executor, {
    get(target, prop) {
      // Drivers with their own batch (libsql-style `batch(queries[])`) keep it.
      if (prop === 'batch' && !Reflect.has(target, 'batch')) {
        return <V>(callback: () => V): V => (writes ? writes.inBatchScope(callback) : callback())
      }
      const value = Reflect.get(target, prop, target)
      if (typeof value !== 'function' || typeof prop !== 'string') return value

      if (SELECT_ROOTS.has(prop)) {
        return (...args: unknown[]) => wrapSelectChain(value.apply(target, args), queries)
      }
      const writeKind = WRITE_ROOTS[prop]
      if (writeKind && writes) {
        return (...args: unknown[]) => wrapWriteChain(value.apply(target, args), writeKind, writes, target)
      }
      if (prop === 'transaction') {
        return (callback: (tx: object) => unknown, ...rest: unknown[]) => {
          const run = () => value.call(target, (tx: object) => callback(wrapExecutor(tx, queries, writes)), ...rest)
          return writes ? writes.inTransactionScope(run) : run()
        }
      }
      return (...args: unknown[]) => value.apply(target, args)
    },
  }) as T
}

function wrapSelectChain(builder: unknown, queries: QueryTracker): unknown {
  return wrapChain(builder, (target, invoke) => {
    queries.track(target as unknown as SelectBuilderLike)
    return invoke()
  })
}

function wrapWriteChain(builder: unknown, kind: WriteKind, writes: WriteTracker, executor: object): unknown {
  return wrapChain(builder, (target, invoke, prop, args) => {
    const config = (target as { config: unknown }).config
    if (prop === 'then' || prop === 'catch' || prop === 'finally') {
      // Each continuation executes the builder (drizzle semantics); route that
      // execution through capture, then delegate to the captured promise.
      const executed = writes.capture(kind, config, executor as never, () =>
        Promise.resolve(target as PromiseLike<unknown>),
      )
      return (executed[prop] as (...a: unknown[]) => unknown)(...args)
    }
    return writes.capture(kind, config, executor as never, invoke)
  })
}

type OnTerminal = (
  target: object,
  invoke: () => unknown,
  prop: 'then' | 'catch' | 'finally' | string,
  args: unknown[],
) => unknown

/** Follow a builder chain: every drizzle entity a method returns stays wrapped,
 *  and invoking a terminal on a thenable (executable) builder runs `onTerminal`. */
function wrapChain(entity: unknown, onTerminal: OnTerminal): unknown {
  if (!isDrizzleEntity(entity)) return entity
  return new Proxy(entity, {
    get(target, prop) {
      const value = Reflect.get(target, prop, target)
      if (typeof value !== 'function' || typeof prop !== 'string') return value
      if (TERMINALS.has(prop) && typeof (target as { then?: unknown }).then === 'function') {
        return (...args: unknown[]) => onTerminal(target, () => value.apply(target, args), prop, args)
      }
      return (...args: unknown[]) => wrapChain(value.apply(target, args), onTerminal)
    },
  })
}

function isDrizzleEntity(value: unknown): value is object {
  if (typeof value !== 'object' || value === null) return false
  const constructor = (value as { constructor?: unknown }).constructor as Record<symbol, unknown> | undefined
  return constructor?.[ENTITY_KIND] !== undefined
}
