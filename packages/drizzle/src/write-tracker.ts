export { createWriteTracker }
export type { WriteTracker, WriteKind }

import { AsyncLocalStorage } from 'node:async_hooks'
import { Param, getTableColumns, is, type SQL, type Table } from 'drizzle-orm'
import { REQUEST_CONTEXT, getRawContext, type RequestContext } from 'telefunc'
import type { TableChange, WriteSet } from './change-router.js'
import { tableNameOf, toDbRow, type Row } from './row.js'
import { isPromise } from './utils.js'

type WriteKind = 'insert' | 'update' | 'delete'

type WriteTracker = {
  /** Wrap one write execution: read old images before, capture new ones after,
   *  deliver to the active scope. Preserves the execution's sync/async shape. */
  capture: <T>(kind: WriteKind, config: unknown, executor: Executor, execute: () => T) => T
  /** Buffer writes during a transaction; deliver on commit, discard on rollback. */
  inTransactionScope: <T>(execute: () => T) => T
  /** Buffer writes during a batch; deliver on completion, even when it throws
   *  (without a transaction the writes already executed). */
  inBatchScope: <T>(execute: () => T) => T
}

/** The runtime config drizzle write builders carry. */
type WriteConfig = {
  table: Table
  values?: Record<string, unknown>[]
  set?: Record<string, unknown>
  where?: SQL
  returning?: unknown
  select?: unknown
  onConflict?: unknown
}

/** The unwrapped db (or transaction) a captured builder belongs to, used for
 *  pre-write reads on the same session. Never the proxied one: internal reads
 *  must not be tracked themselves. */
type Executor = {
  select: () => {
    from: (table: Table) => {
      where: (condition: SQL | undefined) => PromiseLike<Record<string, unknown>[]> & { all?: () => unknown }
    }
  }
}

const WRITE_BUFFER: unique symbol = Symbol.for('telefunc.drizzle.writeBuffer')

type Sink = { changes: TableChange[]; flushed?: boolean }

function createWriteTracker(emit: (writes: WriteSet) => void): WriteTracker {
  // Transaction and batch scopes; survives awaits inside the callback.
  const scopes = new AsyncLocalStorage<Sink>()

  function deliver(changes: TableChange[]): void {
    if (changes.length === 0) return
    const sink = scopes.getStore() ?? telefunctionSink()
    if (sink) sink.changes.push(...changes)
    else emit(consolidate(changes))
  }

  /** Writes inside a telefunction batch up and flush once the telefunction settles
   *  (also on throw: the statements already executed). Later writes from the same
   *  context (timers that outlive the call) emit immediately. */
  function telefunctionSink(): Sink | null {
    const raw = getRawContext()
    const requestContext = raw?.[REQUEST_CONTEXT] as RequestContext | undefined
    if (!raw || !requestContext) return null
    let sink = raw[WRITE_BUFFER] as Sink | undefined
    if (!sink) {
      const created: Sink = { changes: [] }
      raw[WRITE_BUFFER] = created
      requestContext.onTelefunctionSettled(() => {
        created.flushed = true
        if (created.changes.length > 0) emit(consolidate(created.changes))
        created.changes = []
      })
      sink = created
    }
    return sink.flushed ? null : sink
  }

  function capture<T>(kind: WriteKind, rawConfig: unknown, executor: Executor, execute: () => T): T {
    const config = rawConfig as WriteConfig
    const table = config.table

    if (kind === 'insert') {
      if (config.select || config.onConflict) {
        // INSERT…SELECT and upserts touch rows we never saw; row-precise feeding
        // would corrupt join state, so signal a lossy table change instead.
        return afterExecute(execute(), () =>
          deliver([{ table: tableNameOf(table), added: [], removed: [], lossy: true }]),
        )
      }
      return afterExecute(execute(), (result) => deliver([insertChange(table, config, result)]))
    }

    // The result of DELETE…RETURNING is exactly the old images; everything else
    // reads them with the write's own WHERE before the write executes.
    const oldRows = kind === 'delete' && config.returning ? [] : preSelect(executor, table, config.where)
    const proceed = (old: Row[] | null): T =>
      afterExecute(execute(), (result) =>
        deliver([
          kind === 'update' ? updateChange(table, config, old, result) : deleteChange(table, config, old, result),
        ]),
      )
    return isPromise(oldRows) ? (oldRows.then(proceed) as T) : proceed(oldRows)
  }

  function inTransactionScope<T>(execute: () => T): T {
    const sink: Sink = { changes: [] }
    // A throw means drizzle rolled back: the sink is simply dropped.
    const result = scopes.run(sink, execute)
    if (isPromise(result)) {
      return result.then((value) => {
        deliver(sink.changes)
        return value
      }) as T
    }
    deliver(sink.changes)
    return result
  }

  function inBatchScope<T>(execute: () => T): T {
    const sink: Sink = { changes: [] }
    const flush = () => deliver(sink.changes)
    let result: T
    try {
      result = scopes.run(sink, execute)
    } catch (error) {
      flush()
      throw error
    }
    if (isPromise(result)) {
      return result.then(
        (value) => {
          flush()
          return value
        },
        (error) => {
          flush()
          throw error
        },
      ) as T
    }
    flush()
    return result
  }

  return { capture, inTransactionScope, inBatchScope }
}

// ── Change extraction ───────────────────────────────────────────────

function insertChange(table: Table, config: WriteConfig, result: unknown): TableChange {
  const added = isRowArray(result, config)
    ? result.map((row) => toDbRow(table, row))
    : (config.values ?? []).map((values) => toDbRow(table, boundValues(values)))
  enrichGeneratedId(table, added, result)
  return { table: tableNameOf(table), added, removed: [] }
}

function updateChange(table: Table, config: WriteConfig, oldRows: Row[] | null, result: unknown): TableChange {
  const added = isRowArray(result, config)
    ? result.map((row) => toDbRow(table, row))
    : (oldRows ?? []).map((old) => mergeSet(table, old, config.set ?? {}))
  return { table: tableNameOf(table), added, removed: oldRows ?? [], lossy: oldRows === null }
}

function deleteChange(table: Table, config: WriteConfig, oldRows: Row[] | null, result: unknown): TableChange {
  const removed = isRowArray(result, config) ? result.map((row) => toDbRow(table, row)) : (oldRows ?? [])
  return { table: tableNameOf(table), added: [], removed, lossy: oldRows === null }
}

/** Old images, read with the write's own WHERE. Returns null when the WHERE can't
 *  be replayed as a plain select (e.g. `update().from()` referencing another table);
 *  the change is then lossy and over-invalidates instead of failing the write. */
function preSelect(executor: Executor, table: Table, where: SQL | undefined): Row[] | null | Promise<Row[] | null> {
  const toRows = (resolved: unknown) => (resolved as Record<string, unknown>[]).map((row) => toDbRow(table, row))
  try {
    const query = executor.select().from(table).where(where)
    // Synchronous drivers (better-sqlite3) expose .all(); thenable otherwise.
    const rows = typeof query.all === 'function' ? query.all() : query
    return isPromise(rows) ? rows.then(toRows, () => null) : toRows(rows)
  } catch {
    return null
  }
}

/** Values from `.values()` / `.set()` are Params (known) or SQL expressions
 *  (unknown: key omitted, predicates then treat the column as a match). */
function boundValues(values: Record<string, unknown>): Record<string, unknown> {
  const known: Record<string, unknown> = {}
  for (const [key, value] of Object.entries(values)) {
    if (is(value, Param)) known[key] = value.value
  }
  return known
}

function mergeSet(table: Table, old: Row, set: Record<string, unknown>): Row {
  const columns = getTableColumns(table)
  const merged: Row = { ...old }
  for (const [key, value] of Object.entries(set)) {
    const name = columns[key]?.name
    if (!name) continue
    if (is(value, Param)) merged[name] = value.value
    else delete merged[name]
  }
  return merged
}

function isRowArray(result: unknown, config: WriteConfig): result is Record<string, unknown>[] {
  return Boolean(config.returning) && Array.isArray(result)
}

/** A single-row insert through a synchronous driver reports `lastInsertRowid`:
 *  fill the generated primary key so predicates on it stay precise. */
function enrichGeneratedId(table: Table, added: Row[], result: unknown): void {
  if (added.length !== 1) return
  const rowid = (result as { lastInsertRowid?: number | bigint } | null)?.lastInsertRowid
  if (rowid === undefined || rowid === null) return
  const primary = Object.values(getTableColumns(table)).find((column) => column.primary)
  if (primary && !(primary.name in added[0]!)) added[0]![primary.name] = Number(rowid)
}

function consolidate(changes: TableChange[]): WriteSet {
  const byTable = new Map<string, TableChange>()
  for (const change of changes) {
    const merged = byTable.get(change.table)
    if (!merged) {
      byTable.set(change.table, { ...change })
    } else {
      merged.added = [...merged.added, ...change.added]
      merged.removed = [...merged.removed, ...change.removed]
      merged.lossy = merged.lossy || change.lossy
    }
  }
  return [...byTable.values()]
}

function afterExecute<T>(result: T, then: (result: unknown) => void): T {
  if (isPromise(result)) {
    return result.then((value) => {
      then(value)
      return value
    }) as T
  }
  then(result)
  return result
}
