export { ChangeRouter }
export type { TableChange, WriteSet, RowSeeder }

import { D2, type RootStreamBuilder } from '@electric-sql/d2ts'
import { Broadcast } from 'telefunc'
import { tableNameOf, type Row } from './row.js'
import type { CompiledQuery, HydrationPlan } from './d2ts-compiler.js'

/** One table's rows changed. `removed` carries old images (retractions);
 *  an update is one removed row plus one added row. */
type TableChange = {
  table: string
  added: Row[]
  removed: Row[]
  /** An update whose old image was unavailable: precise retraction is impossible,
   *  every query reading this table must fire. */
  lossy?: boolean
}

type WriteSet = TableChange[]

/** Runs a hydration plan: a full-table read narrowed by the plan's pushed-down WHERE. */
type RowSeeder = (plan: HydrationPlan) => Promise<Row[]>

/** Broadcast key namespace for ORM-mode fan-out, one key per table. */
const LIVE_KEY_PREFIX = '__live__:'

type GraphEntry = {
  compiled: CompiledQuery
  inputs: Map<string, RootStreamBuilder<Row>>
  graph: D2
  version: number
  dirty: boolean
  /** A lossy write hit this stateful graph: its join state can no longer be trusted,
   *  so it fires on every dependency change from now on. */
  tainted: boolean
  phase: 'hydrating' | 'live'
  backlog: WriteSet[]
  subscribers: Set<() => void>
}

/** Maps table changes to the d2ts graphs of subscribed queries. Identical queries
 *  (same rendered SQL + params) share one graph; the graph lives as long as it has
 *  subscribers and is destroyed at zero (re-created and re-hydrated on demand). */
class ChangeRouter {
  private readonly graphs = new Map<string, GraphEntry>()
  private readonly tableToGraphs = new Map<string, Set<GraphEntry>>()
  private readonly fanoutSubscriptions = new Map<string, () => void>()
  private readonly fanout: boolean

  /** `fanout` publishes writes per table over telefunc's Broadcast (`__live__:{table}`),
   *  which delivers in-memory on a single server and across instances when the app
   *  installs a broadcast transport. CDC mode skips it: Postgres is the fan-out. */
  constructor(options: { fanout: boolean }) {
    this.fanout = options.fanout
  }

  subscribe(compiled: CompiledQuery, seed: RowSeeder, onInvalidate: () => void): () => void {
    let entry = this.graphs.get(compiled.key)
    if (!entry) {
      entry = this.createGraph(compiled, seed)
    }
    entry.subscribers.add(onInvalidate)
    return () => {
      entry.subscribers.delete(onInvalidate)
      if (entry.subscribers.size === 0) this.destroyGraph(entry)
    }
  }

  /** Emit locally produced writes (ORM mode). Self-delivery happens through the
   *  Broadcast subscription, so local and remote writes take the same path. */
  publish(writes: WriteSet): void {
    for (const change of writes) {
      Broadcast.publish(LIVE_KEY_PREFIX + change.table, change)
    }
  }

  /** Feed a write set into every graph reading the affected tables. */
  ingest(writes: WriteSet): void {
    const affected = new Set<GraphEntry>()
    for (const change of writes) {
      for (const entry of this.tableToGraphs.get(change.table) ?? []) affected.add(entry)
    }
    for (const entry of affected) this.apply(entry, writes)
  }

  private createGraph(compiled: CompiledQuery, seed: RowSeeder): GraphEntry {
    const graph = new D2({ initialFrontier: 0 })
    const entry: GraphEntry = {
      compiled,
      graph,
      inputs: new Map(),
      version: 0,
      dirty: false,
      tainted: false,
      phase: compiled.hydrate.length > 0 ? 'hydrating' : 'live',
      backlog: [],
      subscribers: new Set(),
    }
    for (const [table, input] of compiled.build(graph, () => {
      entry.dirty = true
    })) {
      entry.inputs.set(table, input)
    }
    graph.finalize()

    this.graphs.set(compiled.key, entry)
    for (const table of compiled.tables) {
      let entries = this.tableToGraphs.get(table)
      if (!entries) {
        entries = new Set()
        this.tableToGraphs.set(table, entries)
        this.subscribeFanout(table)
      }
      entries.add(entry)
    }

    if (entry.phase === 'hydrating') void this.hydrate(entry, seed)
    return entry
  }

  private async hydrate(entry: GraphEntry, seed: RowSeeder): Promise<void> {
    try {
      for (const plan of entry.compiled.hydrate) {
        const rows = await seed(plan)
        entry.inputs.get(tableNameOf(plan.table))?.sendData(
          entry.version,
          rows.map((row) => [row, 1] as [Row, number]),
        )
      }
      this.advance(entry)
      // The seed is the baseline, not a change.
      entry.dirty = false
    } catch (error) {
      // Without its baseline the join state cannot be trusted; degrade to firing
      // on every dependency change instead of staying silently imprecise.
      entry.tainted = true
      console.error('[@telefunc/drizzle] query hydration failed, over-invalidating from now on:', error)
    }
    entry.phase = 'live'
    const backlog = entry.backlog
    entry.backlog = []
    // A removal that raced the seed may retract a row the seed never contained,
    // which would drive the join state negative and silence later identical
    // inserts. Taint instead of guessing which side of the snapshot it fell on.
    const hydrated = new Set(entry.compiled.hydrate.map((plan) => tableNameOf(plan.table)))
    for (const writes of backlog) {
      if (writes.some((change) => hydrated.has(change.table) && (change.removed.length > 0 || change.lossy))) {
        entry.tainted = true
      }
      this.apply(entry, writes)
    }
  }

  private apply(entry: GraphEntry, writes: WriteSet): void {
    if (entry.phase === 'hydrating') {
      entry.backlog.push(writes)
      return
    }
    let force = entry.tainted
    let fed = false
    for (const change of writes) {
      if (!entry.compiled.tables.has(change.table)) continue
      if (change.lossy) {
        force = true
        if (entry.compiled.stateful) entry.tainted = true
      }
      const updates: [Row, number][] = [
        ...change.added.map((row) => [row, 1] as [Row, number]),
        ...change.removed.map((row) => [row, -1] as [Row, number]),
      ]
      if (updates.length > 0) {
        entry.inputs.get(change.table)?.sendData(entry.version, updates)
        fed = true
      }
    }
    if (fed) this.advance(entry)
    if (entry.dirty || force) {
      entry.dirty = false
      for (const subscriber of [...entry.subscribers]) subscriber()
    }
  }

  /** One version per write batch: every input's frontier advances together, so
   *  stateful operators finalize and the graph fires at most once per batch. */
  private advance(entry: GraphEntry): void {
    entry.version++
    for (const input of entry.inputs.values()) input.sendFrontier(entry.version)
    entry.graph.run()
  }

  private destroyGraph(entry: GraphEntry): void {
    this.graphs.delete(entry.compiled.key)
    for (const table of entry.compiled.tables) {
      const entries = this.tableToGraphs.get(table)
      entries?.delete(entry)
      if (entries?.size === 0) {
        this.tableToGraphs.delete(table)
        this.fanoutSubscriptions.get(table)?.()
        this.fanoutSubscriptions.delete(table)
      }
    }
  }

  private subscribeFanout(table: string): void {
    if (!this.fanout) return
    const unsubscribe = Broadcast.subscribe<TableChange>(LIVE_KEY_PREFIX + table, (change) => {
      this.ingest([change])
    })
    this.fanoutSubscriptions.set(table, unsubscribe)
  }
}
