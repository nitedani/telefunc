export { compileQuery, conservativeQuery }
export type { CompiledQuery, HydrationPlan, SelectConfig }

import {
  MessageType,
  concat,
  distinct,
  filter,
  join,
  keyBy,
  map,
  output,
  reduce,
  topK,
  type D2,
  type IStreamBuilder,
  type JoinType,
  type RootStreamBuilder,
} from '@electric-sql/d2ts'
import { Column, Param, SQL, StringChunk, and, is, isTable, type Table } from 'drizzle-orm'
import {
  alwaysMatch,
  collectTables,
  compareValues,
  compileCondition,
  conjunctsOf,
  type CompiledCondition,
} from './predicate.js'
import { tableNameOf, type Row } from './row.js'

/** The runtime config drizzle select builders carry (`builder.config`; same shape
 *  for pg and sqlite). Read-only here; constructs we don't compile degrade to
 *  conservative invalidation instead of being rejected. */
type SelectConfig = {
  table: unknown
  fields: Record<string, unknown>
  where?: SQL
  having?: SQL
  joins?: { table: unknown; on: SQL | undefined; joinType: string }[]
  groupBy?: unknown[]
  orderBy?: unknown[]
  limit?: unknown
  offset?: unknown
  distinct?: boolean | { on: unknown[] }
  setOperators: { type: string; isAll: boolean; rightSelect: unknown }[]
  withList?: unknown[]
}

type HydrationPlan = { table: Table; where: SQL | undefined }

type CompiledQuery = {
  /** Stable identity shared by structurally identical queries: rendered SQL + params. */
  key: string
  /** Real table names the query depends on. */
  tables: Set<string>
  /** False when some construct compiled conservatively (it then always fires). */
  exact: boolean
  /** Graph holds cross-row state (a d2ts join); lossy writes permanently taint it. */
  stateful: boolean
  /** Tables to seed into the graph before live feeding. Only join graphs hydrate:
   *  a join starting from empty state would miss rows joining pre-existing data,
   *  while every other operator can at most over-fire from empty state. */
  hydrate: HydrationPlan[]
  /** Assemble the dataflow into a fresh graph. Returns one input per table;
   *  `onDirty` fires when the query's output changed at the current version. */
  build: (graph: D2, onDirty: () => void) => Map<string, RootStreamBuilder<Row>>
}

function compileQuery(config: SelectConfig, identity: { sql: string; params: unknown[] }): CompiledQuery | null {
  const compiled = compileSelect(config)
  if (compiled.tables.size === 0) return null
  return {
    key: `${identity.sql}\n${JSON.stringify(identity.params)}`,
    tables: compiled.tables,
    exact: compiled.exact,
    stateful: compiled.stateful,
    hydrate: compiled.hydrate,
    build(graph, onDirty) {
      const builder = new GraphBuilder(graph, onDirty)
      const stream = compiled.assemble(builder)
      if (stream) builder.observe(stream)
      return builder.inputs
    },
  }
}

/** Table-level subscription for reads without a compilable builder config
 *  (the relational `db.query.*` API): any change to any table fires. Queries
 *  over the same table set share one graph regardless of their filters. */
function conservativeQuery(tables: Iterable<string>): CompiledQuery {
  const tableSet = new Set(tables)
  return {
    key: `tables\n${[...tableSet].sort().join(',')}`,
    tables: tableSet,
    exact: false,
    stateful: false,
    hydrate: [],
    build(graph, onDirty) {
      const builder = new GraphBuilder(graph, onDirty)
      builder.fireOnAnyChange(tableSet)
      return builder.inputs
    },
  }
}

// ── Select compilation ──────────────────────────────────────────────

type CompiledSelect = {
  tables: Set<string>
  exact: boolean
  stateful: boolean
  hydrate: HydrationPlan[]
  /** Build this select's dataflow; returns its output stream, or null when the
   *  compilation already observes its streams directly (conservative modes). */
  assemble: (builder: GraphBuilder) => IStreamBuilder<unknown> | null
}

function compileSelect(config: SelectConfig): CompiledSelect {
  const ctx = new CompileContext()

  const sources = collectSources(config, ctx)
  if (!sources) return compileConservatively(config, ctx)

  pushdownConjuncts(config.where, sources, ctx)
  const joins = compileJoins(config, sources, ctx)
  if (config.joins?.length && !joins) {
    // Some ON condition is not an equi-join over the involved tables: fall back to
    // firing on any row that passes its own table's pushed-down conjuncts.
    return perTableFallback(sources, ctx)
  }

  const grouping = compileGrouping(config, sources, ctx)
  const ordering = grouping ? skipOrdering(config, ctx) : compileOrdering(config, sources, ctx)
  const isDistinct = compileDistinct(config, ctx)
  const arms = compileSetOperators(config, ctx)

  // Invariant: build() creates an input for every dependency table. Tables that are
  // neither a source here nor in an arm (subquery references) fire on any change.
  const covered = new Set([...sources.map((source) => source.name), ...arms.flatMap((arm) => [...arm.tables])])
  ctx.addOpaqueUncovered(covered)

  const stateful = (joins ?? []).length > 0
  return {
    tables: ctx.tables,
    exact: ctx.exact,
    stateful: stateful || arms.some((arm) => arm.stateful),
    hydrate: [
      ...(stateful ? sources.map((source) => ({ table: source.table, where: source.filterSql })) : []),
      ...arms.flatMap((arm) => arm.hydrate),
    ],
    assemble(builder) {
      let stream = assemblePipeline(builder, sources, joins ?? [], grouping, ordering, isDistinct)
      for (const arm of arms) {
        const armStream = arm.assemble(builder)
        if (armStream) stream = stream.pipe(concat(armStream)) as IStreamBuilder<unknown>
      }
      builder.fireOnAnyChange(ctx.opaqueTables)
      return stream
    },
  }
}

class CompileContext {
  tables = new Set<string>()
  /** Tables we can't evaluate row-precisely (subqueries, CTEs): any change fires. */
  opaqueTables = new Set<string>()
  exact = true

  addOpaque(names: Iterable<string>): void {
    for (const name of names) {
      this.tables.add(name)
      this.opaqueTables.add(name)
    }
    this.exact = false
  }

  addOpaqueUncovered(covered: Set<string>): void {
    const uncovered = [...this.tables].filter((table) => !covered.has(table) && !this.opaqueTables.has(table))
    if (uncovered.length > 0) this.addOpaque(uncovered)
  }
}

/** Everything-is-opaque degradation: any change to any referenced table fires. */
function compileConservatively(config: SelectConfig, ctx: CompileContext): CompiledSelect {
  ctx.addOpaque(collectReferencedTables(config))
  return {
    tables: ctx.tables,
    exact: false,
    stateful: false,
    hydrate: [],
    assemble(builder) {
      builder.fireOnAnyChange(ctx.opaqueTables)
      return null
    },
  }
}

/** Join fallback: each table's rows flow through that table's pushed-down filter. */
function perTableFallback(sources: Source[], ctx: CompileContext): CompiledSelect {
  ctx.exact = false
  return {
    tables: ctx.tables,
    exact: false,
    stateful: false,
    hydrate: [],
    assemble(builder) {
      for (const source of sources) builder.observe(sourceStream(builder, source))
      builder.fireOnAnyChange(ctx.opaqueTables)
      return null
    },
  }
}

// ── Sources and WHERE pushdown ──────────────────────────────────────

type Source = {
  table: Table
  name: string
  filter: CompiledCondition
  filterSql: SQL | undefined
}

function collectSources(config: SelectConfig, ctx: CompileContext): Source[] | null {
  const fromTables: Table[] = []
  for (const tableLike of [config.table, ...(config.joins ?? []).map((joinConfig) => joinConfig.table)]) {
    if (!isTable(tableLike)) return null
    fromTables.push(tableLike)
    ctx.tables.add(tableNameOf(tableLike))
  }
  for (const cte of config.withList ?? []) {
    const cteTables = new Set<string>()
    collectTables(cte, cteTables)
    ctx.addOpaque(cteTables)
  }
  return fromTables.map((table) => ({ table, name: tableNameOf(table), filter: alwaysMatch(), filterSql: undefined }))
}

/** Distribute the WHERE conjuncts onto the single source each one references.
 *  Cross-table conjuncts (beyond join ONs) are skipped, which only widens matches. */
function pushdownConjuncts(where: SQL | undefined, sources: Source[], ctx: CompileContext): void {
  if (!where) return
  const bySource = new Map<Source, SQL[]>()
  for (const conjunct of conjunctsOf(where)) {
    const condition = compileCondition(conjunct)
    for (const table of condition.tables) ctx.tables.add(table)
    if (!condition.exact) ctx.exact = false
    const owners = sources.filter((source) => condition.tables.has(source.name))
    if (condition.tables.size === 1 && owners.length === 1) {
      bySource.set(owners[0]!, [...(bySource.get(owners[0]!) ?? []), conjunct])
    } else {
      ctx.exact = false
    }
  }
  for (const [source, conjuncts] of bySource) {
    source.filterSql = conjuncts.length === 1 ? conjuncts[0] : and(...conjuncts)
    source.filter = compileCondition(source.filterSql)
  }
}

// ── Joins ───────────────────────────────────────────────────────────

type CompiledJoin = {
  rightSource: number
  type: JoinType
  /** Composite join key read from the already-joined tuple. */
  leftKey: (tuple: (Row | null)[]) => string
  /** Composite join key read from the new source's row. */
  rightKey: (row: Row) => string
}

const JOIN_TYPES: Record<string, JoinType> = { inner: 'inner', left: 'left', right: 'right', full: 'full' }

function compileJoins(config: SelectConfig, sources: Source[], ctx: CompileContext): CompiledJoin[] | null {
  const joins: CompiledJoin[] = []
  for (const [joinIndex, joinConfig] of (config.joins ?? []).entries()) {
    const rightSource = joinIndex + 1
    const type = JOIN_TYPES[joinConfig.joinType]
    if (!type || !joinConfig.on) return null
    for (const table of compileCondition(joinConfig.on).tables) ctx.tables.add(table)
    const pairs = equiJoinPairs(joinConfig.on, sources, rightSource)
    if (!pairs) return null
    joins.push({
      rightSource,
      type,
      leftKey: (tuple) => stableKey(pairs.map(({ left }) => tuple[left.source]?.[left.column.name])),
      rightKey: (row) => stableKey(pairs.map(({ right }) => row[right.column.name])),
    })
  }
  return joins
}

type ColumnRef = { source: number; column: Column }

/** Parse a join ON as a conjunction of column equalities between the new source
 *  and already-joined sources. Anything else makes the join non-compilable. */
function equiJoinPairs(
  on: SQL,
  sources: Source[],
  rightSource: number,
): { left: ColumnRef; right: ColumnRef }[] | null {
  const pairs: { left: ColumnRef; right: ColumnRef }[] = []
  for (const conjunct of conjunctsOf(on)) {
    const columns = equalityColumns(conjunct)
    if (!columns) return null
    const a = resolveColumn(columns[0], sources)
    const b = resolveColumn(columns[1], sources)
    if (!a || !b) return null
    if (a.source === rightSource && b.source < rightSource) pairs.push({ left: b, right: a })
    else if (b.source === rightSource && a.source < rightSource) pairs.push({ left: a, right: b })
    else return null
  }
  return pairs.length > 0 ? pairs : null
}

/** Match the exact `eq(colA, colB)` chunk shape. */
function equalityColumns(condition: SQL): [Column, Column] | null {
  const chunks = meaningfulChunks(condition)
  if (chunks.length === 1 && is(chunks[0], SQL)) return equalityColumns(chunks[0])
  if (chunks.length !== 3) return null
  const [left, op, right] = chunks
  if (is(left, Column) && is(right, Column) && chunkText(op) === ' = ') return [left, right]
  return null
}

/** Columns of aliased tables (self-joins) resolve by table object identity;
 *  plain columns by table name. */
function resolveColumn(column: Column, sources: Source[]): ColumnRef | null {
  const byIdentity = sources.findIndex((source) => source.table === column.table)
  if (byIdentity >= 0) return { source: byIdentity, column }
  const byName = sources.findIndex((source) => source.name === tableNameOf(column.table))
  return byName >= 0 ? { source: byName, column } : null
}

// ── Grouping and aggregates ─────────────────────────────────────────

type Grouping = {
  groupKey: (tuple: (Row | null)[]) => string
  /** Per-row vector of aggregate inputs; the reducer folds vectors into a result tuple. */
  rowVector: (tuple: (Row | null)[]) => unknown[]
  reducer: (values: [unknown[], number][]) => [unknown[], number][]
  having: ((aggregates: unknown[]) => boolean) | null
}

type AggregateField = { fn: 'count' | 'sum' | 'avg' | 'min' | 'max'; column: ColumnRef | null }

function compileGrouping(config: SelectConfig, sources: Source[], ctx: CompileContext): Grouping | null {
  const aggregates: AggregateField[] = []
  for (const field of Object.values(config.fields)) {
    const aggregate = parseAggregate(field, sources)
    if (aggregate) aggregates.push(aggregate)
    else if (!is(field, Column) && !isJoinFieldMap(field)) ctx.exact = false
  }
  if (aggregates.length === 0 && !config.groupBy?.length) return null

  const groupColumns: ColumnRef[] = []
  for (const expression of config.groupBy ?? []) {
    const ref = is(expression, Column) ? resolveColumn(expression, sources) : null
    if (!ref) {
      // Grouping by an expression: group membership is undecidable per row.
      ctx.exact = false
      return null
    }
    groupColumns.push(ref)
  }

  const having = compileHaving(config.having, aggregates, sources)
  if (config.having && !having) ctx.exact = false

  return {
    groupKey: (tuple) => stableKey(groupColumns.map(({ source, column }) => tuple[source]?.[column.name])),
    rowVector: (tuple) => aggregates.map(({ column }) => (column ? tuple[column.source]?.[column.column.name] : null)),
    reducer: (values) => [[foldAggregates(aggregates, values), 1]],
    having,
  }
}

/** Recognize `count()` / `count(col)` / `sum|avg|min|max(col)` field chunks:
 *  `[Str"fn(", Column | SQL(*), Str")"]`. */
function parseAggregate(field: unknown, sources: Source[]): AggregateField | null {
  const sqlExpr = is(field, SQL.Aliased) ? field.sql : is(field, SQL) ? field : null
  if (!sqlExpr) return null
  const chunks = meaningfulChunks(sqlExpr)
  if (chunks.length !== 3 || !chunkText(chunks[2])?.startsWith(')')) return null
  const fn = chunkText(chunks[0])?.match(/^(count|sum|avg|min|max)\($/)?.[1] as AggregateField['fn'] | undefined
  if (!fn) return null
  if (is(chunks[1], Column)) {
    const column = resolveColumn(chunks[1], sources)
    return column ? { fn, column } : null
  }
  if (fn === 'count' && is(chunks[1], SQL) && sqlText(chunks[1]) === '*') return { fn: 'count', column: null }
  return null
}

/** The joined text of an SQL made purely of string chunks, else null. */
function sqlText(sqlExpr: SQL): string | null {
  const texts = sqlExpr.queryChunks.map(chunkText)
  return texts.every((text) => text !== null) ? texts.join('') : null
}

/** With joins, `db.select()` nests plain fields per table: `{ todos: {...}, users: {...} }`. */
function isJoinFieldMap(field: unknown): boolean {
  return (
    typeof field === 'object' &&
    field !== null &&
    !is(field, SQL) &&
    !is(field, SQL.Aliased) &&
    Object.values(field).every((value) => is(value, Column))
  )
}

function foldAggregates(aggregates: AggregateField[], values: [unknown[], number][]): unknown[] {
  return aggregates.map((aggregate, index) => {
    let count = 0
    let sum = 0
    let min: number | undefined
    let max: number | undefined
    for (const [vector, multiplicity] of values) {
      const value = aggregate.column ? vector[index] : true
      if (value === null || value === undefined) continue
      count += multiplicity
      if (aggregate.fn === 'count') continue
      const numeric = typeof value === 'number' ? value : Number(value)
      sum += numeric * multiplicity
      // min/max over retractions would need the full value multiset; tracking the
      // positive side can only over-fire on no-op changes, never miss a real one.
      if (multiplicity > 0) {
        min = min === undefined || numeric < min ? numeric : min
        max = max === undefined || numeric > max ? numeric : max
      }
    }
    switch (aggregate.fn) {
      case 'count':
        return count
      case 'sum':
        return count === 0 ? null : sum
      case 'avg':
        return count === 0 ? null : sum / count
      case 'min':
        return min ?? null
      case 'max':
        return max ?? null
    }
  })
}

/** HAVING conjuncts of the shape `aggregate(...) <op> value`, matched against the
 *  selected aggregates. Any other shape disables HAVING filtering (wider, safe). */
function compileHaving(
  having: SQL | undefined,
  aggregates: AggregateField[],
  sources: Source[],
): ((aggregateValues: unknown[]) => boolean) | null {
  if (!having) return null
  const tests: ((aggregateValues: unknown[]) => boolean)[] = []
  for (const conjunct of conjunctsOf(having)) {
    const chunks = meaningfulChunks(conjunct)
    if (chunks.length !== 3) return null
    const [left, op, right] = chunks
    const aggregate = parseAggregate(left, sources)
    const operator = chunkText(op)?.trim()
    if (!aggregate || !operator || !COMPARATORS[operator]) return null
    const index = aggregates.findIndex(
      (candidate) => candidate.fn === aggregate.fn && candidate.column?.column === aggregate.column?.column,
    )
    // The bound arrives as a Param when compared against a column, but as a bare
    // primitive chunk when the left side is an SQL expression like count().
    const boundValue = is(right, Param) ? right.value : right
    if (index < 0 || (typeof boundValue !== 'number' && typeof boundValue !== 'string')) return null
    const comparator = COMPARATORS[operator]!
    const bound = Number(boundValue)
    if (Number.isNaN(bound)) return null
    tests.push((aggregateValues) => {
      const value = Number(aggregateValues[index])
      return Number.isNaN(value) ? true : comparator(order(value, bound))
    })
  }
  return tests.length === 0 ? null : (values) => tests.every((test) => test(values))
}

const COMPARATORS: Record<string, (order: number) => boolean> = {
  '=': (o) => o === 0,
  '<>': (o) => o !== 0,
  '>': (o) => o > 0,
  '>=': (o) => o >= 0,
  '<': (o) => o < 0,
  '<=': (o) => o <= 0,
}

function order(a: number, b: number): number {
  return a < b ? -1 : a > b ? 1 : 0
}

// ── Ordering (LIMIT/OFFSET via topK) ────────────────────────────────

type Ordering = {
  comparator: (a: (Row | null)[], b: (Row | null)[]) => number
  limit: number | undefined
  offset: number | undefined
}

function compileOrdering(config: SelectConfig, sources: Source[], ctx: CompileContext): Ordering | null {
  // `.orderBy/.limit/.offset` after a set operator land on the last operator entry.
  const effective: Pick<SelectConfig, 'limit' | 'offset' | 'orderBy'> =
    config.setOperators.length > 0
      ? (config.setOperators[config.setOperators.length - 1] as unknown as SelectConfig)
      : config
  if (effective.limit === undefined && effective.offset === undefined) return null

  const terms = (effective.orderBy ?? []).map((expression) => parseOrderTerm(expression, sources))
  if (
    terms.length === 0 ||
    terms.some((term) => term === null) ||
    typeof effective.limit !== 'number' ||
    (effective.offset !== undefined && typeof effective.offset !== 'number')
  ) {
    // LIMIT without a compilable ORDER BY: result membership is undecidable per row;
    // fire on any change that passes the filters instead of maintaining a topK.
    ctx.exact = false
    return null
  }
  const orderTerms = terms as { ref: ColumnRef; direction: 1 | -1 }[]
  return {
    comparator: (a, b) => {
      for (const { ref, direction } of orderTerms) {
        const result = compareTupleValues(a[ref.source]?.[ref.column.name], b[ref.source]?.[ref.column.name])
        if (result !== 0) return result * direction
      }
      return 0
    },
    limit: effective.limit,
    offset: effective.offset as number | undefined,
  }
}

function skipOrdering(config: SelectConfig, ctx: CompileContext): null {
  if (config.limit !== undefined || config.offset !== undefined) ctx.exact = false
  return null
}

/** `asc(col)` / `desc(col)` compile to `sql\`${col} asc\``; a bare column is ascending. */
function parseOrderTerm(expression: unknown, sources: Source[]): { ref: ColumnRef; direction: 1 | -1 } | null {
  if (is(expression, Column)) {
    const ref = resolveColumn(expression, sources)
    return ref ? { ref, direction: 1 } : null
  }
  if (!is(expression, SQL)) return null
  const chunks = meaningfulChunks(expression)
  if (chunks.length !== 2 || !is(chunks[0], Column)) return null
  const direction = chunkText(chunks[1])?.trim()
  const ref = resolveColumn(chunks[0], sources)
  if (!ref || (direction !== 'asc' && direction !== 'desc')) return null
  return { ref, direction: direction === 'asc' ? 1 : -1 }
}

function compareTupleValues(a: unknown, b: unknown): number {
  if (a == null || b == null) return a == null && b == null ? 0 : a == null ? -1 : 1
  return compareValues(a, b) ?? 0
}

// ── DISTINCT and set operators ──────────────────────────────────────

function compileDistinct(config: SelectConfig, ctx: CompileContext): boolean {
  if (config.distinct === true) return true
  if (config.distinct) {
    // DISTINCT ON: non-key column changes alter the result without changing the key,
    // so a keyed distinct would wrongly suppress them. Skip the operator instead.
    ctx.exact = false
  }
  return false
}

function compileSetOperators(config: SelectConfig, ctx: CompileContext): CompiledSelect[] {
  const arms: CompiledSelect[] = []
  for (const operator of config.setOperators) {
    // Only UNION ALL maps 1:1 onto concat; union/intersect/except still fire on any
    // arm change, they just can't suppress no-op duplicates.
    if (operator.type !== 'union' || !operator.isAll) ctx.exact = false
    const armConfig = selectConfigOf(operator.rightSelect)
    if (!armConfig) {
      ctx.exact = false
      continue
    }
    const arm = compileSelect(armConfig)
    for (const table of arm.tables) ctx.tables.add(table)
    if (!arm.exact) ctx.exact = false
    arms.push(arm)
  }
  return arms
}

function selectConfigOf(builder: unknown): SelectConfig | null {
  if (typeof builder !== 'object' || builder === null) return null
  return (
    (builder as { config?: SelectConfig }).config ?? (builder as { _?: { config?: SelectConfig } })._?.config ?? null
  )
}

// ── Chunk and table helpers ─────────────────────────────────────────

function meaningfulChunks(condition: SQL): unknown[] {
  return condition.queryChunks.filter((chunk) => !(is(chunk, StringChunk) && chunk.value.join('') === ''))
}

function chunkText(chunk: unknown): string | null {
  return is(chunk, StringChunk) ? chunk.value.join('') : null
}

function stableKey(values: unknown[]): string {
  return JSON.stringify(values) ?? 'undefined'
}

function collectReferencedTables(config: SelectConfig): Set<string> {
  const tables = new Set<string>()
  collectTables(config.table, tables)
  for (const joinConfig of config.joins ?? []) {
    collectTables(joinConfig.table, tables)
    collectTables(joinConfig.on, tables)
  }
  collectTables(config.where, tables)
  for (const cte of config.withList ?? []) collectTables(cte, tables)
  return tables
}

// ── Graph assembly ──────────────────────────────────────────────────

class GraphBuilder {
  readonly inputs = new Map<string, RootStreamBuilder<Row>>()

  constructor(
    private readonly graph: D2,
    private readonly onDirty: () => void,
  ) {}

  inputFor(table: string): RootStreamBuilder<Row> {
    let input = this.inputs.get(table)
    if (!input) {
      input = this.graph.newInput<Row>()
      this.inputs.set(table, input)
    }
    return input
  }

  observe(stream: IStreamBuilder<unknown>): void {
    stream.pipe(
      output((message) => {
        if (message.type === MessageType.DATA && message.data.collection.getInner().length > 0) this.onDirty()
      }),
    )
  }

  fireOnAnyChange(tables: Iterable<string>): void {
    for (const table of tables) this.observe(this.inputFor(table))
  }
}

function sourceStream(builder: GraphBuilder, source: Source): IStreamBuilder<Row> {
  const { mightMatch } = source.filter
  return builder.inputFor(source.name).pipe(filter((row) => mightMatch(row)))
}

function assemblePipeline(
  builder: GraphBuilder,
  sources: Source[],
  joins: CompiledJoin[],
  grouping: Grouping | null,
  ordering: Ordering | null,
  isDistinct: boolean,
): IStreamBuilder<unknown> {
  // The pipeline value is a tuple of per-source rows, aligned with `sources`.
  let tuples: IStreamBuilder<(Row | null)[]> = sourceStream(builder, sources[0]!).pipe(map((row) => [row]))

  for (const joinPlan of joins) {
    const right = sourceStream(builder, sources[joinPlan.rightSource]!)
    const nulls: (Row | null)[] = new Array(joinPlan.rightSource).fill(null)
    tuples = tuples
      .pipe(keyBy(joinPlan.leftKey))
      .pipe(join(right.pipe(keyBy(joinPlan.rightKey)), joinPlan.type))
      .pipe(map(([, [tuple, row]]) => [...(tuple ?? nulls), row ?? null]))
  }

  if (grouping) {
    const { groupKey, rowVector, reducer, having } = grouping
    let groups: IStreamBuilder<[string, unknown[]]> = tuples
      .pipe(map((tuple) => [groupKey(tuple), rowVector(tuple)] as [string, unknown[]]))
      .pipe(reduce(reducer))
    if (having) groups = groups.pipe(filter(([, aggregateValues]) => having(aggregateValues)))
    return groups
  }

  if (ordering) {
    return tuples
      .pipe(keyBy(() => 'all'))
      .pipe(topK(ordering.comparator, { limit: ordering.limit, offset: ordering.offset }))
  }

  if (isDistinct) {
    return tuples.pipe(keyBy((tuple) => stableKey(tuple))).pipe(distinct())
  }

  return tuples
}
