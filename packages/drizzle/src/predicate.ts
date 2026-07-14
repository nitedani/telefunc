export { compileCondition, conjunctsOf, alwaysMatch, compareValues, collectTables }
export type { CompiledCondition }

import { Column, Param, Placeholder, SQL, StringChunk, Subquery, is, isTable } from 'drizzle-orm'
import { MISSING, tableNameOf, type Row } from './row.js'

/** A compiled WHERE/ON condition. Evaluation is three-valued (true / false / unknown);
 *  `mightMatch` folds it conservatively: `false` only when the row provably fails. */
type CompiledCondition = {
  mightMatch: (row: Row) => boolean
  /** Real table names referenced by the condition (columns and subqueries). */
  tables: Set<string>
  /** False when some atom didn't parse; such atoms always match. */
  exact: boolean
}

function alwaysMatch(): CompiledCondition {
  return { mightMatch: () => true, tables: new Set(), exact: true }
}

function compileCondition(condition: SQL | undefined): CompiledCondition {
  if (!condition) return alwaysMatch()
  const ctx: ParseContext = { tables: new Set(), exact: true }
  const evaluate = parseExpression(tokenize(condition.queryChunks, ctx), ctx)
  return { mightMatch: (row) => evaluate(row) !== false, tables: ctx.tables, exact: ctx.exact }
}

/** Decompose a condition into its top-level AND conjuncts, so each conjunct can be
 *  pushed down to the single table it references. A top-level OR (or `between`, which
 *  carries a literal " and ") stays one conjunct. */
function conjunctsOf(condition: SQL): SQL[] {
  const isText = (chunk: unknown, text: string) => is(chunk, StringChunk) && chunk.value.join('') === text
  let chunks = condition.queryChunks.filter((chunk) => !isText(chunk, ''))
  if (chunks.length >= 2 && isText(chunks[0], '(') && isText(chunks[chunks.length - 1], ')')) {
    chunks = chunks.slice(1, -1)
  }
  // and() wraps its joined operands in one nested SQL: `(`, SQL[a and b and c], `)`
  if (chunks.length === 1 && is(chunks[0], SQL)) return conjunctsOf(chunks[0])

  const groups: unknown[][] = [[]]
  for (const chunk of chunks) {
    if (isText(chunk, ' and ')) groups.push([])
    else groups[groups.length - 1]!.push(chunk)
  }
  if (groups.length === 1 || groups.some((group) => group.length !== 1 || !is(group[0], SQL))) return [condition]
  return groups.flatMap((group) => conjunctsOf(group[0] as SQL))
}

// ── Tokenizing ──────────────────────────────────────────────────────
// drizzle conditions carry no AST, only `SQL.queryChunks`: interleaved string
// fragments, Column objects, and Param-wrapped values. Sub-conditions (the
// operands of and/or/not) arrive as nested SQL chunks, which preserves grouping.

type ParseContext = { tables: Set<string>; exact: boolean }

/** Value of an atom operand that cannot be known at compile time (placeholder,
 *  subquery, raw SQL). Comparisons against it evaluate to unknown. */
const OPAQUE: unique symbol = Symbol('opaque')

type Token =
  | { kind: 'text'; text: string }
  | { kind: 'column'; column: Column }
  | { kind: 'value'; value: unknown }
  | { kind: 'values'; values: unknown[] }
  | { kind: 'nested'; evaluate: Evaluate }

type Evaluate = (row: Row) => boolean | undefined

function tokenize(chunks: unknown[], ctx: ParseContext): Token[] {
  const tokens: Token[] = []
  for (const chunk of chunks) {
    if (is(chunk, StringChunk)) {
      const text = chunk.value.join('')
      if (text !== '') tokens.push({ kind: 'text', text })
    } else if (is(chunk, Column)) {
      ctx.tables.add(tableNameOf(chunk.table))
      tokens.push({ kind: 'column', column: chunk })
    } else if (is(chunk, Param)) {
      tokens.push({ kind: 'value', value: chunk.value })
    } else if (is(chunk, SQL)) {
      tokens.push({ kind: 'nested', evaluate: parseExpression(tokenize(chunk.queryChunks, ctx), ctx) })
    } else if (Array.isArray(chunk)) {
      // `inArray()` embeds the list as a raw array of Params
      tokens.push({ kind: 'values', values: chunk.map((item) => (is(item, Param) ? item.value : OPAQUE)) })
    } else if (chunk === undefined) {
      // drizzle's own builder skips undefined chunks
    } else if (is(chunk, Placeholder)) {
      tokens.push({ kind: 'value', value: OPAQUE })
    } else if (typeof chunk === 'object' && chunk !== null) {
      // Subquery, embedded select builder, Table, View, SQL.Aliased, pgEnum, ... —
      // not evaluable per row, but any tables inside become dependencies.
      collectTables(chunk, ctx.tables)
      ctx.exact = false
      tokens.push({ kind: 'value', value: OPAQUE })
    } else {
      // bare primitive interpolated without a Param wrapper (like/ilike patterns)
      tokens.push({ kind: 'value', value: chunk })
    }
  }
  return tokens
}

/** Collect the real table names reachable from any drizzle SQL-ish value:
 *  conditions, subqueries, embedded select builders, columns, tables.
 *  Leaf SQLWrappers answer `getSQL()` with an SQL containing themselves, so the
 *  walk keeps a seen set to terminate on that self-reference. */
function collectTables(value: unknown, into: Set<string>, seen = new Set<object>()): void {
  if (typeof value !== 'object' || value === null || seen.has(value)) return
  seen.add(value)
  if (is(value, Column)) {
    into.add(tableNameOf(value.table))
  } else if (isTable(value)) {
    into.add(tableNameOf(value))
  } else if (is(value, Subquery)) {
    // drizzle records used tables as "schema.name"; write events arrive under the bare name
    const used = (value as unknown as { _?: { usedTables?: string[] } })._?.usedTables ?? []
    for (const name of used) into.add(name.slice(name.indexOf('.') + 1))
  } else if (is(value, SQL.Aliased)) {
    collectTables(value.sql, into, seen)
  } else if (is(value, SQL)) {
    for (const chunk of value.queryChunks) collectTables(chunk, into, seen)
  } else if (is(value, Param)) {
    // terminal; a Param's encoder column does not make its table a dependency
  } else if ('getSQL' in value && typeof value.getSQL === 'function') {
    collectTables((value as { getSQL: () => SQL }).getSQL(), into, seen)
  }
}

// ── Parsing ─────────────────────────────────────────────────────────

function parseExpression(tokens: Token[], ctx: ParseContext): Evaluate {
  const inner = stripParens(tokens)

  // Atoms first: `between` contains a literal " and " that must not be split on.
  const atom = parseAtom(inner)
  if (atom) return atom

  for (const [separator, combine] of [
    [' or ', combineOr],
    [' and ', combineAnd],
  ] as const) {
    const groups = splitOn(inner, separator)
    if (groups.length > 1) {
      return combine(groups.map((group) => parseExpression(group, ctx)))
    }
  }

  if (inner[0]?.kind === 'text' && inner[0].text === 'not ') {
    const operand = parseExpression(inner.slice(1), ctx)
    return (row) => invert(operand(row))
  }
  if (inner.length === 1 && inner[0]!.kind === 'nested') return inner[0]!.evaluate

  // Unrecognized shape (raw sql``, exotic operators): always match.
  ctx.exact = false
  return () => undefined
}

function stripParens(tokens: Token[]): Token[] {
  const first = tokens[0]
  const last = tokens[tokens.length - 1]
  if (tokens.length >= 2 && first?.kind === 'text' && last?.kind === 'text') {
    const open = first.text.endsWith('(')
    const close = last.text.startsWith(')')
    if (open && close) {
      const stripped: Token[] = tokens.slice(1, -1)
      if (first.text !== '(') stripped.unshift({ kind: 'text', text: first.text.slice(0, -1) })
      if (last.text !== ')') stripped.push({ kind: 'text', text: last.text.slice(1) })
      return stripped
    }
  }
  return tokens
}

function splitOn(tokens: Token[], separator: string): Token[][] {
  const groups: Token[][] = [[]]
  for (const token of tokens) {
    if (token.kind === 'text' && token.text === separator) groups.push([])
    else groups[groups.length - 1]!.push(token)
  }
  return groups
}

// ── Atoms ───────────────────────────────────────────────────────────

const COMPARATORS: Record<string, (order: number) => boolean> = {
  '=': (order) => order === 0,
  '<>': (order) => order !== 0,
  '>': (order) => order > 0,
  '>=': (order) => order >= 0,
  '<': (order) => order < 0,
  '<=': (order) => order <= 0,
}

function parseAtom(tokens: Token[]): Evaluate | null {
  const only = tokens.length === 1 ? tokens[0] : undefined
  if (only?.kind === 'text') {
    // `inArray(col, [])` compiles to literal "false", `notInArray(col, [])` to "true"
    if (only.text === 'false') return () => false
    if (only.text === 'true') return () => true
    return null
  }

  const [left, op, right, ...rest] = tokens
  if (left?.kind !== 'column') return null
  const column = left.column

  if (op?.kind !== 'text') return null
  const operator = op.text.trim()

  if (op.text === ' is null' && right === undefined) {
    return (row) => mapKnown(columnValue(row, column), (value) => value === null)
  }
  if (op.text === ' is not null' && right === undefined) {
    return (row) => mapKnown(columnValue(row, column), (value) => value !== null)
  }

  const comparator = COMPARATORS[operator]
  if (comparator && rest.length === 0 && right) {
    const rightValue = operandReader(right)
    if (!rightValue) return null
    return (row) =>
      compareNullable(columnValue(row, column), rightValue(row), (order) =>
        order === undefined ? undefined : comparator(order),
      )
  }

  if ((operator === 'in' || operator === 'not in') && right?.kind === 'values' && rest.length === 0) {
    const negate = operator === 'not in'
    const { values } = right
    return (row) => {
      const result = mapKnown(columnValue(row, column), (value) => {
        if (value === null) return false
        let sawOpaque = false
        for (const candidate of values) {
          if (candidate === OPAQUE) sawOpaque = true
          else if (compareValues(value, candidate) === 0) return true
        }
        return sawOpaque ? undefined : false
      })
      return negate ? invert(result) : result
    }
  }

  if (['like', 'not like', 'ilike', 'not ilike'].includes(operator) && right && rest.length === 0) {
    const pattern = right.kind === 'value' ? right.value : OPAQUE
    if (typeof pattern !== 'string') return null
    const regex = likeToRegExp(pattern, operator.endsWith('ilike'))
    const negate = operator.startsWith('not ')
    return (row) => {
      const result = mapKnown(columnValue(row, column), (value) =>
        value === null ? false : typeof value === 'string' ? regex.test(value) : undefined,
      )
      return negate ? invert(result) : result
    }
  }

  if (op.text === ' between ' && right && rest.length === 2 && rest[0]!.kind === 'text' && rest[0]!.text === ' and ') {
    const low = operandReader(right)
    const high = operandReader(rest[1]!)
    if (!low || !high) return null
    return (row) => {
      const value = columnValue(row, column)
      return combineAndValues([
        compareNullable(value, low(row), (order) => (order === undefined ? undefined : order >= 0)),
        compareNullable(value, high(row), (order) => (order === undefined ? undefined : order <= 0)),
      ])
    }
  }

  return null
}

/** An atom operand: a bound value or another column of the row. */
function operandReader(token: Token): ((row: Row) => unknown) | null {
  if (token.kind === 'value') return () => token.value
  if (token.kind === 'column') return (row) => columnValue(row, token.column)
  return null
}

function columnValue(row: Row, column: Column): unknown {
  return column.name in row ? row[column.name] : MISSING
}

// ── Three-valued evaluation ─────────────────────────────────────────
// `undefined` means unknown. SQL semantics: comparing against NULL is not a match;
// comparing against an unknown value might be.

function mapKnown(value: unknown, f: (value: unknown) => boolean | undefined): boolean | undefined {
  return value === MISSING || value === OPAQUE ? undefined : f(value)
}

function compareNullable(
  a: unknown,
  b: unknown,
  f: (order: number | undefined) => boolean | undefined,
): boolean | undefined {
  if (a === MISSING || a === OPAQUE || b === MISSING || b === OPAQUE) return undefined
  if (a === null || b === null) return false
  return f(compareValues(a, b))
}

function invert(value: boolean | undefined): boolean | undefined {
  return value === undefined ? undefined : !value
}

function combineAndValues(values: (boolean | undefined)[]): boolean | undefined {
  let result: boolean | undefined = true
  for (const value of values) {
    if (value === false) return false
    if (value === undefined) result = undefined
  }
  return result
}

function combineAnd(operands: Evaluate[]): Evaluate {
  return (row) => combineAndValues(operands.map((operand) => operand(row)))
}

function combineOr(operands: Evaluate[]): Evaluate {
  return (row) => {
    let result: boolean | undefined = false
    for (const operand of operands) {
      const value = operand(row)
      if (value === true) return true
      if (value === undefined) result = undefined
    }
    return result
  }
}

/** Total order over driver-level values, bridging producer differences:
 *  CDC delivers pg-parsed values, ORM capture delivers the user's JS values,
 *  SQLite stores booleans as 0/1. Returns undefined for incomparable pairs. */
function compareValues(a: unknown, b: unknown): number | undefined {
  if (a instanceof Date || b instanceof Date) {
    const aTime = toTime(a)
    const bTime = toTime(b)
    return aTime === undefined || bTime === undefined ? undefined : order(aTime, bTime)
  }
  if (typeof a === 'boolean' || typeof b === 'boolean') {
    const aNum = toBoolNumber(a)
    const bNum = toBoolNumber(b)
    return aNum === undefined || bNum === undefined ? undefined : order(aNum, bNum)
  }
  if (typeof a === 'bigint' || typeof b === 'bigint') {
    try {
      return order(BigInt(a as string | number | bigint), BigInt(b as string | number | bigint))
    } catch {
      return undefined
    }
  }
  if (typeof a === 'number' || typeof b === 'number') {
    const aNum = toNumber(a)
    const bNum = toNumber(b)
    return aNum === undefined || bNum === undefined ? undefined : order(aNum, bNum)
  }
  if (typeof a === 'string' && typeof b === 'string') return order(a, b)
  if (typeof a === 'object' && typeof b === 'object') {
    // pg arrays, json columns: equality is decidable, ordering is not
    return JSON.stringify(a) === JSON.stringify(b) ? 0 : undefined
  }
  return undefined
}

function order<T extends number | string | bigint>(a: T, b: T): number {
  return a < b ? -1 : a > b ? 1 : 0
}

function toTime(value: unknown): number | undefined {
  if (value instanceof Date) return value.getTime()
  if (typeof value === 'string' || typeof value === 'number') {
    const time = new Date(value).getTime()
    return Number.isNaN(time) ? undefined : time
  }
  return undefined
}

function toBoolNumber(value: unknown): number | undefined {
  if (typeof value === 'boolean') return Number(value)
  if (value === 0 || value === 1) return value
  return undefined
}

function toNumber(value: unknown): number | undefined {
  if (typeof value === 'number') return value
  if (typeof value === 'string' && value !== '' && Number.isFinite(Number(value))) return Number(value)
  return undefined
}

function likeToRegExp(pattern: string, caseInsensitive: boolean): RegExp {
  const escaped = pattern
    .replace(/[.*+?^${}()|[\]\\]/g, '\\$&')
    .replace(/%/g, '.*')
    .replace(/_/g, '.')
  return new RegExp(`^${escaped}$`, caseInsensitive ? 'is' : 's')
}
