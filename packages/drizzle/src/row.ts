export { tableNameOf, toDbRow, sortedRow, MISSING }
export type { Row }

import { getTableColumns, getTableName, type Table } from 'drizzle-orm'

/** A table row keyed by database column names. A missing key means the value is
 *  unknown (e.g. a database default the writer never saw); predicates treat unknown
 *  as a match so unknowns can only over-invalidate, never under-invalidate. */
type Row = Record<string, unknown>

/** Sentinel for "column value unknown" during predicate evaluation. */
const MISSING: unique symbol = Symbol('missing')

const IS_ALIAS = Symbol.for('drizzle:IsAlias')
const ORIGINAL_NAME = Symbol.for('drizzle:OriginalName')

/** Real table name; aliased tables (self-joins) resolve to their original name,
 *  which is the name write events arrive under. */
function tableNameOf(table: Table): string {
  const symbols = table as unknown as Record<symbol, unknown>
  return symbols[IS_ALIAS] ? (symbols[ORIGINAL_NAME] as string) : getTableName(table)
}

/** Translate a TS-keyed object (drizzle result row, `.values()` input, `.set()` input)
 *  into a `Row` keyed by database column names. Keys are sorted so structurally equal
 *  rows serialize identically regardless of producer (d2ts consolidates by value). */
function toDbRow(table: Table, tsRow: Record<string, unknown>): Row {
  const columns = getTableColumns(table)
  const row: Row = {}
  for (const tsKey of Object.keys(columns).sort()) {
    if (tsKey in tsRow) row[columns[tsKey]!.name] = tsRow[tsKey]
  }
  return row
}

/** Canonical key order for rows that arrive already DB-keyed (CDC events), so they
 *  consolidate against rows produced by `toDbRow`. */
function sortedRow(row: Row): Row {
  const sorted: Row = {}
  for (const key of Object.keys(row).sort()) sorted[key] = row[key]
  return sorted
}
