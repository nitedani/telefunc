export { createQueryTracker }
export type { QueryTracker, SelectBuilderLike }

import { useInvalidate } from '@telefunc/tanstack-query/server'
import { REQUEST_CONTEXT, getRawContext, type RequestContext } from 'telefunc'
import type { SQL, Table } from 'drizzle-orm'
import type { ChangeRouter, RowSeeder } from './change-router.js'
import { compileQuery, type SelectConfig } from './d2ts-compiler.js'
import { toDbRow } from './row.js'

type QueryTracker = {
  /** Called when an awaited select starts executing. Inside a telefunc request it
   *  subscribes the query's graph BEFORE the read runs (a change landing between
   *  read and subscribe is then never lost) and wires it to the request's
   *  invalidation trigger; anywhere else the select just runs. */
  track: (builder: SelectBuilderLike) => void
}

type SelectBuilderLike = {
  config: SelectConfig
  toSQL: () => { sql: string; params: unknown[] }
}

/** The unwrapped db, for hydration reads (they must not track themselves). */
type SeedExecutor = {
  select: () => {
    from: (table: Table) => { where: (condition: SQL | undefined) => PromiseLike<Record<string, unknown>[]> }
  }
}

function createQueryTracker(router: ChangeRouter, seedDb: SeedExecutor): QueryTracker {
  const tracked = new WeakSet<object>()

  const seed: RowSeeder = async (plan) => {
    const rows = await seedDb.select().from(plan.table).where(plan.where)
    return rows.map((row) => toDbRow(plan.table, row))
  }

  return {
    track(builder) {
      if (tracked.has(builder)) return
      tracked.add(builder)

      const raw = getRawContext()
      const requestContext = raw?.[REQUEST_CONTEXT] as RequestContext | undefined
      if (!raw || !requestContext) return

      const compiled = compileQuery(builder.config, builder.toSQL())
      if (!compiled) return

      const invalidate = useInvalidate()
      // A fresh closure per subscription: the router's subscriber set must not
      // collapse two queries of the same request sharing one trigger.
      const unsubscribe = router.subscribe(compiled, seed, () => invalidate())
      requestContext.onClose(unsubscribe)
    },
  }
}
