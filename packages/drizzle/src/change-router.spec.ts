import { describe, expect, it, vi } from 'vitest'
import { boolean, integer, pgTable, text } from 'drizzle-orm/pg-core'
import { drizzle } from 'drizzle-orm/node-postgres'
import { and, eq } from 'drizzle-orm'
import { ChangeRouter, type RowSeeder, type WriteSet } from './change-router.js'
import { compileQuery, type CompiledQuery, type SelectConfig } from './d2ts-compiler.js'
import type { Row } from './row.js'

const users = pgTable('users', {
  id: integer('id').primaryKey(),
  name: text('name'),
  teamId: integer('team_id'),
})

const todos = pgTable('todos', {
  id: integer('id').primaryKey(),
  text: text('text'),
  done: boolean('done'),
  userId: integer('user_id'),
})

const db = drizzle.mock()

function compile(builder: { toSQL(): { sql: string; params: unknown[] } }): CompiledQuery {
  const compiled = compileQuery((builder as unknown as { config: SelectConfig }).config, builder.toSQL())
  if (!compiled) throw new Error('expected query to be compilable')
  return compiled
}

const noSeed: RowSeeder = () => Promise.resolve([])

function insert(table: string, rows: Row[]): WriteSet {
  return [{ table, added: rows, removed: [] }]
}

const tick = () => new Promise((resolve) => setTimeout(resolve, 0))

describe('ChangeRouter', () => {
  it('routes matching changes to subscribers, drops non-matching ones', () => {
    const router = new ChangeRouter({ fanout: false })
    const fired = vi.fn()
    router.subscribe(compile(db.select().from(todos).where(eq(todos.userId, 5))), noSeed, fired)

    router.ingest(insert('todos', [{ id: 1, user_id: 3 }]))
    expect(fired).not.toHaveBeenCalled()
    router.ingest(insert('todos', [{ id: 2, user_id: 5 }]))
    expect(fired).toHaveBeenCalledTimes(1)
    router.ingest(insert('users', [{ id: 1 }]))
    expect(fired).toHaveBeenCalledTimes(1)
  })

  it('identical queries share one graph, every subscriber fires', () => {
    const router = new ChangeRouter({ fanout: false })
    const first = vi.fn()
    const second = vi.fn()
    router.subscribe(compile(db.select().from(todos).where(eq(todos.userId, 5))), noSeed, first)
    router.subscribe(compile(db.select().from(todos).where(eq(todos.userId, 5))), noSeed, second)

    router.ingest(insert('todos', [{ id: 1, user_id: 5 }]))
    expect(first).toHaveBeenCalledTimes(1)
    expect(second).toHaveBeenCalledTimes(1)
  })

  it('coalesces a multi-row batch into one invalidation per graph', () => {
    const router = new ChangeRouter({ fanout: false })
    const fired = vi.fn()
    router.subscribe(compile(db.select().from(todos)), noSeed, fired)

    router.ingest(insert('todos', [{ id: 1 }, { id: 2 }, { id: 3 }]))
    expect(fired).toHaveBeenCalledTimes(1)
  })

  it('destroys the graph when the last subscriber leaves', () => {
    const router = new ChangeRouter({ fanout: false })
    const first = vi.fn()
    const second = vi.fn()
    const unsubFirst = router.subscribe(compile(db.select().from(todos)), noSeed, first)
    const unsubSecond = router.subscribe(compile(db.select().from(todos)), noSeed, second)

    unsubFirst()
    router.ingest(insert('todos', [{ id: 1 }]))
    expect(first).not.toHaveBeenCalled()
    expect(second).toHaveBeenCalledTimes(1)

    unsubSecond()
    router.ingest(insert('todos', [{ id: 2 }]))
    expect(second).toHaveBeenCalledTimes(1)
  })

  it('hydrates join graphs and buffers writes that arrive mid-hydration', async () => {
    const router = new ChangeRouter({ fanout: false })
    const fired = vi.fn()
    let releaseSeed!: () => void
    const gate = new Promise<void>((resolve) => {
      releaseSeed = resolve
    })
    const seed: RowSeeder = async (plan) => {
      await gate
      // pre-existing user 42 on team 7; the todos side has no pre-existing rows
      return plan.table === users ? [{ id: 42, name: 'eva', team_id: 7 }] : []
    }

    const query = compile(
      db.select().from(todos).innerJoin(users, eq(todos.userId, users.id)).where(eq(users.teamId, 7)),
    )
    router.subscribe(query, seed, fired)

    // arrives while hydration is still pending: must be buffered, not lost
    router.ingest(insert('todos', [{ id: 1, user_id: 42 }]))
    expect(fired).not.toHaveBeenCalled()

    releaseSeed()
    await tick()
    expect(fired).toHaveBeenCalledTimes(1)

    // joining against the hydrated row keeps working
    router.ingest(insert('todos', [{ id: 2, user_id: 42 }]))
    expect(fired).toHaveBeenCalledTimes(2)
    router.ingest(insert('todos', [{ id: 3, user_id: 999 }]))
    expect(fired).toHaveBeenCalledTimes(2)
  })

  it('hydration itself does not invalidate', async () => {
    const router = new ChangeRouter({ fanout: false })
    const fired = vi.fn()
    const seed: RowSeeder = (plan) =>
      Promise.resolve(plan.table === users ? [{ id: 1, team_id: 7 }] : [{ id: 1, user_id: 1, done: 0 }])
    router.subscribe(compile(db.select().from(todos).innerJoin(users, eq(todos.userId, users.id))), seed, fired)
    await tick()
    expect(fired).not.toHaveBeenCalled()
  })

  it('lossy changes force a fire and permanently taint stateful graphs', () => {
    const router = new ChangeRouter({ fanout: false })

    const filterFired = vi.fn()
    router.subscribe(compile(db.select().from(todos).where(eq(todos.userId, 5))), noSeed, filterFired)
    // update with unknown old image, new row does not match the filter
    router.ingest([{ table: 'todos', added: [{ id: 1, user_id: 3 }], removed: [], lossy: true }])
    expect(filterFired).toHaveBeenCalledTimes(1)
    // stateless graphs recover: the next non-matching write stays quiet
    router.ingest(insert('todos', [{ id: 2, user_id: 3 }]))
    expect(filterFired).toHaveBeenCalledTimes(1)

    const joinFired = vi.fn()
    router.subscribe(
      compile(
        db
          .select()
          .from(todos)
          .innerJoin(users, and(eq(todos.userId, users.id), eq(users.teamId, 1))),
      ),
      noSeed,
      joinFired,
    )
    router.ingest([{ table: 'todos', added: [], removed: [], lossy: true }])
    expect(joinFired).toHaveBeenCalledTimes(1)
    // tainted: every later dependency change fires, matching or not
    router.ingest(insert('todos', [{ id: 9, user_id: 999 }]))
    expect(joinFired).toHaveBeenCalledTimes(2)
  })

  it('a removal racing hydration taints the graph instead of under-counting', async () => {
    const router = new ChangeRouter({ fanout: false })
    const fired = vi.fn()
    let releaseSeed!: () => void
    const gate = new Promise<void>((resolve) => {
      releaseSeed = resolve
    })
    const seed: RowSeeder = async () => {
      await gate
      // the seed read runs after the racing delete: the row is already gone
      return []
    }
    router.subscribe(
      compile(db.select().from(todos).innerJoin(users, eq(todos.userId, users.id))),
      seed,
      fired,
    )

    // a delete commits while hydration is in flight; its retraction targets a row
    // the seed never contained
    router.ingest([{ table: 'users', added: [], removed: [{ id: 1, team_id: 7 }] }])
    releaseSeed()
    await tick()

    // tainted: any later dependency change fires, the join state is not trusted
    router.ingest(insert('todos', [{ id: 9, user_id: 12345 }]))
    expect(fired).toHaveBeenCalled()
  })

  it('fanout: published writes come back through Broadcast, and the key is released on destroy', () => {
    const router = new ChangeRouter({ fanout: true })
    const fired = vi.fn()
    const unsubscribe = router.subscribe(compile(db.select().from(todos).where(eq(todos.done, false))), noSeed, fired)

    router.publish(insert('todos', [{ id: 1, done: false }]))
    expect(fired).toHaveBeenCalledTimes(1)

    unsubscribe()
    router.publish(insert('todos', [{ id: 2, done: false }]))
    expect(fired).toHaveBeenCalledTimes(1)
  })
})
