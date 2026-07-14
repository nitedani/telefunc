import { describe, expect, it } from 'vitest'
import { D2 } from '@electric-sql/d2ts'
import { and, asc, count, eq, gt, sql } from 'drizzle-orm'
import { boolean, integer, pgTable, text } from 'drizzle-orm/pg-core'
import { drizzle } from 'drizzle-orm/node-postgres'
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

function compile(builder: { config?: unknown; toSQL(): { sql: string; params: unknown[] } }): CompiledQuery {
  const compiled = compileQuery((builder as { config: SelectConfig }).config, builder.toSQL())
  if (!compiled) throw new Error('expected query to be compilable')
  return compiled
}

/** Drives a compiled graph the way the change router does: one version per batch,
 *  every input's frontier advances each batch, fires count as dirty runs. */
function instantiate(compiled: CompiledQuery) {
  const graph = new D2({ initialFrontier: 0 })
  let fires = 0
  const inputs = compiled.build(graph, () => fires++)
  graph.finalize()
  let version = 0

  return {
    inputs: [...inputs.keys()],
    feed(changes: Record<string, [Row, number][]>) {
      const before = fires
      for (const [table, rows] of Object.entries(changes)) {
        const input = inputs.get(table)
        if (!input) throw new Error(`no input for table ${table}`)
        input.sendData(version, rows)
      }
      version++
      for (const input of inputs.values()) input.sendFrontier(version)
      graph.run()
      return fires > before
    },
  }
}

describe('compileQuery', () => {
  it('plain filter: fires only for rows matching the WHERE', () => {
    const query = compile(db.select().from(todos).where(eq(todos.userId, 5)))
    expect(query.exact).toBe(true)
    expect(query.stateful).toBe(false)
    expect(query.hydrate).toEqual([])
    expect([...query.tables]).toEqual(['todos'])

    const graph = instantiate(query)
    expect(graph.feed({ todos: [[{ id: 1, user_id: 3 }, 1]] })).toBe(false)
    expect(graph.feed({ todos: [[{ id: 2, user_id: 5 }, 1]] })).toBe(true)
    // deleting the matching row fires too
    expect(graph.feed({ todos: [[{ id: 2, user_id: 5 }, -1]] })).toBe(true)
  })

  it('identity: same SQL and params share a key, different params do not', () => {
    const a = compile(db.select().from(todos).where(eq(todos.userId, 5)))
    const b = compile(db.select().from(todos).where(eq(todos.userId, 5)))
    const c = compile(db.select().from(todos).where(eq(todos.userId, 6)))
    expect(a.key).toBe(b.key)
    expect(a.key).not.toBe(c.key)
  })

  it('equi-join: hydrated state makes inserts joining pre-existing rows fire', () => {
    const query = compile(
      db
        .select()
        .from(todos)
        .innerJoin(users, eq(todos.userId, users.id))
        .where(and(eq(users.teamId, 7), eq(todos.done, false))),
    )
    expect(query.stateful).toBe(true)
    expect(query.hydrate.map((plan) => plan.where !== undefined)).toEqual([true, true])
    expect([...query.tables].sort()).toEqual(['todos', 'users'])

    const graph = instantiate(query)
    // hydration seed: one pre-existing user on team 7
    graph.feed({ users: [[{ id: 42, name: 'eva', team_id: 7 }, 1]] })
    // a todo for that user joins the hydrated row
    expect(graph.feed({ todos: [[{ id: 1, done: false, user_id: 42 }, 1]] })).toBe(true)
    // a todo for an unknown user matches nothing
    expect(graph.feed({ todos: [[{ id: 2, done: false, user_id: 999 }, 1]] })).toBe(false)
    // pushdown: a done todo is filtered before the join
    expect(graph.feed({ todos: [[{ id: 3, done: true, user_id: 42 }, 1]] })).toBe(false)
  })

  it('aggregates: count fires on group changes, HAVING filters groups', () => {
    const query = compile(
      db.select({ userId: todos.userId, n: count() }).from(todos).groupBy(todos.userId).having(gt(count(), 1)),
    )
    const graph = instantiate(query)
    // first row: count becomes 1, HAVING count > 1 keeps the group out
    expect(graph.feed({ todos: [[{ id: 1, user_id: 5 }, 1]] })).toBe(false)
    // second row: count becomes 2, group enters the result
    expect(graph.feed({ todos: [[{ id: 2, user_id: 5 }, 1]] })).toBe(true)
  })

  it('partial compilation: unsupported field skipped, WHERE still filters', () => {
    const query = compile(
      db
        .select({ id: todos.id, rank: sql<number>`row_number() over (order by ${todos.id})` })
        .from(todos)
        .where(eq(todos.userId, 5)),
    )
    expect(query.exact).toBe(false)

    const graph = instantiate(query)
    expect(graph.feed({ todos: [[{ id: 1, user_id: 3 }, 1]] })).toBe(false)
    expect(graph.feed({ todos: [[{ id: 2, user_id: 5 }, 1]] })).toBe(true)
  })

  it('limit + orderBy compiles to a topK over the filtered rows', () => {
    const query = compile(db.select().from(todos).where(eq(todos.done, false)).orderBy(asc(todos.id)).limit(2))
    expect(query.exact).toBe(true)

    const graph = instantiate(query)
    expect(graph.feed({ todos: [[{ id: 10, done: false }, 1]] })).toBe(true)
    expect(graph.feed({ todos: [[{ id: 20, done: false }, 1]] })).toBe(true)
    // a non-matching row never reaches the topK
    expect(graph.feed({ todos: [[{ id: 1, done: true }, 1]] })).toBe(false)
  })

  it('limit without orderBy degrades to filter-only', () => {
    const query = compile(db.select().from(todos).where(eq(todos.done, false)).limit(5))
    expect(query.exact).toBe(false)
    const graph = instantiate(query)
    expect(graph.feed({ todos: [[{ id: 1, done: false }, 1]] })).toBe(true)
    expect(graph.feed({ todos: [[{ id: 2, done: true }, 1]] })).toBe(false)
  })

  it('union: both arms contribute tables and fire', () => {
    const query = compile(
      db
        .select({ id: todos.id })
        .from(todos)
        .where(eq(todos.done, false))
        .unionAll(db.select({ id: users.id }).from(users).where(eq(users.teamId, 1))),
    )
    expect([...query.tables].sort()).toEqual(['todos', 'users'])
    expect(query.exact).toBe(true)

    const graph = instantiate(query)
    expect(graph.feed({ todos: [[{ id: 1, done: false }, 1]] })).toBe(true)
    expect(graph.feed({ users: [[{ id: 2, team_id: 1 }, 1]] })).toBe(true)
    expect(graph.feed({ users: [[{ id: 3, team_id: 2 }, 1]] })).toBe(false)
  })

  it('IN (subquery): subquery tables fire on any change', () => {
    const teamUserIds = db.select({ id: users.id }).from(users).where(eq(users.teamId, 7))
    const query = compile(db.select().from(todos).where(sql`${todos.userId} in ${teamUserIds}`))
    expect(query.exact).toBe(false)
    expect(query.tables.has('users')).toBe(true)

    const graph = instantiate(query)
    expect(graph.feed({ users: [[{ id: 1, team_id: 99 }, 1]] })).toBe(true)
  })

  it('distinct suppresses duplicate inserts', () => {
    const query = compile(db.selectDistinct({ userId: todos.userId }).from(todos).where(eq(todos.done, false)))
    const graph = instantiate(query)
    expect(graph.feed({ todos: [[{ user_id: 5, done: false }, 1]] })).toBe(true)
    // structurally identical row: distinct sees no output change
    expect(graph.feed({ todos: [[{ user_id: 5, done: false }, 1]] })).toBe(false)
  })

  it('non-equi join falls back to per-table filters', () => {
    const query = compile(db.select().from(todos).innerJoin(users, gt(todos.userId, users.id)))
    expect(query.exact).toBe(false)
    expect(query.stateful).toBe(false)
    const graph = instantiate(query)
    expect(graph.feed({ todos: [[{ id: 1 }, 1]] })).toBe(true)
    expect(graph.feed({ users: [[{ id: 1 }, 1]] })).toBe(true)
  })

  it('selecting from a subquery degrades to firing on its tables', () => {
    const inner = db.select().from(todos).where(eq(todos.done, false)).as('inner')
    const query = compile(db.select().from(inner))
    expect(query.exact).toBe(false)
    expect(query.tables.has('todos')).toBe(true)
    const graph = instantiate(query)
    expect(graph.feed({ todos: [[{ id: 1, done: true }, 1]] })).toBe(true)
  })
})
