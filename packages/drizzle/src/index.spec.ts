import 'telefunc/async_hooks'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import Database from 'better-sqlite3'
import { drizzle } from 'drizzle-orm/better-sqlite3'
import { integer, sqliteTable, text } from 'drizzle-orm/sqlite-core'
import { count, eq, sql } from 'drizzle-orm'
import { REQUEST_CONTEXT, getRawContext, provideTelefuncContext } from 'telefunc'
import { reactiveDrizzle } from './index.js'
import { createWriteTracker } from './write-tracker.js'
import type { WriteSet } from './change-router.js'

const users = sqliteTable('users', {
  id: integer('id').primaryKey({ autoIncrement: true }),
  name: text('name').notNull(),
  teamId: integer('team_id').notNull(),
})

const todos = sqliteTable('todos', {
  id: integer('id').primaryKey({ autoIncrement: true }),
  text: text('text').notNull(),
  done: integer('done', { mode: 'boolean' }).notNull().default(false),
  userId: integer('user_id').notNull(),
})

const INVALIDATE_TRIGGER = Symbol.for('telefunc.tanstackQuery.invalidateTrigger')

type Trigger = { pending: boolean }

/** Simulates one telefunc request: fresh context, fake request lifecycle, and
 *  access to the request's invalidation trigger (its `pending` flag flips when the
 *  router fires, since no channel ever connects in these tests). */
async function telefunctionCall<T>(body: () => Promise<T> | T) {
  provideTelefuncContext({})
  const raw = getRawContext()
  if (!raw) throw new Error('async context not installed')
  const closeCallbacks: (() => void)[] = []
  const settledCallbacks: (() => void)[] = []
  let settled = false
  raw[REQUEST_CONTEXT] = {
    onClose: (callback: () => void) => closeCallbacks.push(callback),
    // mirrors RequestContext: registrations after settling fire immediately
    onTelefunctionSettled: (callback: () => void) => (settled ? callback() : settledCallbacks.push(callback)),
  }
  const result = await body()
  settled = true
  for (const callback of settledCallbacks) callback()
  return {
    result,
    fired: () => Boolean((raw[INVALIDATE_TRIGGER] as Trigger | undefined)?.pending),
    reset: () => {
      const trigger = raw[INVALIDATE_TRIGGER] as Trigger | undefined
      if (trigger) trigger.pending = false
    },
    close: () => {
      for (const callback of closeCallbacks) callback()
    },
  }
}

const tick = () => new Promise((resolve) => setTimeout(resolve, 0))

function createDb() {
  const sqlite = new Database(':memory:')
  sqlite.exec(`
    CREATE TABLE users (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT NOT NULL, team_id INTEGER NOT NULL);
    CREATE TABLE todos (id INTEGER PRIMARY KEY AUTOINCREMENT, text TEXT NOT NULL,
                        done INTEGER NOT NULL DEFAULT 0, user_id INTEGER NOT NULL);
  `)
  return { sqlite, db: reactiveDrizzle(drizzle(sqlite)) }
}

let sqlite: Database.Database
let db: ReturnType<typeof createDb>['db']

beforeEach(() => {
  ;({ sqlite, db } = createDb())
})
afterEach(() => sqlite.close())

describe('reactiveDrizzle (ORM interception mode)', () => {
  it('a subscribed query sees its data and is invalidated by a matching write', async () => {
    await db.insert(users).values({ name: 'eva', teamId: 5 })
    const subscriber = await telefunctionCall(() => db.select().from(users).where(eq(users.teamId, 5)))
    expect(subscriber.result).toMatchObject([{ name: 'eva', teamId: 5 }])
    expect(subscriber.fired()).toBe(false)

    const writer = await telefunctionCall(() => db.insert(users).values({ name: 'bob', teamId: 5 }))
    await tick()
    expect(subscriber.fired()).toBe(true)
    expect(writer.fired()).toBe(false)
  })

  it('a write matching a different predicate does not invalidate', async () => {
    const subscriber = await telefunctionCall(() => db.select().from(users).where(eq(users.teamId, 5)))
    await telefunctionCall(() => db.insert(users).values({ name: 'mallory', teamId: 3 }))
    await tick()
    expect(subscriber.fired()).toBe(false)
  })

  it('every client with a matching query is invalidated', async () => {
    const first = await telefunctionCall(() => db.select().from(todos))
    const second = await telefunctionCall(() => db.select().from(todos))
    await telefunctionCall(() => db.insert(todos).values({ text: 'hello', userId: 1 }))
    await tick()
    expect(first.fired()).toBe(true)
    expect(second.fired()).toBe(true)
  })

  it('deletes invalidate, and updates that move a row out of the result invalidate too', async () => {
    await db.insert(todos).values([
      { text: 'a', done: false, userId: 1 },
      { text: 'b', done: false, userId: 1 },
    ])
    const subscriber = await telefunctionCall(() => db.select().from(todos).where(eq(todos.done, false)))

    // the new row (done=true) does not match; only the pre-write read of the old
    // image can tell the subscriber its result lost a row
    await telefunctionCall(() => db.update(todos).set({ done: true }).where(eq(todos.text, 'a')))
    await tick()
    expect(subscriber.fired()).toBe(true)
    subscriber.reset()

    await telefunctionCall(() => db.delete(todos).where(eq(todos.text, 'b')))
    await tick()
    expect(subscriber.fired()).toBe(true)
  })

  it('unsubscribes when the request closes', async () => {
    const subscriber = await telefunctionCall(() => db.select().from(todos))
    subscriber.close()
    await telefunctionCall(() => db.insert(todos).values({ text: 'x', userId: 1 }))
    await tick()
    expect(subscriber.fired()).toBe(false)
  })

  it('transaction inside a telefunction: one flush after the telefunction settles', async () => {
    const subscriber = await telefunctionCall(() => db.select().from(todos))
    await telefunctionCall(() => {
      // better-sqlite3 transactions are synchronous
      db.transaction((tx) => {
        tx.insert(todos).values({ text: 'one', userId: 1 }).run()
        tx.insert(todos).values({ text: 'two', userId: 1 }).run()
      })
    })
    await tick()
    expect(subscriber.fired()).toBe(true)
  })

  it('transaction outside a telefunction emits on commit', async () => {
    const subscriber = await telefunctionCall(() => db.select().from(todos))
    db.transaction((tx) => {
      tx.insert(todos).values({ text: 'sync', userId: 1 }).run()
    })
    await tick()
    expect(subscriber.fired()).toBe(true)
  })

  it('rolled-back transactions do not invalidate', async () => {
    const subscriber = await telefunctionCall(() => db.select().from(todos))
    expect(() =>
      db.transaction((tx) => {
        tx.insert(todos).values({ text: 'doomed', userId: 1 }).run()
        tx.rollback()
      }),
    ).toThrow()
    await tick()
    expect(subscriber.fired()).toBe(false)
  })

  it('JOIN: an insert joining a pre-existing row invalidates (hydrated state)', async () => {
    await db.insert(users).values({ name: 'eva', teamId: 7 })
    const subscriber = await telefunctionCall(() =>
      db.select().from(todos).innerJoin(users, eq(todos.userId, users.id)).where(eq(users.teamId, 7)),
    )
    await tick() // hydration

    await telefunctionCall(() => db.insert(todos).values({ text: 'for eva', userId: 1 }))
    await tick()
    expect(subscriber.fired()).toBe(true)
    subscriber.reset()

    // a todo whose user does not exist joins nothing
    await telefunctionCall(() => db.insert(todos).values({ text: 'orphan', userId: 999 }))
    await tick()
    expect(subscriber.fired()).toBe(false)
  })

  it('aggregates: inserts that change the count invalidate', async () => {
    const subscriber = await telefunctionCall(() => db.select({ n: count() }).from(todos).where(eq(todos.userId, 1)))
    await telefunctionCall(() => db.insert(todos).values({ text: 'x', userId: 1 }))
    await tick()
    expect(subscriber.fired()).toBe(true)
    subscriber.reset()

    await telefunctionCall(() => db.insert(todos).values({ text: 'y', userId: 2 }))
    await tick()
    expect(subscriber.fired()).toBe(false)
  })

  it('cron writes via db.batch() invalidate subscribed clients', async () => {
    await db.insert(todos).values({ text: 'old', userId: 1 })
    const subscriber = await telefunctionCall(() => db.select().from(todos))
    await db.batch(async () => {
      await db.delete(todos).where(eq(todos.text, 'old'))
      await db.insert(todos).values({ text: 'fresh', userId: 1 })
    })
    await tick()
    expect(subscriber.fired()).toBe(true)
  })

  it('partial compilation: window-function field is skipped, WHERE still filters', async () => {
    const subscriber = await telefunctionCall(() =>
      db
        .select({ id: todos.id, rank: sql<number>`row_number() over (order by ${todos.id})` })
        .from(todos)
        .where(eq(todos.userId, 5)),
    )
    await telefunctionCall(() => db.insert(todos).values({ text: 'other team', userId: 3 }))
    await tick()
    expect(subscriber.fired()).toBe(false)

    await telefunctionCall(() => db.insert(todos).values({ text: 'same team', userId: 5 }))
    await tick()
    expect(subscriber.fired()).toBe(true)
  })

  it('a pre-write read that cannot be replayed degrades to lossy instead of failing the write', async () => {
    const emitted: WriteSet[] = []
    const tracker = createWriteTracker((writes) => emitted.push(writes))
    // update().from(other) replays a WHERE that references the other table: the
    // plain pre-select throws, the write itself must still go through
    const executor = {
      select: () => {
        throw new Error('missing FROM-clause entry')
      },
    }
    const result = await tracker.capture('update', { table: todos, set: {} }, executor as never, () =>
      Promise.resolve('write result'),
    )
    expect(result).toBe('write result')
    expect(emitted).toMatchObject([[{ table: 'todos', lossy: true }]])
  })

  it('multi-server: a write on one instance invalidates a subscriber on another', async () => {
    // Two reactiveDrizzle instances over the same database simulate two server
    // processes; the in-memory Broadcast plays the role of the transport.
    const dbA = db
    const dbB = reactiveDrizzle(drizzle(sqlite))

    const subscriber = await telefunctionCall(() => dbB.select().from(todos))
    await telefunctionCall(() => dbA.insert(todos).values({ text: 'from A', userId: 1 }))
    await tick()
    expect(subscriber.fired()).toBe(true)
  })
})
