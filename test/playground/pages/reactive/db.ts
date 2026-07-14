// Server-only module (imported by TeamTodos.telefunc.ts exclusively).
// One drizzle instance, two deployments: the docker cluster provides DATABASE_URL
// and gets Postgres with CDC (each instance consumes the WAL through its own
// replication slot); dev/preview run on SQLite with write interception. The app
// code below is identical either way — reactiveDrizzle picks the mode.
export { db, todos }

import 'telefunc/async_hooks'
import Database from 'better-sqlite3'
import pg from 'pg'
import { reactiveDrizzle } from '@telefunc/drizzle'
import { drizzle as drizzleSqlite } from 'drizzle-orm/better-sqlite3'
import { drizzle as drizzlePg } from 'drizzle-orm/node-postgres'
import { integer as pgInteger, boolean as pgBoolean, pgTable, serial, text as pgText } from 'drizzle-orm/pg-core'
import { integer, sqliteTable, text } from 'drizzle-orm/sqlite-core'

function makeSqlite() {
  const sqlite = new Database(':memory:')
  sqlite.exec(`
    CREATE TABLE IF NOT EXISTS reactive_todos (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      team TEXT NOT NULL,
      text TEXT NOT NULL,
      done INTEGER NOT NULL DEFAULT 0
    )
  `)
  const todos = sqliteTable('reactive_todos', {
    id: integer('id').primaryKey({ autoIncrement: true }),
    team: text('team').notNull(),
    text: text('text').notNull(),
    done: integer('done', { mode: 'boolean' }).notNull().default(false),
  })
  return { db: reactiveDrizzle(drizzleSqlite(sqlite)), todos }
}

async function makePostgres() {
  const pool = new pg.Pool({ connectionString: process.env.DATABASE_URL })
  await pool
    .query(`
      CREATE TABLE IF NOT EXISTS reactive_todos (
        id SERIAL PRIMARY KEY,
        team TEXT NOT NULL,
        text TEXT NOT NULL,
        done BOOLEAN NOT NULL DEFAULT false
      );
      ALTER TABLE reactive_todos REPLICA IDENTITY FULL;
    `)
    .catch((error) => {
      // 6 instances bootstrap concurrently; losing the CREATE race is fine.
      if (error.code !== '42P07' && error.code !== '23505') throw error
    })
  const todos = pgTable('reactive_todos', {
    id: serial('id').primaryKey(),
    team: pgText('team').notNull(),
    text: pgText('text').notNull(),
    done: pgBoolean('done').notNull().default(false),
  })
  const db = reactiveDrizzle(drizzlePg(pool), {
    // replication slots have exactly one consumer: one slot per instance
    cdc: { slot: `telefunc_reactive_${(process.env.INSTANCE_ID ?? 'solo').toLowerCase()}` },
  })
  // The runtime surfaces are equivalent (same columns, same builder API); typing
  // the export against the SQLite schema keeps the telefunctions dialect-free.
  return { db, todos } as unknown as ReturnType<typeof makeSqlite>
}

const { db, todos } = process.env.DATABASE_URL ? await makePostgres() : makeSqlite()
