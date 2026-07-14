// No invalidation code anywhere: the queries below are plain Drizzle, the
// mutations are plain writes, and the query keys on the client are plain local
// keys. @telefunc/drizzle detects which queries each write affects and refetches
// every subscribed client.
export { onGetTodos, onAddTodo, onToggleTodo, onClearTodos }

import { eq } from 'drizzle-orm'
import { db, todos } from './db'

async function onGetTodos(team: string) {
  return await db.select().from(todos).where(eq(todos.team, team)).orderBy(todos.id)
}

async function onAddTodo(team: string, text: string) {
  await db.insert(todos).values({ team, text })
}

async function onToggleTodo(id: number, done: boolean) {
  await db.update(todos).set({ done }).where(eq(todos.id, id))
}

async function onClearTodos(team: string) {
  await db.delete(todos).where(eq(todos.team, team))
}
