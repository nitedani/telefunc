import { describe, expect, it } from 'vitest'
import {
  and,
  between,
  eq,
  gt,
  gte,
  ilike,
  inArray,
  isNotNull,
  isNull,
  like,
  lt,
  lte,
  ne,
  not,
  notInArray,
  or,
  sql,
} from 'drizzle-orm'
import { alias, boolean, integer, pgTable, text, timestamp } from 'drizzle-orm/pg-core'
import { drizzle } from 'drizzle-orm/node-postgres'
import { compileCondition, conjunctsOf } from './predicate.js'

const todos = pgTable('todos', {
  id: integer('id').primaryKey(),
  text: text('text'),
  done: boolean('done'),
  userId: integer('user_id'),
  createdAt: timestamp('created_at'),
})

const users = pgTable('users', {
  id: integer('id').primaryKey(),
  name: text('name'),
})

describe('compileCondition', () => {
  it('no condition matches everything', () => {
    const compiled = compileCondition(undefined)
    expect(compiled.mightMatch({})).toBe(true)
    expect(compiled.exact).toBe(true)
  })

  it('comparison operators', () => {
    expect(compileCondition(eq(todos.userId, 42)).mightMatch({ user_id: 42 })).toBe(true)
    expect(compileCondition(eq(todos.userId, 42)).mightMatch({ user_id: 7 })).toBe(false)
    expect(compileCondition(ne(todos.userId, 42)).mightMatch({ user_id: 7 })).toBe(true)
    expect(compileCondition(ne(todos.userId, 42)).mightMatch({ user_id: 42 })).toBe(false)
    expect(compileCondition(gt(todos.id, 5)).mightMatch({ id: 6 })).toBe(true)
    expect(compileCondition(gt(todos.id, 5)).mightMatch({ id: 5 })).toBe(false)
    expect(compileCondition(gte(todos.id, 5)).mightMatch({ id: 5 })).toBe(true)
    expect(compileCondition(lt(todos.id, 5)).mightMatch({ id: 4 })).toBe(true)
    expect(compileCondition(lte(todos.id, 5)).mightMatch({ id: 6 })).toBe(false)
  })

  it('records referenced tables and exactness', () => {
    const compiled = compileCondition(eq(todos.userId, 1))
    expect([...compiled.tables]).toEqual(['todos'])
    expect(compiled.exact).toBe(true)
  })

  it('and / or / not, including nesting', () => {
    const condition = and(eq(todos.userId, 1), or(eq(todos.done, false), gt(todos.id, 100)))
    const compiled = compileCondition(condition)
    expect(compiled.mightMatch({ user_id: 1, done: false, id: 1 })).toBe(true)
    expect(compiled.mightMatch({ user_id: 1, done: true, id: 200 })).toBe(true)
    expect(compiled.mightMatch({ user_id: 1, done: true, id: 1 })).toBe(false)
    expect(compiled.mightMatch({ user_id: 2, done: false, id: 1 })).toBe(false)
    expect(compiled.exact).toBe(true)

    expect(compileCondition(not(eq(todos.done, true))).mightMatch({ done: false })).toBe(true)
    expect(compileCondition(not(eq(todos.done, true))).mightMatch({ done: true })).toBe(false)
  })

  it('inArray / notInArray, including the empty list', () => {
    expect(compileCondition(inArray(todos.id, [1, 2, 3])).mightMatch({ id: 2 })).toBe(true)
    expect(compileCondition(inArray(todos.id, [1, 2, 3])).mightMatch({ id: 4 })).toBe(false)
    expect(compileCondition(notInArray(todos.id, [1, 2])).mightMatch({ id: 3 })).toBe(true)
    expect(compileCondition(notInArray(todos.id, [1, 2])).mightMatch({ id: 1 })).toBe(false)
    expect(compileCondition(inArray(todos.id, [])).mightMatch({ id: 1 })).toBe(false)
    expect(compileCondition(notInArray(todos.id, [])).mightMatch({ id: 1 })).toBe(true)
  })

  it('like / ilike with % and _ wildcards', () => {
    expect(compileCondition(like(todos.text, 'buy%')).mightMatch({ text: 'buy milk' })).toBe(true)
    expect(compileCondition(like(todos.text, 'buy%')).mightMatch({ text: 'sell milk' })).toBe(false)
    expect(compileCondition(like(todos.text, 'buy%')).mightMatch({ text: 'BUY milk' })).toBe(false)
    expect(compileCondition(ilike(todos.text, 'buy%')).mightMatch({ text: 'BUY milk' })).toBe(true)
    expect(compileCondition(like(todos.text, 'b_y')).mightMatch({ text: 'buy' })).toBe(true)
    expect(compileCondition(like(todos.text, 'b_y')).mightMatch({ text: 'buuy' })).toBe(false)
    // regex metacharacters in the pattern stay literal
    expect(compileCondition(like(todos.text, 'a.c')).mightMatch({ text: 'abc' })).toBe(false)
  })

  it('isNull / isNotNull', () => {
    expect(compileCondition(isNull(todos.text)).mightMatch({ text: null })).toBe(true)
    expect(compileCondition(isNull(todos.text)).mightMatch({ text: 'x' })).toBe(false)
    expect(compileCondition(isNotNull(todos.text)).mightMatch({ text: 'x' })).toBe(true)
    expect(compileCondition(isNotNull(todos.text)).mightMatch({ text: null })).toBe(false)
  })

  it('between', () => {
    const compiled = compileCondition(between(todos.id, 10, 20))
    expect(compiled.mightMatch({ id: 15 })).toBe(true)
    expect(compiled.mightMatch({ id: 10 })).toBe(true)
    expect(compiled.mightMatch({ id: 20 })).toBe(true)
    expect(compiled.mightMatch({ id: 9 })).toBe(false)
    expect(compiled.mightMatch({ id: 21 })).toBe(false)
    expect(compiled.exact).toBe(true)
  })

  it('between inside and() does not get split apart', () => {
    const compiled = compileCondition(and(between(todos.id, 10, 20), eq(todos.done, false)))
    expect(compiled.mightMatch({ id: 15, done: false })).toBe(true)
    expect(compiled.mightMatch({ id: 15, done: true })).toBe(false)
    expect(compiled.mightMatch({ id: 25, done: false })).toBe(false)
  })

  it('column-to-column comparison', () => {
    const compiled = compileCondition(eq(todos.id, todos.userId))
    expect(compiled.mightMatch({ id: 3, user_id: 3 })).toBe(true)
    expect(compiled.mightMatch({ id: 3, user_id: 4 })).toBe(false)
  })

  it('missing column value is unknown: matches, and survives not()', () => {
    expect(compileCondition(eq(todos.userId, 42)).mightMatch({})).toBe(true)
    expect(compileCondition(not(eq(todos.userId, 42))).mightMatch({})).toBe(true)
    expect(compileCondition(inArray(todos.id, [1])).mightMatch({})).toBe(true)
    expect(compileCondition(like(todos.text, 'x%')).mightMatch({})).toBe(true)
  })

  it('explicit null follows SQL comparison semantics', () => {
    expect(compileCondition(eq(todos.userId, 42)).mightMatch({ user_id: null })).toBe(false)
    expect(compileCondition(gt(todos.id, 5)).mightMatch({ id: null })).toBe(false)
    expect(compileCondition(inArray(todos.id, [1, 2])).mightMatch({ id: null })).toBe(false)
  })

  it('unknown only poisons its own branch of and/or', () => {
    // and(unknown, false) is false; and(unknown, true) is unknown (matches)
    expect(compileCondition(and(eq(todos.text, 'a'), eq(todos.done, true))).mightMatch({ done: false })).toBe(false)
    expect(compileCondition(and(eq(todos.text, 'a'), eq(todos.done, true))).mightMatch({ done: true })).toBe(true)
    // or(unknown, true) is true; or(unknown, false) is unknown (matches)
    expect(compileCondition(or(eq(todos.text, 'a'), eq(todos.done, true))).mightMatch({ done: true })).toBe(true)
    expect(compileCondition(or(eq(todos.text, 'a'), eq(todos.done, true))).mightMatch({ done: false })).toBe(true)
  })

  it('value coercion across producers', () => {
    // SQLite stores booleans as 0/1
    expect(compileCondition(eq(todos.done, true)).mightMatch({ done: 1 })).toBe(true)
    expect(compileCondition(eq(todos.done, true)).mightMatch({ done: 0 })).toBe(false)
    // pg numeric arrives as a string
    expect(compileCondition(gt(todos.id, 5)).mightMatch({ id: '6' })).toBe(true)
    expect(compileCondition(gt(todos.id, 5)).mightMatch({ id: '5' })).toBe(false)
    // timestamps: Date vs ISO string
    const cutoff = new Date('2026-01-01T00:00:00Z')
    expect(compileCondition(gt(todos.createdAt, cutoff)).mightMatch({ created_at: '2026-06-01T00:00:00Z' })).toBe(true)
    expect(compileCondition(gt(todos.createdAt, cutoff)).mightMatch({ created_at: '2025-06-01T00:00:00Z' })).toBe(false)
  })

  it('raw sql`` atoms degrade to always-match and mark the condition inexact', () => {
    const condition = and(eq(todos.userId, 1), sql`char_length(${todos.text}) > 3`)
    const compiled = compileCondition(condition)
    expect(compiled.exact).toBe(false)
    // the parseable conjunct still filters
    expect(compiled.mightMatch({ user_id: 2, text: 'abcd' })).toBe(false)
    expect(compiled.mightMatch({ user_id: 1, text: 'x' })).toBe(true)
  })

  it('subqueries are opaque but their tables are recorded', () => {
    const db = drizzle.mock()
    const subquery = db.select({ id: users.id }).from(users).where(eq(users.name, 'a')).as('sq')
    const compiled = compileCondition(inArray(todos.userId, db.select({ id: subquery.id }).from(subquery)))
    expect(compiled.exact).toBe(false)
    expect(compiled.mightMatch({ user_id: 1 })).toBe(true)
    expect(compiled.tables.has('todos')).toBe(true)
  })

  it('conjunctsOf splits top-level and(), flattens nesting, keeps or() and between whole', () => {
    expect(conjunctsOf(and(eq(todos.userId, 1), eq(todos.done, false), gt(todos.id, 5))!)).toHaveLength(3)
    expect(conjunctsOf(and(and(eq(todos.userId, 1), eq(todos.done, false)), gt(todos.id, 5))!)).toHaveLength(3)
    expect(conjunctsOf(or(eq(todos.userId, 1), eq(todos.done, false))!)).toHaveLength(1)
    expect(conjunctsOf(between(todos.id, 1, 2))).toHaveLength(1)
    expect(conjunctsOf(eq(todos.userId, 1))).toHaveLength(1)
  })

  it('aliased tables resolve to their original name', () => {
    const todosAlias = alias(todos, 't2')
    const compiled = compileCondition(eq(todosAlias.userId, 7))
    expect([...compiled.tables]).toEqual(['todos'])
    expect(compiled.mightMatch({ user_id: 7 })).toBe(true)
  })
})
