export { TeamTodos }

import React, { useEffect, useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import { onAddTodo, onClearTodos, onGetTodos, onToggleTodo } from './TeamTodos.telefunc'

declare global {
  interface Window {
    /** Per-team queryFn execution counts; the e2e test asserts invalidation precision. */
    __reactiveFetches: Record<string, number>
  }
}

function TeamTodos() {
  const [hydrated, setHydrated] = useState(false)
  useEffect(() => setHydrated(true), [])

  return (
    <div className="grid grid-cols-2 gap-8">
      {hydrated && <span id="hydrated" />}
      <TeamColumn team="red" accent="text-red-600" ring="focus:ring-red-300" />
      <TeamColumn team="blue" accent="text-blue-600" ring="focus:ring-blue-300" />
    </div>
  )
}

function TeamColumn({ team, accent, ring }: { team: string; accent: string; ring: string }) {
  const [text, setText] = useState('')

  const { data: todos, isLoading } = useQuery({
    queryKey: ['reactive-todos', team],
    queryFn: () => {
      window.__reactiveFetches ??= {}
      window.__reactiveFetches[team] = (window.__reactiveFetches[team] ?? 0) + 1
      return onGetTodos(team)
    },
    staleTime: Infinity,
    refetchOnMount: false,
    refetchOnWindowFocus: false,
  })

  const add = () => {
    if (!text.trim()) return
    onAddTodo(team, text.trim())
    setText('')
  }

  return (
    <div className="rounded-xl border border-zinc-200 p-5 shadow-sm">
      <div className="flex items-baseline justify-between mb-1">
        <h2 className={`capitalize ${accent}`}>Team {team}</h2>
        <button
          id={`${team}-clear`}
          className="text-xs text-zinc-400 hover:text-zinc-600"
          onClick={() => onClearTodos(team)}
        >
          Clear
        </button>
      </div>
      <p className="text-xs text-zinc-400 mb-3">
        queryKey <code>['reactive-todos', '{team}']</code> — no <code>global:</code>, no <code>meta.invalidates</code>
      </p>
      <div className="flex gap-2 mb-4">
        <input
          id={`${team}-input`}
          type="text"
          value={text}
          onChange={(event) => setText(event.target.value)}
          onKeyDown={(event) => event.key === 'Enter' && add()}
          placeholder={`Add a ${team} todo...`}
          className={`border border-zinc-300 rounded px-3 py-1.5 text-sm flex-1 outline-none focus:ring-2 ${ring}`}
        />
        <button id={`${team}-add`} onClick={add}>
          Add
        </button>
      </div>
      <ul id={`${team}-list`} className="space-y-1.5">
        {isLoading ? (
          <li>Loading...</li>
        ) : todos?.length === 0 ? (
          <li className="text-zinc-400 text-sm">No todos yet.</li>
        ) : (
          todos?.map((todo) => (
            <li key={todo.id} className="flex items-center gap-2">
              <input
                type="checkbox"
                checked={todo.done}
                onChange={() => onToggleTodo(todo.id, !todo.done)}
                className="accent-zinc-600"
              />
              <span className={todo.done ? 'line-through text-zinc-400' : ''}>{todo.text}</span>
            </li>
          ))
        )}
      </ul>
    </div>
  )
}
