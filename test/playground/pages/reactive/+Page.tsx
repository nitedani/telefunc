export { Page }

import React from 'react'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { withTelefunc } from '@telefunc/tanstack-query'
import { TeamTodos } from './TeamTodos'

const queryClient = withTelefunc(new QueryClient())

function Page() {
  return (
    <QueryClientProvider client={queryClient}>
      <div className="max-w-3xl mx-auto px-8 py-10">
        <h1>Reactive Queries</h1>
        <p className="mb-6 text-sm text-zinc-500">
          Plain Drizzle queries, plain writes, plain query keys. The database is the only invalidation source: every
          connected client whose query result a write affects refetches, and nothing else does.
        </p>
        <TeamTodos />
      </div>
    </QueryClientProvider>
  )
}
