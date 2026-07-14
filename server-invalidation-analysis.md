# Server-side invalidation and non-global keys

This document analyzes a proposal for `@telefunc/tanstack-query`: allow server-side
`invalidate()` to work with keys that don't carry the `global:` prefix. The motivating
use case is custom change detection, where the server learns about data changes from
the database itself (Postgres LISTEN/NOTIFY, logical replication, Debezium, or any
CDC pipeline) rather than from a client mutation.

The analysis covers the current mechanism, two variants of the proposal, deployment
topology, cost, key semantics, and ends with the use case the discussion actually
narrowed down to, plus a design sketch for it.

## 1. How invalidation works today

Three parties read the `global:` prefix, and each makes a routing decision from it:

1. **Query side** ([client.ts:40-44](packages/tanstack-query/src/client.ts#L40-L44)).
   The persister wraps the queryFn with `withContext` and sends the `queryKey` to the
   server only when `isGlobalKey(queryKey)` is true. Local keys go through untouched;
   the server never learns they exist.
2. **Server side** ([server.ts:24-46](packages/tanstack-query/src/server.ts#L24-L46)).
   The `onTelefunctionResult` hook runs only when the client sent extension data
   (gated in [executeTelefunction.ts:72](packages/telefunc/node/server/runTelefunc/executeTelefunction.ts#L72)).
   When it runs, it subscribes to the Broadcast topic `__tq__:<queryKey[0]>`, filters
   incoming keys with `partialMatchKey`, and pipes matches down a per-query `Channel`
   to the client.
3. **`invalidate()`** ([server.ts:53-55](packages/tanstack-query/src/server.ts#L53-L55))
   publishes the key to that topic. It has no knowledge of subscribers.

Mutations partition `meta.invalidates` by the same prefix
([client.ts:105](packages/tanstack-query/src/client.ts#L105)): global keys are sent to
the server and published through Broadcast, local keys call `invalidateQueries()` on
the current client only.

Two implementation details that matter later:

- The Broadcast topic is derived from the key's first element only
  ([server.ts:15-17](packages/tanstack-query/src/server.ts#L15-L17)). All subscriptions
  under the same top-level key share one topic; per-key matching happens in each
  subscriber callback, server-side.
- The subscription and channel are recreated on every fetch result and the previous
  one is closed ([client.ts:67-68](packages/tanstack-query/src/client.ts#L67-L68)).

## 2. The proposal and its variants

> Server code (a CDC listener, a webhook, a cron job) should be able to call
> `invalidate(['todos'])` and have clients holding `['todos']` refetch, without the
> key being named `global:todos`.

Two variants came up:

- **(a)** Make `invalidate()` reach non-global keys through the existing machinery.
- **(b)** For non-global keys, skip the broadcast mechanism and deliver in-memory.

## 3. Why non-global keys are unreachable today

For the server to refetch a query sitting in a browser tab, two things must exist:

1. **Registration.** The client sends the `queryKey` with the fetch and the server
   opens a push channel for it. Without this the server doesn't know the query exists
   and has no path to the tab.
2. **Delivery.** The publish must reach every client holding the key, because the
   underlying data changed for all of them.

Non-global keys never register (step 1 in section 1), so `invalidate(['todos'])`
publishes to a topic with zero subscribers. It is a silent no-op: no error, no
warning, no effect. The docs state the restriction
([+Page.mdx:81-95](docs/pages/integrations/tanstack-query/+Page.mdx#L81-L95)) but the
code does not enforce it.

## 4. Variant (a): reach non-global keys through the existing machinery

To make `['todos']` server-invalidatable, the client must send the key and hold a
channel for it. That is the entire operational content of `global:`. The variant
therefore collapses into "register every key", which is the same as making every key
global, minus the name.

**Pros**

- No prefix in key names; existing keys work unchanged.
- `invalidate()` works for any key, no naming coordination between client and server.

**Cons**

- Every query on every client registers, holds a channel, and creates a Broadcast
  subscription, whether or not it ever needs server invalidation.
- Removes the opt-out. The prefix currently lets the majority of queries (search
  results, pagination, UI state) stay out of the push machinery entirely.
- Breaks the namespace contract (section 8): key names designed for a per-client
  namespace start colliding across users.

## 5. Variant (b): in-memory delivery for non-global keys

Inspection of the Broadcast adapter undercuts this variant twice:

- **Broadcast is already in-memory.** When no transport is installed, `publish()`
  falls through to `_publishInMemory`, which walks a local
  `Map<string, Set<callback>>`
  ([broadcast.ts:100-107](packages/telefunc/wire-protocol/server/broadcast.ts#L100-L107)).
  The transport is opt-in and configured once per process
  ([broadcast.ts:193-216](packages/telefunc/wire-protocol/server/broadcast.ts#L193-L216)).
  On a single server, "use in-memory instead of broadcast" describes what every
  `invalidate()` call already does.
- **Registration is still missing.** In-memory delivery walks the same subscription
  map that non-global keys never populate. Delivered count is zero with or without a
  transport.

There is also a layering problem: transport selection is a deployment-wide decision
(one adapter per process), while global vs. local is per-key. Making the key name
select the delivery path ties correctness to where the caller happens to run, which
is invisible at the call site.

**Pros**

- None that variant (a) doesn't already have. The transport hop was never the cost
  being avoided, and skipping it changes nothing on a single server.

**Cons**

- All cons of variant (a), plus: on a multi-server deployment, only clients connected
  to the publishing instance refetch. The data changed in a shared database, so this
  is partial delivery, a correctness bug that appears only in production topology and
  never in development.

## 6. Where the triggering event fires

The variants assume the invalidation-triggering event and the subscribed clients live
in the same process. Whether that holds depends on the change-detection mechanism:

| Mechanism | Who receives the event | In-memory delivery sufficient? |
| --- | --- | --- |
| Postgres LISTEN/NOTIFY, one listening connection per instance | Every server instance | Yes: each instance invalidates its own clients; the union covers everyone |
| Logical replication slot | Exactly one consumer (slots are exclusive) | No |
| Debezium / Kafka consumer group | One member of the group | No |
| Dedicated CDC worker process | A process with zero connected clients | No: in-memory delivery reaches nobody |
| Webhook, cron job | Whichever instance the request or schedule lands on | No |

Two conclusions:

1. **The LISTEN/NOTIFY-everywhere pattern works today, with global keys and no
   transport.** Each instance LISTENs, each calls `invalidate(['global:todos'])` when
   notified, each delivers in-memory to its own clients. Postgres is the cross-server
   fan-out; no telefunc transport is configured. One consequence: client-mutation
   global invalidation (`meta.invalidates`) also becomes per-instance in that setup,
   which is consistent if every write goes through the database and CDC is the single
   invalidation path.
2. **The framework cannot assume that topology.** `invalidate()` must behave the same
   whether it's called next to the subscriber or three processes away. That guarantee
   is the application author's (by arranging where the event fires) or the
   transport's; it cannot be a per-key property, because the call site can't see it.

## 7. Cost: are channels cheap?

Mostly yes, and this weakens one argument for the prefix. Channels are multiplexed
over one connection per client (`MuxChannel` entries with a channel index over a
single SSE or WebSocket wire,
[connection.ts](packages/telefunc/wire-protocol/client/connection.ts)). A marginal
channel is a map entry, an index, and a replay buffer, not a socket. If cost were the
whole question, defaulting every key to global would be defensible.

The real costs are different in kind:

- **The cliff is the first channel, not the thousandth.** A client with zero global
  queries does plain HTTP request/response: no persistent connection, stateless
  server, deployable on HTTP-only targets. The first global query forces the SSE/WS
  connection into existence and makes the server hold per-client state
  (subscriptions, replay buffers, reconnect reconciliation). Default-global means
  every deployment pays this always.
- **State scales with clients × queries**, and the subscription churns on every
  refetch (section 1). Cheap individually; default-on multiplies it across queries
  that never use it.

Verdict: cost justifies having an opt-in, but it is not the load-bearing reason for
the prefix. The next section is.

## 8. Key namespaces: what the prefix actually marks

A local key like `['todos']` lives in a per-client namespace. It means "my todos"
because the telefunction behind it is auth-scoped; the same name on two clients
refers to two datasets. A global key lives in one namespace shared by every connected
user, so the name itself must carry the scope: `['global:todos', userId]`.

Auto-globalizing `['todos']` makes every user's mutation invalidate every other
user's `['todos']` query. No data leaks (each refetch is auth-scoped), but one user
adding a todo makes all users refetch `getTodos`: load amplification plus a
cross-user activity signal, on a key whose author designed it for a private
namespace. Whether a name is meaningful deployment-wide is something only the
developer knows; it cannot be inferred from the key.

So `global:` is a semantic declaration ("this name is shared across all users") that
happens to also gate the machinery, not a cost toggle. This reframing also exposes
what the proposal was reaching for: the current design bundles "server-reachable" and
"shared with every user" into one prefix. There are three namespaces, not two:

| Namespace | Meaning | Expressible today |
| --- | --- | --- |
| Tab-local | This client only | `['todos']` |
| User-scoped | All sessions of one user | Only by embedding `userId` in a global key |
| Deployment-wide | Every connected user | `['global:todos']` |

The middle tier is the gap.

## 9. The narrowed use case: user-scoped invalidation

"A query that is not shared with other users but is shared across all of my tabs and
devices, and that the server can invalidate when my data changes."

### What works today

```ts
queryKey: ['global:todos', userId]          // subscribes: all of this user's sessions
invalidate(['global:todos', userId])        // CDC: reaches only this user's sessions
```

Server-side `partialMatchKey` filtering
([server.ts:38](packages/tanstack-query/src/server.ts#L38)) runs in the subscriber
callback, so other users' clients receive nothing. Embedding `userId` in the key is
also standard TanStack practice for user-scoped data: without it, an account switch
on the same client serves user A's cache to user B.

### Weaknesses of the simulation

1. **Boilerplate and duplication.** The client threads its own `userId` into every
   key, restating what the server already knows from auth context on every call.
2. **Client-asserted scope.** The queryKey arrives as extension data; the
   telefunction validates its own arguments, not the key. A client can call
   `getTodos()` (authorized, own data) while subscribing with
   `['global:todos', victimId]`. It receives no data, but it receives invalidation
   pings: an activity signal for another user. Scope is a client claim, not a server
   guarantee.
3. **Fan-out granularity.** The topic is the top-level key only, so every user's
   `global:todos` subscription shares one topic, and every publish runs every
   subscriber's match check, discarding all but one user's.

### Design sketch: a `user:` tier

- A `user:` prefix marks the key as user-scoped. Registration and channel mechanics
  are identical to `global:`.
- `onTelefunctionResult` derives the session identity from telefunc context and
  subscribes to a topic like `__tq__:user:<uid>:todos`. The client never supplies the
  scope.
- Server-side: `invalidate(['user:todos'], { userId })`. The explicit `userId` is
  required because CDC code runs outside any request context, and the CDC event knows
  which row (hence which user) changed.

**Pros**

- Keys stay free of identity boilerplate.
- Scope becomes server-enforced: a client cannot subscribe into another user's scope
  by forging a key segment. Closes the activity-signal hole.
- Per-user topics: a publish touches only that user's subscriptions.
- Keeps the namespace model coherent: three prefixed tiers, each naming who shares
  the key.

**Cons**

- A second prefix to learn and document; `meta.invalidates` partitioning, the
  extension, and `invalidate()` all grow a third branch.
- Requires a stable user identity derivable from telefunc context; apps without auth
  or with custom session shapes need a configuration hook to extract it.
- The existing pattern already covers the functional need; the tier buys enforcement
  and ergonomics, not a new capability class.

## 10. Developer experience comparison

| | Today (`global:` + userId in key) | With a `user:` tier |
| --- | --- | --- |
| Query site | `['global:todos', userId]`, client must know its userId | `['user:todos']` |
| CDC handler | `invalidate(['global:todos', userId])` | `invalidate(['user:todos'], { userId })` |
| Scope enforcement | Client-asserted (forgeable key segment) | Server-derived from auth context |
| Wrong-key failure mode | Silent no-op if prefix forgotten | Same, unless a warning is added (below) |
| Works now | Yes | Requires new feature |

## 11. Recommendations

1. **Keep the current design.** Both proposal variants either collapse into "make
   every key global" or introduce topology-dependent partial delivery. The `global:`
   prefix is a namespace declaration that only the developer can make.
2. **Add a dev-mode warning to `invalidate()`** when the key is not global. Today the
   call is a silent no-op, and a developer wiring up CDC with an unprefixed key gets
   nothing and no signal why. This is the one defect the discussion surfaced in the
   current code.
3. **Document the no-transport LISTEN/NOTIFY pattern** (section 6): global keys, no
   broadcast transport, one listening connection per instance. It serves the CDC use
   case today with zero new code, and it isn't obvious from the current docs that the
   transport is optional in that topology.
4. **Treat the `user:` tier as a candidate feature, not a gap to rush.** The
   functional need is covered by `['global:todos', userId]`. Build the tier when the
   pattern recurs in real apps or when the client-asserted-scope signal leak matters
   for a tenant; the design sketch in section 9 is the starting point.
