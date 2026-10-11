// Channel flow control end to end: a real ClientChannel and ClientConnection talk to a real ChannelMux and
// ServerChannel over in-process WebSockets with a fixed latency, on fake timers, so each run is deterministic.

import { afterEach, beforeEach, expect, test, vi } from 'vitest'

import { ClientBroadcast, ClientChannel } from './client/channel.js'
import { ServerChannel } from './server/channel.js'
import { Broadcast, ServerBroadcast } from './server/server-broadcast.js'
import { ChannelMux, type ServerTransport } from './server/mux.js'
import {
  CHANNEL_TRANSPORT,
  CREDIT_MSG_WINDOW_INITIAL,
  CREDIT_WINDOW_INITIAL_BYTES,
  CREDIT_WINDOW_MAX_BYTES,
  CHANNEL_PING_INTERVAL_MS,
  CHANNEL_RECONNECT_INITIAL_DELAY_MS,
  RECONCILE_TIMEOUT_MS,
  WIRE_PROBE_BYTES,
} from './constants.js'
import { ChannelOverflowError } from './channel-errors.js'
import { TAG, encode } from './shared-ws.js'
import { NetworkError } from '../shared/NetworkError.js'
import { config, setAdapterMaxFrameBytes } from '../node/server/serverConfig.js'

const LATENCY_MS = 5

/** What a socket read hands over at once; the receiver's microtasks run between two reads. */
const READ_BYTES = 64 * 1024

/** One direction of a wire: a frame leaves once the one before it has, and arrives `latencyMs` after it left. */
class Pipe {
  private queue: { frame: Uint8Array<ArrayBuffer>; at: number }[] = []
  private timer: ReturnType<typeof setTimeout> | null = null
  private held = false
  /** What the pipe carries, in bytes: the sender's `bufferedAmount`. */
  bytes = 0
  /** A slow link carries a frame only once the one before it has gone through. */
  bytesPerMs = Infinity
  latencyMs = LATENCY_MS
  private lastAt = 0
  constructor(private readonly deliver: (frame: Uint8Array<ArrayBuffer>) => void) {}
  push(frame: Uint8Array<ArrayBuffer>): void {
    this.lastAt = Math.max(Date.now(), this.lastAt) + frame.byteLength / this.bytesPerMs
    this.queue.push({ frame, at: this.lastAt + this.latencyMs })
    this.bytes += frame.byteLength
    this.schedule()
  }
  /** What the network still carries stops arriving, until `release`. */
  hold(): void {
    this.held = true
  }
  release(): void {
    this.held = false
    this.schedule()
  }
  /** The wire is cut: what it still carries is lost. */
  clear(): void {
    this.queue.length = 0
    this.bytes = 0
  }
  get length(): number {
    return this.queue.length
  }
  private schedule(): void {
    if (this.timer || this.held || this.queue.length === 0) return
    this.timer = setTimeout(
      () => {
        this.timer = null
        let read = 0
        while (!this.held && read < READ_BYTES && this.queue.length > 0 && this.queue[0]!.at <= Date.now()) {
          const { frame } = this.queue.shift()!
          read += frame.byteLength
          this.bytes -= frame.byteLength
          this.deliver(frame)
        }
        this.schedule()
      },
      Math.max(0, this.queue[0]!.at - Date.now()),
    )
  }
}

let loop: Loopback

/** The page's `WebSocket`, wired to `loop.mux`. */
class LoopbackSocket {
  static readonly OPEN = 1
  readyState = 0
  binaryType = 'blob'
  onopen: (() => void) | null = null
  onmessage: ((event: { data: ArrayBuffer }) => void) | null = null
  onclose: (() => void) | null = null
  onerror: (() => void) | null = null
  readonly toServer = new Pipe((frame) => loop.receive(this, frame))
  readonly toPage = new Pipe((frame) => this.onmessage?.({ data: frame.buffer }))
  constructor(_url: string) {
    this.toServer.bytesPerMs = this.toPage.bytesPerMs = loop.linkBytesPerMs
    loop.sockets.push(this)
    setTimeout(() => {
      if (this.readyState !== 0) return
      loop.mux.onConnectionOpen(this, loop.transport)
      this.readyState = 1
      this.onopen?.()
    }, LATENCY_MS)
  }
  get bufferedAmount(): number {
    return this.toServer.bytes
  }
  send(frame: Uint8Array): void {
    loop.sends('page', frame)
    this.toServer.push(frame.slice())
  }
  close(): void {
    this.cut()
  }
  /** The network drops this wire. */
  cut(): void {
    if (this.readyState === 3) return
    this.readyState = 3
    this.toServer.clear()
    this.toPage.clear()
    loop.mux.onConnectionClosed(this, { permanent: false })
    this.onclose?.()
  }
}

class Loopback {
  /** The speed each socket's link starts at, both ways. */
  linkBytesPerMs = Infinity
  readonly mux = new ChannelMux()
  readonly sockets: LoopbackSocket[] = []
  readonly errors: unknown[] = []
  private readonly sessions = new Map<LoopbackSocket, string>()
  readonly transport: ServerTransport<LoopbackSocket> = {
    getSessionId: (socket) => this.sessions.get(socket),
    setSessionId: (socket, id) => void this.sessions.set(socket, id),
    getConnId: () => null,
    wholeMessages: true,
    sendNow: (socket, frame) => {
      this.sends('server', frame)
      socket.toPage.push(frame.slice())
    },
    bufferedAmount: (socket) => socket.toPage.bytes,
    terminateConnection: (socket) => socket.cut(),
  }
  private readonly connectionKey = crypto.randomUUID()
  private readonly pages: ClientChannel[] = []
  private watch: { from: 'page' | 'server'; tag: number; then: () => void } | null = null
  /** Every frame each side sent, as `[tag, channel ix, bytes]`, and when. */
  readonly sent = { page: [] as [number, number, number][], server: [] as [number, number, number][] }
  readonly sentAt = { page: [] as number[], server: [] as number[] }
  /** The tag of each frame that reached the server, and when. */
  readonly arrived: [number, number][] = []
  /** Runs `then` once, as `from` sends its next frame of `tag`. */
  onSend(from: 'page' | 'server', tag: number, then: () => void): void {
    this.watch = { from, tag, then }
  }
  sends(from: 'page' | 'server', frame: Uint8Array): void {
    this.sent[from].push([frame[0]!, frame[1]! | (frame[2]! << 8), frame.byteLength])
    this.sentAt[from].push(Date.now())
    const watch = this.watch
    if (!watch || watch.from !== from || watch.tag !== frame[0]) return
    this.watch = null
    watch.then()
  }
  receive(socket: LoopbackSocket, frame: Uint8Array<ArrayBuffer>): void {
    this.arrived.push([frame[0]!, Date.now()])
    this.mux.onConnectionRawMessage(socket, frame).catch((err) => this.errors.push(err))
  }
  /** The wire the page uses now. */
  get socket(): LoopbackSocket {
    return this.sockets.at(-1)!
  }
  /** A channel the server has registered, and its page end, which all share one connection. */
  open<ClientToServer, ServerToClient>(opts: { registered?: boolean } = {}) {
    const server = new ServerChannel<ClientToServer, ServerToClient>()
    if (opts.registered !== false) this.mux.registerChannel(server)
    const page = new ClientChannel<ClientToServer, ServerToClient>({
      channelId: server.id,
      transports: [CHANNEL_TRANSPORT.WS],
      telefuncUrl: 'http://loopback.test/_telefunc',
      connectionKey: this.connectionKey,
    })
    this.pages.push(page as ClientChannel)
    return { server, page, register: () => this.mux.registerChannel(server) }
  }
  /** A broadcast the server has registered, and its page end, on the same connection. */
  openBroadcast<T>(key: string) {
    const server = new ServerBroadcast<T>({ key })
    this.mux.registerChannel(server)
    const page = new ClientBroadcast<T>({
      channelId: server.id,
      key,
      transports: [CHANNEL_TRANSPORT.WS],
      telefuncUrl: 'http://loopback.test/_telefunc',
      connectionKey: this.connectionKey,
    })
    this.pages.push(page as ClientChannel)
    return { server, page }
  }
  dispose(): void {
    for (const page of this.pages) page.abort()
  }
}

const run = (ms: number) => vi.advanceTimersByTimeAsync(ms)

/** The server's piece sender and receiver of a socket, which the specs read into. */
const piecesOf = (socket: LoopbackSocket) =>
  (
    loop.mux as unknown as {
      connectionEntries: Map<unknown, { state: { pieces: { sender: unknown; receiver: unknown } } }>
    }
  ).connectionEntries.get(socket)?.state.pieces

/** The bytes a PIECE adds to its piece. */
const PIECE_HEADER_BYTES = encode.piece(0, new Uint8Array(0)).byteLength

/** The bytes of the first piece of a frame a side sent since `mark`: its probe's, where it measured no rate. A smaller
 *  piece is a small frame's, announced before the wire knew its link slow. */
const firstPiece = (side: 'page' | 'server', mark: number) => {
  const sent = loop.sent[side]
    .slice(mark)
    .find(([tag, , bytes]) => tag === TAG.PIECE && bytes - PIECE_HEADER_BYTES >= WIRE_PROBE_BYTES)
  return sent === undefined ? undefined : sent[2] - PIECE_HEADER_BYTES
}

/** What the server holds for the page on `socket`: what the wire carries, and what waits for a probe. */
const serverHolds = (socket: LoopbackSocket) =>
  socket.toPage.bytes + (piecesOf(socket)?.sender as { heldBytes: number }).heldBytes

/** Runs until `done`, in steps of `LATENCY_MS`. A page's window doubles about every 50 ms of a saturated stream,
 *  so runs stay short. */
async function runUntil(done: () => boolean, maxMs: number): Promise<void> {
  for (let elapsed = 0; elapsed < maxMs && !done(); elapsed += LATENCY_MS) await run(LATENCY_MS)
}

/** `for (;;) await channel.send(next)`, the channel page's backpressure loop, counting what it has sent. */
function produce<T = number>(
  channel: { send(data: T): Promise<void>; isClosed: boolean },
  { message = (n) => n as T, onSent }: { message?: (n: number) => T; onSent?: (count: number) => void } = {},
) {
  const sent = { count: 0 }
  void (async () => {
    while (!channel.isClosed) {
      const sending = channel.send(message(sent.count++))
      onSent?.(sent.count)
      await sending
    }
  })().catch(() => {})
  return sent
}

/** Takes what arrives, in order: at once, or, when slow, one message every 10 ms. */
function consume<T = number>(
  channel: { listen(cb: (data: T) => void | Promise<void>): unknown },
  { slow = false } = {},
) {
  const received: T[] = []
  const waiting: (() => void)[] = []
  let consumed = 0
  channel.listen((data) => {
    received.push(data)
    if (!slow) {
      consumed++
      return
    }
    return new Promise<void>((resolve) =>
      waiting.push(() => {
        consumed++
        resolve()
      }),
    )
  })
  if (slow) setInterval(() => waiting.shift()?.(), 10)
  return {
    received,
    get consumed() {
      return consumed
    },
  }
}

const flowOf = (channel: unknown) => (channel as { _flow: { msgWindow: number; byteWindow: number } })._flow

/** Where each end's `onClose` leaves what it got: `'open'` until it fires. */
function closedWith(ends: { page: { onClose(cb: (err?: Error) => void): void }; server: typeof ends.page }) {
  const closed: { page: unknown; server: unknown } = { page: 'open', server: 'open' }
  ends.page.onClose((err) => void (closed.page = err))
  ends.server.onClose((err) => void (closed.server = err))
  return closed
}

/** What a channel ends with when a reconnect needs messages `side`'s replay buffer dropped to stay within its size. */
const lostOn = (side: 'server' | 'client') =>
  new NetworkError(
    `Channel closed: a reconnect needed messages the ${side}'s replay buffer had dropped to stay within its size. Raise config.channel.${side}ReplayBuffer, or ${side}ReplayBufferBinary for binary messages and streams.`,
    true,
  )

const KIB = 1024

/** The server's credit left with its page, in bytes. */
const creditOf = (channel: unknown) => {
  const flow = (channel as { _flow: { _limitBytes: number; _sentBytes: number } })._flow
  return flow._limitBytes - flow._sentBytes
}

/** A 16 KiB message that names its place in the stream. */
const message = (n: number) => String(n).padEnd(16 * KIB)

/** `send(n)` for n = 0, 1, 2…, none awaited, until one rejects. */
async function sendUntilRejected(send: (n: number) => Promise<unknown>) {
  let error: unknown
  let sends = 0
  while (error === undefined && sends < 6_000) {
    send(sends++).catch((err: unknown) => (error = err))
    await run(0)
  }
  return { error, sends }
}

beforeEach(() => {
  vi.useFakeTimers()
  loop = new Loopback()
  vi.stubGlobal('WebSocket', LoopbackSocket)
})

afterEach(() => {
  loop.dispose()
  expect(loop.errors).toEqual([])
  vi.unstubAllGlobals()
  vi.useRealTimers()
  config.channel = {}
})

test('a stream the page consumes while the server awaits another channel its first reconcile named keeps flowing past 100 messages', async () => {
  const clock = loop.open<never, number>()
  const page = consume(clock.page)
  produce(clock.server)
  // The page opens a second channel whose server side isn't registered yet, which the server awaits.
  const late = loop.open<never, number>({ registered: false })
  await run(50)
  expect(page.received.length).toBeGreaterThan(CREDIT_MSG_WINDOW_INITIAL)
  late.register()
  const before = page.received.length
  await run(50)
  expect(page.received.length).toBeGreaterThan(before + CREDIT_MSG_WINDOW_INITIAL)
  expect(page.received).toEqual([...page.received.keys()])
})

test('a stream keeps flowing past its grown window after the page opens another channel, and after a reconnect', async () => {
  const clock = loop.open<never, number>()
  const page = consume(clock.page)
  produce(clock.server)
  await runUntil(() => flowOf(clock.page).msgWindow > 4 * CREDIT_MSG_WINDOW_INITIAL, 1_000)
  let window = flowOf(clock.page).msgWindow
  expect(window).toBeGreaterThan(4 * CREDIT_MSG_WINDOW_INITIAL)

  loop.open<never, number>() // the server attaches the stream's channel again, with the new one
  let before = page.received.length
  await run(100)
  expect(page.received.length - before).toBeGreaterThan(2 * window)

  window = flowOf(clock.page).msgWindow
  loop.socket.cut()
  before = page.received.length
  await runUntil(() => page.received.length - before > 2 * window, 1_000)
  expect(page.received.length - before).toBeGreaterThan(2 * window)
  expect(page.received).toEqual([...page.received.keys()])
})

test('an upload keeps flowing past its grown window after a reconnect, the server getting it all in order', async () => {
  const upload = loop.open<number, never>()
  const server = consume(upload.server)
  produce(upload.page)
  await runUntil(() => flowOf(upload.server).msgWindow > 4 * CREDIT_MSG_WINDOW_INITIAL, 1_000)
  const window = flowOf(upload.server).msgWindow
  expect(window).toBeGreaterThan(4 * CREDIT_MSG_WINDOW_INITIAL)
  loop.socket.cut() // what it carries is lost, and replays
  const before = server.received.length
  await runUntil(() => server.received.length - before > 2 * window, 1_000)
  expect(server.received.length - before).toBeGreaterThan(2 * window)
  expect(server.received).toEqual([...server.received.keys()])
})

test('a reattach on the live wire sends no flow-control frames, and one on a new wire repairs with them', async () => {
  const clock = loop.open<never, number>()
  await run(100)
  const flowControl = (from: 'page' | 'server') =>
    loop.sent[from].filter(([tag, ix]) => ix === 0 && (tag === TAG.WINDOW || tag === TAG.MSG_WINDOW)).length
  const before = { page: flowControl('page'), server: flowControl('server') }

  loop.open<never, number>() // its RECONCILE attaches the clock again, on the same wire
  await run(100)
  expect(clock.page.isClosed).toBe(false)
  expect({ page: flowControl('page'), server: flowControl('server') }).toEqual(before)

  loop.socket.cut()
  await run(1_000)
  expect(loop.sockets).toHaveLength(2)
  expect({ page: flowControl('page'), server: flowControl('server') }).toEqual({
    page: before.page + 2,
    server: before.server + 2,
  })
})

test("what the server has in flight to a slow page never exceeds the page's window, also right after a refresh", async () => {
  const clock = loop.open<never, number>()
  const page = consume(clock.page, { slow: true })
  let excess = -Infinity
  produce(clock.server, {
    onSent: (sent) => (excess = Math.max(excess, sent - page.consumed - flowOf(clock.page).msgWindow)),
  })
  await run(3_000)
  expect(page.consumed).toBeGreaterThan(2 * CREDIT_MSG_WINDOW_INITIAL)
  expect(excess).toBeLessThanOrEqual(0)
})

test("what a page has in flight to a slow server listener never exceeds the server's window, so the server buffers no more", async () => {
  const upload = loop.open<string, never>()
  // 64 KiB a message on the wire: the byte window, not the message window, is the one that binds.
  const payload = 'x'.repeat(64 * 1024 - 2)
  const bytes = payload.length + 2
  const server = consume(upload.server, { slow: true })
  let excess = -Infinity
  produce(upload.page, {
    message: () => payload,
    onSent: (sent) => (excess = Math.max(excess, (sent - server.consumed) * bytes - flowOf(upload.server).byteWindow)),
  })
  await run(3_000)
  expect(server.consumed).toBeGreaterThan(64)
  expect(excess).toBeLessThanOrEqual(0)
})

test('a stream whose window grew to 16 MiB, cut off with its window on the wire, resumes after the reconnect without loss', async () => {
  const clock = loop.open<never, string>()
  const got: number[] = []
  clock.page.listen((text) => void got.push(Number.parseInt(text)))
  const closed = closedWith(clock)
  // As the estimator grows it on a path whose BDP is that large (see flow-control.spec.ts).
  const estimator = (clock.page as unknown as { _flow: { _bdp: { growBytes(): void } } })._flow._bdp
  for (let n = 0; n < 3; n++) estimator.growBytes()
  const window = flowOf(clock.page).byteWindow
  expect(window).toBe(16 * KIB * KIB)
  const text = (n: number) => String(n).padEnd(256 * KIB)
  produce(clock.server, { message: text })
  await run(100)

  // From its next refresh on, the page stops hearing from the server, which sends the window that refresh grants. Then
  // the wire drops, with all of it on the way.
  loop.onSend('page', TAG.WINDOW, () => loop.socket.toPage.hold())
  await run(50)
  expect(loop.socket.toPage.bytes).toBeGreaterThan(window / 2)
  loop.socket.cut()
  const before = got.length
  await runUntil(() => got.length > before + (2 * window) / (256 * KIB), 2_000)
  expect(got.length).toBeGreaterThan(before + (2 * window) / (256 * KIB))
  expect(got).toEqual([...got.keys()])
  expect(closed).toEqual({ page: 'open', server: 'open' })
})

test("sends nobody awaits, past the page's window and more than the server's replay holds, end the channel with NetworkError on both ends at a reconnect, the page having got them in order up to there", async () => {
  config.channel.serverReplayBuffer = 4 * KIB
  const feed = loop.open<never, string>()
  const page = consume(feed.page)
  const closed = closedWith(feed)
  await run(100)
  loop.socket.toPage.hold()
  const text = (n: number) => String(n).padEnd(256)
  for (let n = 0; n < 64; n++) feed.server.send(text(n)).catch(() => {})
  await run(50)
  loop.socket.cut()
  await runUntil(() => closed.page !== 'open', 1_000)
  expect(closed).toEqual({ page: lostOn('server'), server: lostOn('server') })
  expect(page.received).toEqual(page.received.map((_, n) => text(n)))
})

test("sends nobody awaits, past the server's window and more than the page's replay holds, end the channel with NetworkError on both ends at a reconnect, the server having got them in order up to there", async () => {
  config.channel.clientReplayBuffer = 4 * KIB
  const upload = loop.open<string, never>()
  const server = consume(upload.server)
  const closed = closedWith(upload)
  await run(100)
  loop.socket.toServer.hold()
  const text = (n: number) => String(n).padEnd(256)
  for (let n = 0; n < 64; n++) upload.page.send(text(n)).catch(() => {})
  await run(50)
  loop.socket.cut()
  await runUntil(() => closed.server !== 'open', 1_000)
  expect(closed).toEqual({ page: lostOn('client'), server: lostOn('client') })
  expect(server.received).toEqual(server.received.map((_, n) => text(n)))
})

test("a page that opens another channel while more of what the server sent is on the wire than the server's replay buffer holds gets it all", async () => {
  config.channel.serverReplayBuffer = 512
  const feed = loop.open<never, string>()
  const page = consume(feed.page)
  const closed = closedWith(feed)
  await run(50)
  loop.socket.toPage.hold()
  const text = (n: number) => String(n).padEnd(64)
  for (let n = 0; n < 32; n++) feed.server.send(text(n)).catch(() => {})
  await run(20)
  expect(loop.socket.toPage.bytes).toBeGreaterThan(2 * 1_024)
  loop.open<never, number>() // its RECONCILE attaches the channel again, on the wire that carries what it sent
  await run(50)
  loop.socket.toPage.release()
  await run(100)
  expect(page.received).toEqual(Array.from({ length: 32 }, (_, n) => text(n)))
  expect(closed).toEqual({ page: 'open', server: 'open' })
})

test("a broadcast whose page is cut off further behind than the server's replay buffer holds leaves it with ChannelOverflowError on both ends, having got what was published before it in order", async () => {
  config.channel.serverReplayBuffer = 1_024
  const key = `room:${crypto.randomUUID()}`
  const room = loop.openBroadcast<string>(key)
  const closed = closedWith(room)
  const seen: string[] = []
  room.page.subscribe((text) => void seen.push(text))
  await run(100)
  Broadcast.publish(key, 'before')
  await run(50)
  loop.socket.toPage.hold()
  const publication = (n: number) => String(n).padEnd(256)
  for (let n = 0; n < 8; n++) Broadcast.publish(key, publication(n))
  await run(50)
  loop.socket.cut()
  await runUntil(() => closed.page !== 'open', 1_000)
  expect(closed.page).toBeInstanceOf(ChannelOverflowError)
  expect(closed.server).toBeInstanceOf(ChannelOverflowError)
  expect(seen).toEqual(['before', ...seen.slice(1).map((_, n) => publication(n))])
})

test('a broadcast whose page was offline while more was published than config.channel.bufferLimit holds closes on both ends with ChannelOverflowError at its reconnect', async () => {
  const key = `room:${crypto.randomUUID()}`
  const room = loop.openBroadcast<string>(key)
  const closed = closedWith(room)
  const seen: string[] = []
  room.page.subscribe((text) => void seen.push(text))
  await run(100)
  Broadcast.publish(key, 'before')
  await run(50)
  loop.socket.cut()
  // Offline, the server holds 512 KiB of text for the page: these are 1 MiB.
  for (let n = 0; n < 8; n++) Broadcast.publish(key, String(n).padEnd(128 * KIB))
  await runUntil(() => closed.page !== 'open', 2_000)
  expect(closed.server).toBeInstanceOf(ChannelOverflowError)
  expect(closed.page).toBeInstanceOf(ChannelOverflowError)
  expect(seen).toEqual(['before'])
})

test('a broadcast whose page was offline while less was published than config.channel.bufferLimit holds gets it all at its reconnect', async () => {
  const key = `room:${crypto.randomUUID()}`
  const room = loop.openBroadcast<string>(key)
  const closed = closedWith(room)
  const seen: string[] = []
  room.page.subscribe((text) => void seen.push(text))
  await run(100)
  loop.socket.cut()
  for (let n = 0; n < 3; n++) Broadcast.publish(key, String(n).padEnd(128 * KIB))
  await runUntil(() => seen.length === 3, 2_000)
  expect(seen).toEqual(Array.from({ length: 3 }, (_, n) => String(n).padEnd(128 * KIB)))
  expect(closed).toEqual({ page: 'open', server: 'open' })
})

// Written in one turn, a burst waits on the wire whatever the page's pace: a bound on it smaller than the largest window
// a page grants refused it to a page that reads at full speed.
test("a burst of sends nobody awaits, past the page's window and within the largest one a page grants, reaches a page that reads", async () => {
  const feed = loop.open<never, string>()
  const page = consume(feed.page)
  await run(100)
  // 30 MiB in one turn, as the playground's push benchmark sends.
  const burst = (n: number) => String(n).padEnd(512 * KIB)
  let error: unknown
  for (let n = 0; n < 60; n++) feed.server.send(burst(n)).catch((err: unknown) => (error ??= err))
  await runUntil(() => page.received.length === 60, 5_000)
  expect(error).toBeUndefined()
  expect(page.received).toEqual(Array.from({ length: 60 }, (_, n) => burst(n)))
})

test("a page that stops reading on a fresh wire has the server hold a channel's sends nobody awaits to its window and the largest window a page grants, on the wire or behind its probe: the next rejects with ChannelOverflowError, and the channel stays open", async () => {
  const feed = loop.open<never, string>()
  const page = consume(feed.page)
  await run(100)
  loop.socket.toPage.hold()
  const { error, sends } = await sendUntilRejected((n) => feed.server.send(message(n)))
  expect(error).toBeInstanceOf(ChannelOverflowError)
  expect(serverHolds(loop.socket)).toBeGreaterThan(CREDIT_WINDOW_INITIAL_BYTES + CREDIT_WINDOW_MAX_BYTES)
  // One message past them, and the headers of each one and of its pieces.
  expect(serverHolds(loop.socket)).toBeLessThanOrEqual(
    CREDIT_WINDOW_INITIAL_BYTES + CREDIT_WINDOW_MAX_BYTES + 192 * KIB,
  )

  // Once the page reads again it gets every message but the refused one, and what is sent after it caught up.
  loop.socket.toPage.release()
  await runUntil(() => page.received.length === sends - 1, 5_000)
  await feed.server.send(message(sends))
  await runUntil(() => page.received.length === sends, 1_000)
  expect(page.received).toEqual([...Array.from({ length: sends - 1 }, (_, n) => message(n)), message(sends)])
})

test("a page that stops reading on a fresh wire has the server hold a channel's ack requests nobody awaits to the largest window a page grants, on the wire or behind its probe: the next rejects with ChannelOverflowError", async () => {
  const feed = loop.open<never, string>()
  feed.page.listen(() => 'ok')
  await run(100)
  loop.socket.toPage.hold()
  const acks: Promise<unknown>[] = []
  const { error, sends } = await sendUntilRejected((n) => {
    const ack = feed.server.send(message(n), { ack: true })
    acks.push(ack)
    return ack
  })
  expect(error).toBeInstanceOf(ChannelOverflowError)
  expect(serverHolds(loop.socket)).toBeGreaterThan(CREDIT_WINDOW_MAX_BYTES)
  expect(serverHolds(loop.socket)).toBeLessThanOrEqual(CREDIT_WINDOW_MAX_BYTES + 192 * KIB)

  loop.socket.toPage.release()
  await run(5_000)
  expect(await Promise.all(acks.slice(0, sends - 1))).toEqual(Array(sends - 1).fill('ok'))
})

test('on a slow link, an awaited stream keeps flowing while a send nobody awaits on the same wire is refused past the largest window a page grants', async () => {
  const stream = loop.open<never, string>()
  const flood = loop.open<never, string>()
  const streamed = consume(stream.page)
  consume(flood.page)
  await run(100)
  loop.socket.toPage.bytesPerMs = 64_000 // 64 MB/s
  let streamError: unknown
  void (async () => {
    while (!stream.server.isClosed) await stream.server.send('x'.repeat(64 * KIB))
  })().catch((err: unknown) => (streamError = err))
  // 256 MB/s.
  let floodError: unknown
  let held = 0
  const flooding = setInterval(() => {
    held = Math.max(held, loop.socket.toPage.bytes)
    if (floodError === undefined) flood.server.send('x'.repeat(256 * KIB)).catch((err: unknown) => (floodError = err))
  }, 1)
  await run(1_000)
  expect(floodError).toBeInstanceOf(ChannelOverflowError)
  const midway = streamed.received.length
  await run(1_000)
  clearInterval(flooding)

  expect(streamError).toBeUndefined()
  expect(midway).toBeGreaterThan(10)
  expect(streamed.received.length - midway).toBeGreaterThan(40)
  // Nothing past the windows and, past the flood's, the largest window a page grants is added.
  expect(held).toBeLessThanOrEqual(
    flowOf(stream.page).byteWindow + flowOf(flood.page).byteWindow + CREDIT_WINDOW_MAX_BYTES + 512 * KIB,
  )
})

test('on a slow link, a chat nobody awaits that the page keeps up with is neither refused nor held up for seconds behind an awaited stream on the same wire', async () => {
  const stream = loop.open<never, string>()
  const chat = loop.open<never, { at: number; text: string }>()
  consume(stream.page)
  const delays: number[] = []
  chat.page.listen(({ at }) => void delays.push(Date.now() - at))
  await run(100)
  loop.socket.toPage.bytesPerMs = 4_000 // 4 MB/s
  let streamError: unknown
  void (async () => {
    while (!stream.server.isClosed) await stream.server.send('x'.repeat(64 * KIB))
  })().catch((err: unknown) => (streamError = err))
  // 1.6 MB/s of 16 KiB messages.
  let chatError: unknown
  let sent = 0
  const chatting = setInterval(() => {
    chat.server.send({ at: Date.now(), text: message(sent++) }).catch((err: unknown) => (chatError ??= err))
  }, 10)
  // By then, probes that counted the stream's own queue as in flight had doubled its window to 8 MiB.
  await run(4_000)
  clearInterval(chatting)
  await runUntil(() => delays.length === sent, 10_000)
  expect(chatError).toBeUndefined()
  expect(streamError).toBeUndefined()
  expect(delays).toHaveLength(sent)
  // Nothing holds it up past the stream's window, drained at the 2.4 MB/s the chat leaves of the link.
  expect(Math.max(...delays)).toBeLessThan(CREDIT_WINDOW_INITIAL_BYTES / 2_400 + 100)
  expect(flowOf(stream.page).byteWindow).toBe(CREDIT_WINDOW_INITIAL_BYTES)
})

test('a send nobody awaits goes out past the window of a page whose listener lags, while the wire holds less than the largest window a page grants', async () => {
  const feed = loop.open<never, string>()
  const page = consume(feed.page, { slow: true })
  await run(100)
  const big = (n: number) => String(n).padEnd(256 * KIB)
  let error: unknown
  let behind = 0
  for (let n = 0; n < 320; n++) {
    feed.server.send(big(n)).catch((err: unknown) => (error = err))
    behind = Math.max(behind, (n + 1 - page.consumed) * (256 * KIB) - flowOf(feed.page).byteWindow)
    await run(1)
  }
  expect(error).toBeUndefined()
  // The page's limit is at most what it consumed and its window, so the server sent this far past it.
  expect(behind).toBeGreaterThan(CREDIT_WINDOW_MAX_BYTES)
  await runUntil(() => page.received.length === 320, 5_000)
  expect(page.received).toEqual(Array.from({ length: 320 }, (_, n) => big(n)))
})

test('a page that stops reading a broadcast on a fresh wire leaves it with ChannelOverflowError on both ends once the server holds its room and a quarter more of publishes for it, on the wire or behind its probe', async () => {
  const key = `room:${crypto.randomUUID()}`
  const room = loop.openBroadcast<string>(key)
  const errors: { server?: Error; page?: Error } = {}
  room.server.onClose((err) => void (errors.server = err))
  room.page.onClose((err) => void (errors.page = err))
  const seen: string[] = []
  room.page.subscribe((text) => void seen.push(text))
  await run(100)
  loop.socket.toPage.hold()
  const publication = (n: number) => String(n).padEnd(256 * KIB)
  let published = 0
  while (!room.server.isClosed && published < 1_000) {
    Broadcast.publish(key, publication(published++))
    await run(0)
  }
  expect(errors.server).toBeInstanceOf(ChannelOverflowError)
  expect(serverHolds(loop.socket)).toBeGreaterThan(CREDIT_WINDOW_MAX_BYTES * 1.25)
  expect(serverHolds(loop.socket)).toBeLessThanOrEqual(CREDIT_WINDOW_MAX_BYTES * 1.25 + 512 * KIB)

  // Once the page reads again it gets every publish sent before the one that found it behind, then the close.
  loop.socket.toPage.release()
  await runUntil(() => errors.page !== undefined, 2_000)
  expect(errors.page).toBeInstanceOf(ChannelOverflowError)
  expect(seen).toEqual(Array.from({ length: published - 1 }, (_, n) => publication(n)))
})

test('on a slow link, a page that keeps up with a broadcast stays in it while an awaited stream fills the wire', async () => {
  const key = `room:${crypto.randomUUID()}`
  const stream = loop.open<never, string>()
  consume(stream.page)
  const room = loop.openBroadcast<string>(key)
  let closed: Error | undefined | null = null
  room.server.onClose((err) => void (closed = err))
  const seen: string[] = []
  room.page.subscribe((text) => void seen.push(text))
  await run(100)
  loop.socket.toPage.bytesPerMs = 4_000 // 4 MB/s
  void (async () => {
    while (!stream.server.isClosed) await stream.server.send('x'.repeat(64 * KIB))
  })().catch(() => {})
  let held = 0
  // 1.6 MB/s of publishes, which wait behind the stream's window on the wire.
  for (let n = 0; n < 300; n++) {
    Broadcast.publish(key, message(n))
    held = Math.max(held, loop.socket.toPage.bytes)
    await run(10)
  }
  await runUntil(() => seen.length === 300, 2_000)
  expect(closed).toBe(null)
  expect(held).toBeGreaterThan(CREDIT_WINDOW_INITIAL_BYTES)
  expect(seen).toEqual(Array.from({ length: 300 }, (_, n) => message(n)))
})

test("a page's consumption of what a broadcast publishes moves the server's limit for it", async () => {
  const key = `room:${crypto.randomUUID()}`
  const room = loop.openBroadcast<string>(key)
  const seen: string[] = []
  room.page.subscribe((text) => void seen.push(text))
  await run(100)
  expect(creditOf(room.server)).toBe(CREDIT_WINDOW_MAX_BYTES)
  const publications = CREDIT_WINDOW_MAX_BYTES / 4 / KIB / KIB + 1
  for (let n = 0; n < publications; n++) {
    Broadcast.publish(key, 'x'.repeat(KIB * KIB))
    await run(1)
  }
  await runUntil(() => seen.length === publications, 1_000)
  await run(100)
  // Sent 17 MiB, of which the last limit leaves less than one not yet counted consumed.
  expect(creditOf(room.server)).toBeGreaterThan(CREDIT_WINDOW_MAX_BYTES - KIB * KIB)
})

test("a broadcast's page acknowledges what it read as a stream's page does, however large its room, so the server's replay for it holds less than a quarter of a stream's window of it", async () => {
  const key = `room:${crypto.randomUUID()}`
  const room = loop.openBroadcast<string>(key)
  const seen: string[] = []
  room.page.subscribe((text) => void seen.push(text))
  await run(100)
  for (let n = 0; n < 12; n++) {
    Broadcast.publish(key, 'x'.repeat(KIB * KIB))
    await run(20)
  }
  await runUntil(() => seen.length === 12, 1_000)
  await run(50)
  const replay = (room.server as unknown as { _replayBuffer: { byteLength: number } })._replayBuffer
  expect(replay.byteLength).toBeLessThan(CREDIT_WINDOW_INITIAL_BYTES / 4)
})

test('on a slow link, producers that await their sends are handed the credit one at a time, and none is refused', async () => {
  const feed = loop.open<never, string>()
  const page = consume(feed.page)
  await run(100)
  loop.socket.toPage.bytesPerMs = 4_000 // 4 MB/s
  let size = KIB
  let error: unknown
  for (let p = 0; p < 8; p++)
    void (async () => {
      while (!feed.server.isClosed) await feed.server.send('x'.repeat(size))
    })().catch((err: unknown) => (error ??= err))
  // They start small: producers that start together each have a frame out as the credit first runs out, and those count.
  await run(100)
  // Every producer waits on credit by now. From here on each frame is 128 KiB.
  size = 128 * KIB
  let held = 0
  const watch = setInterval(() => (held = Math.max(held, loop.socket.toPage.bytes)), 1)
  const before = page.received.length
  await run(2_000)
  clearInterval(watch)
  expect(error).toBeUndefined()
  expect(held).toBeGreaterThan(CREDIT_WINDOW_INITIAL_BYTES)
  expect(page.received.length - before).toBeGreaterThan(40)
})

// 100 KB/s: a 2 MiB window takes 20 s to go through, twice the deadline a ping or pong has, which waits behind it.
test("on a link slower than a window per ping deadline, a stream's page keeps its wire while the stream keeps arriving ahead of its pong", async () => {
  const feed = loop.open<never, string>()
  const page = consume(feed.page)
  await run(100)
  loop.socket.toPage.bytesPerMs = 100
  produce(feed.server, { message: () => 'x'.repeat(64 * KIB) })
  await run(30_000)
  expect(loop.sockets).toHaveLength(1)
  expect(page.received.length).toBeGreaterThan(30)
})

test('a wire that stops delivering in the middle of a stream is taken for dead one pong deadline after its last frame', async () => {
  const feed = loop.open<never, string>()
  consume(feed.page)
  await run(100)
  loop.socket.toPage.bytesPerMs = 4_000 // 4 MB/s
  produce(feed.server, { message: () => 'x'.repeat(16 * KIB) })
  await run(7_000)
  loop.socket.toPage.hold()
  const stoppedAt = Date.now()
  await runUntil(() => loop.sockets.length === 2, 30_000)
  expect(loop.sockets).toHaveLength(2)
  // The pong deadline is two ping intervals; then the first reconnect waits its delay.
  expect(Date.now() - stoppedAt).toBeLessThanOrEqual(
    2 * CHANNEL_PING_INTERVAL_MS + CHANNEL_RECONNECT_INITIAL_DELAY_MS + 100,
  )
})

test("on an uplink slower than a window per ping deadline, the server doesn't cut an upload's wire while the upload keeps arriving ahead of its ping", async () => {
  const feed = loop.open<string, never>()
  const server = consume(feed.server)
  await run(100)
  loop.socket.toServer.bytesPerMs = 100
  const terminateConnection = vi.spyOn(loop.transport, 'terminateConnection')
  produce(feed.page, { message: () => 'x'.repeat(64 * KIB) })
  await run(30_000)
  expect(terminateConnection).not.toHaveBeenCalled()
  expect(server.received.length).toBeGreaterThan(30)
})

// 100 KB/s, with a ping every second: the page's ping waits 20 s behind its 2 MiB window, and the server's refresh for a
// quarter of it comes every 5 s, both past the page's 2 s pong deadline.
test("on an uplink slower than a quarter window per pong deadline, an upload's page keeps its wire while the upload keeps arriving ahead of its ping", async () => {
  config.channel.pingInterval = 1_000
  const feed = loop.open<string, never>()
  const server = consume(feed.server)
  await run(100)
  loop.socket.toServer.bytesPerMs = 100
  produce(feed.page, { message: () => 'x'.repeat(64 * KIB) })
  await run(30_000)
  expect(loop.sockets).toHaveLength(1)
  expect(server.received.length).toBeGreaterThan(40)
})

// The page's socket here reports nothing it holds, as a browser's does of what its network stack and kernel hold: a
// WebSocket's bufferedAmount reads 0 in Chromium while more than a megabyte of the upload waits below it.
test("a download whose path's round trip rises on its wire grows its window to carry the link's rate", async () => {
  const feed = loop.open<never, Uint8Array>()
  let got = 0
  feed.page.listenBinary((data) => void (got += data.byteLength))
  await run(1_000)
  loop.socket.toPage.bytesPerMs = 20_000
  void (async () => {
    while (!feed.server.isClosed) await feed.server.sendBinary(new Uint8Array(64 * KIB))
  })()
  await run(3_000)
  // 20 MB/s over a 200 ms round trip: 4 MB in flight, twice the window the 10 ms round trip measured at attach kept.
  for (const pipe of [loop.socket.toPage, loop.socket.toServer]) pipe.latencyMs = 100
  await run(5_000)
  const before = got
  await run(5_000)
  expect((got - before) / 5_000).toBeGreaterThan(0.9 * 20_000)
  expect(loop.sockets).toHaveLength(1)
})

test("on a slow uplink, the server's window for an upload stays at its initial size, however little the page's socket reports it holds", async () => {
  const feed = loop.open<string, never>()
  const server = consume(feed.server)
  await run(100)
  loop.socket.toServer.bytesPerMs = 100
  produce(feed.page, { message: () => 'x'.repeat(64 * KIB) })
  await run(60_000)
  expect(flowOf(feed.server).byteWindow).toBe(CREDIT_WINDOW_INITIAL_BYTES)
  expect(server.received.length).toBeGreaterThan(80)
})

// 100 KB/s: the RECONCILE naming the new channel waits 20 s behind the 2 MiB window of the upload, twice the time a page
// waits for its RECONCILED on a wire that delivers nothing. The server holds the new channel that long.
test('a channel the page opens while its upload fills a slow uplink attaches on the same wire, however long its RECONCILE waits behind the upload', async () => {
  config.channel.connectTtl = 60_000
  const upload = loop.open<string, never>()
  let uploaded = 0
  upload.server.listen(() => void uploaded++)
  await run(100)
  loop.socket.toServer.bytesPerMs = 100
  produce(upload.page, { message: () => 'x'.repeat(64 * KIB) })
  await run(10_000)
  const late = loop.open<string, never>()
  let arrived = 0
  late.server.listen(() => void arrived++)
  produce(late.page, { message: () => 'y'.repeat(64 * KIB) })
  await run(40_000)
  expect(loop.sockets).toHaveLength(1)
  expect(arrived).toBeGreaterThan(0)
  expect(uploaded).toBeGreaterThan(30)
})

test('a page whose downlink stops while its RECONCILE waits behind its upload takes the wire for dead once it has delivered nothing for the time a page waits for its RECONCILED', async () => {
  config.channel.connectTtl = 60_000
  const upload = loop.open<string, never>()
  consume(upload.server)
  await run(100)
  loop.socket.toServer.bytesPerMs = 100
  produce(upload.page, { message: () => 'x'.repeat(64 * KIB) })
  await run(10_000)
  loop.open<string, never>()
  await run(1_000)
  // The server doesn't cut it: the page finds out on its own, as when the server can't reach it.
  vi.spyOn(loop.transport, 'terminateConnection').mockImplementation(() => {})
  loop.socket.toPage.hold()
  const stoppedAt = Date.now()
  await runUntil(() => loop.sockets.length === 2, 30_000)
  expect(loop.sockets).toHaveLength(2)
  expect(Date.now() - stoppedAt).toBeLessThanOrEqual(RECONCILE_TIMEOUT_MS + CHANNEL_RECONNECT_INITIAL_DELAY_MS + 100)
})

test('on a link too slow for a megabyte in a ping deadline, a megabyte sent at connect, and the next, cross on one wire, either way', async () => {
  config.channel.pingInterval = 1_000
  // 100 KB/s: a megabyte takes 10 s, five pong deadlines.
  loop.linkBytesPerMs = 100
  const { server, page } = loop.open<Uint8Array, Uint8Array>()
  const atServer: number[] = []
  const atPage: number[] = []
  server.listenBinary((data) => void atServer.push(data.byteLength))
  page.listenBinary((data) => void atPage.push(data.byteLength))
  await run(1_000)
  void page.sendBinary(new Uint8Array(1_000_000))
  void server.sendBinary(new Uint8Array(1_000_000))
  await runUntil(() => atServer.length === 1 && atPage.length === 1, 60_000)
  void page.sendBinary(new Uint8Array(1_000_000))
  void server.sendBinary(new Uint8Array(1_000_000))
  await runUntil(() => atServer.length === 2 && atPage.length === 2, 60_000)
  expect(atServer).toEqual([1_000_000, 1_000_000])
  expect(atPage).toEqual([1_000_000, 1_000_000])
  expect(loop.sockets).toHaveLength(1)
})

test.each(['server', 'page'] as const)(
  'on a link too slow for a megabyte in the RECONCILE deadline, a megabyte the %s sends as the page attaches crosses on one wire in the time the link takes',
  async (sender) => {
    // 100 kbit/s: a megabyte takes 80 s, past the 10 s a first wire has to deliver its RECONCILED.
    loop.linkBytesPerMs = 12.5
    const openedAt = Date.now()
    const { server, page } = loop.open<Uint8Array, Uint8Array>()
    let arrivedAt = Number.NaN
    if (sender === 'server') {
      server.onOpen(() => void server.sendBinary(new Uint8Array(1_000_000)))
      page.listenBinary(() => void (arrivedAt = Date.now()))
    } else {
      void page.sendBinary(new Uint8Array(1_000_000))
      server.listenBinary(() => void (arrivedAt = Date.now()))
    }
    await run(90_000)
    expect(arrivedAt - openedAt).toBeLessThanOrEqual(1_000_000 / 12.5 + 1_000)
    expect(loop.sockets).toHaveLength(1)
  },
)

for (const kbit of [10, 5]) {
  test(`on a link of ${kbit} kbit/s, a 40 KiB message crosses on one wire in the time the link takes, either way`, async () => {
    loop.linkBytesPerMs = kbit / 8
    const { server, page } = loop.open<Uint8Array, Uint8Array>()
    const arrived: { page?: number; server?: number } = {}
    page.listenBinary(() => void (arrived.page = Date.now()))
    server.listenBinary(() => void (arrived.server = Date.now()))
    await run(3_000)
    const sentAt = Date.now()
    void page.sendBinary(new Uint8Array(40 * KIB))
    void server.sendBinary(new Uint8Array(40 * KIB))
    const crossMs = (40 * KIB) / (kbit / 8)
    await run(1.2 * crossMs)
    expect(arrived.page! - sentAt).toBeLessThanOrEqual(1.05 * crossMs)
    expect(arrived.server! - sentAt).toBeLessThanOrEqual(1.05 * crossMs)
    expect(loop.sockets).toHaveLength(1)
  })
}

test.each([10, 50, 200].flatMap((rtt) => [8 * KIB, 32 * KIB, 512 * KIB].map((size) => [rtt, size])))(
  'on a fresh wire with a %i ms round trip, a first message of %i B arrives one way later, as a whole message does, either way',
  async (rtt, size) => {
    const { server, page } = loop.open<Uint8Array, Uint8Array>()
    const arrived: { page?: number; server?: number } = {}
    page.listenBinary(() => void (arrived.page = Date.now()))
    server.listenBinary(() => void (arrived.server = Date.now()))
    await run(1_000)
    loop.socket.toServer.latencyMs = loop.socket.toPage.latencyMs = rtt / 2
    const sentAt = Date.now()
    void page.sendBinary(new Uint8Array(size))
    void server.sendBinary(new Uint8Array(size))
    await runUntil(() => arrived.page !== undefined && arrived.server !== undefined, 1_000)
    // Its pieces take a read each, where the in-process socket reads up to `READ_BYTES` of whole messages a tick.
    for (const at of [arrived.page!, arrived.server!])
      expect(at - sentAt).toBeLessThanOrEqual(rtt / 2 + size / READ_BYTES)
  },
)

test.each([1.5, 1])(
  'on a fresh wire of %s kbit/s, an 8 KiB message crosses on one wire in the time the link takes, either way',
  async (kbit) => {
    config.channel.connectTtl = 600_000
    config.channel.reconnectTimeout = 600_000
    loop.linkBytesPerMs = kbit / 8
    const { server, page } = loop.open<Uint8Array, Uint8Array>()
    const arrived: { page?: number; server?: number } = {}
    page.listenBinary(() => void (arrived.page = Date.now()))
    server.listenBinary(() => void (arrived.server = Date.now()))
    await run(5_000)
    const sentAt = Date.now()
    void page.sendBinary(new Uint8Array(8 * KIB))
    void server.sendBinary(new Uint8Array(8 * KIB))
    const crossMs = (8 * KIB) / (kbit / 8)
    await run(1.2 * crossMs)
    expect(arrived.page! - sentAt).toBeLessThanOrEqual(1.01 * crossMs)
    expect(arrived.server! - sentAt).toBeLessThanOrEqual(1.01 * crossMs)
    expect(loop.sockets).toHaveLength(1)
  },
)

test.each((['server', 'page'] as const).flatMap((sender) => [2, 1].map((kbit) => [sender, kbit] as const)))(
  'on a fresh wire, a 4000 B message the %s sends at %s kbit/s crosses on one wire in the time the link takes',
  async (sender, kbit) => {
    config.channel.connectTtl = 600_000
    config.channel.reconnectTimeout = 600_000
    loop.linkBytesPerMs = kbit / 8
    const { server, page } = loop.open<Uint8Array, Uint8Array>()
    let arrivedAt: number | undefined
    ;(sender === 'server' ? page : server).listenBinary(() => void (arrivedAt = Date.now()))
    await run(5_000)
    const sentAt = Date.now()
    void (sender === 'server' ? server : page).sendBinary(new Uint8Array(4_000))
    const crossMs = 4_000 / (kbit / 8)
    await run(1.2 * crossMs)
    expect(arrivedAt! - sentAt).toBeLessThanOrEqual(1.01 * crossMs)
    expect(loop.sockets).toHaveLength(1)
  },
)

test.each([
  [4 * KIB, 2_000],
  [32 * KIB, 500],
  [128 * KIB, 200],
])(
  'a fresh wire carries a push of %i B messages nobody awaits as fast as the same wire does once it measured its rate',
  async (size, count) => {
    const { server, page } = loop.open<string, string>()
    let got = 0
    page.listen(() => void got++)
    await run(100)
    const payload = 'x'.repeat(size)
    const push = async () => {
      const [startedAt, until] = [Date.now(), got + count]
      for (let i = 0; i < count; i++) void server.send(payload)
      await runUntil(() => got === until, 10_000)
      return Date.now() - startedAt
    }
    const fresh = await push()
    await run(1_000)
    expect(fresh).toBeLessThanOrEqual(await push())
    expect(loop.sockets).toHaveLength(1)
  },
)

test.each(
  (['server', 'page'] as const).flatMap((sender) =>
    [
      // kbit/s, bytes, when the link dies, the most it may take to find it dead
      [100, 1_000_000, 5_000, 12_000],
      [1_000, 4 * 1024 * KIB, 1_000, 11_000],
      [100, 1_000_000, 40_000, 25_000],
    ].map(([kbit, bytes, diesAfter, withinMs]) => [sender, kbit!, bytes!, diesAfter!, withinMs!] as const),
  ),
)(
  'a fresh wire on which the %s sends at %i kbit/s %i B, whose link dies %i ms in, is found dead within %i ms',
  async (sender, kbit, bytes, diesAfter, withinMs) => {
    config.channel.connectTtl = 600_000
    config.channel.reconnectTimeout = 600_000
    loop.linkBytesPerMs = kbit / 8
    const { server, page } = loop.open<Uint8Array, Uint8Array>()
    await run(3_000)
    // The server doesn't cut it: the page finds out on its own, as when the server can't reach it.
    vi.spyOn(loop.transport, 'terminateConnection').mockImplementation(() => {})
    void (sender === 'server' ? server : page).sendBinary(new Uint8Array(bytes)).catch(() => {})
    await run(diesAfter)
    ;(sender === 'server' ? loop.socket.toPage : loop.socket.toServer).hold()
    const diedAt = Date.now()
    await runUntil(() => loop.sockets.length === 2, 60_000)
    expect(loop.sockets).toHaveLength(2)
    expect(Date.now() - diedAt).toBeLessThanOrEqual(withinMs)
  },
)

test('a push of 4 KiB messages on a fresh wire cuts only its first', async () => {
  const { server, page } = loop.open<string, string>()
  let got = 0
  page.listen(() => void got++)
  await run(100)
  const mark = loop.sent.server.length
  for (let i = 0; i < 2_000; i++) void server.send('x'.repeat(4 * KIB))
  await runUntil(() => got === 2_000, 10_000)
  // Its empty piece, its probe pair, and its rest.
  expect(loop.sent.server.slice(mark).filter(([tag]) => tag === TAG.PIECE)).toHaveLength(4)
})

test('a fresh wire carrying only small messages stops announcing them once one announcement was acknowledged, either way', async () => {
  const { server, page } = loop.open<Uint8Array, Uint8Array>()
  let got = 0
  page.listenBinary(() => void got++)
  server.listenBinary(() => void got++)
  const announces = () =>
    [loop.sent.page, loop.sent.server].map((sent) => sent.filter(([tag]) => tag === TAG.ANNOUNCE).length)
  const push = async () => {
    for (let i = 0; i < 1_000; i++) {
      void page.sendBinary(new Uint8Array(64))
      void server.sendBinary(new Uint8Array(64))
      await run(1)
    }
  }
  await push()
  const first = announces()
  await push()
  await runUntil(() => got === 4_000, 1_000)
  expect(got).toBe(4_000)
  expect(announces()).toEqual(first)
})

test.each((['server', 'page'] as const).flatMap((sender) => [2, 1].map((kbit) => [sender, kbit] as const)))(
  'a 4000 B message the %s sends as a fresh wire of %s kbit/s opens crosses on that wire',
  async (sender, kbit) => {
    config.channel.connectTtl = 600_000
    config.channel.reconnectTimeout = 600_000
    loop.linkBytesPerMs = kbit / 8
    const { server, page } = loop.open<Uint8Array, Uint8Array>()
    let arrived = false
    ;(sender === 'server' ? page : server).listenBinary(() => void (arrived = true))
    if (sender === 'server') server.onOpen(() => void server.sendBinary(new Uint8Array(4_000)))
    else void page.sendBinary(new Uint8Array(4_000))
    // The handshake and the message, at the link's rate.
    await runUntil(() => arrived, (1.2 * 4_600) / (kbit / 8))
    expect(arrived).toBe(true)
    expect(loop.sockets).toHaveLength(1)
  },
)

test('on a fresh wire with a 600 ms round trip, small messages sent once it opened go unannounced, either way', async () => {
  const { server, page } = loop.open<Uint8Array, Uint8Array>()
  await run(0)
  loop.socket.toServer.latencyMs = loop.socket.toPage.latencyMs = 300
  let got = 0
  page.listenBinary(() => void got++)
  server.listenBinary(() => void got++)
  server.onOpen(() => void server.sendBinary(new Uint8Array(16)))
  void page.sendBinary(new Uint8Array(16))
  await runUntil(() => got === 2, 2_000)
  for (let i = 0; i < 1_000; i++) {
    void page.sendBinary(new Uint8Array(64))
    void server.sendBinary(new Uint8Array(64))
    await run(1)
  }
  await runUntil(() => got === 2_002, 1_000)
  expect(got).toBe(2_002)
  expect([loop.sent.page, loop.sent.server].map((sent) => sent.filter(([tag]) => tag === TAG.ANNOUNCE))).toEqual([
    [],
    [],
  ])
})

test('a WebSocket that cannot be constructed sends the channel to its reconnect loop, it does not throw', async () => {
  config.channel.transports = ['ws']
  vi.stubGlobal('WebSocket', undefined)
  let thrown: unknown
  try {
    loop.open<Uint8Array, Uint8Array>()
    await run(3_000)
  } catch (err) {
    thrown = err
  }
  expect(thrown).toBeUndefined()
})

test('a burst of large frames gets a few PIECES_ACKs, not one each, and the last covers all of it', async () => {
  const { server, page } = loop.open<Uint8Array, Uint8Array>()
  const at = { server: 0, page: 0 }
  server.listenBinary(() => void at.server++)
  page.listenBinary(() => void at.page++)
  await run(100)
  const marks = { page: loop.sent.page.length, server: loop.sent.server.length }
  for (let i = 0; i < 100; i++) {
    void page.sendBinary(new Uint8Array(32_000))
    void server.sendBinary(new Uint8Array(32_000))
  }
  await runUntil(() => at.server === 100 && at.page === 100, 5_000)
  await run(100)
  for (const side of ['page', 'server'] as const) {
    const acks = loop.sent[side].slice(marks[side]).filter(([tag]) => tag === TAG.PIECES_ACK)
    expect(acks.length).toBeGreaterThan(0)
    // The server acknowledges at most every `WIRE_SEND_ACK_GAP_MS`, the page every `WIRE_PIECES_ACK_GAP_MS`.
    expect(acks.length).toBeLessThan(side === 'server' ? 20 : 5)
  }
  const sender = piecesOf(loop.socket)?.sender as { unacknowledged: unknown[] }
  expect(sender.unacknowledged).toHaveLength(0)
  expect(loop.sockets).toHaveLength(1)
})

test('a flood of small frames gets a PIECES_ACK from the server, which the page holds back by, and none from the page', async () => {
  const { server, page } = loop.open<Uint8Array, Uint8Array>()
  const at = { server: 0, page: 0 }
  server.listenBinary(() => void at.server++)
  page.listenBinary(() => void at.page++)
  await run(100)
  const marks = { page: loop.sent.page.length, server: loop.sent.server.length }
  for (let i = 0; i < 100; i++) {
    void page.sendBinary(new Uint8Array(1_000))
    void server.sendBinary(new Uint8Array(1_000))
  }
  await runUntil(() => at.server === 100 && at.page === 100, 5_000)
  await run(100)
  const acks = (side: 'page' | 'server') =>
    loop.sent[side].slice(marks[side]).filter(([tag]) => tag === TAG.PIECES_ACK).length
  expect(acks('server')).toBeGreaterThan(0)
  expect(acks('page')).toBe(0)
})

test('a page that aborts its channel right after a message on a fresh wire hands its socket the abort before it lets the wire go idle', async () => {
  config.channel.idleTimeout = 1
  const { page } = loop.open<Uint8Array, Uint8Array>()
  await run(100)
  const socket = loop.socket
  // Within the room the server gives its page, so the abort goes to the wire at once, behind the message's probe.
  void page.sendBinary(new Uint8Array(10_000)).catch(() => {})
  page.abort()
  await runUntil(() => socket.readyState === 3, 1_000)
  expect(socket.readyState).toBe(3)
  expect(loop.sent.page.map(([tag]) => tag)).toContain(TAG.ABORT)
})

test('a burst of 60 frames of 512 KB a server sends as its channel opens on a fast wire goes at once, whole after its first two', async () => {
  const { server, page } = loop.open<Uint8Array, Uint8Array>()
  let got = 0
  page.listenBinary(() => void got++)
  server.onOpen(() => {
    for (let i = 0; i < 60; i++) void server.sendBinary(new Uint8Array(512_000))
  })
  await runUntil(() => got === 60, 5_000)
  expect(got).toBe(60)
  // The first frame's empty piece, its pair, and its rest in six pieces that at most double; the second in two.
  expect(loop.sent.server.filter(([tag]) => tag === TAG.PIECE)).toHaveLength(11)
  expect(loop.sent.page.filter(([tag]) => tag === TAG.PIECES_ACK).length).toBeLessThanOrEqual(4)
})

test('a server pushing 512 KB frames on a fast wire sends them whole once it measured, however many it queues', async () => {
  config.channel.pingInterval = 1_000
  const { server, page } = loop.open<Uint8Array, Uint8Array>()
  let got = 0
  page.listenBinary(() => void got++)
  await run(100)
  loop.socket.toPage.bytesPerMs = 100_000 // 100 MB/s
  const mark = loop.sent.server.length
  void (async () => {
    for (let i = 0; i < 100; i++) await server.sendBinary(new Uint8Array(512_000))
  })()
  await runUntil(() => got === 100, 10_000)
  const tags = loop.sent.server.slice(mark).map(([tag]) => tag)
  expect(tags.filter((tag) => tag === TAG.BINARY).length).toBeGreaterThan(90)
})

test('on a fresh fast wire, frames of 32 KB and 128 KB go at once, in pieces that grow from a probe pair, either way', async () => {
  const { server, page } = loop.open<Uint8Array, Uint8Array>()
  const atServer: number[] = []
  const atPage: number[] = []
  server.listenBinary((data) => void atServer.push(data.byteLength))
  page.listenBinary((data) => void atPage.push(data.byteLength))
  await run(100)
  const sentAt = Date.now()
  const marks = { page: loop.sent.page.length, server: loop.sent.server.length }
  for (const size of [32_000, 128_000]) {
    void page.sendBinary(new Uint8Array(size))
    void server.sendBinary(new Uint8Array(size))
  }
  await runUntil(() => atServer.length === 2 && atPage.length === 2, 1_000)
  // No round trip: they cross with the latency.
  expect(Date.now() - sentAt).toBeLessThanOrEqual(2 * LATENCY_MS)
  expect(atServer).toEqual([32_000, 128_000])
  expect(atPage).toEqual([32_000, 128_000])
  for (const side of ['page', 'server'] as const) {
    const sent = loop.sent[side].slice(marks[side])
    // A BINARY frame is its 7 B header and its data. Each piece carries at most twice the one before, or 8 KiB; the
    // last two of a frame are cut so the last is the larger.
    expect(sent.filter(([tag]) => tag === TAG.PIECE).map(([, , bytes]) => bytes - PIECE_HEADER_BYTES)).toEqual([
      0, 64, 2_048, 8_192, 7_235, 14_468, 28_936, 33_024, 66_047,
    ])
  }
  expect(loop.sockets).toHaveLength(1)
})

test.each(['page', 'server'] as const)(
  "after a wire is lost with a probe of the %s's unacknowledged, the next wire probes with one small piece from its first, either way",
  async (sender) => {
    const { server, page } = loop.open<Uint8Array, Uint8Array>()
    const atServer: number[] = []
    const atPage: number[] = []
    server.listenBinary((data) => void atServer.push(data.byteLength))
    page.listenBinary((data) => void atPage.push(data.byteLength))
    await run(100)
    const lost = loop.socket
    ;(sender === 'page' ? lost.toServer : lost.toPage).bytesPerMs = 1
    const before = loop.sent[sender].length
    void (sender === 'page' ? page : server).sendBinary(new Uint8Array(128_000))
    await run(100)
    // Behind its announcement, its empty piece, its probe pair, and the rest of it at once.
    expect(loop.sent[sender].slice(before).map(([tag]) => tag)).toEqual([TAG.ANNOUNCE, ...Array(8).fill(TAG.PIECE)])
    lost.cut()
    await runUntil(() => loop.sockets.length === 2, 5_000)
    const marks = { page: loop.sent.page.length, server: loop.sent.server.length }
    await run(1_000)
    void page.sendBinary(new Uint8Array(100_000))
    void server.sendBinary(new Uint8Array(100_000))
    await runUntil(() => atServer.length > 0 && atPage.length > 0, 5_000)
    for (const side of ['page', 'server'] as const) {
      expect(firstPiece(side, marks[side])).toBe(WIRE_PROBE_BYTES)
    }
    expect(loop.sockets).toHaveLength(2)
  },
)

test('after a page gives up a wire whose pongs stopped, with a probe of its own unacknowledged, the next wire probes with one small piece', async () => {
  config.channel.pingInterval = 1_000
  const { page } = loop.open<Uint8Array, Uint8Array>()
  await run(100)
  // Nothing the server sends reaches the page: its pongs stop, and so does the PIECES_ACK for its frame.
  loop.socket.toPage.hold()
  void page.sendBinary(new Uint8Array(128_000))
  await runUntil(() => loop.sockets.length === 2, 10_000)
  const mark = loop.sent.page.length
  await run(1_000)
  void page.sendBinary(new Uint8Array(100_000))
  await run(1_000)
  expect(firstPiece('page', mark)).toBe(WIRE_PROBE_BYTES)
})

test("after a page drops a wire the server still holds open, with a probe of the server's unacknowledged on it, the next wire probes with one small piece", async () => {
  config.channel.pingInterval = 1_000
  const { server, page } = loop.open<Uint8Array, Uint8Array>()
  const atServer: number[] = []
  const atPage: number[] = []
  server.listenBinary((data) => void atServer.push(data.byteLength))
  page.listenBinary((data) => void atPage.push(data.byteLength))
  await run(100)
  // The page waits for what the server announced at the rate it arrives: the wire must die to be dropped.
  void server.sendBinary(new Uint8Array(128_000))
  loop.socket.toPage.hold()
  await runUntil(() => loop.sockets.length === 2, 10_000)
  const marks = { page: loop.sent.page.length, server: loop.sent.server.length }
  await run(1_000)
  void page.sendBinary(new Uint8Array(100_000))
  await runUntil(() => atServer.length > 0, 5_000)
  for (const side of ['page', 'server'] as const) {
    expect(firstPiece(side, marks[side])).toBe(WIRE_PROBE_BYTES)
  }
})

test.each([
  [4600, 500],
  [4200, 900],
  [4800, 300],
  [4900, 400],
])(
  "on a slow uplink, a page whose PING waits behind its probe while it acknowledges the server's download past it keeps its wire (%i ms into a ping interval, the download %i ms later)",
  async (offset, delay) => {
    const { server, page } = loop.open<Uint8Array, Uint8Array>()
    const atServer: number[] = []
    const atPage: number[] = []
    server.listenBinary((data) => void atServer.push(data.byteLength))
    page.listenBinary((data) => void atPage.push(data.byteLength))
    await run(100)
    // A wire lost with the server's probe unacknowledged: the next wire is slow on both ends.
    loop.socket.toPage.bytesPerMs = 1
    void server.sendBinary(new Uint8Array(128_000))
    await run(100)
    loop.socket.cut()
    await runUntil(() => loop.sockets.length === 2 && atPage.length === 1, 10_000)
    loop.socket.toServer.bytesPerMs = 0.25
    let pinged = false
    loop.onSend('page', TAG.PING, () => void (pinged = true))
    await runUntil(() => pinged, 10_000)
    await run(offset)
    void page.sendBinary(new Uint8Array(1_000))
    await run(delay)
    void server.sendBinary(new Uint8Array(20_000))
    await runUntil(() => atServer.length === 1 && atPage.length === 2, 30_000)
    await run(15_000)
    expect(atServer).toEqual([1_000])
    expect(loop.sockets).toHaveLength(2)
  },
)

test('the server holds nothing for empty pieces, takes pieces of any size, and cuts a wire on an empty piece in the middle of a frame', async () => {
  loop.open<Uint8Array, Uint8Array>()
  await run(100)
  const socket = loop.socket
  // The page never reads the PONG each piece gets.
  socket.toPage.hold()
  for (let i = 0; i < 10_000; i++) socket.send(encode.piece(1_000_000, new Uint8Array(0)))
  await run(1_000)
  const receiver = () => piecesOf(socket)!.receiver as { held: number; frame: Uint8Array }
  expect(receiver().held).toBe(0)
  for (let i = 0; i < 3; i++) socket.send(encode.piece(1_000_000, new Uint8Array(1)))
  await run(100)
  expect(socket.readyState).toBe(1)
  expect(receiver().held).toBe(3)
  // What it holds of a frame is one buffer, at most twice what arrived of it.
  expect(receiver().frame.byteLength).toBeLessThanOrEqual(6)
  socket.send(encode.piece(1_000_000, new Uint8Array(0)))
  await run(100)
  expect(socket.readyState).toBe(3)
})

test('pieces of a frame larger than the runtime takes in one message cut the wire, as that message would', async () => {
  setAdapterMaxFrameBytes(1024 * KIB)
  try {
    loop.open<Uint8Array, Uint8Array>()
    await run(100)
    const socket = loop.socket
    socket.send(encode.piece(1024 * KIB + 1, new Uint8Array(0)))
    await run(100)
    expect(socket.readyState).toBe(3)
  } finally {
    setAdapterMaxFrameBytes(undefined)
  }
})

test('a page whose uplink stops with its upload queued on it takes the wire for dead within a pong deadline', async () => {
  config.channel.pingInterval = 1_000
  const feed = loop.open<string, never>()
  consume(feed.server)
  await run(100)
  loop.socket.toServer.bytesPerMs = 100
  produce(feed.page, { message: () => 'x'.repeat(64 * KIB) })
  await run(10_000)
  // The server doesn't cut it: the page finds out on its own, as when the server can't reach it either.
  vi.spyOn(loop.transport, 'terminateConnection').mockImplementation(() => {})
  loop.socket.toServer.hold()
  const stoppedAt = Date.now()
  await runUntil(() => loop.sockets.length === 2, 10_000)
  expect(loop.sockets).toHaveLength(2)
  // The pong deadline is two ping intervals; then the first reconnect waits its delay.
  expect(Date.now() - stoppedAt).toBeLessThanOrEqual(2 * 1_000 + CHANNEL_RECONNECT_INITIAL_DELAY_MS + 100)
})

test('on a slow link, a producer that awaits its sends is not refused a message larger than its credit, sent with little credit left', async () => {
  const feed = loop.open<never, string>()
  const page = consume(feed.page)
  await run(100)
  loop.socket.toPage.bytesPerMs = 1_000 // 1 MB/s
  let error: unknown
  let left = Infinity
  void (async () => {
    // 64 KiB messages, so the byte window binds before the message window does.
    while (creditOf(feed.server) > 64 * KIB) await feed.server.send('x'.repeat(64 * KIB))
    left = creditOf(feed.server)
    await feed.server.send('x'.repeat(CREDIT_WINDOW_INITIAL_BYTES))
    for (let n = 0; n < 8; n++) await feed.server.send(message(n))
  })().catch((err: unknown) => (error ??= err))
  await runUntil(() => page.received.length > 0 && page.received.at(-1) === message(7), 10_000)
  expect(left).toBeGreaterThan(0)
  expect(left).toBeLessThanOrEqual(64 * KIB)
  expect(error).toBeUndefined()
  expect(page.received.at(-1)).toBe(message(7))
})

test('on a slow link, a producer that awaits its sends is not refused for the credit another one took with a message larger than it', async () => {
  const feed = loop.open<never, string>()
  const page = consume(feed.page)
  await run(100)
  loop.socket.toPage.bytesPerMs = 1_000 // 1 MB/s
  // 64 KiB messages, 32 of them in the page's 2 MiB window: the byte window binds before the message window does.
  while (creditOf(feed.server) > 64 * KIB) await feed.server.send('x'.repeat(64 * KIB))
  // Both producers' last sends resolved with credit left. One sends a message past it, then the other sends its next.
  const settled = Promise.allSettled([
    feed.server.send('x'.repeat(CREDIT_WINDOW_INITIAL_BYTES)),
    feed.server.send(message(1)),
  ])
  await runUntil(() => page.received.at(-1) === message(1), 10_000)
  expect((await settled).map((result) => result.status)).toEqual(['fulfilled', 'fulfilled'])
  expect(page.received.at(-1)).toBe(message(1))
})

test("a window refresh, a BDP_PING_ACK and a RECONCILE the page sends while an upload fills its slow uplink reach the server within the wire's queue delay, not behind the upload's window, and the upload keeps the uplink full", async () => {
  const upload = loop.open<Uint8Array, Uint8Array>()
  const download = loop.open<Uint8Array, Uint8Array>()
  let uploaded = 0
  upload.server.listenBinary((data) => void (uploaded += data.byteLength))
  download.page.listenBinary(() => {})
  await run(100)
  loop.socket.toServer.bytesPerMs = 1_250 // a 1.25 MB/s uplink
  const chunk = new Uint8Array(64 * 1024)
  let messages = 0
  void (async () => {
    while (!upload.page.isClosed) {
      messages++
      await upload.page.sendBinary(chunk)
    }
  })().catch(() => {})
  void (async () => {
    while (!download.server.isClosed) await download.server.sendBinary(chunk)
  })().catch(() => {})
  await run(3_000)
  const marks = { at: Date.now(), uploaded }
  loop.open() // the page registers a channel: a RECONCILE
  await run(4_000)
  /** How long each frame of `tag` that the page sent after the mark took to reach the server. */
  const waits = (tag: number) => {
    const departures = loop.sent.page.flatMap(([sent], i) => (sent === tag ? [loop.sentAt.page[i]!] : []))
    const arrivals = loop.arrived.flatMap(([arrived, at]) => (arrived === tag ? [at] : []))
    return arrivals.flatMap((at, i) => (departures[i]! >= marks.at ? [at - departures[i]!] : []))
  }
  for (const tag of [TAG.WINDOW, TAG.BDP_PING_ACK, TAG.RECONCILE]) {
    const delays = waits(tag)
    expect(delays.length, `frames of tag ${tag}`).toBeGreaterThan(0)
    expect(Math.max(...delays), `the longest wait of tag ${tag}`).toBeLessThan(600)
  }
  expect(uploaded - marks.uploaded).toBeGreaterThan(0.6 * 4_000 * 1_250)
  // The RECONCILE's round trip didn't have the page send again what it still held back.
  const sentTwice = loop.sent.page.filter(([tag]) => tag === TAG.BINARY).length - messages
  expect(sentTwice).toBeLessThanOrEqual(0)
})

test('a channel the page closes right after sends its slow uplink held back delivers them all, in order, before it closes', async () => {
  const { server, page } = loop.open<Uint8Array, never>()
  const got: number[] = []
  server.listenBinary((data) => void got.push(new DataView(data.buffer, data.byteOffset).getUint32(0)))
  const closed = closedWith({ page, server })
  await run(100)
  loop.socket.toServer.bytesPerMs = 600 // 600 KB/s
  const counted = (n: number) => Object.assign(new Uint8Array(64 * 1024), { 0: n & 0xff, 1: n >> 8, 2: 0, 3: 0 })
  for (let n = 0; n < 24; n++) void page.sendBinary(counted(n))
  void page.close()
  await runUntil(() => closed.page !== 'open' && closed.server !== 'open', 30_000)
  expect(got.length).toBe(24)
  expect(got).toEqual([...got.keys()].map((n) => new DataView(counted(n).buffer).getUint32(0)))
  expect(closed).toEqual({ page: undefined, server: undefined })
})

const counted = (n: number) => {
  const message = new Uint8Array(64 * KIB)
  new DataView(message.buffer).setUint32(0, n)
  return message
}
const countOf = (message: Uint8Array) => new DataView(message.buffer, message.byteOffset).getUint32(0)

test.each([
  ['every send was queued before', false],
  ['a producer keeps sending while', true],
])(
  'a page whose slow uplink holds its sends back delivers them all, in order, when %s a channel it opened meanwhile attaches',
  async (_, producer) => {
    const { server, page } = loop.open<Uint8Array, never>()
    const got: number[] = []
    server.listenBinary((data) => void got.push(countOf(data)))
    await run(100)
    loop.socket.toServer.bytesPerMs = 600
    const count = producer ? 60 : 28
    let sent = 0
    if (producer)
      void (async () => {
        while (sent < count) await page.sendBinary(counted(sent++))
      })().catch(() => {})
    else for (; sent < count; sent++) void page.sendBinary(counted(sent))
    await run(producer ? 1_000 : 300)
    const late = loop.open<never, number>({ registered: false })
    await run(producer ? 1_500 : 1_000)
    late.register()
    await run(120_000)
    expect(got).toEqual(Array.from({ length: count }, (_, n) => n))
  },
)
