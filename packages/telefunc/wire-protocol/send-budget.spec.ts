import { afterEach, expect, test, vi } from 'vitest'
import {
  CREDIT_WINDOW_MAX_BYTES,
  WIRE_PIECES_ACK_BYTES,
  WIRE_PROBE_FIRST_BYTES,
  WIRE_PROBE_PAIR_BYTES,
  WIRE_QUEUE_DELAY_MS,
  WIRE_SEND_AHEAD_MIN_BYTES,
} from './constants.js'
import { PieceSender } from './pieces.js'
import { SendBudget } from './send-budget.js'
import { TAG } from './shared-ws.js'

afterEach(() => void vi.restoreAllMocks())

const data = (bytes: number) => Object.assign(new Uint8Array(bytes), { 0: TAG.BINARY })
const flowControl = (bytes: number) => Object.assign(new Uint8Array(bytes), { 0: TAG.WINDOW })

/** The bytes of the probe by which its sender measured its rate, so it hands the socket what it is sent at once. */
const PROBE = 5_000

/** A budget on a clock the test sets. */
async function open() {
  const clock = { now: 0 }
  vi.spyOn(performance, 'now').mockImplementation(() => clock.now)
  const sender = new PieceSender(
    () => {},
    () => 0,
  )
  sender.send(data(PROBE), 5_000)
  // It goes behind an announcement, at the end of the task.
  await Promise.resolve()
  sender.acknowledged(WIRE_PROBE_FIRST_BYTES + WIRE_PROBE_PAIR_BYTES, 0, 0)
  sender.acknowledged(PROBE, 0, 0)
  const budget = new SendBudget(sender)
  let sent = PROBE
  /** Sends a data frame and has the server acknowledge it `spanMs` after the one before it. The allowance, with nothing
   *  unacknowledged. */
  const exchange = (bytes: number, spanMs: number) => {
    budget.send(data(bytes), 5_000)
    sent += bytes
    clock.now += 10
    expect(budget.acknowledged(sent, 0, spanMs * 1_000)).toBe(true)
    return budget.room
  }
  return { budget, clock, exchange }
}

// The server acknowledges what arrived once `WIRE_PIECES_ACK_BYTES` did, so that much may have arrived unacknowledged.
test('data goes out until the allowance and what the server holds before it acknowledges are sent, and more as it acknowledges', async () => {
  const { budget } = await open()
  expect(budget.room).toBe(WIRE_SEND_AHEAD_MIN_BYTES)
  budget.send(data(40_000), 5_000)
  expect(budget.room).toBe(WIRE_SEND_AHEAD_MIN_BYTES + WIRE_PIECES_ACK_BYTES - 40_000)
  budget.send(data(40_000), 5_000)
  budget.send(data(40_000), 5_000)
  expect(budget.room).toBeLessThan(0)
  expect(budget.acknowledged(PROBE + 80_000, 0, 0)).toBe(true)
  expect(budget.room).toBeGreaterThan(0)
})

test('the frames that go at once count against the room, and are not dated', async () => {
  const { budget } = await open()
  budget.send(flowControl(WIRE_PIECES_ACK_BYTES + 1_000), 5_000)
  expect(budget.room).toBe(WIRE_SEND_AHEAD_MIN_BYTES - 1_000)
  expect(budget.acknowledged(PROBE + WIRE_PIECES_ACK_BYTES + 1_000, 0, 0)).toBe(true)
  expect(budget.room).toBe(WIRE_SEND_AHEAD_MIN_BYTES)
})

test('the allowance grows by what each acknowledgement covers while no frame waits, to the largest credit window', async () => {
  const { exchange } = await open()
  expect(exchange(32_000, 0)).toBe(WIRE_SEND_AHEAD_MIN_BYTES + 32_000)
  expect(exchange(32_000, 10)).toBe(WIRE_SEND_AHEAD_MIN_BYTES + 2 * 32_000)
  // A frame past the room would go in pieces as the server acknowledges it: each takes the room, up to 1 MB.
  let allowance = WIRE_SEND_AHEAD_MIN_BYTES + 2 * 32_000
  for (let i = 0; i < 100; i++) allowance = exchange(Math.min(allowance, 1_000_000), 10)
  expect(allowance).toBe(CREDIT_WINDOW_MAX_BYTES)
})

test('the allowance falls while frames wait more than the target, to the least', async () => {
  const { exchange } = await open()
  for (let i = 0; i < 20; i++) exchange(32_000, 10)
  // Each frame reaches the server 5 ms later than the one before it, and 10 ms after it.
  let allowance = exchange(32_000, 10)
  let i = 0
  while (allowance > WIRE_SEND_AHEAD_MIN_BYTES && i < 200) {
    const before = allowance
    allowance = exchange(32_000, 15)
    i++
    if (i * 5 > WIRE_QUEUE_DELAY_MS + 5) expect(allowance).toBeLessThanOrEqual(before)
  }
  expect(allowance).toBe(WIRE_SEND_AHEAD_MIN_BYTES)
})

test('a wait that stays over the target with the least allowance is the path having slowed, not a queue', async () => {
  const { exchange } = await open()
  exchange(32_000, 0)
  // The path now takes 200 ms more, for all that follows.
  exchange(32_000, 10 + 200)
  let allowance = 0
  for (let i = 0; i < 20; i++) allowance = exchange(32_000, 10)
  expect(allowance).toBeGreaterThan(2 * WIRE_SEND_AHEAD_MIN_BYTES)
})
