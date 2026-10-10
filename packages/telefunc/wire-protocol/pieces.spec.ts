import { afterEach, expect, test, vi } from 'vitest'
import { PieceReceiver, PieceSender } from './pieces.js'
import { decode, TAG } from './shared-ws.js'

afterEach(() => void vi.restoreAllMocks())

const data = (bytes: number) => Object.assign(new Uint8Array(bytes), { 0: TAG.BINARY })

/** What each message `transmitted` holds: a whole frame's length, or a piece's. */
const lengths = (transmitted: Uint8Array[]) =>
  transmitted.map((message) => {
    const frame = decode(message)
    return frame.tag === TAG.PIECE ? `piece ${frame.piece.byteLength}` : message.byteLength
  })

test("a probe pair's receiver spacing measures the rate the rest of its frame is cut by, a ping interval's eighth", () => {
  const clock = { now: 0 }
  vi.spyOn(performance, 'now').mockImplementation(() => clock.now)
  const transmitted: Uint8Array[] = []
  const sender = new PieceSender(
    (message) => void transmitted.push(message),
    () => 0,
  )
  sender.send(data(100_000), 5_000)
  sender.send(data(10), 5_000)
  expect(lengths(transmitted)).toEqual(['piece 0', 'piece 2048', 'piece 2048'])
  clock.now = 200
  expect(sender.acknowledged(2_048, 0, 0)).toBe(true)
  expect(transmitted).toHaveLength(3)
  // The second arrived 100 ms after the first: 2048 B per 100 ms carries 12 800 B in 5 s / 8.
  expect(sender.acknowledged(4_096, 0, 100_000)).toBe(true)
  expect(lengths(transmitted.slice(3))).toEqual([...Array(7).fill('piece 12800'), 'piece 6304', 10])
})

test('a rate that lapsed still cuts a frame at once, until the next measure replaces it, though lower', () => {
  const clock = { now: 0 }
  vi.spyOn(performance, 'now').mockImplementation(() => clock.now)
  const transmitted: Uint8Array[] = []
  const sender = new PieceSender(
    (message) => void transmitted.push(message),
    () => 0,
  )
  sender.send(data(20_000), 5_000)
  clock.now = 200
  expect(sender.acknowledged(2_048, 0, 0)).toBe(true)
  // 2048 B per 100 ms: 12 800 B in 5 s / 8.
  expect(sender.acknowledged(4_096, 0, 100_000)).toBe(true)
  clock.now = 2_200
  expect(sender.acknowledged(20_000, 0, 0)).toBe(true)
  clock.now = 70_000
  transmitted.length = 0
  sender.send(data(30_000), 5_000)
  expect(lengths(transmitted)).toEqual(['piece 0', 'piece 12800', 'piece 12800', 'piece 4400'])
  // 30 000 B in 10 s: 3 B per ms, 1875 B in 5 s / 8.
  clock.now = 80_000
  expect(sender.acknowledged(50_000, 0, 0)).toBe(true)
  transmitted.length = 0
  sender.send(data(5_000), 5_000)
  expect(lengths(transmitted)).toEqual(['piece 0', 'piece 1875', 'piece 1875', 'piece 1250'])
})

test('an acknowledgement within one tick of a coarsened clock measures a probe at the tick, and nothing of a larger frame', () => {
  vi.spyOn(performance, 'now').mockReturnValue(1_000)
  const transmitted: Uint8Array[] = []
  const sender = new PieceSender(
    (message) => void transmitted.push(message),
    () => 0,
  )
  sender.send(data(20_000), 5_000)
  expect(sender.acknowledged(2_048, 0, 0)).toBe(true)
  expect(sender.acknowledged(4_096, 0, 0)).toBe(true)
  // 2048 B in a tick: what it held goes, and so does a frame that rate carries in 5 s / 8.
  expect(lengths(transmitted.slice(3))).toEqual(['piece 15904'])
  sender.send(data(1_000_000), 5_000)
  expect(lengths(transmitted.slice(4))).toEqual([1_000_000])
  expect(sender.acknowledged(1_020_000, 0, 0)).toBe(true)
  transmitted.length = 0
  sender.send(data(2_000_000), 5_000)
  expect(transmitted.length).toBeGreaterThan(1)
})

test('an acknowledgement counts the bytes handed to the socket, and refuses more, those a probe holds included, or nothing new', () => {
  const sender = new PieceSender(
    () => {},
    () => 0,
  )
  sender.send(new Uint8Array(4_000), 5_000)
  sender.send(new Uint8Array(100), 5_000)
  expect(sender.acknowledged(4_100, 0, 0)).toBe(true)
  expect(sender.acknowledged(4_100, 0, 0)).toBe(false)
  sender.send(new Uint8Array(100), 5_000)
  expect(sender.acknowledged(4_300, 0, 0)).toBe(false)
  expect(sender.acknowledged(4_200, 0, 0)).toBe(true)
  // A probe: two pieces of 2048 B go, the rest of its frame waits for them.
  sender.send(new Uint8Array(20_000), 5_000)
  expect(sender.acknowledged(4_200 + 4_097, 0, 0)).toBe(false)
  expect(sender.acknowledged(4_200 + 4_096, 0, 0)).toBe(true)
  expect(sender.acknowledged(24_200, 0, 0)).toBe(true)
})

test('an acknowledgement is read modulo 2^32', () => {
  const sender = new PieceSender(
    () => {},
    () => 0,
  )
  sender.send(new Uint8Array(5_000), 5_000)
  expect(sender.acknowledged(4_096, 0, 0)).toBe(true)
  expect(sender.acknowledged(5_000, 0, 0)).toBe(true)
  const frame = new Uint8Array(40_000_000)
  for (let sent = 5_000; sent < 2 ** 32 + 40_000_000; sent += frame.byteLength) {
    sender.send(frame, 5_000)
    expect(sender.acknowledged((sent + frame.byteLength) % 2 ** 32, 0, 0)).toBe(true)
  }
})

test('a receiver whose peer holds data back acknowledges frames of any size once more than 16 KiB of them are unacknowledged, with the span between arrivals', () => {
  vi.useFakeTimers()
  vi.spyOn(performance, 'now').mockImplementation(() => Date.now())
  const sent: Uint8Array[] = []
  const receiver = new PieceReceiver((frame) => void sent.push(frame), { peerHoldsBack: true })
  receiver.arrived(8_000, true, Date.now())
  receiver.arrived(8_000, true, Date.now())
  expect(sent).toHaveLength(0)
  vi.advanceTimersByTime(20)
  receiver.arrived(1_000, true, Date.now())
  expect(sent.map((frame) => decode(frame))).toEqual([{ tag: TAG.PIECES_ACK, bytes: 17_000, heldMs: 0, spanUs: 0 }])
  // 100 ms on, 10 KB arrive, and 30 ms later 10 KB more: the second ack covers both.
  vi.advanceTimersByTime(100)
  receiver.arrived(10_000, true, Date.now())
  vi.advanceTimersByTime(30)
  receiver.arrived(10_000, true, Date.now())
  expect(sent.map((frame) => decode(frame))[1]).toEqual({
    tag: TAG.PIECES_ACK,
    bytes: 37_000,
    heldMs: 0,
    spanUs: 130_000,
  })
  vi.useRealTimers()
})

test('a receiver whose peer holds nothing back acknowledges a frame over 16 KiB, at most every 50 ms, and no small frames', () => {
  vi.useFakeTimers()
  vi.spyOn(performance, 'now').mockImplementation(() => Date.now())
  const sent: Uint8Array[] = []
  const receiver = new PieceReceiver((frame) => void sent.push(frame), { peerHoldsBack: false })
  for (let i = 0; i < 1_000; i++) receiver.arrived(64, true, Date.now())
  vi.advanceTimersByTime(1_000)
  expect(sent).toHaveLength(0)
  receiver.arrived(20_000, true, Date.now())
  expect(sent.map((frame) => decode(frame))).toEqual([{ tag: TAG.PIECES_ACK, bytes: 84_000, heldMs: 0, spanUs: 0 }])
  vi.advanceTimersByTime(10)
  receiver.arrived(20_000, true, Date.now())
  expect(sent).toHaveLength(1)
  vi.advanceTimersByTime(40)
  expect(sent.map((frame) => decode(frame))[1]).toMatchObject({ bytes: 104_000, heldMs: 40 })
  vi.useRealTimers()
})

test('a receiver puts back a frame from pieces of any size, acknowledging its first two at once, and holds nothing for empty ones', () => {
  vi.useFakeTimers()
  vi.spyOn(performance, 'now').mockImplementation(() => Date.now())
  const sent: Uint8Array[] = []
  const receiver = new PieceReceiver((frame) => void sent.push(frame), { peerHoldsBack: false })
  const frame = data(10_000).map((byte, i) => (i === 0 ? byte : i % 251))
  expect(receiver.add(10_000, frame.subarray(0, 0), Date.now())).toBeNull()
  expect(receiver.add(10_000, frame.subarray(0, 0), Date.now())).toBeNull()
  expect(receiver.held).toBe(0)
  expect(receiver.add(10_000, frame.subarray(0, 1), Date.now())).toBeNull()
  vi.advanceTimersByTime(7)
  expect(receiver.add(10_000, frame.subarray(1, 300), Date.now())).toBeNull()
  expect(sent.map((ack) => decode(ack))).toEqual([
    { tag: TAG.PIECES_ACK, bytes: 1, heldMs: 0, spanUs: 0 },
    { tag: TAG.PIECES_ACK, bytes: 300, heldMs: 0, spanUs: 7_000 },
  ])
  expect(receiver.add(10_000, frame.subarray(300, 9_999), Date.now())).toBeNull()
  expect(receiver.held).toBe(9_999)
  expect(receiver.add(10_000, frame.subarray(9_999), Date.now())).toEqual(frame)
  expect(receiver.held).toBe(0)
  // A frame of 10 000 B is too small for its last pieces to be acknowledged.
  expect(sent).toHaveLength(2)
  vi.useRealTimers()
})

test.each([
  ['empty after the first', [100, 0]],
  ['longer than what is left of its frame', [100, 9_901]],
])('a receiver refuses a piece %s', (_, sizes) => {
  const receiver = new PieceReceiver(() => {}, { peerHoldsBack: false })
  const frame = data(20_000)
  expect(() => {
    let at = 0
    for (const size of sizes) receiver.add(10_000, frame.subarray(at, (at += size)), 0)
  }).toThrow('PIECE not cut as its frame is')
})
