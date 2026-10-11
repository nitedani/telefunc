export { SendBudget }

import {
  CREDIT_WINDOW_MAX_BYTES,
  WIRE_PIECES_ACK_BYTES,
  WIRE_QUEUE_DELAY_MS,
  WIRE_SEND_AHEAD_MIN_BYTES,
} from './constants.js'
import type { PieceSender } from './pieces.js'
import { isSequencedTag } from './shared-ws.js'
import { Fifo } from '../utils/Fifo.js'
import { unrefTimer } from '../utils/unrefTimer.js'

/** How much of the excess queue a round of acknowledgements sheds. */
const SHED_SHARE = 0.5
/** The least span of arrivals a rate the server took data at is measured over: a frame arrives whole, so a shorter span
 *  can count one that took longer. */
const DRAIN_SPAN_MS = 100
/** The uplink drains at the fastest of this many of the newest such rates. */
const DRAIN_RATES = 8
/** How long a wait stays in the history the download's window reads (see `SendWait`). */
const WAITS_KEPT_MS = 60_000

/** How many bytes of data a page lets its WebSocket's queues hold: the browser's, the network stack's and the path's,
 *  where it can't see them or reorder them, and where a flow-control frame waits behind the data sent before it. A
 *  download whose refreshes wait behind an upload crawls. The page hands data to the socket while the allowance has
 *  room, and holds the rest back: a frame past the room goes in pieces as the room opens (see `PieceSender.dataRoom`),
 *  and the frames that don't wait pass it.
 *
 *  The room is the allowance less what the uplink holds, or less what the server hasn't acknowledged where that is more,
 *  but for what late acknowledgements hide and what the server takes before it acknowledges. What the uplink holds is the
 *  data handed to the socket, drained at the rate the server takes it. While the room is used up and the uplink holds
 *  data, a timer looks again as it drains. The frames that go at once wait behind what it holds, and a page's download
 *  window covers that wait (see `SendWait`): as the uplink's model says when each is handed, and as the server's
 *  acknowledgements measure it for data.
 *
 *  The allowance is what keeps data from waiting long in those queues. A PIECES_ACK says when the newest data it covers
 *  arrived, only as the µs after the data the ack before covered, so the clocks of the two ends need not agree: data's
 *  one-way delay is known up to an offset the same for all. Less what the uplink held ahead of it and its own bytes'
 *  time on the link, it is the path's, and its wait is its delay less the least of those. While the wait is under
 *  `WIRE_QUEUE_DELAY_MS` the allowance grows by the bytes each ack covers, less as the wait nears it; past it, the
 *  allowance falls by the excess at the rate the server took. It is at least what the server takes in
 *  `WIRE_QUEUE_DELAY_MS`, within what keeps it acknowledging. */
class SendBudget {
  private allowance = WIRE_SEND_AHEAD_MIN_BYTES
  /** The data handed to the socket and not covered by an acknowledgement: where each part handed at once ends, of all
   *  the sender handed, its bytes, what the uplink held ahead of it, and when. */
  private readonly sent = new Fifo<{ end: number; bytes: number; ahead: number; at: number }>()
  /** In ms, the server's clock from the first data an acknowledgement covered. */
  private arrivedAt = 0
  /** The least one-way delay of data on the path, up to the offset between the clocks. */
  private leastDelay = Number.POSITIVE_INFINITY
  /** The least time an acknowledgement took back, up to the same offset, and how much longer the newest took: behind
   *  what the server sends, which a download fills, and no queue of the uplink's. Its wait falls no faster than time
   *  passes. */
  private leastBack = Number.POSITIVE_INFINITY
  private lateMs = 0
  private lateAt = 0
  /** In bytes per ms: the rates the server took data at, and the fastest of them, 0 before any. */
  private readonly drainRates: number[] = []
  private drainRate = 0
  /** The bytes and ms of arrivals not yet in a rate. */
  private spanBytes = 0
  private spanMs = 0
  /** What the uplink holds of the data handed to the socket, as of `queuedAt`. */
  private queued = 0
  private queuedAt = 0
  private paceTimer: ReturnType<typeof setTimeout> | null = null
  private paceDue = 0
  /** How long the frames that go at once waited behind the uplink, and data did as the server acknowledged it. */
  private readonly waits = new Waits()
  /** Called when the timer finds the uplink drained some of what it held. */
  onRoom: () => void = () => {}

  constructor(private readonly sender: PieceSender) {
    sender.dataRoom = () => this.roomPast(sender.transmittedBytes)
    sender.onData = (bytes) => this.handed(bytes)
  }

  /** Sends `frame` through the sender. */
  send(frame: Uint8Array<ArrayBuffer>, pingInterval: number): void {
    this.sender.send(frame, pingInterval)
    if (isSequencedTag(frame[0]!)) this.pace()
    else this.waits.note(this.uplinkWait(), performance.now())
  }

  /** `SendWait`: what a frame sent now waits. */
  waitNow(): number {
    return this.uplinkWait()
  }

  /** `SendWait`: the longest a frame that went at once waited since `at`, or one sent now waits. */
  longestWaitSince(at: number): number {
    return Math.max(this.waits.since(at), this.uplinkWait())
  }

  /** The bytes of data it takes now: none, once the room is used or data waits for it. */
  get room(): number {
    return this.roomPast(this.sender.sentBytes)
  }

  /** Sends what the sender holds for room, as far as it goes now. */
  sendHeld(): void {
    this.sender.sendHeld()
  }

  /** A message arrived: where the timer is late to look again, looks now. A hidden tab throttles timers, not messages. */
  received(): void {
    if (this.paceTimer === null || performance.now() < this.paceDue) return
    clearTimeout(this.paceTimer)
    this.lookAgain()
  }

  /** Takes a PIECES_ACK (see `PieceSender.acknowledged`), and sends what it made room for. */
  acknowledged(bytes: number, heldMs: number, spanUs: number): boolean {
    const before = this.sender.acknowledgedBytes
    if (!this.sender.acknowledged(bytes, heldMs, spanUs)) return false
    const acknowledged = this.sender.acknowledgedBytes
    const newly = acknowledged - before
    const spanMs = spanUs / 1000
    this.arrivedAt += spanMs
    const now = performance.now()
    const back = now - this.arrivedAt - heldMs
    if (back < this.leastBack) this.leastBack = back
    this.lateMs = Math.max(back - this.leastBack, this.lateMs - (now - this.lateAt))
    this.lateAt = now
    this.spanBytes += newly
    this.spanMs += spanMs
    if (this.spanMs >= DRAIN_SPAN_MS) this.drained()
    let newest: { end: number; bytes: number; ahead: number; at: number } | undefined
    while (this.sent.length > 0 && this.sent.peek()!.end <= acknowledged) newest = this.sent.shift()
    // What the server acknowledged left the uplink, though the rate it took data at so far says it could still be there.
    this.queued = Math.min(this.inUplink(), this.sender.transmittedBytes - acknowledged)
    if (newest !== undefined) this.measure(newest, newly, spanUs)
    this.sendHeld()
    this.pace()
    return true
  }

  /** The room less what the sender handed past `sent` bytes. */
  private roomPast(sent: number): number {
    const unacknowledged = sent - this.sender.acknowledgedBytes
    const waiting = Math.min(unacknowledged, this.inUplink() + sent - this.sender.transmittedBytes)
    // The server acknowledges what arrived once `WIRE_PIECES_ACK_BYTES` did: up to that much arrived unacknowledged.
    const unarrived = unacknowledged - this.drainRate * this.lateMs - WIRE_PIECES_ACK_BYTES
    return this.allowance - Math.max(waiting, unarrived)
  }

  /** While the room is used up and the uplink holds data, the uplink sets the pace, and data may wait for room: looks
   *  again once the uplink carried what its server acknowledges at once, or all it holds. */
  private pace(): void {
    if (this.drainRate === 0 || this.room > 0 || this.queued === 0 || this.paceTimer !== null) return
    const ms = Math.max(1, Math.min(this.queued, WIRE_PIECES_ACK_BYTES) / this.drainRate)
    this.paceDue = performance.now() + ms
    this.paceTimer = unrefTimer(setTimeout(this.lookAgain, ms))
  }

  private readonly lookAgain = (): void => {
    this.paceTimer = null
    this.onRoom()
    this.pace()
  }

  /** How long a frame handed to the socket now waits behind the data the uplink holds. */
  private uplinkWait(): number {
    return this.drainRate > 0 ? this.inUplink() / this.drainRate : 0
  }

  /** What the uplink holds of the data handed to the socket. */
  private inUplink(): number {
    const now = performance.now()
    this.queued = Math.max(0, this.queued - this.drainRate * (now - this.queuedAt))
    this.queuedAt = now
    return this.queued
  }

  /** Notes `bytes` of data the sender handed the socket. Parts handed within a ms of one another make one sample, dated
   *  by the first. Before the server's rate is measured, its acknowledgements alone bound what the uplink holds. */
  private handed(bytes: number): void {
    const ahead = this.drainRate > 0 ? this.inUplink() : 0
    if (this.drainRate > 0) this.queued = ahead + bytes
    const end = this.sender.transmittedBytes
    const at = performance.now()
    const last = this.sent.peekLast()
    if (last !== undefined && at - last.at < 1) {
      last.bytes += bytes
      last.end = end
    } else this.sent.push({ end, bytes, ahead, at })
  }

  /** Takes the rate of the arrivals not yet in one. */
  private drained(): void {
    this.drainRates.push(this.spanBytes / this.spanMs)
    if (this.drainRates.length > DRAIN_RATES) this.drainRates.shift()
    this.drainRate = Math.max(...this.drainRates)
    this.spanBytes = this.spanMs = 0
  }

  /** Moves the allowance by the delay of `newest`, the newest data an acknowledgement of `newly` bytes covered, `spanUs`
   *  after the data the one before covered. */
  private measure(
    newest: { end: number; bytes: number; ahead: number; at: number },
    newly: number,
    spanUs: number,
  ): void {
    const delay = this.arrivedAt - newest.at
    const path = this.drainRate > 0 ? delay - (newest.ahead + newest.bytes) / this.drainRate : delay
    // A path that stays over the target with the least allowance rose.
    const least = this.least
    const atLeast = this.allowance === least
    if (path < this.leastDelay || (atLeast && path - this.leastDelay >= WIRE_QUEUE_DELAY_MS)) this.leastDelay = path
    const wait = delay - this.leastDelay
    this.waits.note(Math.max(0, wait), performance.now())
    if (wait < WIRE_QUEUE_DELAY_MS) {
      this.allowance += (newly * (WIRE_QUEUE_DELAY_MS - wait)) / WIRE_QUEUE_DELAY_MS
    } else if (spanUs > 0) {
      const rate = newly / (spanUs / 1000)
      this.allowance -= (SHED_SHARE * rate * (wait - WIRE_QUEUE_DELAY_MS) * newly) / this.allowance
    }
    this.allowance = Math.min(CREDIT_WINDOW_MAX_BYTES, Math.max(least, this.allowance))
  }

  /** The least allowance: what the server takes in `WIRE_QUEUE_DELAY_MS`, more than it waits for before it
   *  acknowledges, and `WIRE_SEND_AHEAD_MIN_BYTES` at most or before its rate is measured. */
  private get least(): number {
    if (this.drainRate === 0) return WIRE_SEND_AHEAD_MIN_BYTES
    return Math.min(
      WIRE_SEND_AHEAD_MIN_BYTES,
      Math.max(WIRE_PIECES_ACK_BYTES + 1024, this.drainRate * WIRE_QUEUE_DELAY_MS),
    )
  }
}

/** Waits by when they were seen, for `WAITS_KEPT_MS`. Each drops the earlier ones no longer than it, so the longest since
 *  an instant is the first at or after it, found by halving. */
class Waits {
  private at: number[] = []
  private wait: number[] = []
  private head = 0

  note(wait: number, at: number): void {
    while (this.at.length > this.head && this.wait[this.wait.length - 1]! <= wait) {
      this.at.pop()
      this.wait.pop()
    }
    this.at.push(at)
    this.wait.push(wait)
    while (this.at[this.head]! < at - WAITS_KEPT_MS) this.head++
    if (this.head > 16 && this.head >= this.at.length >>> 1) {
      this.at = this.at.slice(this.head)
      this.wait = this.wait.slice(this.head)
      this.head = 0
    }
  }

  /** The longest wait seen at or after `at`, 0 for none. */
  since(at: number): number {
    let lo = this.head
    let hi = this.at.length
    while (lo < hi) {
      const mid = (lo + hi) >>> 1
      if (this.at[mid]! < at) lo = mid + 1
      else hi = mid
    }
    return lo < this.at.length ? this.wait[lo]! : 0
  }
}
