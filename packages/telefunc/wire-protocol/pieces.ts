export { PieceSender, PieceReceiver }
export type { SendRoom }

import {
  WIRE_ANNOUNCED_MIN_BYTES,
  WIRE_ANNOUNCED_WAIT_MARGIN,
  WIRE_PIECES_ACK_BYTES,
  WIRE_PIECE_RATE_MARGIN,
  WIRE_PIECES_ACK_GAP_MS,
  WIRE_PROBE_CLOCK_MS,
  WIRE_PROBE_FIRST_BYTES,
  WIRE_PROBE_PAIR_BYTES,
  WIRE_ROOM_PIECE_MIN_BYTES,
  WIRE_SEND_ACK_GAP_MS,
} from './constants.js'
import { Link } from './link.js'
import { TAG, assertProtocol, encode, isSequencedTag } from './shared-ws.js'
import { Fifo } from '../utils/Fifo.js'
import { unrefTimer } from '../utils/unrefTimer.js'

/** Whether its receiver acknowledges a frame of this size. */
function acknowledges(bytes: number): boolean {
  return bytes > WIRE_PIECES_ACK_BYTES
}

/** The most the announced message after one of `last` bytes carries; after none, what crosses before any is dated, unless
 *  its ANNOUNCE named a `floor` that dates it. */
function announcedNext(last: number, floor: number): number {
  return last === 0 && floor === 0 ? WIRE_PROBE_FIRST_BYTES : Math.max(2 * last, WIRE_ANNOUNCED_MIN_BYTES)
}

/** How much data a sender hands its socket: a page's `SendBudget`. */
type SendRoom = {
  /** With a rate measured, the most bytes of data it hands the socket now. */
  dataRoom(): number
  /** It handed the socket `bytes` of data. */
  handed(bytes: number): void
}

const UNBOUNDED: SendRoom = { dataRoom: () => Number.POSITIVE_INFINITY, handed: () => {} }

/** One WebSocket's: a frame that may take a ping interval to cross goes in pieces, so its receiver sees it arrive. With a
 *  rate measured, no frame or piece it sends carries more than `wholeUpTo` bytes of the frame. With none, a larger frame
 *  starts with a probe pair, whose acknowledgements measure the rate, and goes at once with the frames sent in its task,
 *  and those sent while the probe is out in theirs, behind an ANNOUNCE of their bytes, each whole or in pieces as
 *  `announcedNext` allows. So does a smaller data frame, unless it crosses within its ping deadline: one of
 *  `WIRE_PROBE_FIRST_BYTES` on any link, a larger one at the floor, the rate the link carries at least by what its
 *  receiver took by an answer, a heartbeat or an acknowledged announcement. A smaller control frame, such as the
 *  handshake's, goes whole. With a rate measured, a data frame past its room (see `SendRoom`) goes in pieces as far as
 *  that goes, and its rest waits for `sendHeld`: the frames after it wait too, but those that
 *  aren't data and go whole, as a window update does. */
class PieceSender {
  /** The frames and pieces sent and not yet acknowledged that measure the rate: the bytes that crossed before each
   *  arrived, when it was sent, the bytes sent through it, and whether it was a probe's. */
  private readonly unacknowledged = new Fifo<{ crossing: number; at: number; end: number; probe: boolean }>()
  /** The bytes of the frames sent in all, and those the receiver has acknowledged; the first's, and when it was sent. */
  sentBytes = 0
  acknowledgedBytes = 0
  private firstBytes = 0
  private firstAt = 0
  /** The bytes of the frames handed to the socket. */
  transmittedBytes = 0
  /** A probe out: the bytes handed to the socket through it, and where its second piece starts, for a pair. */
  private probe: { end: number; second: number | null } | null = null
  /** The rest of a data frame, waiting for room. */
  private rest: { frame: Uint8Array<ArrayBuffer>; offset: number } | null = null
  /** The frames sent after the rest, but those that pass it; else the frames of this task, announced at its end. */
  private readonly held = new Fifo<Uint8Array<ArrayBuffer>>()
  /** The bytes waiting for room, or to be announced. */
  heldBytes = 0
  private announcing = false
  /** The bytes of the newest message it announced; each it announced and its receiver hasn't acknowledged: where it ends,
   *  of all it sent, and when it went; and when the newest acknowledgement came. */
  private lastAnnounced = 0
  private readonly announcedEnds = new Fifo<{ end: number; at: number }>()
  private acknowledgedAt = 0
  /** Each announcement its receiver hasn't acknowledged: where its bytes start and end, of all it sent, and when. */
  private readonly announcements = new Fifo<{ start: number; end: number; at: number }>()
  /** In bytes per ms, the floor its newest ANNOUNCE named (see `Link.floor`). */
  private namedFloor = 0
  /** With no rate measured, a frame this size or smaller goes unannounced. */
  private unannouncedUpTo = WIRE_PROBE_FIRST_BYTES
  private pingInterval = 0
  /** A frame this size or smaller goes whole, a larger one in pieces this size: what the fastest measured rate carries
   *  in a ping interval over `WIRE_PIECE_RATE_MARGIN`; with none, what a probe carries. */
  private wholeUpTo = 2 * WIRE_PROBE_PAIR_BYTES
  /** The largest frame sent: a browser may count a message in full while sending it. */
  private largest = 0

  constructor(
    private readonly transmit: (message: Uint8Array<ArrayBuffer>) => void,
    /** The bytes its socket holds unsent. */
    private readonly queued: () => number,
    /** Its wire's: the rates it measures go there. */
    private readonly link = new Link(),
    private readonly room = UNBOUNDED,
  ) {}

  /** Sends `frame`, whole or in pieces. */
  send(frame: Uint8Array<ArrayBuffer>, pingInterval: number): void {
    if (this.sentBytes === 0) {
      this.firstBytes = frame.byteLength
      this.firstAt = performance.now()
    }
    this.sentBytes += frame.byteLength
    if (pingInterval !== this.pingInterval) {
      this.pingInterval = pingInterval
      this.setWholeUpTo()
    }
    // A PIECES_ACK passes what waits: the probe its peer waits on may be its own.
    if (frame[0] === TAG.PIECES_ACK) return this.pass(frame)
    if (this.rest === null && !this.announcing) {
      if (!this.announces(frame)) return this.pass(frame)
      this.announcing = true
      queueMicrotask(this.announce)
    } else if (this.passesRest(frame)) return this.pass(frame)
    this.held.push(frame)
    this.heldBytes += frame.byteLength
  }

  /** Whether `frame` sent now goes behind an announcement. */
  private announces(frame: Uint8Array<ArrayBuffer>): boolean {
    const bytes = frame.byteLength
    return (
      this.probe !== null ||
      (this.link.fastest === 0 && bytes > this.unannouncedUpTo && (bytes > this.wholeUpTo || isSequencedTag(frame[0]!)))
    )
  }

  /** Whether `frame`, sent while the rest of a data frame waits for room, goes ahead of it: a frame that isn't data and
   *  goes whole, as a flow-control frame does. */
  private passesRest(frame: Uint8Array<ArrayBuffer>): boolean {
    return this.rest !== null && this.probe === null && !isSequencedTag(frame[0]!) && frame.byteLength <= this.wholeUpTo
  }

  /** Hands the socket `frame`, whole or in pieces. */
  private pass(frame: Uint8Array<ArrayBuffer>): void {
    const bytes = frame.byteLength
    if (bytes > this.largest) this.largest = bytes
    if (bytes > this.wholeUpTo || (this.link.fastest > 0 && isSequencedTag(frame[0]!) && bytes > this.room.dataRoom()))
      return this.cut(frame, 0)
    this.transmitted(frame, bytes)
    if (acknowledges(bytes)) this.track(bytes, performance.now(), false)
    this.transmit(frame)
  }

  /** Hands the socket `frame` from `offset` in pieces, of data as far as its room goes once it takes
   *  `WIRE_ROOM_PIECE_MIN_BYTES` or the rest, and holds the rest. */
  private cut(frame: Uint8Array<ArrayBuffer>, offset: number): void {
    const bytes = frame.byteLength
    const now = performance.now()
    const room = Math.max(0, Math.floor(this.room.dataRoom()))
    const end = !isSequencedTag(frame[0]!)
      ? bytes
      : room < Math.min(WIRE_ROOM_PIECE_MIN_BYTES, bytes - offset)
        ? offset
        : Math.min(bytes, offset + room)
    if (end === offset) return this.hold(frame, offset)
    // The first of a frame over `wholeUpTo` is empty: its receiver hears of the frame at once, and acknowledges its first
    // two pieces at once. One cut only for room is no measure.
    if (offset === 0 && bytes > this.wholeUpTo) this.transmit(encode.piece(bytes, frame.subarray(0, 0)))
    const size = Math.max(1, Math.floor(this.wholeUpTo))
    this.transmitted(frame, end - offset)
    if (acknowledges(bytes)) this.track(end - offset, now, false)
    for (let at = offset; at < end; at += size)
      this.transmit(encode.piece(bytes, frame.subarray(at, Math.min(end, at + size))))
    if (end < bytes) this.hold(frame, end)
  }

  /** Counts `bytes` of `frame` handed to the socket. */
  private transmitted(frame: Uint8Array<ArrayBuffer>, bytes: number): void {
    this.transmittedBytes += bytes
    if (isSequencedTag(frame[0]!)) this.room.handed(bytes)
  }

  /** Holds `frame` from `offset`, for room. */
  private hold(frame: Uint8Array<ArrayBuffer>, offset: number): void {
    this.rest = { frame, offset }
    this.heldBytes += frame.byteLength - offset
  }

  /** Announces the frames of this task, and sends them. */
  private readonly announce = (): void => {
    this.announcing = false
    const now = performance.now()
    const start = this.transmittedBytes
    const floorPerS = Math.min(0xffffffff, Math.floor(this.link.floor * 1000))
    this.namedFloor = floorPerS / 1000
    this.transmit(encode.announce(this.heldBytes, floorPerS))
    while (this.held.length > 0) {
      const frame = this.held.shift()!
      const bytes = frame.byteLength
      this.heldBytes -= bytes
      if (bytes > this.largest) this.largest = bytes
      if (this.link.fastest === 0 && this.probe === null && bytes > this.wholeUpTo) this.sendProbe(frame, now)
      else this.sendAnnounced(frame, 0, now)
    }
    this.announcements.push({ start, end: this.transmittedBytes, at: now })
  }

  /** Sends `frame` as two pieces back to back, whose arrivals its receiver dates apart and acknowledges at once, then the
   *  rest of it. */
  private sendProbe(frame: Uint8Array<ArrayBuffer>, now: number): void {
    const bytes = frame.byteLength
    this.transmit(encode.piece(bytes, frame.subarray(0, 0)))
    for (const [at, size] of [
      [0, WIRE_PROBE_FIRST_BYTES],
      [WIRE_PROBE_FIRST_BYTES, WIRE_PROBE_PAIR_BYTES],
    ] as const) {
      this.transmitted(frame, size)
      this.track(size, now, true)
      this.sendAnnouncedMessage(encode.piece(bytes, frame.subarray(at, at + size)), size, this.transmittedBytes, now)
    }
    // Its receiver dates only data apart.
    const second = isSequencedTag(frame[0]!) ? this.transmittedBytes - WIRE_PROBE_PAIR_BYTES : null
    this.probe = { end: this.transmittedBytes, second }
    this.sendAnnounced(frame, WIRE_PROBE_FIRST_BYTES + WIRE_PROBE_PAIR_BYTES, now)
  }

  /** Sends `frame` from `offset` as announced messages: whole if `announcedNext` allows, else in pieces each as large as
   *  it allows after the one before, but the last two, cut so the last is the larger, which the next may carry twice. */
  private sendAnnounced(frame: Uint8Array<ArrayBuffer>, offset: number, now: number): void {
    const bytes = frame.byteLength
    const start = this.transmittedBytes - offset
    this.transmitted(frame, bytes - offset)
    if (acknowledges(bytes)) this.track(bytes - offset, now, false)
    if (offset === 0 && bytes <= announcedNext(this.lastAnnounced, this.namedFloor))
      return this.sendAnnouncedMessage(frame, bytes, this.transmittedBytes, now)
    const sizes: number[] = []
    for (let left = bytes - offset, last = this.lastAnnounced; left > 0; left -= last) {
      last = Math.min(left, announcedNext(last, this.namedFloor))
      sizes.push(last)
    }
    const n = sizes.length
    if (n >= 2 && sizes[n - 1]! < sizes[n - 2]!) {
      const both = sizes[n - 2]! + sizes[n - 1]!
      sizes[n - 2] = Math.ceil(both / 3)
      sizes[n - 1] = both - sizes[n - 2]!
    }
    let at = offset
    for (const size of sizes) {
      this.sendAnnouncedMessage(encode.piece(bytes, frame.subarray(at, at + size)), size, start + at + size, now)
      at += size
    }
  }

  /** Hands the socket `message` at `at`, which carries `bytes` of the frames it announced, up to `end` of all it sent. */
  private sendAnnouncedMessage(message: Uint8Array<ArrayBuffer>, bytes: number, end: number, at: number): void {
    this.lastAnnounced = bytes
    this.announcedEnds.push({ end, at })
    this.transmit(message)
  }

  private track(crossingBytes: number, at: number, probe: boolean): void {
    // What its socket holds unsent crosses before it; a browser may count the message it is sending in full, so less the
    // largest frame.
    this.unacknowledged.push({
      crossing: Math.max(0, this.queued() - this.largest) + crossingBytes,
      at,
      end: this.transmittedBytes,
      probe,
    })
  }

  /** Sets `wholeUpTo` by the rate and the ping interval. */
  private setWholeUpTo(): void {
    this.wholeUpTo =
      this.link.fastest > 0
        ? (this.link.fastest * this.pingInterval) / WIRE_PIECE_RATE_MARGIN
        : 2 * WIRE_PROBE_PAIR_BYTES
    // What crosses within its ping deadline, twice the interval: on any link a PING crosses, or at the floor.
    this.unannouncedUpTo = Math.min(
      this.wholeUpTo,
      Math.max(WIRE_PROBE_FIRST_BYTES, this.link.floor * 2 * this.pingInterval),
    )
  }

  /** Until when its receiver, which acknowledges each announced message, may take to acknowledge the next, at the rate it
   *  measured, or with none, its floor: twice the time that message takes from when it went, or the newest
   *  acknowledgement came if later; 0 for none. */
  get acknowledgedBy(): number {
    const next = this.announcedEnds.peek()
    const rate = this.link.fastest || this.link.floor
    if (next === undefined || rate === 0) return 0
    const from = Math.max(next.at, this.acknowledgedAt)
    return from + (WIRE_ANNOUNCED_WAIT_MARGIN * (next.end - this.acknowledgedBytes)) / rate
  }

  /** Takes a PIECES_ACK: its receiver took `bytes` of the frames in all (mod 2^32), the newest `heldMs` before it sent
   *  the ack, `spanUs` after the newest data the ack before covered. Returns false if that acknowledges nothing sent, or
   *  more than was sent. */
  acknowledged(bytes: number, heldMs: number, spanUs: number): boolean {
    const newly = (bytes - this.acknowledgedBytes) >>> 0
    if (newly < 1 || newly > this.transmittedBytes - this.acknowledgedBytes) return false
    const probe = this.probe
    // The ack of a pair's second piece alone dates it apart from the first: the rate the path took them at.
    if (probe !== null && this.acknowledgedBytes === probe.second && newly === probe.end - probe.second) {
      this.rise(WIRE_PROBE_PAIR_BYTES / Math.max(spanUs / 1000, WIRE_PROBE_CLOCK_MS), performance.now())
    }
    this.acknowledgedBytes += newly
    while (this.announcedEnds.length > 0 && this.announcedEnds.peek()!.end <= this.acknowledgedBytes)
      this.announcedEnds.shift()
    this.acknowledgedAt = performance.now()
    const arrived = this.acknowledgedAt - heldMs
    const oldest = this.announcements.peek()
    if (oldest !== undefined && oldest.end <= this.acknowledgedBytes) {
      let end = oldest.end
      while (this.announcements.length > 0 && this.announcements.peek()!.end <= this.acknowledgedBytes)
        end = this.announcements.shift()!.end
      // What it acknowledged went no earlier than the oldest and arrived by the newest: at most the link's rate.
      this.raiseFloor((end - oldest.start) / Math.max(arrived - oldest.at, WIRE_PROBE_CLOCK_MS))
    }
    while (this.unacknowledged.length > 0 && this.unacknowledged.peek()!.end <= this.acknowledgedBytes) {
      const sent = this.unacknowledged.shift()!
      // Within one tick of a coarsened clock a sample says nothing of the rate, but a probe's, which is small: it
      // crossed within a tick.
      if (sent.probe) this.rise(sent.crossing / Math.max(arrived - sent.at, WIRE_PROBE_CLOCK_MS), arrived)
      else if (arrived > sent.at) this.rise(sent.crossing / (arrived - sent.at), arrived)
    }
    if (probe !== null && this.acknowledgedBytes >= probe.end) {
      this.probe = null
      if (this.rest !== null) this.release()
    }
    return true
  }

  /** Takes a PING or PONG that arrived at `at`, saying its receiver took `bytes` of the frames in all (mod 2^32): as many
   *  crossed since the first was sent. Returns false if that is more than were sent. */
  heartbeat(bytes: number, at: number): boolean {
    // Behind the acknowledged bytes if a PIECES_ACK passed it while it waited behind a probe.
    const took = this.acknowledgedBytes + ((bytes - this.acknowledgedBytes) | 0)
    if (took > this.transmittedBytes) return false
    this.raiseFloor(took / Math.max(at - this.firstAt, WIRE_PROBE_CLOCK_MS))
    return true
  }

  /** Takes a frame that arrived at `at` from a peer that sends none before the first frame it was sent arrived: that frame
   *  crossed by then. */
  answered(at: number): void {
    this.raiseFloor(this.firstBytes / Math.max(at - this.firstAt, WIRE_PROBE_CLOCK_MS))
  }

  private raiseFloor(floor: number): void {
    if (this.link.raiseFloor(floor)) this.setWholeUpTo()
  }

  private rise(rate: number, at: number): void {
    if (this.link.rise(rate, at)) this.setWholeUpTo()
  }

  /** Sends what waits for room, as far as its room goes now. */
  sendHeld(): void {
    if (this.rest !== null && this.probe === null) this.release()
  }

  /** Sends what waited for room, as far as it goes now, and the frames held behind it. */
  private release(): void {
    const { frame, offset } = this.rest!
    this.rest = null
    this.heldBytes -= frame.byteLength - offset
    this.cut(frame, offset)
    while (this.rest === null && this.held.length > 0) {
      const next = this.held.shift()!
      this.heldBytes -= next.byteLength
      this.pass(next)
    }
  }
}

/** One WebSocket's: puts a frame sent in pieces back together, whatever their sizes, and acknowledges the bytes it took.
 *  The two pieces after an empty one are acknowledged at once: their sender may measure the rate by them. Bytes announced
 *  are waited for at the rate their newest message came at, dated apart from the message before it. A receiver whose peer
 *  holds data back by what it acknowledges (`peerHoldsBack`: the server's, see `SendBudget`) acknowledges each announced
 *  message, as its peer's PINGs wait behind them, and once more than `WIRE_PIECES_ACK_BYTES` of frames of any size are
 *  unacknowledged, at most one PIECES_ACK per `WIRE_SEND_ACK_GAP_MS`. Any other acknowledges when a frame over
 *  `WIRE_PIECES_ACK_BYTES` or the last of an announcement arrived, at most one PIECES_ACK per `WIRE_PIECES_ACK_GAP_MS`:
 *  all a sender needs that cuts its pieces by the rate it measures, and stops announcing by the rate it carries. */
class PieceReceiver {
  /** The frame not yet whole: one buffer, grown as its pieces arrive, so what it holds is what arrived. */
  private frame = new Uint8Array(0)
  private received = 0
  private total = 0
  private data = false
  /** The pieces still to acknowledge at once. */
  private probing = 0
  /** The bytes announced still to come; the newest message of them: when it arrived, its bytes, and in bytes per ms, the
   *  rate it came at; and the floor the newest ANNOUNCE named. */
  private announced = 0
  private announcedAt = 0
  private lastAnnounced = 0
  private rate = 0
  private floor = 0
  /** The bytes of the frames it took in all, and when it last acknowledged. */
  took = 0
  private acknowledgedTook = 0
  private newestAt = 0
  /** When the newest data frame arrived, and when the newest the last acknowledgement covered did. */
  private newestDataAt: number | null = null
  private acknowledgedDataAt: number | null = null
  private ackedAt = Number.NEGATIVE_INFINITY
  private ackTimer: ReturnType<typeof setTimeout> | null = null

  constructor(
    private readonly send: (frame: Uint8Array<ArrayBuffer>) => void,
    private readonly options: { peerHoldsBack: boolean },
  ) {}

  /** After a whole frame of `bytes` arrived at `at` in one message. A data frame dates the ack's span: it takes the
   *  queues an upload fills, where the frames that go at once pass them. */
  arrived(bytes: number, data: boolean, at: number): void {
    this.took += bytes
    const announced = this.paced(bytes, at)
    if (!this.options.peerHoldsBack && !acknowledges(bytes) && !(announced && this.announced === 0)) return
    this.date(data, at)
    if (!this.options.peerHoldsBack || announced || this.took - this.acknowledgedTook > WIRE_PIECES_ACK_BYTES)
      this.acknowledgeSoon()
  }

  /** Takes an ANNOUNCE that arrived at `at`: `bytes` of frames follow it at once, over a link that carries `floor` bytes
   *  per ms at least, 0 for none named. */
  announce(bytes: number, at: number, floor = 0): void {
    // Its sender sends all it announced before it announces more.
    assertProtocol(this.announced === 0, 'ANNOUNCE before what the one before announced arrived')
    this.announced = bytes
    this.announcedAt = at
    this.floor = floor
  }

  /** Until when it waits for the next message of what was announced: twice the time `announcedNext` of the newest takes at
   *  the rate it came at, or before any, at the floor, but no slower than a link a PING crosses carries,
   *  `WIRE_PROBE_FIRST_BYTES` per `pingDeadline`; 0 for none. */
  expectedBy(pingDeadline: number): number {
    const rate = this.rate || this.floor
    if (this.announced === 0 || rate === 0) return 0
    const next = Math.min(this.announced, announcedNext(this.lastAnnounced, this.floor))
    return (
      this.announcedAt + (WIRE_ANNOUNCED_WAIT_MARGIN * next) / Math.max(rate, WIRE_PROBE_FIRST_BYTES / pingDeadline)
    )
  }

  /** Dates a message of `bytes` that arrived at `at`, if it was announced, and returns whether it was. */
  private paced(bytes: number, at: number): boolean {
    if (this.announced === 0) return false
    this.rate = bytes / Math.max(at - this.announcedAt, WIRE_PROBE_CLOCK_MS)
    this.announcedAt = at
    this.lastAnnounced = bytes
    this.announced = Math.max(0, this.announced - bytes)
    return true
  }

  /** Bytes it holds of a frame not yet whole. */
  get held(): number {
    return this.received
  }

  /** Takes a piece that arrived at `at`, and returns the frame once its last piece arrived. */
  add(total: number, piece: Uint8Array, at: number): Uint8Array<ArrayBuffer> | null {
    const bytes = piece.byteLength
    if (this.received === 0) {
      this.total = total
      if (bytes === 0) {
        this.probing = 2
        return null
      }
      this.data = isSequencedTag(piece[0]!)
    }
    assertProtocol(total === this.total && bytes > 0 && bytes <= total - this.received, 'PIECE not cut as its frame is')
    if (this.received + bytes > this.frame.byteLength) {
      const grown = new Uint8Array(Math.min(total, Math.max(2 * this.frame.byteLength, this.received + bytes)))
      grown.set(this.frame.subarray(0, this.received))
      this.frame = grown
    }
    this.frame.set(piece, this.received)
    this.received += bytes
    this.took += bytes
    this.date(this.data, at)
    const announced = this.paced(bytes, at)
    const whole = this.received === total
    if (this.probing > 0) {
      this.probing--
      this.acknowledge()
    } else if (
      this.options.peerHoldsBack
        ? announced || this.took - this.acknowledgedTook > WIRE_PIECES_ACK_BYTES
        : (whole && acknowledges(total)) || (announced && this.announced === 0)
    )
      this.acknowledgeSoon()
    if (!whole) return null
    const frame = this.frame
    this.frame = new Uint8Array(0)
    this.received = this.probing = 0
    return frame
  }

  private date(data: boolean, at: number): void {
    this.newestAt = at
    if (data) this.newestDataAt = at
  }

  private acknowledgeSoon(): void {
    const wait =
      this.ackedAt + (this.options.peerHoldsBack ? WIRE_SEND_ACK_GAP_MS : WIRE_PIECES_ACK_GAP_MS) - this.newestAt
    if (wait <= 0) this.acknowledge()
    else this.ackTimer ??= unrefTimer(setTimeout(() => this.acknowledge(), wait))
  }

  private acknowledge(): void {
    if (this.ackTimer !== null) clearTimeout(this.ackTimer)
    this.ackTimer = null
    this.ackedAt = performance.now()
    this.acknowledgedTook = this.took
    const span =
      this.acknowledgedDataAt === null || this.newestDataAt === this.acknowledgedDataAt
        ? 0
        : Math.round((this.newestDataAt! - this.acknowledgedDataAt) * 1000)
    this.acknowledgedDataAt = this.newestDataAt
    // Rounded down: its sender then dates the arrival no earlier than it was, so the rate it takes is never higher
    this.send(encode.piecesAck(this.took, Math.floor(this.ackedAt - this.newestAt), span))
  }
}
