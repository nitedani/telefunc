export { PieceSender, PieceReceiver }

import {
  WIRE_PIECES_ACK_BYTES,
  WIRE_PIECE_RATE_MARGIN,
  WIRE_PIECE_RATE_WINDOW_MS,
  WIRE_PIECES_ACK_GAP_MS,
  WIRE_PROBE_BYTES,
  WIRE_PROBE_CLOCK_MS,
  WIRE_PROBE_PAIR_BYTES,
  WIRE_SEND_ACK_GAP_MS,
} from './constants.js'
import { TAG, assertProtocol, encode, isSequencedTag } from './shared-ws.js'
import { Fifo } from '../utils/Fifo.js'
import { unrefTimer } from '../utils/unrefTimer.js'

/** Whether its receiver acknowledges a frame of this size. */
function acknowledges(bytes: number): boolean {
  return bytes > WIRE_PIECES_ACK_BYTES
}

/** One WebSocket's: a frame that may take a ping interval to cross goes in pieces, so its receiver sees it arrive. No
 *  frame or piece it sends carries more than `wholeUpTo` bytes of the frame. With no rate measured, a frame larger than
 *  a probe carries sends its first bytes as the probe; the rest of it, and every frame after it but a PIECES_ACK, waits
 *  for the probe's acknowledgement, which measures the rate. */
class PieceSender {
  /** The link is known to be too slow for big whole frames. */
  private slow = false
  /** The frames and pieces sent and not yet acknowledged that measure the rate: the bytes that crossed before each
   *  arrived, when it was sent, the bytes sent through it, whether it was a probe's, and whether it was cut by no rate
   *  or a lapsed one. */
  private readonly unacknowledged: { crossing: number; at: number; end: number; probe: boolean; blind: boolean }[] = []
  /** The bytes of the frames sent in all, and those the receiver has acknowledged. */
  sentBytes = 0
  acknowledgedBytes = 0
  /** The bytes of the frames handed to the socket. */
  private transmittedBytes = 0
  /** A probe out: the bytes handed to the socket through it, and where its second piece starts, for a pair. */
  private probe: { end: number; second: number | null } | null = null
  /** What waits for the probe's acknowledgement: the rest of its frame, then the frames sent after it. */
  private rest: { frame: Uint8Array<ArrayBuffer>; offset: number } | null = null
  private readonly held = new Fifo<Uint8Array<ArrayBuffer>>()
  /** The bytes waiting for a probe's acknowledgement. */
  heldBytes = 0
  private pingInterval = 0
  /** The largest frame sent: a browser may count a message in full while sending it. */
  private largest = 0
  /** In bytes per ms, from a frame sent to its arrival: never faster than it and what was queued ahead of it crossed.
   *  Past `WIRE_PIECE_RATE_WINDOW_MS` after it last rose, it still cuts, until the next measure replaces it. */
  private fastest = 0
  private roseAt = 0

  constructor(
    private readonly transmit: (message: Uint8Array<ArrayBuffer>) => void,
    /** The bytes its socket holds unsent. */
    private readonly queued: () => number,
  ) {}

  /** Sends `frame`, whole or in pieces. */
  send(frame: Uint8Array<ArrayBuffer>, pingInterval: number): void {
    this.sentBytes += frame.byteLength
    this.pingInterval = pingInterval
    // A PIECES_ACK passes what waits: the probe its peer waits on may be its own.
    if (this.probe === null || frame[0] === TAG.PIECES_ACK) return this.transmitFrom(frame, 0)
    this.held.push(frame)
    this.heldBytes += frame.byteLength
  }

  /** Hands the socket `frame` from `offset`, whole or in pieces. */
  private transmitFrom(frame: Uint8Array<ArrayBuffer>, offset: number): void {
    const bytes = frame.byteLength
    if (bytes > this.largest) this.largest = bytes
    const wholeUpTo = this.wholeUpTo()
    if (offset === 0 && bytes <= wholeUpTo) {
      this.transmittedBytes += bytes
      if (acknowledges(bytes)) this.track(bytes, performance.now(), false)
      return this.transmit(frame)
    }
    const now = performance.now()
    // The first is empty: its receiver hears of the frame at once, before the first piece of it can cross.
    if (offset === 0) this.transmit(encode.piece(bytes, frame.subarray(0, 0)))
    if (this.fastest === 0) return this.sendProbe(frame, now)
    const size = Math.max(1, Math.floor(wholeUpTo))
    this.transmittedBytes += bytes - offset
    if (acknowledges(bytes)) this.track(bytes - offset, now, false)
    for (let at = offset; at < bytes; at += size) this.transmit(encode.piece(bytes, frame.subarray(at, at + size)))
  }

  /** Sends the first pieces of `frame` as a probe, and holds the rest: on a link known slow one small piece, else two
   *  back to back, whose arrivals the receiver dates apart. */
  private sendProbe(frame: Uint8Array<ArrayBuffer>, now: number): void {
    const size = this.slow ? WIRE_PROBE_BYTES : WIRE_PROBE_PAIR_BYTES
    const count = this.slow ? 1 : 2
    for (let at = 0; at < count * size; at += size) {
      this.transmittedBytes += size
      this.track(size, now, true)
      this.transmit(encode.piece(frame.byteLength, frame.subarray(at, at + size)))
    }
    // Its receiver dates only data apart.
    const pair = count === 2 && isSequencedTag(frame[0]!)
    this.probe = { end: this.transmittedBytes, second: pair ? this.transmittedBytes - size : null }
    this.rest = { frame, offset: count * size }
    this.heldBytes += frame.byteLength - count * size
  }

  private track(crossingBytes: number, at: number, probe: boolean): void {
    // What its socket holds unsent crosses before it; a browser may count the message it is sending in full, so less the
    // largest frame.
    this.unacknowledged.push({
      crossing: Math.max(0, this.queued() - this.largest) + crossingBytes,
      at,
      end: this.transmittedBytes,
      probe,
      blind: this.lapsed(at),
    })
  }

  /** A frame this size or smaller goes whole, a larger one in pieces this size: what the fastest measured rate carries
   *  in a ping interval over `WIRE_PIECE_RATE_MARGIN`; with none, what a probe carries. */
  private wholeUpTo(): number {
    if (this.fastest === 0) return this.probeCarries()
    return (this.fastest * this.pingInterval) / WIRE_PIECE_RATE_MARGIN
  }

  private probeCarries(): number {
    return this.slow ? WIRE_PROBE_BYTES : 2 * WIRE_PROBE_PAIR_BYTES
  }

  /** Probes with one small piece, until a measured rate says otherwise. */
  markSlow(): void {
    this.slow = true
  }

  /** Whether its receiver hasn't acknowledged a probe, or a frame cut by a lapsed rate. */
  get stalled(): boolean {
    return this.unacknowledged.some((sent) => sent.blind)
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
    const arrived = performance.now() - heldMs
    let covered = 0
    while (covered < this.unacknowledged.length && this.unacknowledged[covered]!.end <= this.acknowledgedBytes)
      covered++
    for (const sent of this.unacknowledged.splice(0, covered)) {
      // Within one tick of a coarsened clock a sample says nothing of the rate, but a probe's, which is small: it
      // crossed within a tick.
      if (sent.probe) this.rise(sent.crossing / Math.max(arrived - sent.at, WIRE_PROBE_CLOCK_MS), arrived)
      else if (arrived > sent.at) this.rise(sent.crossing / (arrived - sent.at), arrived)
    }
    if (probe !== null && this.acknowledgedBytes >= probe.end) this.release()
    return true
  }

  private rise(rate: number, at: number): void {
    if (!(rate > this.fastest) && !this.lapsed(at)) return
    this.fastest = rate
    this.roseAt = at
  }

  private lapsed(at: number): boolean {
    return this.fastest === 0 || at - this.roseAt > WIRE_PIECE_RATE_WINDOW_MS
  }

  /** Sends what waited for the probe, now that it measured a rate. */
  private release(): void {
    const { frame, offset } = this.rest!
    this.probe = this.rest = null
    this.heldBytes -= frame.byteLength - offset
    this.transmitFrom(frame, offset)
    while (this.probe === null && this.held.length > 0) {
      const next = this.held.shift()!
      this.heldBytes -= next.byteLength
      this.transmitFrom(next, 0)
    }
  }
}

/** One WebSocket's: puts a frame sent in pieces back together, whatever their sizes, and acknowledges the bytes it took.
 *  A frame's first two pieces are acknowledged at once: their sender may wait on them to measure the rate. A receiver
 *  whose peer holds data back by what it acknowledges (`peerHoldsBack`: the server's, see `SendBudget`) acknowledges
 *  once more than `WIRE_PIECES_ACK_BYTES` of frames of any size are unacknowledged, at most one PIECES_ACK per
 *  `WIRE_SEND_ACK_GAP_MS`. Any other acknowledges when a frame over `WIRE_PIECES_ACK_BYTES` arrived, at most one
 *  PIECES_ACK per `WIRE_PIECES_ACK_GAP_MS`: all a sender needs that cuts its pieces by the rate it measures. */
class PieceReceiver {
  /** The frame not yet whole: one buffer, grown as its pieces arrive, so what it holds is what arrived. */
  private frame = new Uint8Array(0)
  private received = 0
  private total = 0
  private pieces = 0
  private data = false
  /** The bytes of the frames it took in all, and when it last acknowledged. */
  private took = 0
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
    if (!this.options.peerHoldsBack && !acknowledges(bytes)) return
    this.date(data, at)
    if (this.took - this.acknowledgedTook > WIRE_PIECES_ACK_BYTES) this.acknowledgeSoon()
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
      if (bytes === 0) return null
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
    const whole = this.received === total
    if (++this.pieces <= 2) this.acknowledge()
    else if (
      this.options.peerHoldsBack
        ? this.took - this.acknowledgedTook > WIRE_PIECES_ACK_BYTES
        : whole && acknowledges(total)
    )
      this.acknowledgeSoon()
    if (!whole) return null
    const frame = this.frame
    this.frame = new Uint8Array(0)
    this.received = this.pieces = 0
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
