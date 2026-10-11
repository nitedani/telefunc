export { BdpEstimator }
export type { GrowDecision, AxisDecision }

import {
  BDP_PING_MAX_INTERVAL_MS,
  BDP_PING_MIN_INTERVAL_MS,
  CREDIT_MSG_WINDOW_INITIAL,
  CREDIT_MSG_WINDOW_MAX,
  CREDIT_WINDOW_INITIAL_BYTES,
  CREDIT_WINDOW_MAX_BYTES,
} from '../constants.js'

/** Per-axis verdict on a settled probe. `grow` = the sample saturated ≥ 2/3 of the current window, which is not at
 *  cap, and for the byte window, the sample leaves the sender's queue out: taken at the path's round trip where an
 *  attach measured it, the receiver's or the sender's, and where none did, the window starved the sender's wire.
 *  `sample-too-small` = the sample didn't. `wire-busy` = the sender's wire held an eighth of the window or more whenever
 *  its credit ran out.
 *  `window-grew` = the probe went out after a window grew, so what the sender says covers the smaller one. */
type AxisDecision = 'grow' | 'sample-too-small' | 'wire-busy' | 'window-grew' | 'at-cap'

/** `acknowledged` is true when a ping was in flight to settle (false for
 *  a stale ack arriving after a transport reset, and for an attach's probe). Per-axis decisions are
 *  independent — caller commits each axis's grow separately. */
type GrowDecision = { acknowledged: boolean; bytes: AxisDecision; msgs: AxisDecision }

const NOT_SETTLED: GrowDecision = { acknowledged: false, bytes: 'sample-too-small', msgs: 'sample-too-small' }

/**
 * gRPC-style adaptive flow-control window. Maintained per channel on the receive
 * side; the resulting `byteWindow` is advertised to the peer via the existing
 * `WINDOW` frame so the sender stops blocking once the wire's BDP is reached.
 *
 *   1. On the first byte received after a previous sample completes, issue a
 *      `BDP_PING` and snapshot `bytesReceived`. Only one ping is in flight at a time
 *      so the cadence self-paces to ~1 ping per RTT during active traffic and zero
 *      while idle. `BDP_PING_MIN_INTERVAL_MS` further caps the rate on loopback /
 *      sub-ms RTT where unthrottled gRPC pacing would churn the event loop.
 *   2. The sender echoes `BDP_PING_ACK` immediately, but the ack waits behind what the
 *      sender's socket and the path still hold, so the gap between snapshot and ack also
 *      counts that queue: a sender that fills the window faster than the path drains
 *      saturates any window. Only the byte window bounds what waits on the wire, so only
 *      its sample leaves the queue out; the message window bounds how many frames the
 *      receiver dispatches per round trip, which `FlowControl` gates on its own load. Three
 *      things tell the queue from the path:
 *      - an attach carries a probe the sender answers before any of the channel's frames,
 *        so its round trip is the path's. A later sample counts at that round trip, as its
 *        delivery rate times the path's RTT, which is BBR's estimate: that leaves out a
 *        queue wherever it is, the kernel's send buffer included. A server's receive side
 *        has no attach of its own to probe: each ack of its sender, the page, says the round
 *        trip the page's attach measured, where it probed one. Where flow-control frames wait
 *        for a batched POST, which an attach's RECONCILE doesn't, the attach isn't probed;
 *      - a round the window held back: the window starved the sender's wire (below); the
 *        receiver had less than an eighth of the window waiting to go out on its wire as the
 *        ping went out, which the ping waits behind, or behind a full queue where frames wait
 *        for room; and in the eighth of the round trip before the ack, the wire delivered,
 *        from any channel, less than half what the channel's own rate would, where a queue
 *        the ack waits behind delivers at the link's rate up to it. Other channels' few
 *        frames are no queue.
 *        Its round trip is the path's, which may have risen since an attach measured it;
 *      - where neither side measured, the window starved the sender's wire: the ack says the
 *        least the wire held whenever the sender's credit ran out since it last answered one
 *        (see `FlowControl.onPing`), which sees what the runtime buffers, and that was under
 *        an eighth of the window.
 *   3. If the sample saturates ≥ 2/3 of the window, and the queue can't account for it, the
 *      window was the bottleneck → that axis's decision is `grow`; caller may call
 *      `grow()` to double it (clamped to its cap, see `capByteWindow`). The caller applies any
 *      additional gates (e.g. runtime saturation in `FlowControl`) before committing.
 *      Otherwise leave it; producer or wire is the limit, not us.
 *   4. Once `window` reaches the cap, probing stops entirely — no further growth
 *      is possible, so the round-trips would be pure overhead.
 *
 * Scope: pure bytes/RTT (BDP). Cross-cutting policies — CPU saturation, congestion,
 * fairness — live above this layer (see `FlowControl`), which can refuse to commit
 * a `grow()` even when this estimator says the link has byte-room.
 *
 * Grow-only by design (same as gRPC). Oversized `W` costs nothing as long as the
 * sender doesn't fill it — receiver memory = unconsumed bytes, not `W`. Shrinking
 * adds oscillation risk and decay-tuning complexity for no real win; the only
 * downside of an oversized `W` is the worst-case admissible buffering, capped at MAX.
 *
 * Why a dedicated `BDP_PING` frame rather than piggybacking on the user-level ping
 * (`CHANNEL_PING_INTERVAL_MS`): the user-ping is configurable for connection health
 * (defaults to 5 s, often set to 30 s+); BDP needs RTT-scale cadence to adapt within
 * a handful of round-trips.
 */
class BdpEstimator {
  // Byte axis
  private _byteWindow: number = CREDIT_WINDOW_INITIAL_BYTES
  /** The largest the byte window gets: `CREDIT_WINDOW_MAX_BYTES`, or less where the sender's replay holds less. */
  private _byteWindowMax: number = CREDIT_WINDOW_MAX_BYTES
  private _bytesAtPingSent = 0
  private _bytesReceived = 0
  // Message-count axis
  private _msgWindow: number = CREDIT_MSG_WINDOW_INITIAL
  private _msgsAtPingSent = 0
  private _msgsReceived = 0
  /** The message window before its last growth, until the next ping settles or the wire is replaced. */
  private _msgWindowBeforeGrowth = 0
  // Shared probe
  /** Probes started, attach probes included: each takes the next number. */
  private _probes = 0
  /** The number of the ping in flight, or the last one. */
  private _ping = 0
  private _pingInFlight = false
  private _pingSentAt = 0
  /** Attach probes not answered yet. Each measures its wire's round trip and settles no growth, so none holds up a ping:
   *  one lost with its wire, or with an upgrade that didn't happen, costs nothing. */
  private _attachProbes: { probe: number; sentAt: number; wire: number }[] = []
  /** The least round trip a probe took on the wire an attach's probe last measured, since it did. `Infinity` before. */
  private _pathRtt = Infinity
  private _pathWire = -1
  /** The round trip a round the window held back took, which a queue can't have lengthened, since the wire was
   *  replaced: lowered as `_pathRtt` is. `Infinity` before. */
  private _boundRtt = Infinity
  /** What this side had waiting to go out on its wire as the ping in flight went out. */
  private _aheadOfPing = 0
  private _lastPingAt = 0
  /** The byte window grew since the last ping went out. */
  private _grewSincePing = false
  /** The ping in flight went out after the byte window grew. The sender answers on the credit it had since it answered the
   *  one before, which the smaller window granted. */
  private _pingFollowsGrowth = false
  /** Adaptive probe interval: snaps to `MIN` on grow, doubles up to `MAX` on
   *  non-grow. Slows but never freezes — a real rate change rediscovers. */
  private _probeIntervalMs = BDP_PING_MIN_INTERVAL_MS

  /** The byte window the estimator sets, which `FlowControl` grants unless it grants more. */
  get byteWindow(): number {
    return this._byteWindow
  }

  /** The largest the byte window gets (see `capByteWindow`). */
  get byteWindowMax(): number {
    return this._byteWindowMax
  }

  /** Currently advertised message-count window. */
  get msgWindow(): number {
    return this._msgWindow
  }

  /** The number of the ping in flight, which its `BDP_PING` carries. */
  get probe(): number {
    return this._ping
  }

  /** The round trip of the path an attach's probe on `wire` measured (see `probeAttach`), `Infinity` where none did. */
  pathRtt(wire: number): number {
    return wire === this._pathWire ? this._pathRtt : Infinity
  }

  /** Record one received frame (pre-app-processing). Returns true iff a `BDP_PING`, of the
   *  number `probe` reads, should be emitted: caller fires `sendBdpPing()` synchronously, while this side has what
   *  `backlog` says waiting to go out on its wire, `undefined` where the runtime can't tell. A single probe
   *  collects samples for both axes — `onPingAck` then derives independent
   *  byte-sample / msg-sample saturation decisions. */
  onReceive(bytes: number, backlog: () => number | undefined): boolean {
    let probe = false
    // Skip probe only when *both* axes have already hit their cap — otherwise one of them
    // might still want to grow.
    if (!this._pingInFlight && !(this._byteWindow >= this._byteWindowMax && this._msgWindow >= CREDIT_MSG_WINDOW_MAX)) {
      const now = Date.now()
      if (now - this._lastPingAt >= this._probeIntervalMs) {
        // Snapshot BEFORE crediting the triggering frame — it counts as the first
        // in-flight byte. Otherwise a window-bound producer always samples 0.
        this._sendPing()
        this._aheadOfPing = backlog() ?? 0
        this._pingFollowsGrowth = this._grewSincePing
        this._grewSincePing = false
        this._lastPingAt = now
        probe = true
      }
    }
    this._bytesReceived += bytes
    this._msgsReceived += 1
    return probe
  }

  /** Start a probe that goes out with an attach on `wire`, and return its number, which the RECONCILE entry carries, or
   *  `undefined` where that wire's round trip is measured already. Wires are numbered in the order they attach: an
   *  answer measures a round trip for a later wire than the last measured, lowers it for that wire, and one for an
   *  earlier wire, gone since, is ignored. */
  probeAttach(wire: number): number | undefined {
    if (wire === this._pathWire) return undefined
    this._attachProbes = this._attachProbes.filter((attach) => attach.wire >= wire)
    this._probes = (this._probes + 1) >>> 0
    this._attachProbes.push({ probe: this._probes, sentAt: performance.now(), wire })
    return this._probes
  }

  /** Settle the `BDP_PING` in flight against its `BDP_PING_ACK`, which says the least the sender's wire held whenever its
   *  credit ran out, and the path's round trip as the sender measured it, `Infinity` where it measured none.
   *  `deliveredBefore(ms)`: the bytes the wire delivered, from any channel, in the `ms` before the ack arrived. Returns
   *  per-axis grow suggestions. Caller decides whether to actually `growBytes()` / `growMsgs()` (e.g. after applying the
   *  CPU-lag gate). */
  onPingAck(
    probe: number,
    senderBacklog: number,
    senderPathRtt: number,
    deliveredBefore: (ms: number) => number,
  ): GrowDecision {
    const attach = this._attachProbes.find((pending) => pending.probe === probe)
    if (attach) {
      this._attachProbes = this._attachProbes.filter((pending) => pending !== attach)
      const { sentAt, wire } = attach
      if (wire < this._pathWire) return NOT_SETTLED
      const rtt = performance.now() - sentAt
      this._pathRtt = wire === this._pathWire ? Math.min(this._pathRtt, rtt) : rtt
      this._pathWire = wire
      return NOT_SETTLED
    }
    if (!this._pingInFlight || probe !== this._ping) return NOT_SETTLED
    this._pingInFlight = false
    const now = performance.now()
    const rtt = now - this._pingSentAt
    // An attach's probe may have waited behind other channels' frames, or on the channel's registration: a later one
    // that took less shows the path takes no more.
    if (this._pathRtt < Infinity && rtt < this._pathRtt) this._pathRtt = rtt
    if (this._boundRtt < Infinity && rtt < this._boundRtt) this._boundRtt = rtt
    const received = this._bytesReceived - this._bytesAtPingSent
    const starved = senderBacklog * 8 < this._byteWindow
    // The window held the sender back, the ping didn't wait behind this side's own sends, and in the eighth of the round
    // trip before the ack the wire delivered under half what the channel did on average. A queue the ack waits behind
    // delivers at the link's rate up to it, at least the channel's rate, another channel's frames too. So the round trip
    // is the path's, however much less one measured before.
    if (
      starved &&
      this._aheadOfPing * 8 < this._byteWindow &&
      received * 3 >= this._byteWindow * 2 &&
      deliveredBefore(rtt / 8) * 16 < received
    )
      this._boundRtt = rtt
    // Where an attach measured the path's round trip, here or at the sender, that tells a queue from the path, wherever
    // the queue is, as does a round the window held back, which took longer if the path's rose since. Else only the sender
    // can, where its runtime reports what its wire holds.
    let pathRtt = Math.min(this._pathRtt, senderPathRtt)
    if (this._boundRtt < Infinity) pathRtt = pathRtt < Infinity ? Math.max(pathRtt, this._boundRtt) : this._boundRtt
    const measured = pathRtt < Infinity
    const atPathRtt = pathRtt < rtt ? pathRtt / rtt : 1
    const byteSample = received * atPathRtt
    const msgSample = this._msgsReceived - this._msgsAtPingSent
    const bytes: AxisDecision =
      this._byteWindow >= this._byteWindowMax
        ? 'at-cap'
        : byteSample * 3 < this._byteWindow * 2
          ? 'sample-too-small'
          : measured
            ? 'grow'
            : this._pingFollowsGrowth
              ? 'window-grew'
              : starved
                ? 'grow'
                : 'wire-busy'
    const msgWindow = (starved && this._msgWindowBeforeGrowth) || this._msgWindow
    this._msgWindowBeforeGrowth = 0
    const msgs: AxisDecision =
      this._msgWindow >= CREDIT_MSG_WINDOW_MAX ? 'at-cap' : msgSample * 3 < msgWindow * 2 ? 'sample-too-small' : 'grow'
    // Cadence: snap to MIN on grow (more headroom may exist), exponential
    // backoff on a verdict against growing (converged or temporarily quiet).
    if (bytes === 'grow' || msgs === 'grow') {
      this._probeIntervalMs = BDP_PING_MIN_INTERVAL_MS
    } else if (bytes !== 'window-grew') {
      this._probeIntervalMs = Math.min(BDP_PING_MAX_INTERVAL_MS, this._probeIntervalMs * 2)
    }
    return { acknowledged: true, bytes, msgs }
  }

  /** Commit a byte-window doubling. Idempotent at the cap. */
  growBytes(): void {
    this._byteWindow = Math.min(this._byteWindowMax, this._byteWindow * 2)
    this._grewSincePing = true
  }

  /** Grow-only bump of the byte window (clamped to its cap). */
  bumpInitialByteWindow(bytes: number): void {
    const window = Math.min(this._byteWindowMax, bytes)
    if (window <= this._byteWindow) return
    this._byteWindow = window
    this._grewSincePing = true
  }

  /** The byte window gets to `bytes` at most, `CREDIT_WINDOW_MAX_BYTES` if more, and is lowered to it. */
  capByteWindow(bytes: number): void {
    this._byteWindowMax = Math.min(CREDIT_WINDOW_MAX_BYTES, bytes)
    this._byteWindow = Math.min(this._byteWindow, this._byteWindowMax)
  }

  /** Commit a message-window doubling. Idempotent at the cap. */
  growMsgs(): void {
    this._msgWindowBeforeGrowth = this._msgWindow
    this._msgWindow = Math.min(CREDIT_MSG_WINDOW_MAX, this._msgWindow * 2)
  }

  /** Drop the ping in flight (its ack rode the prior wire). Preserves window AND
   *  cadence — both are link properties; a real rate change rediscovers. */
  reset(): void {
    this._pingInFlight = false
    this._boundRtt = Infinity
    this._msgWindowBeforeGrowth = 0
    this._bytesAtPingSent = this._bytesReceived
    this._msgsAtPingSent = this._msgsReceived
  }

  private _sendPing(): void {
    this._probes = (this._probes + 1) >>> 0
    this._ping = this._probes
    this._pingInFlight = true
    this._pingSentAt = performance.now()
    this._bytesAtPingSent = this._bytesReceived
    this._msgsAtPingSent = this._msgsReceived
  }
}
