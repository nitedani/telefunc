export { ClientConnection }
export type { MuxChannel, MuxConnection }

import { parse } from '@brillout/json-serializer/parse'
import { makeAbortError, makeBugError } from '../../client/remoteTelefunctionCall/errors.js'
import { assert } from '../../utils/assert.js'
import { ChannelClosedError, ChannelNetworkError } from '../channel-errors.js'
import {
  CHANNEL_UPGRADE_TIMEOUT_MS,
  CHANNEL_CLIENT_REPLAY_BUFFER_BYTES,
  CHANNEL_IDLE_TIMEOUT_MS,
  CHANNEL_PING_INTERVAL_MS,
  CHANNEL_RECONNECT_INITIAL_DELAY_MS,
  CHANNEL_RECONNECT_MAX_DELAY_MS,
  CHANNEL_RECONNECT_TIMEOUT_MS,
  SSE_FLUSH_THROTTLE_MS,
  SSE_POST_IDLE_FLUSH_DELAY_MS,
  SSE_RECONCILE_DEADLINE_MS,
  WS_PROBE_TIMEOUT_MS,
  type ChannelTransports,
} from '../constants.js'
import { encodeLengthPrefixedFrames } from '../frame.js'
import { ReplayBuffer } from '../replay-buffer.js'
import { REQUEST_KIND, REQUEST_KIND_HEADER, getMarkedRequestUrl } from '../request-kind.js'
import { encodeSseRequest } from '../sse-request.js'
import { TAG, decode, encode, encodeCtrl } from '../shared-ws.js'
import type {
  AckResultStatus,
  CtrlMessage,
  CtrlReconcile,
  CtrlReconciled,
  CtrlSettings,
  CtrlUpgraded,
} from '../shared-ws.js'
import { base64urlToUint8Array } from '../base64url.js'
import { DeadlineScheduler } from './deadlineScheduler.js'
import { CHANNEL_TRANSPORT, type ChannelTransport } from '../constants.js'

type PendingAck = {
  resolve: (result: unknown) => void
  reject: (err: Error) => void
}

type BufferedFrame = {
  kind: OutboundFrameKind
  frame: Uint8Array<ArrayBuffer>
  channelIx: number
  seq?: number
}

type OutboundFrameKind = 'reconcile' | 'reconcile-release' | 'control' | 'ack' | 'data' | 'heartbeat'

type OutboundFrame = {
  kind: OutboundFrameKind
  frame: Uint8Array<ArrayBuffer>
}

interface MuxChannel {
  readonly id: string
  readonly defer: boolean
  readonly isClosed: boolean
  readonly isOpen: boolean
  _onTransportOpen(): void
  _onTransportMessage(data: string): void
  _onTransportBinaryMessage(data: Uint8Array): void
  _onTransportAckReqMessage(data: string, seq: number): Promise<void>
  _onTransportCloseRequest(timeoutMs: number): void
  _onTransportCloseAck(): void
  _onTransportClose(err?: Error): void
}

interface MuxConnection {
  send(channel: MuxChannel, data: string): void
  sendTextAckReq(channel: MuxChannel, data: string): Promise<unknown>
  sendBinary(channel: MuxChannel, data: Uint8Array): void
  sendAckRes(channel: MuxChannel, ackedSeq: number, result: string, status?: AckResultStatus): void
  sendAbort(channel: MuxChannel): void
  sendCloseRequest(channel: MuxChannel, timeoutMs: number): void
  sendCloseAck(channel: MuxChannel): void
  sendPause(channel: MuxChannel): void
  sendResume(channel: MuxChannel): void
  unregister(channel: MuxChannel, err?: Error): void
}

type ReconcileOutcome = {
  frames: OutboundFrame[]
  channelsToOpen: MuxChannel[]
  reconcileComplete: boolean
}

type ReconcileBatch = {
  reconcileFrame: OutboundFrame
  movedBufferedFrames: OutboundFrame[]
}

type ReconcileBufferedFramesMode = 'batch-on-reconcile' | 'release-after-reconciled'

type ClientConnectionOptions = {
  transports: ChannelTransports
  fetchImpl: typeof fetch
}

type ClientChannelTransport = {
  readonly type: ChannelTransport
  readonly reconnectTimeoutMessage: string
  readonly supportsPauseResume: boolean
  readonly sendReconcileOnOpen: boolean
  /** Initial reconcile buffered frames mode for this transport. */
  readonly reconcileMode: ReconcileBufferedFramesMode
  probe(): Promise<(() => void) | null>
  start(): void
  hasActiveTransport(): boolean
  isConnecting(): boolean
  sendFrame(frame: OutboundFrame): void
  abandonActiveTransport(): void
  closeAbandonedTransport(): void
  applyReconciledSettings(ctrl: CtrlSettings): void
  /** Wait until the transport-specific upgrade precondition is met before attempting handoff. */
  prepareForUpgrade(): Promise<void>
  dispose(): void
}

type SseInitialBatchStage = {
  initialFrames: OutboundFrame[]
  movedOutboxFrames: Uint8Array<ArrayBuffer>[]
  movedOutboxDeadlines: number[]
  movedBufferedFrames: OutboundFrame[]
}

type UpgradeState = {
  active: boolean
  disabled: boolean
  probeAbort: (() => void) | null
  retiringTransport: ClientChannelTransport | null
  nextTransport: ClientChannelTransport | null
  oldInboundClosed: boolean
  outboundCutoverPending: boolean
  outboundPromoted: boolean
  bufferedInboundFrames: Uint8Array<ArrayBuffer>[] | null
  timeout: ReturnType<typeof setTimeout> | null
}

type InboundFrame = ReturnType<typeof decode>

class ClientConnection implements MuxConnection {
  private static cache = new Map<string, ClientConnection>()

  static getOrCreate(telefuncUrl: string, channel: MuxChannel, options: ClientConnectionOptions): ClientConnection {
    const key = `${options.transports.join(',')}:${telefuncUrl}`
    let connection = ClientConnection.cache.get(key)
    if (!connection || connection.closed) {
      connection = new ClientConnection(telefuncUrl, options, key)
      ClientConnection.cache.set(key, connection)
    }
    connection.register(channel)
    return connection
  }

  private readonly cacheKey: string
  private readonly telefuncUrl: string
  private readonly connectionOptions: ClientConnectionOptions
  private reconcileBufferedFramesMode: ReconcileBufferedFramesMode
  private transport: ClientChannelTransport

  private closed = false
  private connected = false
  private pingInterval: ReturnType<typeof setInterval> | null = null
  private pongTimer: ReturnType<typeof setTimeout> | null = null
  private ttl: ReturnType<typeof setTimeout> | null = null
  private reconnectTimer: ReturnType<typeof setTimeout> | null = null
  private reconnectAttempt = 0
  private reconnectStart = 0
  private readonly upgrade: UpgradeState = {
    active: false,
    disabled: false,
    probeAbort: null,
    retiringTransport: null,
    nextTransport: null,
    oldInboundClosed: false,
    outboundCutoverPending: false,
    outboundPromoted: false,
    bufferedInboundFrames: null,
    timeout: null,
  }

  // Protocol state
  private sessionId: string | null = null
  private nextIndex = 0
  private reconciling = false
  private reconcileIxes = new Set<number>()
  private channels = new Map<number, MuxChannel>()
  private channelIndex = new Map<MuxChannel, number>()
  private sendBuffer: BufferedFrame[] = []
  private lastSeqByChannel = new Map<number, number>()
  private replayBuffers = new Map<number, ReplayBuffer>()
  private pendingAcks = new Map<string, PendingAck>()
  private reconnectTimeoutMs = CHANNEL_RECONNECT_TIMEOUT_MS
  private idleTimeoutMs = CHANNEL_IDLE_TIMEOUT_MS
  private clientReplayBufferBytes = CHANNEL_CLIENT_REPLAY_BUFFER_BYTES
  private pingIntervalMs = CHANNEL_PING_INTERVAL_MS
  private pongTimeoutMs = CHANNEL_PING_INTERVAL_MS * 2
  private readonly dispatchFrame = (frame: InboundFrame): void => {
    switch (frame.tag) {
      case TAG.CTRL:
        this.handleCtrl(frame.ctrl)
        return
      case TAG.TEXT:
      case TAG.TEXT_ACK_REQ:
      case TAG.BINARY:
        this.handleDataFrame(frame)
        return
      case TAG.ACK_RES:
        this.handleAckRes(frame.index, frame.ackedSeq, frame.text, frame.status)
    }
  }
  private constructor(telefuncUrl: string, options: ClientConnectionOptions, cacheKey: string) {
    this.cacheKey = cacheKey
    this.telefuncUrl = telefuncUrl
    this.connectionOptions = options
    this.transport = TRANSPORT_REGISTRY[options.transports[0]!](telefuncUrl, options, this)
    this.reconcileBufferedFramesMode = this.transport.reconcileMode
  }

  private canSendImmediately(channelIx?: number): boolean {
    if (this.isAwaitingOutgoingCutover()) return false
    if (!this.connected || !this.transport.hasActiveTransport() || this.reconciling) return false
    if (channelIx === undefined) return true
    return this.channels.get(channelIx)?.isOpen === true
  }

  private isAwaitingOutgoingCutover(): boolean {
    return this.upgrade.outboundCutoverPending
  }

  private register(channel: MuxChannel): void {
    this.clearTimer('ttl')
    const ix = this.nextIndex++
    this.channels.set(ix, channel)
    this.channelIndex.set(channel, ix)
    this.replayBuffers.set(ix, new ReplayBuffer(this.clientReplayBufferBytes, this.reconnectTimeoutMs))

    if (!this.transport.hasActiveTransport() && !this.transport.isConnecting()) {
      this.transport.start()
      return
    }
    if (this.connected && !this.reconciling && !this.isAwaitingOutgoingCutover()) {
      const reconcileBatch = this.stageReconcileBatch()
      this.sendReconcileBatch(reconcileBatch)
    }
  }

  unregister(channel: MuxChannel, err = new ChannelClosedError()): void {
    const ix = this.channelIndex.get(channel)
    if (ix === undefined) return
    this.releaseChannel(ix, channel, err)
    this.startTtlIfIdle()
  }

  send(channel: MuxChannel, data: string): void {
    const ix = this.channelIndex.get(channel)
    if (ix === undefined) return
    const replay = this.replayBuffers.get(ix)!
    const seq = replay.nextSeq()
    const frame = encode.text(ix, data, seq)
    if (!this.canSendImmediately(ix)) {
      this.sendBuffer.push({ kind: 'data', frame, channelIx: ix, seq })
      return
    }
    replay.push(seq, frame)
    this.transport.sendFrame({ kind: 'data', frame })
  }

  sendTextAckReq(channel: MuxChannel, data: string): Promise<unknown> {
    const ix = this.channelIndex.get(channel)
    if (ix === undefined) return Promise.reject(new ChannelClosedError())
    const replay = this.replayBuffers.get(ix)!
    const seq = replay.nextSeq()
    const promise = new Promise<unknown>((resolve, reject) => {
      this.pendingAcks.set(`${ix}:${seq}`, { resolve, reject })
    })
    const frame = encode.textAckReq(ix, data, seq)
    if (!this.canSendImmediately(ix)) {
      this.sendBuffer.push({ kind: 'ack', frame, channelIx: ix, seq })
      return promise
    }
    replay.push(seq, frame)
    this.transport.sendFrame({ kind: 'ack', frame })
    return promise
  }

  sendBinary(channel: MuxChannel, data: Uint8Array): void {
    const ix = this.channelIndex.get(channel)
    if (ix === undefined) return
    const replay = this.replayBuffers.get(ix)!
    const seq = replay.nextSeq()
    const frame = encode.binary(ix, data, seq)
    if (!this.canSendImmediately(ix)) {
      this.sendBuffer.push({ kind: 'data', frame, channelIx: ix, seq })
      return
    }
    replay.push(seq, frame)
    this.transport.sendFrame({ kind: 'data', frame })
  }

  sendAckRes(channel: MuxChannel, ackedSeq: number, result: string, status: AckResultStatus = 'ok'): void {
    const ix = this.channelIndex.get(channel)
    if (ix === undefined) return
    const replay = this.replayBuffers.get(ix)!
    const seq = replay.nextSeq()
    const frame = encode.ackRes(ix, seq, ackedSeq, result, status)
    if (!this.canSendImmediately(ix)) {
      this.sendBuffer.push({ kind: 'ack', frame, channelIx: ix, seq })
      return
    }
    replay.push(seq, frame)
    this.transport.sendFrame({ kind: 'ack', frame })
  }

  sendAbort(channel: MuxChannel): void {
    const ix = this.channelIndex.get(channel)
    if (ix === undefined) return
    const frame = encodeCtrl({ t: 'close', ix, timeoutMs: 0 })
    if (this.canSendImmediately(ix)) {
      this.transport.sendFrame({ kind: 'control', frame })
    } else {
      this.sendBuffer.push({ kind: 'control', frame, channelIx: ix, seq: undefined })
    }
  }

  sendCloseRequest(channel: MuxChannel, timeoutMs: number): void {
    const ix = this.channelIndex.get(channel)
    if (ix === undefined) return
    const frame = encodeCtrl({ t: 'close', ix, timeoutMs })
    if (!this.canSendImmediately(ix)) {
      this.sendBuffer.push({ kind: 'control', frame, channelIx: ix, seq: undefined })
      return
    }
    this.transport.sendFrame({ kind: 'control', frame })
  }

  sendCloseAck(channel: MuxChannel): void {
    const ix = this.channelIndex.get(channel)
    if (ix === undefined) return
    const frame = encodeCtrl({ t: 'close-ack', ix })
    if (!this.canSendImmediately(ix)) {
      this.sendBuffer.push({ kind: 'control', frame, channelIx: ix, seq: undefined })
      return
    }
    this.transport.sendFrame({ kind: 'control', frame })
  }

  sendPause(channel: MuxChannel): void {
    if (!this.transport.supportsPauseResume) return
    const ix = this.channelIndex.get(channel)
    if (ix === undefined) return
    const frame = encodeCtrl({ t: 'pause', ix })
    if (!this.canSendImmediately(ix)) {
      this.sendBuffer.push({ kind: 'control', frame, channelIx: ix, seq: undefined })
      return
    }
    this.transport.sendFrame({ kind: 'control', frame })
  }

  sendResume(channel: MuxChannel): void {
    if (!this.transport.supportsPauseResume) return
    const ix = this.channelIndex.get(channel)
    if (ix === undefined) return
    const frame = encodeCtrl({ t: 'resume', ix })
    if (!this.canSendImmediately(ix)) {
      this.sendBuffer.push({ kind: 'control', frame, channelIx: ix, seq: undefined })
      return
    }
    this.transport.sendFrame({ kind: 'control', frame })
  }

  _onTransportOpen(transport: ClientChannelTransport): void {
    if (this.closed) return
    this.connected = true
    this.reconnectAttempt = 0
    this.reconnectStart = 0
    if (transport === this.upgrade.nextTransport) {
      assert(this.sessionId)
      transport.sendFrame({ kind: 'control', frame: encodeCtrl({ t: 'upgrade', sessionId: this.sessionId }) })
      this.startUpgradeTimeout(() => {
        if (this.upgrade.nextTransport !== transport) return
        this.cancelUpgradeAttempt()
      })
      return
    }
    if (!transport.sendReconcileOnOpen) return
    const reconcileBatch = this.stageReconcileBatch()
    this.sendReconcileBatch(reconcileBatch)
  }

  _onTransportFrame(transport: ClientChannelTransport, raw: Uint8Array<ArrayBuffer>): void {
    const frame = decode(raw)
    const bufferedTransport = this.upgrade.bufferedInboundFrames ? this.upgrade.nextTransport : null

    if (transport === this.upgrade.nextTransport && frame.tag === TAG.CTRL && frame.ctrl.t === 'upgraded') {
      this.handleUpgraded(transport, frame.ctrl)
      return
    }

    if (transport === this.upgrade.retiringTransport && frame.tag === TAG.CTRL && frame.ctrl.t === 'fin') {
      this.handleServerFin()
      return
    }

    if (transport === bufferedTransport) {
      const bufferedInboundFrames = this.upgrade.bufferedInboundFrames
      assert(bufferedInboundFrames)
      bufferedInboundFrames.push(raw)
      return
    }

    this.dispatchFrame(frame)
  }

  private flushHandoffBuffer(): void {
    const buffer = this.upgrade.bufferedInboundFrames
    this.upgrade.bufferedInboundFrames = null
    if (!buffer) return
    for (const raw of buffer) this.dispatchFrame(decode(raw))
  }

  /**
   * Server fin closes the old server->client path only.
   *
   * At this point the client can activate inbound WS immediately, but outbound
   * traffic must stay on the old transport until the old outbox is drained and the
   * client has emitted its own directional fin.
   */
  private handleServerFin(): void {
    const retiringTransport = this.upgrade.retiringTransport
    if (!retiringTransport) return
    if (this.upgrade.oldInboundClosed) return
    this.upgrade.oldInboundClosed = true
    if (this.upgrade.bufferedInboundFrames !== null) this.flushHandoffBuffer()
    this.maybeCompleteUpgradeHandoff()
  }

  private async finishOutgoingUpgradeAfterDrain(retiringTransport: ClientChannelTransport): Promise<void> {
    await retiringTransport.prepareForUpgrade()
    if (this.closed || this.upgrade.retiringTransport !== retiringTransport || !this.upgrade.outboundCutoverPending)
      return
    retiringTransport.sendFrame({ kind: 'control', frame: encodeCtrl({ t: 'fin' }) })
    await retiringTransport.prepareForUpgrade()
    if (this.closed || this.upgrade.retiringTransport !== retiringTransport || !this.upgrade.outboundCutoverPending)
      return
    this.finishOutgoingUpgrade()
  }

  /**
   * Final client-side cutover for the client->server direction.
   *
   * This runs once the old outbound path is drained, client `fin` is appended to
   * the retiring outbox, and the final old-path flush carrying that `fin` resolves.
   */
  private finishOutgoingUpgrade(): void {
    const nextTransport = this.upgrade.nextTransport
    assert(nextTransport)
    this.transport = nextTransport
    this.reconcileBufferedFramesMode = nextTransport.reconcileMode
    this.upgrade.outboundCutoverPending = false
    this.upgrade.outboundPromoted = true
    if (this.connected && !this.reconciling && this.hasPendingChannelOpen()) {
      this.flushSendBufferImmediately()
      if (this.sendBuffer.length > 0) {
        const reconcileBatch = this.stageReconcileBatch()
        this.sendReconcileBatch(reconcileBatch)
      }
    }
    this.maybeCompleteUpgradeHandoff()
  }

  private maybeCompleteUpgradeHandoff(): void {
    const retiringTransport = this.upgrade.retiringTransport
    if (!retiringTransport || !this.upgrade.oldInboundClosed || !this.upgrade.outboundPromoted) return
    this.clearUpgradeTimeout()
    this.upgrade.retiringTransport = null
    this.upgrade.nextTransport = null
    this.upgrade.oldInboundClosed = false
    this.upgrade.outboundPromoted = false
    retiringTransport.abandonActiveTransport()
    retiringTransport.dispose()
  }

  private flushSendBufferImmediately(): void {
    if (!this.connected || !this.transport.hasActiveTransport() || this.reconciling) return
    let flushed = 0
    for (const entry of this.sendBuffer) {
      if (!this.channels.get(entry.channelIx)?.isOpen) break
      if (entry.seq !== undefined) this.replayBuffers.get(entry.channelIx)?.push(entry.seq, entry.frame)
      this.transport.sendFrame({ kind: entry.kind, frame: entry.frame })
      flushed++
    }
    if (flushed > 0) this.sendBuffer.splice(0, flushed)
  }

  private startUpgradeTimeout(onTimeout: () => void): void {
    this.clearUpgradeTimeout()
    this.upgrade.timeout = setTimeout(() => {
      this.upgrade.timeout = null
      if (this.closed) return
      onTimeout()
    }, CHANNEL_UPGRADE_TIMEOUT_MS)
  }

  private clearUpgradeTimeout(): void {
    const timeout = this.upgrade.timeout
    if (!timeout) return
    clearTimeout(timeout)
    this.upgrade.timeout = null
  }

  private cancelUpgradeAttempt(): void {
    this.clearUpgradeTimeout()
    this.disposeCandidateTransport()
    this.upgrade.retiringTransport = null
    this.upgrade.oldInboundClosed = false
    this.upgrade.outboundCutoverPending = false
    this.upgrade.outboundPromoted = false
    this.upgrade.bufferedInboundFrames = null
    this.upgrade.active = false
  }

  private hasPendingChannelOpen(): boolean {
    for (const channel of this.channels.values()) {
      if (!channel.isOpen) return true
    }
    return false
  }

  private disposeCandidateTransport(): void {
    const nextTransport = this.upgrade.nextTransport
    if (!nextTransport) return
    nextTransport.abandonActiveTransport()
    nextTransport.dispose()
    this.upgrade.nextTransport = null
  }

  _onTransportClosed(transport: ClientChannelTransport, rejectedInitial = false): void {
    if (this.closed) return
    if (transport === this.upgrade.nextTransport && transport !== this.transport) {
      this.cancelUpgradeAttempt()
      return
    }
    if (transport === this.upgrade.retiringTransport && this.upgrade.nextTransport) {
      this.upgrade.oldInboundClosed = true
      if (this.upgrade.bufferedInboundFrames !== null) this.flushHandoffBuffer()
      if (!this.upgrade.outboundPromoted) {
        this.finishOutgoingUpgrade()
      } else {
        this.maybeCompleteUpgradeHandoff()
      }
      return
    }
    if (transport !== this.transport) {
      return
    }
    if (this.upgrade.active) {
      this.upgrade.active = false
      this.upgrade.probeAbort?.()
      this.upgrade.probeAbort = null
    }
    const err = new ChannelNetworkError(
      rejectedInitial
        ? `Server rejected ${this.transport.type === CHANNEL_TRANSPORT.SSE ? 'SSE' : 'WebSocket'} connection`
        : 'Connection dropped',
    )
    this.handleTransportLoss(err, rejectedInitial)
  }

  private handleCtrl(ctrl: CtrlMessage): void {
    if (!ctrl || typeof ctrl !== 'object') return
    switch (ctrl.t) {
      case 'pong':
        this.resetPongTimer()
        return
      case 'close':
        this.channels.get(ctrl.ix)?._onTransportCloseRequest(ctrl.timeoutMs)
        return
      case 'close-ack':
        this.channels.get(ctrl.ix)?._onTransportCloseAck()
        return
      case 'abort':
        this.closeRemoteChannel(ctrl.ix, makeAbortError(parse(ctrl.abortValue)))
        this.startTtlIfIdle()
        return
      case 'error':
        this.closeRemoteChannel(ctrl.ix, makeBugError())
        this.startTtlIfIdle()
        return
      case 'fin':
        this.handleServerFin()
        return
      case 'upgraded':
        return
      case 'reconciled':
        this.handleReconciled(ctrl)
        return
    }
  }

  private applyServerSettings(ctrl: CtrlSettings & { sessionId: string }): void {
    this.sessionId = ctrl.sessionId
    if (ctrl.reconnectTimeout) this.reconnectTimeoutMs = ctrl.reconnectTimeout
    if (ctrl.idleTimeout) this.idleTimeoutMs = ctrl.idleTimeout
    if (ctrl.clientReplayBuffer) this.clientReplayBufferBytes = ctrl.clientReplayBuffer
    if (ctrl.pingInterval) {
      this.pingIntervalMs = ctrl.pingInterval
      this.pongTimeoutMs = ctrl.pingInterval * 2
    }
  }

  private handleUpgraded(transport: ClientChannelTransport, ctrl: CtrlUpgraded): void {
    transport.applyReconciledSettings(ctrl)
    this.applyServerSettings(ctrl)

    const retiringTransport = this.upgrade.retiringTransport
    assert(retiringTransport)
    this.upgrade.active = false
    this.upgrade.outboundCutoverPending = true
    this.upgrade.outboundPromoted = false
    this.upgrade.bufferedInboundFrames = []
    this.startUpgradeTimeout(() => {
      if (this.upgrade.retiringTransport !== retiringTransport) return
      if (!this.upgrade.outboundPromoted) this.finishOutgoingUpgrade()
      this.handleServerFin()
    })
    if (this.upgrade.oldInboundClosed) this.flushHandoffBuffer()
    void this.finishOutgoingUpgradeAfterDrain(retiringTransport)
    this.startPing()
    this.startTtlIfIdle()
  }

  private handleReconciled(ctrl: CtrlReconciled): void {
    this.transport.applyReconciledSettings(ctrl)
    const outcome = this.applyReconciled(ctrl)
    this.transport.closeAbandonedTransport()
    for (const frame of outcome.frames) this.transport.sendFrame(frame)
    for (const channel of outcome.channelsToOpen) channel._onTransportOpen()
    if (outcome.reconcileComplete) {
      this.startPing()
      this.startTtlIfIdle()
    }
    this.maybeStartUpgrade(ctrl)
  }

  // ── SSE→WS upgrade ──

  private maybeStartUpgrade(ctrl: CtrlReconciled): void {
    if (this.upgrade.disabled) return
    const nextTransport = UPGRADE_PATH[this.transport.type]
    if (!nextTransport || this.upgrade.active) return
    if (!this.connectionOptions.transports.includes(nextTransport) || !ctrl.transports.includes(nextTransport)) return
    this.upgrade.active = true
    void this.probeAndUpgrade(nextTransport)
  }

  private abortUpgradeAndReconnectSse(err: Error): void {
    this.clearUpgradeTimeout()
    this.disposeCandidateTransport()
    const retiringTransport = this.upgrade.retiringTransport
    if (retiringTransport) {
      retiringTransport.abandonActiveTransport()
      retiringTransport.dispose()
      this.upgrade.retiringTransport = null
    }
    this.upgrade.oldInboundClosed = false
    this.upgrade.outboundCutoverPending = false
    this.upgrade.outboundPromoted = false
    this.upgrade.bufferedInboundFrames = null
    this.upgrade.disabled = true
    this.transport.abandonActiveTransport()
    this.transport.dispose()
    this.transport = TRANSPORT_REGISTRY[CHANNEL_TRANSPORT.SSE](this.telefuncUrl, this.connectionOptions, this)
    this.reconcileBufferedFramesMode = this.transport.reconcileMode
    this.handleTransportLoss(err)
  }

  private async probeAndUpgrade(targetTransport: ChannelTransport): Promise<void> {
    const from = this.transport
    const to = TRANSPORT_REGISTRY[targetTransport](this.telefuncUrl, this.connectionOptions, this)
    const closeProbe = await to.probe()
    if (!closeProbe || !this.upgrade.active) {
      closeProbe?.()
      this.upgrade.active = false
      return
    }

    this.upgrade.probeAbort = closeProbe
    if (this.upgrade.probeAbort === closeProbe) this.upgrade.probeAbort = null
    if (!this.upgrade.active) {
      closeProbe()
      return
    }

    const prepared = await this.prepareUpgradeHandoff(from)
    if (!prepared || !this.upgrade.active) {
      closeProbe()
      this.upgrade.active = false
      return
    }

    this.upgrade.active = false
    this.upgrade.retiringTransport = from
    this.upgrade.nextTransport = to
    to.start()
  }

  private async prepareUpgradeHandoff(transport: ClientChannelTransport): Promise<boolean> {
    return await new Promise<boolean>((resolve) => {
      const timeout = setTimeout(() => resolve(false), CHANNEL_UPGRADE_TIMEOUT_MS)
      void transport.prepareForUpgrade().then(
        () => {
          clearTimeout(timeout)
          resolve(true)
        },
        () => {
          clearTimeout(timeout)
          resolve(false)
        },
      )
    })
  }

  // ── Ping ──

  private startPing(): void {
    this.resetPongTimer()
    if (this.pingInterval) return
    this.pingInterval = setInterval(() => {
      if (!this.canSendImmediately()) return
      this.transport.sendFrame({ kind: 'heartbeat', frame: encodeCtrl({ t: 'ping' }) })
    }, this.pingIntervalMs)
  }

  private stopPing(): void {
    if (this.pingInterval) {
      clearInterval(this.pingInterval)
      this.pingInterval = null
    }
    if (this.pongTimer) {
      clearTimeout(this.pongTimer)
      this.pongTimer = null
    }
  }

  private resetPongTimer(): void {
    if (this.pongTimer) clearTimeout(this.pongTimer)
    this.pongTimer = setTimeout(() => {
      this.stopPing()
      this.connected = false
      this.transport.abandonActiveTransport()
      this.handleTransportLoss(new ChannelNetworkError(this.transport.reconnectTimeoutMessage))
    }, this.pongTimeoutMs)
  }

  private handleTransportLoss(err: Error, rejected = false): void {
    if (this.closed) return
    this.connected = false
    this.stopPing()
    this.reconciling = false
    this.reconcileIxes.clear()
    this.clearTimer('ttl')
    if (this.upgrade.retiringTransport) {
      this.abortUpgradeAndReconnectSse(err)
      return
    }

    if (rejected && this.reconnectAttempt === 0) {
      this.closeAll(err instanceof Error ? err : new ChannelNetworkError('Connection dropped'))
      this.dispose()
      return
    }
    if (this.channels.size === 0) {
      this.dispose()
      return
    }
    if (!this.reconnectStart) this.reconnectStart = Date.now()
    if (Date.now() - this.reconnectStart > this.reconnectTimeoutMs) {
      this.closeAll(err instanceof Error ? err : new ChannelNetworkError('Connection dropped'))
      this.dispose()
      return
    }
    if (this.reconnectTimer) return
    const delay = Math.min(
      CHANNEL_RECONNECT_INITIAL_DELAY_MS * 2 ** this.reconnectAttempt,
      CHANNEL_RECONNECT_MAX_DELAY_MS,
    )
    this.reconnectAttempt++
    this.reconnectTimer = setTimeout(() => {
      this.reconnectTimer = null
      this.transport.start()
    }, delay)
  }

  private startTtlIfIdle(): void {
    if (this.closed || this.channels.size > 0 || this.ttl) return
    this.ttl = setTimeout(() => {
      if (this.channels.size === 0) this.dispose()
    }, this.idleTimeoutMs)
  }

  private dispose(): void {
    if (this.closed) return
    this.closed = true
    this.connected = false
    this.clearTimer('ttl')
    this.clearTimer('reconnectTimer')
    this.stopPing()
    this.clearUpgradeTimeout()
    this.upgrade.probeAbort?.()
    this.upgrade.probeAbort = null
    this.disposeCandidateTransport()
    const retiringTransport = this.upgrade.retiringTransport
    if (retiringTransport) {
      retiringTransport.abandonActiveTransport()
      retiringTransport.dispose()
      this.upgrade.retiringTransport = null
    }
    this.upgrade.oldInboundClosed = false
    this.upgrade.outboundCutoverPending = false
    this.upgrade.outboundPromoted = false
    this.upgrade.bufferedInboundFrames = null
    this.upgrade.active = false
    this.transport.dispose()
    for (const replayBuffer of this.replayBuffers.values()) replayBuffer.dispose()
    for (const [, pending] of this.pendingAcks) pending.reject(new ChannelNetworkError('Connection closed'))
    this.channels.clear()
    this.channelIndex.clear()
    this.sendBuffer = []
    this.lastSeqByChannel.clear()
    this.replayBuffers.clear()
    this.pendingAcks.clear()
    this.reconcileIxes.clear()
    this.reconciling = false
    ClientConnection.cache.delete(this.cacheKey)
  }

  private clearTimer(name: 'ttl' | 'reconnectTimer'): void {
    const timer = this[name]
    if (!timer) return
    clearTimeout(timer)
    this[name] = null
  }

  // ── Protocol internals ──

  buildReconcileFrame(): OutboundFrame {
    this.reconciling = true
    this.reconcileIxes = new Set()
    const open: CtrlReconcile['open'] = []
    for (const [ix, channel] of this.channels) {
      this.reconcileIxes.add(ix)
      const entry: CtrlReconcile['open'][number] = { id: channel.id, ix, lastSeq: this.lastSeqByChannel.get(ix) ?? 0 }
      if (channel.defer) entry.defer = true
      open.push(entry)
    }
    const reconcile: CtrlReconcile = { t: 'reconcile', open }
    if (this.sessionId) reconcile.sessionId = this.sessionId
    return { kind: 'reconcile', frame: encodeCtrl(reconcile) }
  }

  drainBufferedFramesForReconcile(): OutboundFrame[] {
    if (this.reconcileBufferedFramesMode !== 'batch-on-reconcile') return []
    return this.drainBufferedFrames(this.channels, undefined, 'reconcile')
  }

  stageReconcileBatch(): ReconcileBatch {
    const reconcileFrame = this.buildReconcileFrame()
    const movedBufferedFrames = this.drainBufferedFramesForReconcile()
    return { reconcileFrame, movedBufferedFrames }
  }

  private sendReconcileBatch(reconcileBatch: ReconcileBatch): void {
    this.transport.sendFrame(reconcileBatch.reconcileFrame)
    for (const frame of reconcileBatch.movedBufferedFrames) this.transport.sendFrame(frame)
  }

  private appendReconcileBatch(target: OutboundFrame[], reconcileBatch: ReconcileBatch): void {
    target.push(reconcileBatch.reconcileFrame)
    for (const frame of reconcileBatch.movedBufferedFrames) target.push(frame)
  }

  private applyReconciled(ctrl: CtrlReconciled): ReconcileOutcome {
    this.sessionId = ctrl.sessionId
    if (ctrl.reconnectTimeout) this.reconnectTimeoutMs = ctrl.reconnectTimeout
    if (ctrl.idleTimeout) this.idleTimeoutMs = ctrl.idleTimeout
    if (ctrl.clientReplayBuffer) this.clientReplayBufferBytes = ctrl.clientReplayBuffer
    if (ctrl.pingInterval) {
      this.pingIntervalMs = ctrl.pingInterval
      this.pongTimeoutMs = ctrl.pingInterval * 2
    }

    const serverMap = new Map<number, number>()
    for (const channel of ctrl.open) {
      serverMap.set(channel.ix, channel.lastSeq)
    }
    const reconcileIxes = this.reconcileIxes
    this.reconcileIxes = new Set()
    const releaseFrames: OutboundFrame[] = []
    const channelsToOpen: MuxChannel[] = []
    let hasNewChannels = false

    for (const [ix, channel] of this.channels) {
      if (!reconcileIxes.has(ix)) {
        if (!serverMap.has(ix)) hasNewChannels = true
        continue
      }
      if (!serverMap.has(ix)) {
        const err = new ChannelNetworkError('Channel not acknowledged by server after reconnect')
        this.releaseChannel(ix, channel, err)
        channel._onTransportClose(err)
        continue
      }
      const replay = this.replayBuffers.get(ix)
      if (replay)
        for (const frame of replay.getAfter(serverMap.get(ix)!))
          releaseFrames.push({ kind: 'reconcile-release', frame })
      if (!channel.isClosed) channelsToOpen.push(channel)
    }

    for (const frame of this.drainBufferedFrames(serverMap, this.channels, 'reconcile-release'))
      releaseFrames.push(frame)

    if (hasNewChannels) {
      const reconcileBatch = this.stageReconcileBatch()
      this.appendReconcileBatch(releaseFrames, reconcileBatch)
    } else {
      this.reconciling = false
    }

    return { frames: releaseFrames, channelsToOpen, reconcileComplete: !hasNewChannels }
  }

  private closeRemoteChannel(ix: number, err?: Error): void {
    const channel = this.channels.get(ix)
    if (!channel) return
    this.releaseChannel(ix, channel, err ?? new ChannelClosedError())
    channel._onTransportClose(err)
  }

  private handleAckRes(index: number, ackedSeq: number, text: string, status: AckResultStatus = 'ok'): void {
    const key = `${index}:${ackedSeq}`
    const pending = this.pendingAcks.get(key)
    if (!pending) return
    this.pendingAcks.delete(key)
    switch (status) {
      case 'ok':
        pending.resolve(parse(text))
        return
      case 'abort':
        pending.reject(makeAbortError(parse(text)))
        return
      case 'error':
        pending.reject(makeBugError(text || undefined))
    }
  }

  private handleDataFrame(frame: { tag: number; index: number; seq: number; text?: string; data?: Uint8Array }): void {
    if (frame.seq && !this.trackSeq(frame.index, frame.seq)) return
    if (frame.tag === TAG.TEXT_ACK_REQ) {
      void this.channels.get(frame.index)?._onTransportAckReqMessage(frame.text!, frame.seq)
      return
    }
    if (frame.tag === TAG.TEXT) {
      this.channels.get(frame.index)?._onTransportMessage(frame.text!)
      return
    }
    if (frame.tag === TAG.BINARY) this.channels.get(frame.index)?._onTransportBinaryMessage(frame.data!)
  }

  private closeAll(err: Error): void {
    for (const [ix, channel] of this.channels) {
      this.clearPendingAcks(ix, err)
      channel._onTransportClose(err)
    }
    this.dispose()
  }

  private trackSeq(ix: number, seq: number): boolean {
    const prev = this.lastSeqByChannel.get(ix) ?? 0
    if (seq <= prev) return false
    this.lastSeqByChannel.set(ix, seq)
    return true
  }

  private drainBufferedFrames(
    releasableChannels: Set<number> | Map<number, unknown>,
    retainedChannels: Set<number> | Map<number, unknown> | undefined,
    kind: 'reconcile' | 'reconcile-release',
  ): OutboundFrame[] {
    const frames: OutboundFrame[] = []
    const sendBuffer = this.sendBuffer
    let writeIx = 0
    for (let readIx = 0; readIx < sendBuffer.length; readIx++) {
      const entry = sendBuffer[readIx]!
      const frame = entry.frame
      const channelIx = entry.channelIx
      const seq = entry.seq
      if (!releasableChannels.has(channelIx)) {
        if (retainedChannels?.has(channelIx)) sendBuffer[writeIx++] = entry
        continue
      }
      if (seq !== undefined) this.replayBuffers.get(channelIx)?.push(seq, frame)
      frames.push({ kind, frame })
    }
    sendBuffer.length = writeIx
    return frames
  }

  private clearPendingAcks(ix: number, err: Error): void {
    const prefix = `${ix}:`
    for (const [key, pending] of this.pendingAcks) {
      if (!key.startsWith(prefix)) continue
      this.pendingAcks.delete(key)
      pending.reject(err)
    }
  }

  private releaseChannel(ix: number, channel: MuxChannel, err: Error): void {
    this.channels.delete(ix)
    this.channelIndex.delete(channel)
    this.lastSeqByChannel.delete(ix)
    const replayBuffer = this.replayBuffers.get(ix)
    replayBuffer?.dispose()
    this.replayBuffers.delete(ix)
    this.clearPendingAcks(ix, err)
  }
}

class WsTransport implements ClientChannelTransport {
  readonly type = CHANNEL_TRANSPORT.WS
  readonly reconnectTimeoutMessage = 'WebSocket reconnect timed out'
  readonly supportsPauseResume = true
  readonly sendReconcileOnOpen = true
  readonly reconcileMode = 'release-after-reconciled' as const
  private probedWs: WebSocket | null = null
  private ws: WebSocket | null = null
  private abandonedWs: WebSocket | null = null
  private connecting = false
  private everOpened = false

  private readonly wsUrl: string

  constructor(
    telefuncUrl: string,
    private readonly owner: ClientConnection,
  ) {
    const base = typeof window === 'undefined' ? undefined : window.location.href
    const url = new URL(telefuncUrl, base)
    url.protocol = url.protocol === 'https:' ? 'wss:' : 'ws:'
    this.wsUrl = url.href
  }

  async probe(): Promise<(() => void) | null> {
    const ws = await new Promise<WebSocket | null>((resolve) => {
      let ws: WebSocket
      try {
        ws = new WebSocket(this.wsUrl)
      } catch {
        resolve(null)
        return
      }
      ws.binaryType = 'arraybuffer'
      const timer = setTimeout(() => {
        ws.onopen = ws.onmessage = ws.onclose = ws.onerror = null
        ws.close()
        resolve(null)
      }, WS_PROBE_TIMEOUT_MS)
      ws.onopen = () => ws.send(encodeCtrl({ t: 'ping' }))
      ws.onmessage = ({ data }: MessageEvent) => {
        const frame = decode(new Uint8Array(data as ArrayBuffer))
        if (frame.tag === TAG.CTRL && frame.ctrl.t === 'pong') {
          clearTimeout(timer)
          ws.onopen = ws.onmessage = ws.onclose = ws.onerror = null
          resolve(ws)
        }
      }
      ws.onclose = () => {
        clearTimeout(timer)
        resolve(null)
      }
      ws.onerror = () => {}
    })
    if (!ws) return null
    this.probedWs = ws
    return () => {
      if (this.probedWs === ws) this.probedWs = null
      try {
        ws.close()
      } catch {}
    }
  }

  prepareForUpgrade(): Promise<void> {
    return Promise.resolve()
  }

  start(): void {
    if (this.connecting || this.ws) return

    const wsProbed = this.probedWs
    if (wsProbed) {
      this.probedWs = null
      this.ws = wsProbed
      this.setupHandlers(wsProbed)
      this.handleOpen(wsProbed)
      return
    }

    this.connecting = true

    let ws: WebSocket
    try {
      ws = new WebSocket(this.wsUrl)
    } catch {
      this.connecting = false
      this.owner._onTransportClosed(this, false)
      return
    }

    this.ws = ws
    ws.binaryType = 'arraybuffer'

    ws.onopen = () => {
      if (this.ws !== ws) return
      this.handleOpen(ws)
    }

    this.setupHandlers(ws)
  }

  private handleOpen(ws: WebSocket): void {
    if (this.ws !== ws) return
    this.everOpened = true
    this.connecting = false
    this.owner._onTransportOpen(this)
  }

  private setupHandlers(ws: WebSocket): void {
    ws.onmessage = ({ data }: MessageEvent) => {
      this.owner._onTransportFrame(this, new Uint8Array(data as ArrayBuffer))
    }
    ws.onclose = () => {
      if (this.ws === ws) this.ws = null
      this.connecting = false
      this.owner._onTransportClosed(this, !this.everOpened)
    }
    ws.onerror = () => {}
  }

  hasActiveTransport(): boolean {
    return this.ws !== null
  }

  isConnecting(): boolean {
    return this.connecting
  }

  sendFrame(frame: OutboundFrame): void {
    const ws = this.ws
    assert(ws)
    ws.send(frame.frame)
  }

  abandonActiveTransport(): void {
    const ws = this.ws
    if (!ws) return
    this.ws = null
    this.closeAbandonedTransport()
    this.abandonedWs = ws
    ws.onopen = ws.onerror = ws.onclose = null
    ws.onmessage = ({ data }: MessageEvent) => {
      const frame = decode(new Uint8Array(data as ArrayBuffer))
      if (frame.tag === TAG.TEXT || frame.tag === TAG.BINARY) {
        this.owner._onTransportFrame(this, new Uint8Array(data as ArrayBuffer))
      }
    }
  }

  closeAbandonedTransport(): void {
    const ws = this.abandonedWs
    if (!ws) return
    this.abandonedWs = null
    ws.onmessage = ws.onclose = null
    try {
      ws.close()
    } catch {}
  }

  applyReconciledSettings(): void {}

  dispose(): void {
    this.connecting = false
    const wsProbed = this.probedWs
    this.probedWs = null
    if (wsProbed) {
      wsProbed.onopen = wsProbed.onmessage = wsProbed.onerror = wsProbed.onclose = null
      try {
        wsProbed.close(1000)
      } catch {}
    }
    const ws = this.ws
    this.ws = null
    if (ws) {
      ws.onopen = ws.onmessage = ws.onerror = ws.onclose = null
      try {
        ws.close(1000)
      } catch {}
    }
    this.closeAbandonedTransport()
  }
}

class SseTransport implements ClientChannelTransport {
  readonly type = CHANNEL_TRANSPORT.SSE
  readonly reconnectTimeoutMessage = 'SSE reconnect timed out'
  readonly supportsPauseResume = false
  readonly sendReconcileOnOpen = false
  readonly reconcileMode = 'batch-on-reconcile' as const
  async probe(): Promise<(() => void) | null> {
    throw new Error('SSE transport does not implement probe()')
  }

  private readonly connId = crypto.randomUUID()
  private connecting = false
  private startTimer: ReturnType<typeof setTimeout> | null = null
  private streamAbort: AbortController | null = null
  private abandonedStream: AbortController | null = null
  private readonly abandonedControllers = new WeakSet<AbortController>()
  private outboxFrames: Uint8Array<ArrayBuffer>[] = []
  private outboxDeadlines: number[] = []
  private readonly flushScheduler = new DeadlineScheduler(() => {
    void this.flushOutbox()
  })
  private flushing = false
  private lastPostStartedAt = 0
  private flushThrottleMs = SSE_FLUSH_THROTTLE_MS
  private postIdleFlushDelayMs = SSE_POST_IDLE_FLUSH_DELAY_MS
  private heartbeatFlushDelayMs = Math.floor(CHANNEL_PING_INTERVAL_MS / 2)
  private drainCallbacks: Array<() => void> = []

  constructor(
    private readonly telefuncUrl: string,
    fetchImpl: typeof fetch,
    private readonly owner: ClientConnection,
  ) {
    this.fetchImpl = fetchImpl.bind(globalThis)
  }

  private readonly fetchImpl: typeof fetch

  prepareForUpgrade(): Promise<void> {
    if (!this.flushing && this.outboxFrames.length === 0) return Promise.resolve()
    return new Promise<void>((resolve) => {
      this.drainCallbacks.push(resolve)
    })
  }

  start(): void {
    if (this.connecting || this.streamAbort) return
    this.connecting = true
    // Match WS batching behavior: wait one reconcile window so startup code can
    // register channels and queue payload before we build the initial SSE batch.
    this.startTimer = setTimeout(() => {
      this.startTimer = null
      if (!this.connecting || this.streamAbort) return
      void this.openStream()
    }, SSE_RECONCILE_DEADLINE_MS)
  }

  hasActiveTransport(): boolean {
    return this.streamAbort !== null
  }

  isConnecting(): boolean {
    return this.connecting
  }

  sendFrame(frame: OutboundFrame): void {
    const now = Date.now()
    const deadlineAt = this.getFrameDeadline(frame.kind, now)
    this.outboxFrames.push(frame.frame)
    this.outboxDeadlines.push(deadlineAt)
    this.scheduleFlush()
    if (deadlineAt <= now) void this.flushOutbox()
  }

  private async openStream(): Promise<void> {
    const abortController = new AbortController()
    this.streamAbort = abortController
    const stage = this.stageInitialBatch()
    let response: Response
    try {
      response = await this.fetchImpl(getMarkedRequestUrl(this.telefuncUrl, REQUEST_KIND.SSE), {
        method: 'POST',
        headers: {
          Accept: 'text/event-stream',
          'Content-Type': 'application/octet-stream',
          [REQUEST_KIND_HEADER]: REQUEST_KIND.SSE,
        },
        body: encodeSseRequest({
          connId: this.connId,
          stream: true,
          batch: encodeLengthPrefixedFrames(stage.initialFrames, (entry) => entry.frame),
        }),
        signal: abortController.signal,
      })
    } catch {
      this.rollbackInitialBatch(stage)
      if (this.streamAbort === abortController) this.streamAbort = null
      this.connecting = false
      this.owner._onTransportClosed(this, false)
      return
    }

    if (!response.ok || !response.body) {
      this.rollbackInitialBatch(stage)
      if (this.streamAbort === abortController) this.streamAbort = null
      this.connecting = false
      this.owner._onTransportClosed(this, true)
      return
    }

    this.connecting = false
    this.streamAbort = abortController
    this.owner._onTransportOpen(this)
    if (this.outboxFrames.length > 0) void this.flushOutbox()

    const reader = createSseEventStreamReader(response.body.getReader(), abortController)
    try {
      while (true) {
        const entry = await reader.readNextEntry()
        if (!entry) break
        if (entry.frame) this.owner._onTransportFrame(this, entry.frame)
      }
    } catch {
      if (abortController.signal.aborted) return
    } finally {
      reader.cancel()
      if (this.streamAbort === abortController) this.streamAbort = null
      if (!this.abandonedControllers.has(abortController)) this.owner._onTransportClosed(this, false)
    }
  }

  private stageInitialBatch(): SseInitialBatchStage {
    const reconcileBatch = this.owner.stageReconcileBatch()
    const initialFrames: OutboundFrame[] = []
    initialFrames.push(reconcileBatch.reconcileFrame)
    for (const frame of reconcileBatch.movedBufferedFrames) initialFrames.push(frame)
    const movedBufferedFrames = reconcileBatch.movedBufferedFrames
    const movedOutboxFrames = this.outboxFrames
    const movedOutboxDeadlines = this.outboxDeadlines
    this.outboxFrames = []
    this.outboxDeadlines = []
    for (const frame of movedOutboxFrames) initialFrames.push({ kind: 'data', frame })
    return { initialFrames, movedOutboxFrames, movedOutboxDeadlines, movedBufferedFrames }
  }

  private rollbackInitialBatch(stage: SseInitialBatchStage): void {
    if (stage.movedOutboxFrames.length === 0 && stage.movedBufferedFrames.length === 0) return
    const now = Date.now()
    const movedBufferedOutboxFrames = stage.movedBufferedFrames.map((entry) => entry.frame)
    const movedBufferedDeadlines = stage.movedBufferedFrames.map((entry) => this.getFrameDeadline(entry.kind, now))
    this.outboxFrames = stage.movedOutboxFrames.concat(movedBufferedOutboxFrames, this.outboxFrames)
    this.outboxDeadlines = stage.movedOutboxDeadlines.concat(movedBufferedDeadlines, this.outboxDeadlines)
  }

  private async flushOutbox(): Promise<void> {
    if (!this.hasActiveTransport() || this.flushing || this.outboxFrames.length === 0) return
    this.flushScheduler.cancel()
    this.flushing = true
    try {
      const now = Date.now()
      const queuedFrames = this.outboxFrames.splice(0, this.outboxFrames.length)
      const queuedDeadlines = this.outboxDeadlines.splice(0, this.outboxDeadlines.length)
      this.lastPostStartedAt = now

      try {
        const response = await this.fetchImpl(getMarkedRequestUrl(this.telefuncUrl, REQUEST_KIND.SSE), {
          method: 'POST',
          headers: {
            'Content-Type': 'application/octet-stream',
            [REQUEST_KIND_HEADER]: REQUEST_KIND.SSE,
          },
          body: encodeSseRequest({ connId: this.connId, batch: encodeLengthPrefixedFrames(queuedFrames) }),
        })
        if (!response.ok) throw new Error('POST failed')
      } catch {
        this.outboxFrames = queuedFrames.concat(this.outboxFrames)
        this.outboxDeadlines = queuedDeadlines.concat(this.outboxDeadlines)
        this.abandonActiveTransport()
        this.owner._onTransportClosed(this, false)
        return
      }
    } finally {
      this.flushing = false
      if (this.outboxFrames.length > 0) {
        this.scheduleFlush()
      } else {
        const cbs = this.drainCallbacks.splice(0)
        for (const cb of cbs) cb()
      }
    }
  }

  private scheduleFlush(): void {
    if (this.outboxFrames.length === 0 || !this.hasActiveTransport()) return
    let earliest = Infinity
    for (const deadlineAt of this.outboxDeadlines) if (deadlineAt < earliest) earliest = deadlineAt
    this.flushScheduler.schedule(earliest)
  }

  private getFrameDeadline(kind: OutboundFrameKind, now = Date.now()): number {
    switch (kind) {
      case 'reconcile':
      case 'reconcile-release':
        return now + SSE_RECONCILE_DEADLINE_MS
      case 'control':
        return now
      case 'heartbeat':
        return now + this.heartbeatFlushDelayMs
      case 'ack':
      case 'data':
        return (
          now +
          (now - this.lastPostStartedAt >= this.flushThrottleMs ? this.postIdleFlushDelayMs : this.flushThrottleMs)
        )
    }
  }

  abandonActiveTransport(): void {
    const abortController = this.streamAbort
    if (!abortController) return
    this.streamAbort = null
    this.closeAbandonedTransport()
    this.abandonedStream = abortController
    this.abandonedControllers.add(abortController)
    const cbs = this.drainCallbacks.splice(0)
    for (const cb of cbs) cb()
  }

  closeAbandonedTransport(): void {
    const abortController = this.abandonedStream
    if (!abortController) return
    this.abandonedStream = null
    abortController.abort()
  }

  applyReconciledSettings(ctrl: CtrlSettings): void {
    if (ctrl.sseFlushThrottle) this.flushThrottleMs = ctrl.sseFlushThrottle
    if (ctrl.ssePostIdleFlushDelay) this.postIdleFlushDelayMs = ctrl.ssePostIdleFlushDelay
    this.heartbeatFlushDelayMs = Math.floor(ctrl.pingInterval / 2)
  }

  dispose(): void {
    this.connecting = false
    if (this.startTimer) {
      clearTimeout(this.startTimer)
      this.startTimer = null
    }
    this.flushScheduler.cancel()
    this.outboxFrames = []
    this.outboxDeadlines = []
    this.streamAbort?.abort()
    this.streamAbort = null
    this.closeAbandonedTransport()
    const cbs = this.drainCallbacks.splice(0)
    for (const cb of cbs) cb()
  }
}

// ── Transport registry ──

/** Maps each ChannelTransport to a factory that creates the corresponding ClientChannelTransport. */
const TRANSPORT_REGISTRY: Record<
  ChannelTransport,
  (telefuncUrl: string, options: ClientConnectionOptions, owner: ClientConnection) => ClientChannelTransport
> = {
  [CHANNEL_TRANSPORT.WS]: (telefuncUrl, _options, owner) => new WsTransport(telefuncUrl, owner),
  [CHANNEL_TRANSPORT.SSE]: (telefuncUrl, options, owner) => new SseTransport(telefuncUrl, options.fetchImpl, owner),
}

/** Defines which transport can upgrade to which. */
const UPGRADE_PATH: Partial<Record<ChannelTransport, ChannelTransport>> = {
  [CHANNEL_TRANSPORT.SSE]: CHANNEL_TRANSPORT.WS,
}

function createSseEventStreamReader(
  reader: ReadableStreamDefaultReader<Uint8Array<ArrayBuffer>>,
  abortController: AbortController,
): {
  cancel: () => void
  readNextEntry: () => Promise<{ comment?: string; frame?: Uint8Array<ArrayBuffer> } | null>
} {
  const decoder = new TextDecoder()
  let lineBuf = ''
  let pendingComment: string | null = null
  let pendingData = ''
  let readyComment: string | null = null
  let readyFrame: Uint8Array<ArrayBuffer> | null = null
  let cancelled = false

  const cancel = () => {
    if (cancelled) return
    cancelled = true
    reader.cancel().catch(() => {})
  }

  abortController.signal.addEventListener('abort', cancel, { once: true })

  const processBufferedLines = () => {
    const lines = lineBuf.split('\n')
    if (lines.length === 1) return
    lineBuf = lines.pop()!

    for (let index = 0; index < lines.length; index++) {
      const line = lines[index]!
      if (line.startsWith(':')) {
        pendingComment = line
        continue
      }
      if (line.startsWith('data: ')) {
        pendingData = line.slice(6)
        continue
      }
      if (line !== '') continue

      if (pendingComment !== null) {
        readyComment = pendingComment
        pendingComment = null
      }
      if (pendingData !== '') {
        readyFrame = base64urlToUint8Array(pendingData)
        pendingData = ''
      }
      if (readyComment === null && readyFrame === null) continue

      const remainingLines = lines.slice(index + 1)
      if (remainingLines.length > 0) {
        lineBuf = `${remainingLines.join('\n')}\n${lineBuf}`
      }
      return
    }
  }

  const readNextEntry = async (): Promise<{ comment?: string; frame?: Uint8Array<ArrayBuffer> } | null> => {
    while (true) {
      if (readyComment !== null) {
        const comment = readyComment
        readyComment = null
        return { comment }
      }
      if (readyFrame !== null) {
        const frame = readyFrame
        readyFrame = null
        return { frame }
      }

      processBufferedLines()
      if (readyComment !== null || readyFrame !== null) continue

      let done: boolean
      let value: Uint8Array<ArrayBuffer> | undefined
      let readError: unknown
      try {
        ;({ done, value } = await reader.read())
      } catch (err) {
        readError = err
        done = true
      }
      if (done) {
        if (abortController.signal.aborted || cancelled) return null
        throw readError ?? new Error('Connection lost before all SSE frames were received.')
      }
      lineBuf += decoder.decode(value!, { stream: true })
    }
  }

  return { cancel, readNextEntry }
}
