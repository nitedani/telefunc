export { ServerConnection, ProtocolViolationError }
export type { ServerTransport }

import { unrefTimer } from '../../utils/unrefTimer.js'
import { getGlobalObject } from '../../utils/getGlobalObject.js'
import { getServerConfig } from '../../node/server/serverConfig.js'
import type { ServerChannel } from './channel.js'
import { getChannelRegistry, onChannelCreated, setChannelDefaults } from './channel.js'
import { ReplayBuffer } from '../replay-buffer.js'
import { IndexedPeer } from './IndexedPeer.js'
import type { PeerSender } from './IndexedPeer.js'
import { TAG, decode, encodeCtrl } from '../shared-ws.js'
import type { CtrlMessage, CtrlReconcile, CtrlSettings, CtrlUpgrade } from '../shared-ws.js'
import { CHANNEL_PING_INTERVAL_MIN_MS, type ChannelTransports } from '../constants.js'

type DecodedFrame = ReturnType<typeof decode>

type MuxServerOptions = {
  reconnectTimeout: number
  idleTimeout: number
  pingInterval: number
  pingDeadline: number
  serverReplayBuffer: number
  clientReplayBuffer: number
  connectTtl: number
  bufferLimit: number
  sseFlushThrottle: number
  ssePostIdleFlushDelay: number
  replayMaxAge: number
  transports: ChannelTransports
}

type ChannelEntry = { channel: ServerChannel; lastClientSeq: number; replay: ReplayBuffer }
type PendingUpgrade = {
  oldConnection: unknown
  newConnection: unknown
  /** Rebind all channel peers so future server->client traffic leaves on the new transport. */
  activateOutboundTransport: () => void
  newSendFin: () => void | Promise<void>
  outboundActivated: boolean
}
type SessionState = {
  ixMap: Map<number, ChannelEntry>
  /** Sends a fin ctrl frame on the current transport. Set at end of each reconcile. */
  sendFin: (() => void | Promise<void>) | null
  /** Connection that currently owns server→client traffic for this session. */
  sendConnection: unknown | null
  /** Connection that currently owns client→server traffic for this session. */
  receiveConnection: unknown | null
  /** Pending asymmetric handoff: server->client may already move before client->server does. */
  pendingUpgrade: PendingUpgrade | null
  /** Old transport close to ignore after a successful fin-driven handoff. */
  ignoredCloseConnection: unknown | null
}
type ReconcileResult = { sessionId: string }

const globalObject = getGlobalObject('wire-protocol/server/connection.ts', {
  sessionStates: new Map<string, SessionState>(),
  transportChannels: new Map<string, ChannelEntry>(),
})

class ProtocolViolationError extends Error {}

function resolveMuxServerOptions(): MuxServerOptions {
  const channelConfig = getServerConfig().channel
  const reconnectTimeout = channelConfig.reconnectTimeout
  const idleTimeout = channelConfig.idleTimeout
  const pingInterval = Math.max(channelConfig.pingInterval, CHANNEL_PING_INTERVAL_MIN_MS)
  const pingDeadline = pingInterval * 2
  const serverReplayBuffer = channelConfig.serverReplayBuffer
  const clientReplayBuffer = channelConfig.clientReplayBuffer
  const connectTtl = channelConfig.connectTtl
  const bufferLimit = channelConfig.bufferLimit
  const sseFlushThrottle = channelConfig.sseFlushThrottle
  const ssePostIdleFlushDelay = channelConfig.ssePostIdleFlushDelay
  const transports = channelConfig.transports
  return {
    reconnectTimeout,
    idleTimeout,
    pingInterval,
    pingDeadline,
    serverReplayBuffer,
    clientReplayBuffer,
    connectTtl,
    bufferLimit,
    sseFlushThrottle,
    ssePostIdleFlushDelay,
    replayMaxAge: pingDeadline + reconnectTimeout + 1_000,
    transports,
  }
}

type ConnectionState = {
  pingTimer: ReturnType<typeof setTimeout> | null
  terminatePermanently: boolean | null
  reconciling: boolean
  sendChain: Promise<void> | null
}

type RequestedOpen = { id: string; ix: number; defer?: boolean }

type ServerTransport<TConnection> = {
  getSessionId(connection: TConnection): string | undefined
  setSessionId(connection: TConnection, sessionId: string): void
  sendNow(connection: TConnection, frame: Uint8Array<ArrayBuffer>): void | Promise<void>
  terminateConnection(connection: TConnection): void
}

class ServerConnection<TConnection> {
  private readonly options: MuxServerOptions
  private readonly transport: ServerTransport<TConnection>
  private readonly sessionStates = globalObject.sessionStates
  private readonly transportChannels = globalObject.transportChannels
  private readonly connectionStates = new Map<TConnection, ConnectionState>()

  constructor(transport: ServerTransport<TConnection>) {
    this.transport = transport
    const resolvedOptions = resolveMuxServerOptions()
    this.options = resolvedOptions
    setChannelDefaults({
      connectTtl: resolvedOptions.connectTtl,
      bufferLimit: resolvedOptions.bufferLimit,
    })
  }

  onConnectionOpen(connection: TConnection): void {
    this.getOrCreateConnectionState(connection)
    this.resetPingTimer(connection)
  }

  async onConnectionRawMessage(connection: TConnection, rawFrame: Uint8Array<ArrayBuffer>): Promise<void> {
    const state = this.getOrCreateConnectionState(connection)
    try {
      const pending = this.handleFrame(connection, decode(rawFrame), false)
      if (pending) await pending
    } catch {
      state.terminatePermanently = true
      this.transport.terminateConnection(connection)
    }
  }

  async onConnectionRawMessageDeferredReconciled(
    connection: TConnection,
    rawFrame: Uint8Array<ArrayBuffer>,
  ): Promise<string | null> {
    const state = this.getOrCreateConnectionState(connection)
    try {
      const result = this.handleFrame(connection, decode(rawFrame), true)
      if (result) return await result
      return result
    } catch {
      state.terminatePermanently = true
      this.transport.terminateConnection(connection)
      return null
    }
  }

  onConnectionClosed(connection: TConnection, isPermanent: boolean): void {
    this.clearPingTimer(connection)
    this.connectionStates.delete(connection)
    this.handleConnectionClose(connection, isPermanent)
  }

  consumePermanentTermination(connection: TConnection): boolean | null {
    return this.getOrCreateConnectionState(connection).terminatePermanently
  }

  /** Connection-scoped outbound send gate.
   *
   *  Every server-to-client frame for one transport connection flows through here, whether it
   *  originates from mux control handling (`reconcile` replay, `reconciled`, `pong`) or from a
   *  `ServerChannel` via `IndexedPeer` and the `PeerSender` closure created during reconcile.
   *
   *  Its job is to preserve strict wire order across the whole connection while still keeping an
   *  idle fast path cheap:
   *  - if no prior send is in flight, it calls `_sendNow()` immediately;
   *  - if `_sendNow()` completes synchronously, this method returns `void` and the connection
   *    stays idle;
   *  - if `_sendNow()` returns a promise, that promise becomes the connection's active send chain;
   *  - while a send chain exists, later sends are appended behind it and therefore cannot overtake
   *    earlier frames on the wire.
   *
   *  This method knows nothing about per-channel pause/disconnect state; that is handled earlier
   *  by `ServerChannel._sendBinaryAwaitable()`. That is also where ws-side flow control is applied:
   *  client pause/resume signals toggle per-channel sendability before a frame reaches this layer.
   *
   *  `_sendNow()` is the transport-specific leaf:
   *  - ws sends synchronously once the channel has already been deemed sendable;
   *  - sse may return a promise here when stream backpressure requires waiting.
   */
  protected send(connection: TConnection, frame: Uint8Array<ArrayBuffer>, onCommit?: () => void): void | Promise<void> {
    const state = this.getOrCreateConnectionState(connection)
    if (!state.sendChain) {
      onCommit?.()
      const pending = this.transport.sendNow(connection, frame)
      if (!pending) return
      const chain = pending.finally(() => {
        if (state.sendChain === chain) state.sendChain = null
      })
      state.sendChain = chain
      return chain
    }
    const chain = state.sendChain
      .then(() => {
        onCommit?.()
        return this.transport.sendNow(connection, frame)
      })
      .finally(() => {
        if (state.sendChain === chain) state.sendChain = null
      })
    state.sendChain = chain
    return chain
  }

  handleConnectionClose(connection: TConnection, permanent: boolean): void {
    const sessionId = this.transport.getSessionId(connection)
    if (!sessionId) return
    const sessionState = this.sessionStates.get(sessionId)
    if (!sessionState) return

    if (sessionState.ignoredCloseConnection === connection) {
      sessionState.ignoredCloseConnection = null
      return
    }

    if (sessionState.pendingUpgrade?.oldConnection === connection) {
      this.activatePendingUpgradeOutbound(sessionState, sessionState.pendingUpgrade)
      this.activatePendingUpgradeInbound(sessionState, sessionState.pendingUpgrade, null)
      return
    }

    if (sessionState.pendingUpgrade?.newConnection === connection) {
      if (sessionState.sendConnection === connection || sessionState.receiveConnection === connection) {
        sessionState.sendFin = null
        sessionState.sendConnection = null
        sessionState.receiveConnection = null
        sessionState.pendingUpgrade = null
        sessionState.ignoredCloseConnection = null
        if (permanent) {
          for (const entry of sessionState.ixMap.values()) {
            if (!entry.channel._didShutdown) entry.channel._onPeerClose()
          }
          sessionState.ixMap.clear()
          this.sessionStates.delete(sessionId)
          return
        }
        for (const entry of sessionState.ixMap.values()) {
          entry.channel._onPeerDisconnect(this.options.reconnectTimeout)
        }
        return
      }
      sessionState.pendingUpgrade = null
      return
    }

    sessionState.sendFin = null
    sessionState.sendConnection = null
    sessionState.receiveConnection = null
    sessionState.pendingUpgrade = null
    sessionState.ignoredCloseConnection = null

    if (permanent) {
      for (const entry of sessionState.ixMap.values()) {
        if (!entry.channel._didShutdown) entry.channel._onPeerClose()
      }
      sessionState.ixMap.clear()
      this.sessionStates.delete(sessionId)
      return
    }

    for (const entry of sessionState.ixMap.values()) {
      entry.channel._onPeerDisconnect(this.options.reconnectTimeout)
    }
  }

  private async handleCtrl(
    connection: TConnection,
    ctrl: CtrlMessage,
    deferReconciled: boolean,
  ): Promise<string | null> {
    if (ctrl.t === 'reconcile') {
      const { sessionId } = await this.reconcile(connection, ctrl)
      if (!deferReconciled) {
        this.sendReconciled(connection, sessionId)
        return null
      }
      return sessionId
    }
    if (ctrl.t === 'upgrade') {
      this.upgrade(connection, ctrl)
      this.sendUpgraded(connection, ctrl.sessionId)
      return null
    }
    if (ctrl.t === 'fin') {
      const sessionState = this.getSessionStateOrThrow(this.transport.getSessionId(connection))
      const pendingUpgrade = sessionState.pendingUpgrade
      if (!pendingUpgrade || pendingUpgrade.oldConnection !== connection) return null
      this.activatePendingUpgradeInbound(sessionState, pendingUpgrade, connection)
      return null
    }
    if (ctrl.t === 'ping') {
      this.resetPingTimer(connection)
      this.send(connection, encodeCtrl({ t: 'pong' }))
      return null
    }
    const sessionState = this.getSessionStateOrThrow(this.transport.getSessionId(connection))
    if (sessionState.receiveConnection !== connection) return null
    switch (ctrl.t) {
      case 'close': {
        const entry = sessionState.ixMap.get(ctrl.ix)
        if (!entry) return null
        entry.channel._onPeerCloseRequest(ctrl.timeoutMs)
        return null
      }
      case 'close-ack': {
        const entry = sessionState.ixMap.get(ctrl.ix)
        if (!entry) return null
        entry.channel._onPeerCloseAck()
        return null
      }
      case 'pause': {
        const entry = sessionState.ixMap.get(ctrl.ix)
        if (entry) entry.channel._onPeerPause()
        return null
      }
      case 'resume': {
        const entry = sessionState.ixMap.get(ctrl.ix)
        if (entry) entry.channel._onPeerResume()
        return null
      }
    }
    return null
  }

  private async reconcile(connection: TConnection, ctrl: CtrlReconcile): Promise<ReconcileResult> {
    const state = this.getOrCreateConnectionState(connection)
    const prevSessionId = ctrl.sessionId
    const sessionState = prevSessionId
      ? this.getSessionStateOrThrow(prevSessionId)
      : {
          ixMap: new Map<number, ChannelEntry>(),
          sendFin: null,
          sendConnection: null,
          receiveConnection: null,
          pendingUpgrade: null,
          ignoredCloseConnection: null,
        }
    state.reconciling = true
    this.resetPingTimer(connection)
    const prevIxMap = new Map(sessionState.ixMap)
    sessionState.ixMap.clear()

    await this.awaitDeferredChannels(ctrl.open)
    const { openedIxs } = await this.openSessionChannels(connection, sessionState, ctrl.open, {
      replayAfter: ({ lastSeq }) => lastSeq,
      deferAttach: false,
    })

    for (const [ix, entry] of prevIxMap) {
      if (openedIxs.has(ix)) continue
      if (!entry.channel._didShutdown) entry.channel._onPeerRecoveryFailure()
    }

    if (prevSessionId) this.sessionStates.delete(prevSessionId)
    const sessionId = crypto.randomUUID()
    this.sessionStates.set(sessionId, sessionState)
    this.transport.setSessionId(connection, sessionId)
    const newSendFin = () => this.send(connection, encodeCtrl({ t: 'fin' }))
    sessionState.sendFin = newSendFin
    sessionState.sendConnection = connection
    sessionState.receiveConnection = connection
    sessionState.pendingUpgrade = null
    state.reconciling = false
    this.resetPingTimer(connection)
    return { sessionId }
  }

  private upgrade(connection: TConnection, ctrl: CtrlUpgrade): void {
    const sessionState = this.getSessionStateOrThrow(ctrl.sessionId)
    if (sessionState.pendingUpgrade) throw new ProtocolViolationError()
    if (!sessionState.sendConnection) throw new ProtocolViolationError()

    this.transport.setSessionId(connection, ctrl.sessionId)
    const sender: PeerSender = {
      send: (frame, onCommit) => this.send(connection, frame as Uint8Array<ArrayBuffer>, onCommit),
    }
    const newSendFin = () => this.send(connection, encodeCtrl({ t: 'fin' }))

    sessionState.pendingUpgrade = {
      oldConnection: sessionState.sendConnection,
      newConnection: connection,
      activateOutboundTransport: () => {
        for (const [ix, entry] of sessionState.ixMap) {
          entry.channel.attachPeer(new IndexedPeer(sender, ix, entry.replay))
        }
      },
      newSendFin,
      outboundActivated: false,
    }

    // Start the server->client half of the handoff eagerly. The client->server half
    // stays on the old transport until the client's final old-path fin flush resolves.
    const pendingUpgrade = sessionState.pendingUpgrade
    const finPending = sessionState.sendFin?.()
    if (finPending) {
      void finPending.then(() => {
        if (sessionState.pendingUpgrade !== pendingUpgrade) return
        this.activatePendingUpgradeOutbound(sessionState, pendingUpgrade)
      })
      return
    }
    this.activatePendingUpgradeOutbound(sessionState, pendingUpgrade)
  }

  private activatePendingUpgradeOutbound(sessionState: SessionState, pendingUpgrade: PendingUpgrade): void {
    if (pendingUpgrade.outboundActivated) return
    pendingUpgrade.outboundActivated = true
    pendingUpgrade.activateOutboundTransport()
    sessionState.sendFin = pendingUpgrade.newSendFin
    sessionState.sendConnection = pendingUpgrade.newConnection
  }

  private activatePendingUpgradeInbound(
    sessionState: SessionState,
    pendingUpgrade: PendingUpgrade,
    ignoredCloseConnection: unknown | null,
  ): void {
    sessionState.receiveConnection = pendingUpgrade.newConnection
    sessionState.pendingUpgrade = null
    sessionState.ignoredCloseConnection = ignoredCloseConnection
  }

  private async awaitDeferredChannels(open: readonly RequestedOpen[]): Promise<void> {
    const registry = getChannelRegistry()
    await Promise.all(
      open
        .filter(({ id, defer }) => defer && (!registry.get(id) || registry.get(id)!._didShutdown))
        .map(
          ({ id }) =>
            new Promise<void>((resolve) => {
              onChannelCreated(id, resolve)
              setTimeout(resolve, this.options.connectTtl)
            }),
        ),
    )
  }

  private async openSessionChannels<TRequestedOpen extends RequestedOpen>(
    connection: TConnection,
    sessionState: SessionState,
    open: readonly TRequestedOpen[],
    opts: {
      replayAfter: ((entry: TRequestedOpen) => number) | null
      deferAttach: boolean
    },
  ): Promise<{ openedIxs: Set<number>; activateOutboundTransport: Array<() => void> }> {
    const registry = getChannelRegistry()
    const sender: PeerSender = {
      send: (frame, onCommit) => this.send(connection, frame as Uint8Array<ArrayBuffer>, onCommit),
    }
    const openedIxs = new Set<number>()
    const activateOutboundTransport: Array<() => void> = []

    for (const requestedOpen of open) {
      const { id, ix } = requestedOpen
      openedIxs.add(ix)
      const channel = registry.get(id)
      if (!channel || channel._didShutdown) continue

      const {
        replay = new ReplayBuffer(this.options.serverReplayBuffer, this.options.replayMaxAge),
        lastClientSeq = 0,
      } = this.transportChannels.get(id) ?? {}
      const channelEntry: ChannelEntry = { channel, lastClientSeq, replay }

      const replayAfter = opts.replayAfter?.(requestedOpen)
      if (replayAfter !== undefined && replayAfter !== null) {
        for (const frame of replay.getAfter(replayAfter)) {
          const pending = this.send(connection, frame as Uint8Array<ArrayBuffer>)
          if (pending) await pending
        }
      }

      sessionState.ixMap.set(ix, channelEntry)
      this.transportChannels.set(id, channelEntry)
      channel._onShutdown(() => {
        this.transportChannels.delete(id)
        replay.dispose()
      })

      const attachPeer = () => channel.attachPeer(new IndexedPeer(sender, ix, replay))
      if (opts.deferAttach) {
        activateOutboundTransport.push(attachPeer)
      } else {
        attachPeer()
      }
    }

    return { openedIxs, activateOutboundTransport }
  }

  sendReconciled(connection: TConnection, sessionId: string): void | Promise<void> {
    const sessionState = this.getSessionStateOrThrow(sessionId)
    const open: CtrlReconcile['open'] = []
    for (const [ix, entry] of sessionState.ixMap) {
      open.push({ id: entry.channel.id, ix, lastSeq: entry.lastClientSeq })
    }
    return this.send(
      connection,
      encodeCtrl({
        t: 'reconciled',
        sessionId,
        open,
        ...this.getCtrlSettings(),
        transports: this.options.transports,
      }),
    )
  }

  sendUpgraded(connection: TConnection, sessionId: string): void | Promise<void> {
    return this.send(
      connection,
      encodeCtrl({
        t: 'upgraded',
        sessionId,
        ...this.getCtrlSettings(),
      }),
    )
  }

  private getCtrlSettings(): CtrlSettings {
    return {
      reconnectTimeout: this.options.reconnectTimeout,
      idleTimeout: this.options.idleTimeout,
      pingInterval: this.options.pingInterval,
      clientReplayBuffer: this.options.clientReplayBuffer,
      sseFlushThrottle: this.options.sseFlushThrottle,
      ssePostIdleFlushDelay: this.options.ssePostIdleFlushDelay,
    }
  }

  private getSessionStateOrThrow(sessionId: string | undefined): SessionState {
    if (!sessionId) throw new ProtocolViolationError()
    const sessionState = this.sessionStates.get(sessionId)
    if (!sessionState) throw new ProtocolViolationError()
    return sessionState
  }

  private getOrCreateConnectionState(connection: TConnection): ConnectionState {
    let state = this.connectionStates.get(connection)
    if (!state) {
      state = { pingTimer: null, terminatePermanently: null, reconciling: false, sendChain: null }
      this.connectionStates.set(connection, state)
    }
    return state
  }

  private clearPingTimer(connection: TConnection): void {
    const state = this.connectionStates.get(connection)
    if (!state?.pingTimer) return
    clearTimeout(state.pingTimer)
    state.pingTimer = null
  }

  private resetPingTimer(connection: TConnection): void {
    const state = this.getOrCreateConnectionState(connection)
    this.clearPingTimer(connection)
    state.pingTimer = unrefTimer(
      setTimeout(() => {
        state.pingTimer = null
        if (state.reconciling) return
        this.transport.terminateConnection(connection)
        state.terminatePermanently = false
      }, this.options.pingDeadline),
    )
  }

  private handleFrame(
    connection: TConnection,
    frame: DecodedFrame,
    deferReconciled: boolean,
  ): null | Promise<string | null> {
    switch (frame.tag) {
      case TAG.CTRL:
        return this.handleCtrl(connection, frame.ctrl, deferReconciled)
      case TAG.TEXT: {
        const sessionState = this.getSessionStateOrThrow(this.transport.getSessionId(connection))
        if (sessionState.receiveConnection !== connection) return null
        const entry = sessionState.ixMap.get(frame.index)
        if (!entry) return null
        if (frame.seq && frame.seq <= entry.lastClientSeq) return null
        if (frame.seq) entry.lastClientSeq = frame.seq
        entry.channel._onPeerMessage(frame.text)
        return null
      }
      case TAG.TEXT_ACK_REQ: {
        const sessionState = this.getSessionStateOrThrow(this.transport.getSessionId(connection))
        if (sessionState.receiveConnection !== connection) return null
        const entry = sessionState.ixMap.get(frame.index)
        if (!entry) return null
        if (frame.seq && frame.seq <= entry.lastClientSeq) return null
        if (frame.seq) entry.lastClientSeq = frame.seq
        entry.channel._onPeerAckReqMessage(frame.text, frame.seq)
        return null
      }
      case TAG.BINARY: {
        const sessionState = this.getSessionStateOrThrow(this.transport.getSessionId(connection))
        if (sessionState.receiveConnection !== connection) return null
        const entry = sessionState.ixMap.get(frame.index)
        if (!entry) return null
        if (frame.seq && frame.seq <= entry.lastClientSeq) return null
        if (frame.seq) entry.lastClientSeq = frame.seq
        entry.channel._onPeerBinaryMessage(frame.data)
        return null
      }
      case TAG.ACK_RES: {
        const sessionState = this.getSessionStateOrThrow(this.transport.getSessionId(connection))
        if (sessionState.receiveConnection !== connection) return null
        const entry = sessionState.ixMap.get(frame.index)
        if (!entry) return null
        entry.channel._onPeerAckRes(frame.ackedSeq, frame.text, frame.status)
        return null
      }
    }
  }
}
