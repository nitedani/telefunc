export { ServerPieceReceiver }

import { WIRE_PIECES_ACK_BYTES, WIRE_SEND_ACK_GAP_MS, WIRE_SEND_ACK_MIN_BYTES, WIRE_SEND_ACK_MS } from '../constants.js'
import { PieceReceiver } from '../pieces.js'

/** The server's: its peer, the page, holds data back by what it acknowledges (see `SendBudget`), so it acknowledges each
 *  announced message, as the page's PINGs wait behind them, once more than `WIRE_PIECES_ACK_BYTES` of frames of any size
 *  are unacknowledged, at most one PIECES_ACK per `WIRE_SEND_ACK_GAP_MS`, data within `WIRE_SEND_ACK_MS`, and what it
 *  took in the message of its answer to it (`answering`). */
class ServerPieceReceiver extends PieceReceiver {
  /** Its side answers what arrived: the answer's message acknowledges the data it took, so the page's next request
   *  doesn't wait for the acknowledgement's timer. */
  answering(): void {
    if (this.took - this.acknowledgedTook >= WIRE_SEND_ACK_MIN_BYTES) this.acknowledgeWithNext()
  }

  protected tookFrame(_bytes: number, data: boolean, announced: boolean, at: number): void {
    this.date(data, at)
    this.tookData(data)
    if (announced || this.took - this.acknowledgedTook > WIRE_PIECES_ACK_BYTES) this.acknowledgeSoon()
  }

  protected tookPiece(_whole: boolean, announced: boolean, data: boolean): void {
    this.tookData(data)
    if (announced || this.took - this.acknowledgedTook > WIRE_PIECES_ACK_BYTES) this.acknowledgeSoon()
  }

  /** Acknowledges data within `WIRE_SEND_ACK_MS`, once it took enough since the last acknowledgement. */
  private tookData(data: boolean): void {
    if (data && this.took - this.acknowledgedTook >= WIRE_SEND_ACK_MIN_BYTES) this.acknowledgeIn(WIRE_SEND_ACK_MS)
  }

  protected tookClose(): void {}

  protected ackWait(): number {
    return this.ackedAt + WIRE_SEND_ACK_GAP_MS - this.newestAt
  }
}
