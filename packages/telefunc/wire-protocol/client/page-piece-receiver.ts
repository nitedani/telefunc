export { PagePieceReceiver }

import { WIRE_PIECES_ACK_GAP_MS } from '../constants.js'
import { PieceReceiver, acknowledges } from '../pieces.js'
import type { Uplink } from './uplink.js'

/** The page's: it acknowledges when a frame over `WIRE_PIECES_ACK_BYTES` arrived or an announcement closed, at most one
 *  PIECES_ACK per `WIRE_PIECES_ACK_GAP_MS` and none before its uplink's share for them allows one, a frame in the message
 *  its side sends next: all a sender needs that cuts its pieces by the rate it measures, and stops announcing by the
 *  rate it carries. */
class PagePieceReceiver extends PieceReceiver {
  constructor(
    send: (frame: Uint8Array<ArrayBuffer>) => void,
    carry: (frame: () => Uint8Array<ArrayBuffer> | null) => void,
    private readonly uplink: Uplink,
  ) {
    super(send, carry)
  }

  protected tookFrame(bytes: number, data: boolean, _announced: boolean, at: number): void {
    if (!acknowledges(bytes)) return
    this.date(data, at)
    this.acknowledgeSoon(true)
  }

  protected tookPiece(whole: boolean): void {
    if (whole && acknowledges(this.total)) this.acknowledgeSoon(true)
  }

  protected tookClose(at: number): void {
    this.date(false, at)
    this.acknowledgeSoon()
  }

  protected ackWait(): number {
    return Math.max(this.ackedAt + WIRE_PIECES_ACK_GAP_MS - this.newestAt, this.uplink.acks.wait())
  }
}
