export { nodeReadableToWebRequest }

import type { Readable } from 'node:stream'
import { assertIsNotBrowser } from './assertIsNotBrowser.js'
assertIsNotBrowser()

function nodeReadableToWebRequest(
  readable: Readable,
  url: string,
  method: string,
  headers: Record<string, string | string[] | undefined>,
): Request {
  const body = new ReadableStream({
    start(controller) {
      readable.on('data', (chunk: Buffer) => controller.enqueue(new Uint8Array(chunk)))
      readable.on('end', () => controller.close())
      readable.on('error', (err) => controller.error(err))
    },
  })
  const headerPairs: [string, string][] = []
  for (const [key, value] of Object.entries(headers)) {
    if (value === undefined) continue
    if (Array.isArray(value)) {
      for (const v of value) headerPairs.push([key, v])
    } else {
      headerPairs.push([key, value])
    }
  }
  return new Request(url, {
    method,
    headers: headerPairs,
    body,
    // @ts-ignore duplex required for streaming request bodies
    duplex: 'half',
  })
}
