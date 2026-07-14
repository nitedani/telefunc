export { testRun }

import { page, test, expect, expectLog, run, getServerUrl, autoRetry, fetchHtml } from '@brillout/test-e2e'
import { testCounter } from '../utils'
import { testFileUpload } from './pages/file-upload/e2e-test'
<<<<<<< Updated upstream
=======
import { testFileDownload } from './pages/file-download/e2e-test'
import { testStreaming } from './pages/streaming/e2e-test'
import { testAbort } from './pages/abort/e2e-test'
import { testClose } from './pages/close/e2e-test'
import { testChannel } from './pages/channel/e2e-test'
import { testFunction } from './pages/function/e2e-test'
import { testStreamToServer } from './pages/stream-to-server/e2e-test'
import { testLiveQuery } from './pages/live-query/e2e-test'
import { testReactive } from './pages/reactive/e2e-test'
import { testRxjs } from './pages/rxjs/e2e-test'
import { testPublish } from './pages/publish/e2e-test'
>>>>>>> Stashed changes

function testRun(cmd: 'npm run dev' | 'npm run preview') {
  run(cmd, {
    tolerateError(log) {
      return log.logText.includes('File arguments are being consumed out of order')
    },
  })

  const isDev = cmd === 'npm run dev'

  test('hello', async () => {
    {
      const html = await fetchHtml('/')
      expect(html).toContain('Loading...')
      expect(html).not.toContain('Eva')
    }
    {
      page.goto(`${getServerUrl()}/`)
      // `autoRetry` to ensure that async JavaScript has loaded & executed
      await autoRetry(async () => {
        expect(await page.textContent('body')).toContain('Welcome Eva')
      })
      expect(await page.textContent('body')).not.toContain('Loading')
    }
  })

  test('counter', async () => {
    await testCounter()
  })

  testFileUpload()

<<<<<<< Updated upstream
=======
  testFileDownload()

  testStreaming()

  testAbort()

  testClose()

  testChannel(isDev)

  testFunction()

  testStreamToServer()

  testLiveQuery()

  testReactive()

  testRxjs()

  testPublish()

>>>>>>> Stashed changes
  if (!isDev) {
    test('shield() generation', async () => {
      {
        const resp = await makeTelefuncHttpRequest('Jon')
        expect(resp.status).toBe(200)
        const { ret } = await resp.json()
        expect(ret.message).toBe('Welcome Jon')
      }
      {
        const resp = await makeTelefuncHttpRequest(1337)
        expect(resp.status).toBe(422)
        expect(await resp.text()).toBe('Shield Validation Error')
        // [14:10:31.724][/.test-preview.test.ts][npm run preview][stderr] Shield Validation Error: the arguments passed to the telefunction onLoad() (/pages/index/Hello.telefunc.ts) have the wrong type. Arguments: `[{"name":1337}]`. Wrong type: [root] > [tuple: element 0] > [object: value of key `name`] is `number` but should be `string`.
        expectLog('Shield Validation Error', {
          filter: (log) =>
            log.logSource === 'stderr' && log.logText.includes('onLoad()') && log.logText.includes('Hello.telefunc.ts'),
        })
      }
    })
  }
}

async function makeTelefuncHttpRequest(name: string | number) {
  const resp = await fetch(`${getServerUrl()}/_telefunc`, {
    method: 'POST',
    body: JSON.stringify({
      file: '/pages/index/Hello.telefunc.ts',
      name: 'onLoad',
      args: [{ name }],
    }),
  })
  return resp
}
