export { testReactive }

import { autoRetry, expect, getServerUrl, page, test } from '@brillout/test-e2e'
import { waitForHydration } from '../../e2e-utils'

function testReactive() {
  test('reactive: zero-annotation invalidation reaches every client', async () => {
    await page.goto(`${getServerUrl()}/reactive`)
    await waitForHydration()
    await autoRetry(async () => {
      expect(await page.textContent('#red-list')).toContain('No todos yet')
    })

    // A second browser context: separate cookie jar, so in the docker cluster the
    // sticky load balancer routes it to a different server instance — the write
    // below then travels database → CDC → other instance → channel.
    const browser = page.context().browser()!
    const context2 = await browser.newContext()
    const page2 = await context2.newPage()
    await page2.goto(`${getServerUrl()}/reactive`)
    await autoRetry(async () => {
      expect(await page2.locator('#hydrated').count()).toBe(1)
      expect(await page2.textContent('#red-list')).toContain('No todos yet')
    })

    const fetches = (tab: typeof page, team: string) =>
      tab.evaluate((t: string) => window.__reactiveFetches?.[t] ?? 0, team)
    const blueFetchesBefore = await fetches(page2, 'blue')

    // Add on tab 1: both tabs refetch the red list — through plain local query keys
    await page.fill('#red-input', 'Ship reactive queries')
    await page.click('#red-add')
    await autoRetry(async () => {
      expect(await page.textContent('#red-list')).toContain('Ship reactive queries')
    })
    await autoRetry(async () => {
      expect(await page2.textContent('#red-list')).toContain('Ship reactive queries')
    })

    // Precision: the write matched team 'red' only — tab 2's blue query never refetched
    expect(await fetches(page2, 'blue')).toBe(blueFetchesBefore)

    // Update: toggling done on tab 1 reaches tab 2
    await page.click('#red-list input[type=checkbox]')
    await autoRetry(async () => {
      expect(await page2.locator('#red-list input[type=checkbox]').isChecked()).toBe(true)
    })

    // Delete: clearing on tab 2 reaches tab 1
    await page2.click('#red-clear')
    await autoRetry(async () => {
      expect(await page.textContent('#red-list')).toContain('No todos yet')
    })

    await page2.close()
    await context2.close()
  })
}
