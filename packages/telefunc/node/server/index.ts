export { telefunc } from './telefunc.js'
import { config } from './serverConfig.js'
export { config }
export { config as telefuncConfig }
<<<<<<< Updated upstream
export { getContext, provideTelefuncContext } from './getContext.js'
=======
export { getContext, provideTelefuncContext } from './context/getContext.js'
export { getRawContext, isAsyncMode } from './context/context.js'
export { PROVIDED_CONTEXT } from './context/getContext.js'
export { REQUEST_CONTEXT } from './context/requestContext.js'
export type { RequestContext } from './context/requestContext.js'
export type { Context } from './context/context.js'
>>>>>>> Stashed changes
export { Abort } from './Abort.js'
export { shield } from './shield.js'
export { onBug } from './runTelefunc/onBug.js'

// In order to allow users to override `Telefunc.Context`, we need to export `Telefunc` (even if the user never imports `Telefunc`)
export type { Telefunc } from './getContext/TelefuncNamespace.js'

export { decorateTelefunction as __decorateTelefunction } from './runTelefunc/decorateTelefunction.js'

import { assertUsage } from '../../utils/assert.js'

assertServerSide()

function assertServerSide() {
  const isBrowser = typeof window !== 'undefined' && 'innerHTML' in (window?.document?.body || {})
  assertUsage(
    !isBrowser,
    [
      'You are loading the `telefunc` module in the browser, but',
      'the `telefunc` module can only be imported in Node.js.',
    ].join(' '),
  )
}
