export { assert, assertUsage }

function assertUsage(condition: unknown, message: string): asserts condition {
  if (!condition) throw new Error(`[@telefunc/drizzle] ${message}`)
}

function assert(condition: unknown): asserts condition {
  if (!condition) {
    throw new Error(
      '[@telefunc/drizzle] You stumbled upon a bug. Reach out at https://github.com/brillout/telefunc/issues/new and include this error stack.',
    )
  }
}
