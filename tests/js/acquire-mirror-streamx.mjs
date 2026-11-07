// `streamx` for the acquire scenario harness: the acquirer only wraps an async generator, and the fake node
// iterates it directly.
export const Readable = {
  from (iterable) {
    return iterable
  }
}
