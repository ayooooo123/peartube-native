// `b4a` for the acquire scenario harness: only `toString` is used.
export default {
  toString (bytes, encoding) {
    return Buffer.from(bytes).toString(encoding === 'base64' ? 'base64' : encoding)
  },
  alloc (size) {
    return Buffer.alloc(size)
  }
}
