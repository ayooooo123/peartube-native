// The worklet only frames bytes; Buffer covers every `b4a` call it makes.
export default {
  alloc: length => Buffer.alloc(length),
  from: value => (Buffer.isBuffer(value) ? value : Buffer.from(value)),
  concat: list => Buffer.concat(list),
  indexOf: (buffer, byte) => buffer.indexOf(byte),
  toString: (buffer, encoding = 'utf8') => Buffer.from(buffer.buffer, buffer.byteOffset, buffer.byteLength).toString(encoding),
  isBuffer: Buffer.isBuffer
}
