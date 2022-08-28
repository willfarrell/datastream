/* global CompressionStream, DecompressionStream */
// CompressionStream
// - https://caniuse.com/?search=CompressionStream
// - not supported on firefox - https://bugzilla.mozilla.org/show_bug.cgi?id=1586639
// - not supported in safari

export const deflateCompressStream = (options, streamOptions) => {
  return new CompressionStream('deflate')
}
export const deflateDecompressStream = (options, streamOptions) => {
  return new DecompressionStream('deflate')
}

export default {
  compressStream: deflateCompressStream,
  decompressStream: deflateDecompressStream
}
