/* global CompressionStream, DecompressionStream */
// CompressionStream
// - https://caniuse.com/?search=CompressionStream
// - not supported on firefox - https://bugzilla.mozilla.org/show_bug.cgi?id=1586639
// - not supported in safari

export const gzipCompressStream = (options, streamOptions) => {
  return new CompressionStream('gzip')
}
export const gzipDecompressStream = (options, streamOptions) => {
  return new DecompressionStream('gzip')
}

export default {
  compressStream: gzipCompressStream,
  decompressStream: gzipDecompressStream
}
