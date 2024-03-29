import { createGzip, createGunzip } from 'node:zlib'

// quality -1 - 9
export const gzipCompressStream = ({ quality } = {}, streamOptions = {}) => {
  const options = streamOptions
  options.level = quality
  return createGzip(options)
}
export const gzipDecompressStream = (options, streamOptions) => {
  return createGunzip(streamOptions)
}

export default {
  compressStream: gzipCompressStream,
  decompressStream: gzipDecompressStream
}
