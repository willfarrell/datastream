import { createDeflate, createInflate } from 'node:zlib'

// TODO benchmark against `fflate`
// quality -1 - 9
export const deflateCompressStream = ({ quality } = {}, streamOptions = {}) => {
  const options = streamOptions
  options.level = quality
  return createDeflate(options)
}
export const deflateDecompressStream = (options, streamOptions) => {
  return createInflate(streamOptions)
}

export default {
  compressStream: deflateCompressStream,
  decompressStream: deflateDecompressStream
}
