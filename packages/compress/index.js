import brotli from '@datastream/compress/brotli'
import gzip from '@datastream/compress/gzip'
import deflate from '@datastream/compress/deflate'

export const brotliCompressStream = brotli.compressStream
export const brotliDecompressStream = brotli.decompressStream
export const gzipCompressStream = gzip.compressStream
export const gzipDecompressStream = gzip.decompressStream
export const deflateCompressStream = deflate.compressStream
export const deflateDecompressStream = deflate.decompressStream

export default {
  brotliCompressStream,
  brotliDecompressStream,
  gzipCompressStream,
  gzipDecompressStream,
  deflateCompressStream,
  deflateDecompressStream
}
