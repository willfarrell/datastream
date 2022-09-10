// CompressionStream
// - https://caniuse.com/?search=CompressionStream
// - doesn't support `br` - https://github.com/httptoolkit/brotli-wasm
// - not supported on firefox - https://bugzilla.mozilla.org/show_bug.cgi?id=1586639
// - not supported in safari
import { createTransformStream } from '@datastream/core'
import brotliPromise from 'brotli-wasm' // Import the default export
const { CompressStream, DecompressStream, BrotliStreamResult } =
  await brotliPromise // Import is async in browsers due to wasm requirements!

// https://github.com/httptoolkit/brotli-wasm/issues/14
export const brotliCompressStream = ({ quality } = {}, streamOptions) => {
  const engine = new CompressStream(quality ?? 11)
  const transform = (chunk, enqueue) => {
    enqueue(engine.compress(chunk))
  }
  streamOptions.flush = (enqueue) => {
    if (engine.result() === BrotliStreamResult.NeedsMoreInput) {
      enqueue(engine.compress(undefined, 100))
    }
  }
  return createTransformStream(transform, streamOptions)
}
export const brotliDecompressStream = (options, streamOptions) => {
  const engine = new DecompressStream()
  const transform = (chunk, enqueue) => {
    enqueue(engine.decompress(chunk))
  }
  streamOptions.flush = (enqueue) => {
    if (engine.result() === BrotliStreamResult.NeedsMoreInput) {
      enqueue(engine.decompress(undefined, 100))
    }
  }
  return createTransformStream(transform, streamOptions)
}

export default {
  compressStream: brotliCompressStream,
  decompressStream: brotliDecompressStream
}
