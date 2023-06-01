import { createTransformStream } from '@datastream/core'

// TODO replace with web version
export const base64EncodeStream = (options = {}, streamOptions) => {
  let extra
  const transform = (chunk, enqueue) => {
    if (extra) {
      chunk = Buffer.concat([extra, chunk])
      extra = null
    }

    // 3 bytes == 4 char
    const remaining = chunk.length % 3
    if (remaining > 0) {
      extra = chunk.slice(chunk.length - remaining)
      chunk = chunk.slice(0, chunk.length - remaining)
    }

    enqueue(Buffer.from(chunk).toString('base64'))
  }
  const flush = (enqueue) => {
    if (extra) {
      enqueue(Buffer.from(extra).toString('base64'))
    }
  }
  return createTransformStream(transform, flush, streamOptions)
}

export const base64DecodeStream = (options = {}, streamOptions = {}) => {
  let extra = ''
  const transform = (chunk, enqueue) => {
    chunk = extra + chunk

    // 4 char == 3 bytes
    const remaining = chunk.length % 4

    extra = chunk.slice(chunk.length - remaining)
    chunk = chunk.slice(0, chunk.length - remaining)

    enqueue(Buffer.from(chunk, 'base64'))
  }
  const flush = (enqueue) => {
    if (extra) {
      enqueue(Buffer.from(extra, 'base64'))
    }
  }
  streamOptions.decodeStrings = false
  return createTransformStream(transform, flush, streamOptions)
}

export default {
  encodeStream: base64EncodeStream,
  decodeStream: base64DecodeStream
}
