import { createTransformStream } from '@datastream/core'

export const base64EncodeStream = (options = {}, streamOptions) => {
  let extra = ''
  const transform = (chunk, enqueue) => {
    if (extra) {
      chunk = extra + chunk
      extra = null
    }

    // 3 bytes == 4 char
    const remaining = chunk.length % 3
    if (remaining > 0) {
      extra = chunk.slice(chunk.length - remaining)
      chunk = chunk.slice(0, chunk.length - remaining)
    }

    enqueue(btoa(chunk))
  }
  const flush = (enqueue) => {
    if (extra) {
      enqueue(btoa(extra))
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

    enqueue(atob(chunk))
  }
  const flush = (enqueue) => {
    if (extra) {
      enqueue(atob(extra))
    }
  }
  streamOptions.decodeStrings = false
  return createTransformStream(transform, flush, streamOptions)
}

export default {
  encodeStream: base64EncodeStream,
  decodeStream: base64DecodeStream
}
