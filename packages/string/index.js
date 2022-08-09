import { createReadableStream, createTransformStream } from '@datastream/core'

export const stringReadableStream = (string, options) => {
  return createReadableStream(string, options)
}

export const stringLengthStream = (options) => {
  let value = 0
  const transform = (chunk) => {
    value += chunk.length
  }
  const stream = createTransformStream(transform, {
    ...options,
    objectMode: false
  })
  stream.result = () => ({ key: options?.key ?? 'length', value })
  return stream
}

export const stringOutputStream = (options = { key: 'output' }) => {
  const { key } = options
  let value = ''
  const transform = (chunk) => {
    // if (Buffer.isBuffer(chunk)) {
    //   value += Buffer.from(chunk).toString("utf8") // use decodeStrings?
    // } else {
    value += chunk
    // }
  }
  const stream = createTransformStream(transform, {
    ...options,
    objectMode: false
  })
  stream.result = () => ({ key, value })
  return stream
}

export default {
  readableStream: stringReadableStream,
  lengthStream: stringLengthStream,
  outputStream: stringOutputStream
}
