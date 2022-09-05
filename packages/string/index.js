import {
  createReadableStream,
  createPassThroughStream
} from '@datastream/core'

export const stringReadableStream = (input, streamOptions) => {
  return createReadableStream(input, streamOptions)
}

export const stringLengthStream = ({ resultKey } = {}, streamOptions) => {
  let value = 0
  const transform = (chunk) => {
    value += chunk.length
  }
  const stream = createPassThroughStream(transform, streamOptions)
  stream.result = () => ({ key: resultKey ?? 'length', value })
  return stream
}

export default {
  readableStream: stringReadableStream,
  lengthStream: stringLengthStream
}
