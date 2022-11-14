import {
  createReadableStream,
  createPassThroughStream,
  createTransformStream
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

export const stringSkipConsecutiveDuplicates = (options, streamOptions) => {
  let previousChunk
  const transform = (chunk, enqueue) => {
    if (chunk !== previousChunk) {
      enqueue(chunk)
      previousChunk = chunk
    }
  }
  return createTransformStream(transform, streamOptions)
}

export default {
  readableStream: stringReadableStream,
  lengthStream: stringLengthStream,
  skipConsecutiveDuplicates: stringSkipConsecutiveDuplicates
}
