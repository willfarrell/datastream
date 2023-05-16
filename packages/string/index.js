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
  const passThrough = (chunk) => {
    value += chunk.length
  }
  const stream = createPassThroughStream(passThrough, streamOptions)
  stream.result = () => ({ key: resultKey ?? 'length', value })
  return stream
}
export const stringCountStream = (
  { substr, resultKey } = {},
  streamOptions
) => {
  let value = 0
  const passThrough = (chunk) => {
    let cursor = 0
    while (cursor < chunk.length) {
      cursor = chunk.indexOf(substr, cursor + 1)
      if (cursor === -1) {
        break
      }
      value += 1
    }
  }
  const stream = createPassThroughStream(passThrough, streamOptions)
  stream.result = () => ({ key: resultKey ?? 'count', value })
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
