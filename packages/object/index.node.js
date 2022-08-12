import { Transform } from 'node:stream'
import {
  makeOptions,
  createReadableStream,
  createTransformStream
} from '@datastream/core'

export const objectReadableStream = (array = [], options) => {
  return createReadableStream(array, options)
}

export const objectCountStream = (options) => {
  let value = 0
  const transform = () => {
    value += 1
  }
  const stream = createTransformStream(transform, options)
  stream.result = () => ({ key: options?.key ?? 'count', value })
  return stream
}

export const objectRateStream = (keys, options) => {
  let value = 0
  let time
  const stream = new Transform({
    ...makeOptions(options),
    readableObjectMode: true,
    writableObjectMode: true,
    async transform (chunk, encoding, callback) {
      value += 1
      time ??= process.hrtime.bigint()
      this.push(chunk)
      callback()
    },
    async flush (callback) {
      time =
        Number.parseInt((process.hrtime.bigint() - time).toString()) /
        1_000_000_000 // /s
      callback()
    }
  })
  stream.result = () => ({
    key: options?.key ?? 'rate',
    value: value / time,
    unit: 'chunk/s'
  })
  return stream
}

export const objectBatchStream = (keys, options) => {
  let previousId
  let batch
  return new Transform({
    ...makeOptions(options),
    readableObjectMode: true,
    writableObjectMode: true,
    async transform (chunk, encoding, callback) {
      const id = keys.map((key) => chunk[key]).join('#')
      if (previousId !== id) {
        if (batch) {
          this.push(batch)
        }
        previousId = id
        batch = []
      }
      batch.push(chunk)
      callback()
    },
    async flush (callback) {
      if (batch) {
        this.push(batch)
      }
      callback()
    }
  })
}

export const objectOutputStream = (options) => {
  const value = []
  const transform = (chunk) => {
    value.push(chunk)
  }
  const stream = createTransformStream(transform, {
    ...options,
    objectMode: true
  })
  stream.result = () => ({ key: options?.key ?? 'output', value })
  return stream
}

export default {
  readableStream: objectReadableStream,
  countStream: objectCountStream,
  batchStream: objectBatchStream,
  outputStream: objectOutputStream
}
