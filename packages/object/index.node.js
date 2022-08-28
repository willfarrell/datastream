import { Transform } from 'node:stream'
import {
  makeOptions,
  createReadableStream,
  createPassThroughStream,
  createTransformStream
} from '@datastream/core'

export const objectReadableStream = (input = [], streamOptions) => {
  return createReadableStream(input, streamOptions)
}

export const objectCountStream = ({ resultKey } = {}, streamOptions) => {
  let value = 0
  const transform = () => {
    value += 1
  }
  const stream = createPassThroughStream(transform, streamOptions)
  stream.result = () => ({ key: resultKey ?? 'count', value })
  return stream
}

export const objectBatchStream = ({ keys }, streamOptions) => {
  let previousId
  let batch
  return new Transform({
    ...makeOptions(streamOptions),
    async transform (chunk, encoding, callback) {
      const id = keys.map((key) => chunk[key]).join(' ')
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

export const objectPivotLongToWideStream = (
  { keys, valueParam, delimiter },
  streamOptions
) => {
  delimiter ??= ' '

  // if (!Array.isArray(keys)) keys = [keys]
  const transform = (chunks) => {
    if (!Array.isArray(chunks)) {
      throw new Error('Expected chunk to be array, use with objectBatchStream')
    }
    const row = chunks[0]

    for (const chunk of chunks) {
      const keyParam = keys.map((key) => chunk[key]).join(delimiter)
      row[keyParam] = chunk[valueParam]
    }

    for (const key of keys) {
      delete row[key]
    }
    delete row[valueParam]

    return row
  }
  return createTransformStream(transform, streamOptions)
}

export const objectPivotWideToLongStream = (
  { keys, keyParam, valueParam },
  streamOptions
) => {
  keyParam ??= 'keyParam'
  valueParam ??= 'valueParam'

  return new Transform({
    ...makeOptions(streamOptions),
    async transform (chunk, encoding, callback) {
      const row = { ...chunk }
      for (const key of keys) {
        delete row[key]
      }
      for (const key of keys) {
        if (chunk[key]) {
          this.push({ ...row, [keyParam]: key, [valueParam]: chunk[key] })
        }
      }
      callback()
    }
  })
}

export const objectKeyValueStream = ({ key, value }, streamOptions) => {
  const transform = (chunk) => {
    chunk = { [chunk[key]]: chunk[value] }
    return chunk
  }
  return createTransformStream(transform, streamOptions)
}

export const objectKeyValuesStream = ({ key, values }, streamOptions) => {
  const transform = (chunk) => {
    const value =
      typeof values === 'undefined'
        ? chunk
        : values.reduce((value, key) => {
          value[key] = chunk[key]
          return value
        }, {})
    chunk = {
      [chunk[key]]: value
    }
    return chunk
  }
  return createTransformStream(transform, streamOptions)
}

export const objectOutputStream = ({ resultKey } = {}, streamOptions) => {
  const value = []
  const transform = (chunk) => {
    value.push(chunk)
  }
  const stream = createPassThroughStream(transform, streamOptions)
  stream.result = () => ({ key: resultKey ?? 'output', value })
  return stream
}

export default {
  readableStream: objectReadableStream,
  countStream: objectCountStream,
  batchStream: objectBatchStream,
  pivotLongToWideStream: objectPivotLongToWideStream,
  pivotWideToLongStream: objectPivotWideToLongStream,
  keyValueStream: objectKeyValueStream,
  keyValuesStream: objectKeyValuesStream,
  outputStream: objectOutputStream
}
