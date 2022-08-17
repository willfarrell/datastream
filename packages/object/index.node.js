import { Transform } from 'node:stream'
import {
  makeOptions,
  createReadableStream,
  createPassThroughStream,
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
  const stream = createPassThroughStream(transform, options)
  stream.result = () => ({ key: options?.key ?? 'count', value })
  return stream
}

/* export const objectRateStream = (options) => {
  let value = 0
  let time
  const transform = (chunk) => {
    value += 1
    time ??= process.hrtime.bigint()
  }
  const flush = () => {
    time =
      Number.parseInt((process.hrtime.bigint() - time).toString()) /
      1_000_000_000 // /s
  }
  const stream = createPassThroughStream(transform, flush, options)
  stream.result = () => ({
    key: options?.key ?? 'rate',
    value: value / time,
    unit: 'chunk/s',
  })
  return stream
} */

export const objectBatchStream = (keys, options) => {
  let previousId
  let batch
  return new Transform({
    ...makeOptions(options),
    readableObjectMode: true,
    writableObjectMode: true,
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

const objectPivotLongToWideDefaults = {
  delimiter: ' '
}
export const objectPivotLongToWideStream = (config, options) => {
  const { keys, valueParam, delimiter } = {
    ...objectPivotLongToWideDefaults,
    ...config
  }
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
  return createTransformStream(transform, { ...options, objectMode: true })
}

const objectPivotWideToLongDefaults = {
  keyParam: 'keyParam',
  valueParam: 'valueParam'
}
export const objectPivotWideToLongStream = (config, options) => {
  const { keys, keyParam, valueParam } = {
    ...objectPivotWideToLongDefaults,
    ...config
  }
  return new Transform({
    ...makeOptions({ ...options, objectMode: true }),
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

export const objectOutputStream = (options) => {
  const value = []
  const transform = (chunk) => {
    value.push(chunk)
  }
  const stream = createPassThroughStream(transform, {
    ...options,
    objectMode: true
  })
  stream.result = () => ({ key: options?.key ?? 'output', value })
  return stream
}

export default {
  readableStream: objectReadableStream,
  countStream: objectCountStream,
  // rateStream: objectRateStream,
  batchStream: objectBatchStream,
  pivotLongToWideStream: objectPivotLongToWideStream,
  pivotWideToLongStream: objectPivotWideToLongStream,
  outputStream: objectOutputStream
}
