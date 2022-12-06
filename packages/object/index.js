import {
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
  const transform = (chunk, enqueue) => {
    const id = keys.map((key) => chunk[key]).join(' ')
    if (previousId !== id) {
      if (batch) {
        enqueue(batch)
      }
      previousId = id
      batch = []
    }
    batch.push(chunk)
  }
  const flush = (enqueue) => {
    if (batch) {
      enqueue(batch)
    }
  }
  return createTransformStream(transform, flush, streamOptions)
}

export const objectPivotLongToWideStream = (
  { keys, valueParam, delimiter },
  streamOptions
) => {
  delimiter ??= ' '

  // if (!Array.isArray(keys)) keys = [keys]
  const transform = (chunks, enqueue) => {
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

    enqueue(row)
  }
  return createTransformStream(transform, streamOptions)
}

export const objectPivotWideToLongStream = (
  { keys, keyParam, valueParam },
  streamOptions
) => {
  keyParam ??= 'keyParam'
  valueParam ??= 'valueParam'

  const transform = (chunk, enqueue) => {
    const row = { ...chunk }
    for (const key of keys) {
      delete row[key]
    }
    for (const key of keys) {
      if (chunk[key]) {
        enqueue({ ...row, [keyParam]: key, [valueParam]: chunk[key] })
      }
    }
  }
  return createTransformStream(transform, streamOptions)
}

export const objectKeyValueStream = ({ key, value }, streamOptions) => {
  const transform = (chunk, enqueue) => {
    chunk = { [chunk[key]]: chunk[value] }
    enqueue(chunk)
  }
  return createTransformStream(transform, streamOptions)
}

export const objectKeyValuesStream = ({ key, values }, streamOptions) => {
  const transform = (chunk, enqueue) => {
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
    enqueue(chunk)
  }
  return createTransformStream(transform, streamOptions)
}

export const objectSkipConsecutiveDuplicates = (options, streamOptions) => {
  let previousChunk
  const transform = (chunk, enqueue) => {
    const chunkStringified = JSON.stringify(chunk)
    if (chunkStringified !== previousChunk) {
      enqueue(chunk)
      previousChunk = chunkStringified
    }
  }
  return createTransformStream(transform, streamOptions)
}

export default {
  readableStream: objectReadableStream,
  countStream: objectCountStream,
  batchStream: objectBatchStream,
  pivotLongToWideStream: objectPivotLongToWideStream,
  pivotWideToLongStream: objectPivotWideToLongStream,
  keyValueStream: objectKeyValueStream,
  keyValuesStream: objectKeyValuesStream,
  skipConsecutiveDuplicates: objectSkipConsecutiveDuplicates
}
