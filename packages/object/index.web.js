/* global TransformStream */
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

export const objectBatchStream = (keys, options) => {
  let previousId
  let batch
  return new TransformStream(
    {
      transform (chunk, controller) {
        const id = keys.map((key) => chunk[key]).join('#')
        if (previousId !== id) {
          if (batch) {
            controller.enqueue(batch)
          }
          previousId = id
          batch = []
        }
        batch.push(chunk)
      },
      flush (controller) {
        controller.enqueue(batch)
        controller.terminate()
      }
    },
    makeOptions(options)
  )
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

  return new TransformStream(
    {
      transform (chunk, controller) {
        const row = { ...chunk }
        for (const key of keys) {
          delete row[key]
        }
        for (const key of keys) {
          if (chunk[key]) {
            controller.enqueue({
              ...row,
              [keyParam]: key,
              [valueParam]: chunk[key]
            })
          }
        }
      }
    },
    makeOptions(options)
  )
}

export const objectOutputStream = (options) => {
  const value = []
  const transform = (chunk) => {
    value.push(chunk)
  }
  const stream = createTransformStream(transform, options)
  stream.result = () => ({ key: options?.key ?? 'output', value })
  return stream
}

export default {
  readableStream: objectReadableStream,
  countStream: objectCountStream,
  batchStream: objectBatchStream,
  pivotLongToWideStream: objectPivotLongToWideStream,
  pivotWideToLongStream: objectPivotWideToLongStream,
  outputStream: objectOutputStream
}
