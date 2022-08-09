import { Readable, Transform, Writable } from 'node:stream'
import { pipeline as pipelinePromise } from 'node:stream/promises'

export const pipeline = async (streams, { signal } = {}) => {
  // Ensure stream ends with only writable
  const lastStream = streams[streams.length - 1]
  if (isReadable(lastStream)) {
    streams.push(
      createWritableStream(() => {}, {
        signal,
        objectMode: lastStream._readableState.objectMode
      })
    )
  }

  await pipelinePromise(streams, { signal })

  const output = {}
  for (const stream of streams) {
    if (typeof stream.result === 'function') {
      const { key, value } = await stream.result()
      output[key] = value
    }
  }

  return output
}

export const pipejoin = (streams) => {
  return streams.reduce((pipeline, stream, idx) => {
    return pipeline.pipe(stream)
  })
}

export const streamToArray = async (stream) => {
  const value = []
  for await (const chunk of stream) {
    value.push(chunk)
  }
  return value
}

export const streamToString = async (stream) => {
  let value = ''
  for await (const chunk of stream) {
    value += chunk
  }
  return value
}

/* export const streamToBuffer = async (stream) => {
  let value = []
  for await (const chunk of stream) {
    value.push(Buffer.from(chunk))
  }
  return Buffer.concat(value)
} */

export const isReadable = (stream) => {
  return !!stream._readableState
}

export const isWritable = (stream) => {
  return !!stream._writableState
}

export const makeOptions = ({
  highWaterMark,
  chunkSize,
  objectMode,
  ...options
} = {}) => {
  return {
    writableHighWaterMark: highWaterMark,
    writableObjectMode: objectMode,
    readableObjectMode: objectMode,
    readableHighWaterMark: highWaterMark,
    highWaterMark,
    chunkSize,
    objectMode,
    ...options
  }
}

export const createReadableStream = (input, options) => {
  // string doesn't chunk, and is slow
  if (typeof input === 'string') {
    function * iterator () {
      const size = options?.chunkSize ?? options?.highWaterMark ?? 16 * 1024
      let position = 0
      const length = input.length
      while (position < length) {
        yield input.substring(position, position + size)
        position += size
      }
    }
    return Readable.from(iterator(), { objectMode: true, ...options })
  }
  return Readable.from(input, { objectMode: true, ...options })
}

export const createTransformStream = (fn = (chunk) => chunk, options) => {
  return new Transform({
    ...makeOptions({ objectMode: true, ...options }),
    transform (chunk, encoding, callback) {
      chunk = fn(chunk)
      this.push(chunk)
      callback()
    }
  })
}

export const createWritableStream = (fn = () => {}, options) => {
  return new Writable({
    objectMode: true,
    ...options,
    write (chunk, encoding, callback) {
      fn(chunk)
      callback()
    }
  })
}
