import { Readable, Transform, Writable } from 'node:stream'
import { pipeline as pipelinePromise } from 'node:stream/promises'
import { setTimeout } from 'node:timers/promises'
import cloneable from 'cloneable-readable'

export const pipeline = async (streams, streamOptions = {}) => {
  for (let idx = 0, l = streams.legnth; idx < l; idx++) {
    if (typeof streams[idx].then === 'function') {
      throw new Error(`Promise instead of stream passed in at index ${idx}`)
    }
  }
  // Ensure stream ends with only writable
  const lastStream = streams[streams.length - 1]
  if (isReadable(lastStream)) {
    streamOptions.objectMode = lastStream._readableState.objectMode
    streams.push(createWritableStream(() => {}, streamOptions))
  }
  await pipelinePromise(streams, streamOptions)
  return result(streams)
}

export const pipejoin = (streams) => {
  return streams.reduce((pipeline, stream, idx) => {
    if (typeof stream.then === 'function') {
      throw new Error(`Promise instead of stream passed in at index ${idx}`)
    }
    return pipeline.pipe(stream)
  })
}

export const result = async (streams) => {
  const output = {}
  for (const stream of streams) {
    if (typeof stream.result === 'function') {
      const { key, value } = await stream.result()
      if (key) {
        output[key] = value
      }
    }
  }
  return output
}

// Not possible in WebStream
export const backpressureGuage = (streams) => {
  const keys = Object.keys(streams)
  const values = Object.values(streams)
  const metrics = {}
  for (let i = 0, l = values.length; i < l; i++) {
    const value = values[i]
    metrics[keys[i]] = { timeline: [], total: {} }
    let timestamp
    let startTimestamp
    value.on('pause', () => {
      timestamp = Date.now() // process.hrtime.bigint()
    })
    value.on('resume', () => {
      if (timestamp) {
        // Number.parseInt(  (process.hrtime.bigint() - pauseTimestamp).toString()  ) / 1_000_000 // ms
        const duration = Date.now() - timestamp
        metrics[keys[i]].timeline.push({ timestamp, duration })
      } else {
        startTimestamp = Date.now()
      }
    })
    value.on('end', () => {
      const duration = Date.now() - startTimestamp
      metrics[keys[i]].total = { timestamp: startTimestamp, duration }
    })
  }
  return metrics
}

export const streamToArray = async (stream) => {
  const value = []
  for await (const chunk of stream) {
    value.push(chunk)
  }
  return value
}

export const streamToObject = async (stream) => {
  const value = {}
  for await (const chunk of stream) {
    Object.assign(value, chunk)
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

export const streamToBuffer = async (stream) => {
  const value = []
  for await (const chunk of stream) {
    value.push(Buffer.from(chunk))
  }
  return Buffer.concat(value)
}

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
  signal,
  ...streamOptions
} = {}) => {
  objectMode ??= true
  return {
    writableHighWaterMark: highWaterMark,
    writableObjectMode: objectMode,
    readableObjectMode: objectMode,
    readableHighWaterMark: highWaterMark,
    highWaterMark,
    chunkSize,
    objectMode,
    signal,
    ...streamOptions
  }
}

export const createReadableStream = (input = '', streamOptions) => {
  // string doesn't chunk, and is slow
  if (typeof input === 'string') {
    return createReadableStreamFromString(input, streamOptions)
  } else if (typeof input === 'object' && input.byteLength) {
    return createReadableStreamFromArrayBuffer(input, streamOptions)
  }
  return Readable.from(input, streamOptions)
}

export const createReadableStreamFromString = (input, streamOptions) => {
  function * iterator (input) {
    const size = streamOptions?.chunkSize ?? 16 * 1024
    let position = 0
    const length = input.length
    while (position < length) {
      yield input.substring(position, position + size)
      position += size
    }
  }
  return Readable.from(iterator(input), streamOptions)
}

export const createReadableStreamFromArrayBuffer = (input, streamOptions) => {
  function * iterator (input) {
    const size = streamOptions?.chunkSize ?? 16 * 1024
    const bytes = new Uint8Array(input)
    let position = 0
    const length = bytes.byteLength
    while (position < length) {
      yield bytes.subarray(position, (position += size))
      position += size
    }
  }
  return Readable.from(iterator(input), streamOptions)
}

export const createPassThroughStream = (
  passThrough = (chunk) => chunk,
  flush,
  streamOptions
) => {
  if (typeof flush !== 'function') {
    streamOptions = flush
    flush = undefined
  }
  return new Transform({
    ...makeOptions(streamOptions),
    async transform (chunk, encoding, callback) {
      try {
        await passThrough(chunk)
        this.push(chunk)
        callback()
      } catch (e) {
        callback(e)
      }
    },
    async flush (callback) {
      try {
        if (flush) {
          await flush()
        }
        callback()
      } catch (e) {
        callback(e)
      }
    }
  })
}

export const createTransformStream = (
  transform = (chunk, enqueue) => enqueue(chunk),
  flush,
  streamOptions
) => {
  if (typeof flush !== 'function') {
    streamOptions = flush
    flush = undefined
  }
  return new Transform({
    ...makeOptions(streamOptions),
    async transform (chunk, encoding, callback) {
      const enqueue = (chunk, encoding) => {
        this.push(chunk, encoding)
      }
      try {
        await transform(chunk, enqueue)
        callback()
      } catch (e) {
        callback(e)
      }
    },
    async flush (callback) {
      try {
        if (flush) {
          const enqueue = (chunk, encoding) => {
            this.push(chunk, encoding)
          }
          await flush(enqueue)
        }
        callback()
      } catch (e) {
        callback(e)
      }
    }
  })
}

export const createWritableStream = (
  write = () => {},
  final,
  streamOptions
) => {
  if (typeof final !== 'function') {
    streamOptions = final
    final = undefined
  }
  return new Writable({
    ...makeOptions(streamOptions),
    async write (chunk, encoding, callback) {
      try {
        await write(chunk)
        callback()
      } catch (e) {
        callback(e)
      }
    },
    async final (callback) {
      try {
        if (final) {
          await final()
        }
        callback()
      } catch (e) {
        callback(e)
      }
    }
  })
}

export const createBranchStream = (
  { streams, resultKey } = {},
  streamOptions
) => {
  const stream = cloneable(createPassThroughStream(undefined, streamOptions))
  streams.unshift(stream.clone())
  const value = pipeline(streams, streamOptions)
  stream.result = async () => {
    return {
      key: resultKey ?? 'branch',
      value: await value
    }
  }
  return stream
}

/* export const tee = (sourceStream) => {
  const stream = cloneable(sourceStream)
  return [stream, stream.clone()]
} */

export const timeout = (ms, { signal } = {}) => {
  return setTimeout(ms, { signal })
}
