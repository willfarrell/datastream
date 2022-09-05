/* global ReadableStream, TransformStream, WritableStream */
export const pipeline = async (streams, streamOptions) => {
  // Ensure stream ends with only writable
  const lastStream = streams[streams.length - 1]
  if (isReadable(lastStream)) {
    streams.push(createWritableStream(() => {}, streamOptions))
  }

  await pipejoin(streams)
  return result(streams)
}

export const pipejoin = (streams) => {
  const lastIndex = streams.length - 1
  return streams.reduce((pipeline, stream, idx) => {
    if (idx === lastIndex && stream.getWriter) {
      return pipeline.pipeTo(stream)
    }
    return pipeline.pipeThrough(stream)
  })
}

export const result = async (streams) => {
  const output = {}
  for (const stream of streams) {
    if (typeof stream.result === 'function') {
      const { key, value } = await stream.result() // tap, sensor, readOut, dial, signal, output, result
      output[key] = value
    }
  }
  return output
}

// const arr = await streamToArray(read.pipeThrough(transform))
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

/* export const streamToBuffer = async (stream) => {
  let byteLength = 0
  let value = []
  for await (const chunk of stream) {
    byteLength += chunk.length
    value.push([new Uint8Array(chunk),byteLength])
  }
  return value.reduce((buffer, set) => {
    if (!buffer) buffer = new Uint8Array(byteLength)
    buffer.set(...set)
    return buffer
  })
} */

export const isReadable = (stream) => {
  return typeof stream.pipeTo === 'function' || !!stream.readable || false // TODO find better solution
}

export const isWritable = (stream) => {
  return typeof stream.pipeTo === 'undefined' || !!stream.writable || false // TODO find better solution
}

export const makeOptions = ({
  highWaterMark,
  chunkSize,
  signal,
  ...streamOptions
} = {}) => {
  return {
    writableStrategy: {
      highWaterMark,
      size: { chunk: chunkSize }
    },
    readableStrategy: {
      highWaterMark,
      size: { chunk: chunkSize }
    },
    signal,
    ...streamOptions
  }
}

export const createReadableStream = (input = '', streamOptions) => {
  const queued = []
  const stream = new ReadableStream(
    {
      async start (controller) {
        while (queued.length) {
          const chunk = queued.shift()
          controller.enqueue(chunk)
        }
        if (typeof input === 'string') {
          const chunkSize = streamOptions?.chunkSize ?? 16 * 1024
          let position = 0
          const length = input.length
          while (position < length) {
            const chunk = input.substring(position, position + chunkSize)
            controller.enqueue(chunk)
            position += chunkSize
          }
        } else if (Array.isArray(input)) {
          // TODO update to for(;;) loop, faster
          for (const chunk of input) {
            controller.enqueue(chunk)
          }
        } else {
          for await (const chunk of input) {
            controller.enqueue(chunk)
          }
        }

        controller.close()
      },
      async pull (controller) {
        while (queued.length) {
          const chunk = queued.shift()
          controller.enqueue(chunk)
        }
      }
    },
    makeOptions(streamOptions)
  )
  stream.push = queued.push
  return stream
}

export const createPassThroughStream = (
  passThrough = (chunk) => {},
  streamOptions
) => {
  return new TransformStream(
    {
      start () {},
      async transform (chunk, controller) {
        await passThrough(Object.freeze(chunk))
        controller.enqueue(chunk)
      },
      async flush (controller) {
        if (typeof streamOptions?.flush === 'function') {
          await streamOptions.flush()
        }
        controller.terminate()
      }
    },
    makeOptions(streamOptions)
  )
}

export const createTransformStream = (
  transform = (chunk, enqueue) => enqueue(chunk),
  streamOptions
) => {
  return new TransformStream(
    {
      start () {},
      async transform (chunk, controller) {
        const enqueue = (chunk, encoding) => {
          controller.enqueue(chunk, encoding)
        }
        await transform(chunk, enqueue)
      },
      async flush (controller) {
        if (typeof streamOptions?.flush === 'function') {
          const enqueue = (chunk, encoding) => {
            controller.enqueue(chunk, encoding)
          }
          await streamOptions.flush(enqueue)
        }
        controller.terminate()
      }
    },
    makeOptions(streamOptions)
  )
}

export const createWritableStream = (write = () => {}, streamOptions) => {
  return new WritableStream(
    {
      async write (chunk) {
        await write(chunk)
      },
      async final () {
        if (typeof streamOptions?.final === 'function') {
          await streamOptions.final()
        }
      }
    },
    makeOptions(streamOptions)
  )
}

export const tee = (sourceStream) => {
  return sourceStream.tee()
}

// Polyfill for `import { setTimeout } from 'node:timers/promises'`
export const timeout = (ms, { signal } = {}) => {
  if (signal?.aborted) {
    return Promise.reject(new Error('Aborted', 'AbortError'))
  }
  return new Promise((resolve, reject) => {
    const abortHandler = () => {
      clearTimeout(timeout)
      reject(new Error('Aborted', 'AbortError'))
    }
    if (signal) signal.addEventListener('abort', abortHandler)
    const timeout = setTimeout(() => {
      resolve()
      if (signal) signal.removeEventListener('abort', abortHandler)
    }, ms)
  })
}
