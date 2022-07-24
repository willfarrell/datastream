
export const pipeline = async (streams, options) => {
  // Ensure stream ends with only writable
  const lastStream = streams[streams.length - 1]
  if (isReadable(lastStream)) {
    streams.push(createWritableStream(() => {},options))
  }

  await pipejoin(streams)

  const output = {}
  for (const stream of streams) {
    if (typeof stream.result === 'function') {
      const { key, value } = await stream.result() // tap, sensor, readOut, dial, signal, output, result
      output[key] = value
    }
  }

  return output
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

// const arr = await streamToArray(read.pipeThrough(transform))
export const streamToArray = async (stream) => {
  let value = []
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

/*export const streamToBuffer = async (stream) => {
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
}*/

export const isReadable = (stream) => {
  return typeof stream.pipeTo === 'function' || !!stream.readable || false // TODO find better solution
}

export const isWritable = (stream) => {
  return typeof stream.pipeTo === 'undefined' || !!stream.writable || false // TODO find better solution
}

export const makeOptions = ({highWaterMark, chunkSize, ...options} = {}) => {
  return {
    writableStrategy: {
      highWaterMark,
      size: { chunk: chunkSize }
    },
    readableStrategy: {
      highWaterMark,
      size: { chunk: chunkSize }
    },
    ...options
  }
}

export const createReadableStream = (input, options) => {
  return new ReadableStream({
    async start(controller) {
      if (typeof input === 'string') {
        const highWaterMark = options?.highWaterMark ?? 16 * 1024
        let position = 0
        const length = input.length
        while (position < length) {
          const chunk = input.substring(position, position + highWaterMark)
          controller.enqueue(chunk)
          position += highWaterMark
        }
      } else if (Array.isArray(input)) {
        for(const chunk of input) {
          controller.enqueue(chunk)
        }
      } else {
        for await (const chunk of input) {
          controller.enqueue(chunk)
        }
      }
      controller.close()
    }
  }, makeOptions(options))
}
export const createTransformStream = (fn = () => {}, options) => {
  return new TransformStream({
    start() {},
    transform(chunk, controller) {
      fn(chunk)
      controller.enqueue(chunk)
    }
  }, makeOptions(options))
}

export const createWritableStream = (fn = () => {}, options) => {
  return new WritableStream({
    write(chunk) {
      fn(chunk)
    }
  }, makeOptions(options))
}
