import { Readable, Transform, PassThrough, Writable } from 'node:stream';
import { pipeline as pipelinePromise } from 'node:stream/promises';

export const pipeline = async (streams, { signal } = {}) => {

		// Ensure stream ends with only writable
		const lastStream = streams[streams.length - 1]
		if (isReadable(lastStream)) {
			streams.push(createWritableStream(() => {}, { signal, objectMode: lastStream._readableState.objectMode }))
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
	let value = []
	for await (const chunk of stream) {
		value.push(Buffer.from(chunk))
	}
	return Buffer.concat(value)
}*/

export const isReadable = (stream) => {
	return !!stream._readableState
}

export const isWritable = (stream) => {
	return !!stream._writableState
}

export const makeOptions = ({highWaterMark, chunkSize, objectMode, ...options} = {}) => {
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

export const createReadableStream = (data, options) => {
	return Readable.from(data, options)
}

export const createTransformStream = (fn = () => {}, options) => {
	return new Transform({
		...makeOptions({objectMode: true, ...options}),
		transform(chunk, encoding, callback) {
			fn(chunk)
			this.push(chunk)
			callback()
		}
	})
}

export const createWritableStream = (fn = () => {}, options) => {
	return new Writable({
		objectMode: true,
		...options,
		write(chunk, encoding, callback) {
			fn(chunk)
			callback()
		}
	})
}
