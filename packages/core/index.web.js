/* global ReadableStream, TransformStream, WritableStream */

export const pipeline = async (streams, streamOptions = {}) => {
	// Ensure stream ends with only writable
	const lastStream = streams[streams.length - 1];
	if (isReadable(lastStream)) {
		streams.push(createWritableStream(() => {}, streamOptions));
	}

	await pipejoin(streams);
	return result(streams);
};

export const pipejoin = (streams) => {
	const lastIndex = streams.length - 1;
	return streams.reduce((pipeline, stream, idx) => {
		if (typeof stream.then === "function") {
			throw new Error(`Promise instead of stream passed in at index ${idx}`);
		}
		if (idx === lastIndex && stream.getWriter) {
			return pipeline.pipeTo(stream);
		}
		return pipeline.pipeThrough(stream);
	});
};

export const result = async (streams) => {
	const output = {};
	for (const stream of streams) {
		if (typeof stream.result === "function") {
			const { key, value } = await stream.result();
			if (key) {
				output[key] = value;
			}
		}
	}
	return output;
};

export const streamToArray = async (stream) => {
	const value = [];
	for await (const chunk of stream) {
		value.push(chunk);
	}
	return value;
};

export const streamToObject = async (stream) => {
	const value = {};
	for await (const chunk of stream) {
		Object.assign(value, chunk);
	}
	return value;
};

export const streamToString = async (stream) => {
	let value = "";
	for await (const chunk of stream) {
		value += chunk;
	}
	return value;
};

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
	return typeof stream.pipeTo === "function" || !!stream.readable; // TODO find better solution
};

export const isWritable = (stream) => {
	return typeof stream.pipeTo === "undefined" || !!stream.writable; // TODO find better solution
};

export const makeOptions = ({
	highWaterMark,
	chunkSize,
	signal,
	...streamOptions
} = {}) => {
	return {
		writableStrategy: {
			highWaterMark,
			size: { chunk: chunkSize },
		},
		readableStrategy: {
			highWaterMark,
			size: { chunk: chunkSize },
		},
		signal,
		...streamOptions,
	};
};

export const createReadableStream = (input, streamOptions = {}) => {
	const queued = [];
	const stream = new ReadableStream(
		{
			async start(controller) {
				while (queued.length) {
					const chunk = queued.shift();
					controller.enqueue(chunk);
				}
				if (typeof input === "string") {
					const chunkSize = streamOptions?.chunkSize ?? 16 * 1024;
					let position = 0;
					const length = input.length;
					while (position < length) {
						const chunk = input.substring(position, position + chunkSize);
						controller.enqueue(chunk);
						position += chunkSize;
					}
					controller.close();
				} else if (Array.isArray(input)) {
					// TODO update to for(;;) loop, faster
					for (const chunk of input) {
						controller.enqueue(chunk);
					}
					controller.close();
				} else if (["function", "object"].includes(typeof input)) {
					for await (const chunk of input) {
						controller.enqueue(chunk);
					}
					controller.close();
				}
			},
			pull(controller) {
				while (queued.length) {
					const chunk = queued.shift();
					if (chunk === null) {
						controller.close();
					} else {
						controller.enqueue(chunk);
					}
				}
			},
		},
		makeOptions(streamOptions),
	);
	stream.push = (chunk) => queued.push(chunk);
	return stream;
};

export const createPassThroughStream = (passThrough, flush, streamOptions) => {
	passThrough ??= (_chunk) => {};
	if (typeof flush !== "function") {
		streamOptions = flush;
		flush = undefined;
	}
	return new TransformStream(
		{
			start() {},
			async transform(chunk, controller) {
				await passThrough(chunk);
				controller.enqueue(chunk);
			},
			async flush(controller) {
				if (flush) {
					await flush();
				}
				controller.terminate();
			},
		},
		makeOptions(streamOptions),
	);
};

export const createTransformStream = (transform, flush, streamOptions) => {
	transform ??= (chunk, enqueue) => enqueue(chunk);
	if (typeof flush !== "function") {
		streamOptions = flush;
		flush = undefined;
	}
	return new TransformStream(
		{
			start() {},
			async transform(chunk, controller) {
				const enqueue = (chunk, encoding) => {
					controller.enqueue(chunk, encoding);
				};
				await transform(chunk, enqueue);
			},
			async flush(controller) {
				if (flush) {
					const enqueue = (chunk, encoding) => {
						controller.enqueue(chunk, encoding);
					};
					await flush(enqueue);
				}
				controller.terminate();
			},
		},
		makeOptions(streamOptions),
	);
};

export const createWritableStream = (write, close, streamOptions) => {
	write ??= () => {};
	if (typeof close !== "function") {
		streamOptions = close;
		close = undefined;
	}
	return new WritableStream(
		{
			async write(chunk) {
				await write(chunk);
			},
			async close() {
				if (close) {
					await close();
				}
			},
		},
		makeOptions(streamOptions),
	);
};

export const createBranchStream = (
	{ streams, resultKey } = {},
	streamOptions = {},
) => {
	// TODO refactor, not good enough
	// https://streams.spec.whatwg.org/#rs-model
	const branchStream = createReadableStream(undefined, streamOptions);
	const passThrough = (chunk) => {
		branchStream.push(chunk);
	};
	const flush = () => {
		branchStream.push(null);
	};
	const stream = createPassThroughStream(passThrough, flush, streamOptions);

	streams.unshift(branchStream);
	const value = pipeline(streams, streamOptions);
	stream.result = async () => {
		return {
			key: resultKey ?? "branch",
			value, // await causes: Promise resolution is still pending but the event loop has already resolved
		};
	};
	return stream;
};

/* export const tee = (sourceStream) => {
  return sourceStream.tee()
} */

// Polyfill for `import { setTimeout } from 'node:timers/promises'`
export const timeout = (ms, { signal } = {}) => {
	if (signal?.aborted) {
		return Promise.reject(new Error("Aborted", "AbortError"));
	}
	return new Promise((resolve, reject) => {
		const abortHandler = () => {
			clearTimeout(timeout);
			reject(new Error("Aborted", "AbortError"));
		};
		if (signal) signal.addEventListener("abort", abortHandler);
		setTimeout(() => {
			resolve();
			if (signal) signal.removeEventListener("abort", abortHandler);
		}, ms);
	});
};
