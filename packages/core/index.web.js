// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
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
	const value = Object.create(null);
	for await (const chunk of stream) {
		Object.assign(value, chunk);
	}
	return { ...value };
};

export const streamToString = async (stream) => {
	const chunks = [];
	for await (const chunk of stream) {
		chunks.push(chunk);
	}
	return chunks.join("");
};

export const isReadable = (stream) => {
	return stream instanceof ReadableStream || !!stream.readable;
};

export const isWritable = (stream) => {
	return stream instanceof WritableStream || !!stream.writable;
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
	const maxQueueSize = streamOptions.highWaterMark ?? 1024;
	const queued = [];
	const stream = new ReadableStream(
		{
			async start(controller) {
				while (queued.length) {
					const chunk = queued.shift();
					controller.enqueue(chunk);
				}
				if (typeof input === "string") {
					const chunkSize = streamOptions?.chunkSize ?? 16_384; // 16KB
					let position = 0;
					const length = input.length;
					while (position < length) {
						const chunk = input.substring(position, position + chunkSize);
						controller.enqueue(chunk);
						position += chunkSize;
					}
					controller.close();
				} else if (Array.isArray(input)) {
					for (let i = 0, l = input.length; i < l; i++) {
						controller.enqueue(input[i]);
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
	stream.push = (chunk) => {
		if (queued.length >= maxQueueSize) {
			throw new Error(
				`createReadableStream queue size (${queued.length}) exceeds limit (${maxQueueSize})`,
			);
		}
		queued.push(chunk);
	};
	return stream;
};

export const createPassThroughStream = (passThrough, flush, streamOptions) => {
	passThrough ??= (_chunk) => {};
	if (typeof flush !== "function") {
		streamOptions = flush;
		flush = undefined;
	}
	const { signal } = streamOptions ?? {};
	return new TransformStream(
		{
			start(controller) {
				if (signal) {
					signal.addEventListener("abort", () => {
						controller.error(
							signal.reason ?? new DOMException("Aborted", "AbortError"),
						);
					});
				}
			},
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
	const { signal } = streamOptions ?? {};
	return new TransformStream(
		{
			start(controller) {
				if (signal) {
					signal.addEventListener("abort", () => {
						controller.error(
							signal.reason ?? new DOMException("Aborted", "AbortError"),
						);
					});
				}
			},
			async transform(chunk, controller) {
				const enqueue = (chunk) => {
					controller.enqueue(chunk);
				};
				await transform(chunk, enqueue);
			},
			async flush(controller) {
				if (flush) {
					const enqueue = (chunk) => {
						controller.enqueue(chunk);
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
	const { signal } = streamOptions ?? {};
	return new WritableStream(
		{
			start(controller) {
				if (signal) {
					signal.addEventListener("abort", () => {
						controller.error(
							signal.reason ?? new DOMException("Aborted", "AbortError"),
						);
					});
				}
			},
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

export const timeout = (ms, { signal } = {}) => {
	if (signal?.aborted) {
		return Promise.reject(
			new Error("Aborted", { cause: { code: "AbortError" } }),
		);
	}
	return new Promise((resolve, reject) => {
		const abortHandler = () => {
			clearTimeout(timerId);
			signal.removeEventListener("abort", abortHandler);
			reject(new Error("Aborted", { cause: { code: "AbortError" } }));
		};
		if (signal) signal.addEventListener("abort", abortHandler);
		const timerId = setTimeout(() => {
			if (signal) signal.removeEventListener("abort", abortHandler);
			resolve();
		}, ms);
	});
};
