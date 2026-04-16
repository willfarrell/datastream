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

export const streamToArray = async (stream, { maxBufferSize } = {}) => {
	const value = [];
	let size = 0;
	for await (const chunk of stream) {
		if (maxBufferSize != null) {
			size += chunk?.length ?? chunk?.byteLength ?? 1;
			if (size > maxBufferSize) {
				throw new Error(
					`streamToArray buffer exceeds maxBufferSize (${maxBufferSize})`,
				);
			}
		}
		value.push(chunk);
	}
	return value;
};

export const streamToObject = async (stream, { maxBufferSize } = {}) => {
	const value = Object.create(null);
	let size = 0;
	for await (const chunk of stream) {
		if (maxBufferSize != null) {
			size += chunk?.length ?? chunk?.byteLength ?? 1;
			if (size > maxBufferSize) {
				throw new Error(
					`streamToObject buffer exceeds maxBufferSize (${maxBufferSize})`,
				);
			}
		}
		Object.assign(value, chunk);
	}
	return { ...value };
};

export const streamToString = async (stream, { maxBufferSize } = {}) => {
	const chunks = [];
	let size = 0;
	for await (const chunk of stream) {
		if (maxBufferSize != null) {
			size += chunk?.length ?? chunk?.byteLength ?? 0;
			if (size > maxBufferSize) {
				throw new Error(
					`streamToString buffer exceeds maxBufferSize (${maxBufferSize})`,
				);
			}
		}
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
	const size = chunkSize != null ? () => chunkSize : undefined;
	return {
		writableStrategy: {
			highWaterMark,
			size,
		},
		readableStrategy: {
			highWaterMark,
			size,
		},
		signal,
		...streamOptions,
	};
};

export const createReadableStream = (input, streamOptions = {}) => {
	const maxQueueSize = streamOptions.highWaterMark ?? 1024;
	const chunkSize = streamOptions?.chunkSize ?? 16_384; // 16KB
	if (chunkSize <= 0) throw new Error("chunkSize must be a positive number");
	const queued = [];
	const { readableStrategy } = makeOptions(streamOptions);
	const stream = new ReadableStream(
		{
			async start(controller) {
				while (queued.length) {
					const chunk = queued.shift();
					controller.enqueue(chunk);
				}
				if (typeof input === "string") {
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
				} else if (typeof input === "object" && input.byteLength) {
					const bytes = new Uint8Array(input.buffer ?? input);
					let position = 0;
					const length = bytes.byteLength;
					while (position < length) {
						controller.enqueue(bytes.subarray(position, position + chunkSize));
						position += chunkSize;
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
		readableStrategy,
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
	const { writableStrategy, readableStrategy } = makeOptions(streamOptions);
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
		writableStrategy,
		readableStrategy,
	);
};

export const createTransformStream = (transform, flush, streamOptions) => {
	transform ??= (chunk, enqueue) => enqueue(chunk);
	if (typeof flush !== "function") {
		streamOptions = flush;
		flush = undefined;
	}
	const { signal } = streamOptions ?? {};
	const { writableStrategy, readableStrategy } = makeOptions(streamOptions);
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
		writableStrategy,
		readableStrategy,
	);
};

export const createWritableStream = (write, close, streamOptions) => {
	write ??= () => {};
	if (typeof close !== "function") {
		streamOptions = close;
		close = undefined;
	}
	const { signal } = streamOptions ?? {};
	const { writableStrategy } = makeOptions(streamOptions);
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
		writableStrategy,
	);
};

export const timeout = (ms, { signal } = {}) => {
	if (signal?.aborted) {
		return Promise.reject(
			new Error("Aborted", { cause: { code: "AbortError" } }),
		);
	}
	return new Promise((resolve, reject) => {
		let settled = false;
		const abortHandler = () => {
			if (settled) return;
			settled = true;
			clearTimeout(timerId);
			signal.removeEventListener("abort", abortHandler);
			reject(new Error("Aborted", { cause: { code: "AbortError" } }));
		};
		if (signal) signal.addEventListener("abort", abortHandler);
		const timerId = setTimeout(() => {
			if (settled) return;
			settled = true;
			if (signal) signal.removeEventListener("abort", abortHandler);
			resolve();
		}, ms);
	});
};
