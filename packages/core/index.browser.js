// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
import {
	concatBytes,
	createChunkDecoder,
	runAbort,
	sanitizeObject,
} from "./helpers.js";

// Abort errors a pipe destination one macrotask late. Node's pipeTo writes a
// chunk it already read into a destination errored in the meantime, and that
// write's PromiseReject surfaces as an unhandledRejection even though pipeTo
// marks it handled. Deferring lets in-flight chunks land first; the pipe then
// sees the error and shuts down before reading another.
// ponytail: workaround for a Node webstreams bug (seen on 26.5), drop once fixed upstream.
const errorOnAbort = (controller, reason) =>
	setTimeout(() => controller.error(reason));

export const pipeline = async (streams, streamOptions = {}) => {
	streams = [...streams];
	const lastStream = streams[streams.length - 1];
	if (isReadable(lastStream)) {
		// Web Streams have no objectMode flag, so (unlike the Node build, which
		// derives objectMode from the source) the auto-appended terminal sink
		// intentionally just forwards the caller's streamOptions.
		streams.push(createWritableStream(() => {}, streamOptions));
	}

	// The signal goes to the terminal pipeTo only (even a caller-supplied
	// writable, not just the appended sink). Aborting it cancels upstream through
	// every pipeThrough, reaching the source's cancel() / iterator return().
	// Handing the signal to each pipeThrough as well races that propagation and
	// leaves the source un-cancelled (leaked paginated fetches / DB cursors).
	await join(streams, { signal: streamOptions.signal });
	return result(streams);
};

export const pipejoin = (streams) => join(streams);

const join = (streams, pipeOptions) => {
	return streams.reduce((pipeline, stream, idx) => {
		if (typeof stream.then === "function") {
			throw new Error(`Promise instead of stream passed in at index ${idx}`);
		}
		if (idx === 0) {
			return stream;
		}
		if (stream.getWriter) {
			return pipeline.pipeTo(stream, pipeOptions);
		}
		return pipeline.pipeThrough(stream);
	}, undefined);
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
	const limit = maxBufferSize ?? Number.POSITIVE_INFINITY;
	const value = [];
	let size = 0;
	for await (const chunk of stream) {
		size += chunk?.length ?? chunk?.byteLength ?? 1;
		if (size > limit) {
			throw new RangeError(
				`streamToArray buffer exceeds maxBufferSize (${maxBufferSize})`,
			);
		}
		value.push(chunk);
	}
	return value;
};

export const streamToObject = async (stream, { maxBufferSize } = {}) => {
	const limit = maxBufferSize ?? Number.POSITIVE_INFINITY;
	const value = Object.create(null);
	let size = 0;
	for await (const chunk of stream) {
		size += chunk?.length ?? chunk?.byteLength ?? 1;
		if (size > limit) {
			throw new RangeError(
				`streamToObject buffer exceeds maxBufferSize (${maxBufferSize})`,
			);
		}
		Object.assign(value, chunk);
	}
	return sanitizeObject(value);
};

export const streamToString = async (stream, { maxBufferSize } = {}) => {
	const limit = maxBufferSize ?? Number.POSITIVE_INFINITY;
	const chunks = [];
	let size = 0;
	const decoder = createChunkDecoder();
	for await (const chunk of stream) {
		size += chunk?.length ?? chunk?.byteLength ?? 0;
		if (size > limit) {
			throw new RangeError(
				`streamToString buffer exceeds maxBufferSize (${maxBufferSize})`,
			);
		}
		if (ArrayBuffer.isView(chunk) || chunk instanceof ArrayBuffer) {
			chunks.push(decoder.decode(chunk));
		} else {
			chunks.push(`${chunk ?? ""}`);
		}
	}
	chunks.push(decoder.flush());
	return chunks.join("");
};

export const streamToBuffer = async (stream, { maxBufferSize } = {}) => {
	const limit = maxBufferSize ?? Number.POSITIVE_INFINITY;
	const value = [];
	let size = 0;
	for await (const chunk of stream) {
		const buf = ArrayBuffer.isView(chunk)
			? new Uint8Array(chunk.buffer, chunk.byteOffset, chunk.byteLength)
			: chunk instanceof ArrayBuffer
				? new Uint8Array(chunk)
				: new TextEncoder().encode(chunk ?? "");
		size += buf.byteLength;
		if (size > limit) {
			throw new RangeError(
				`streamToBuffer buffer exceeds maxBufferSize (${maxBufferSize})`,
			);
		}
		value.push(buf);
	}
	return concatBytes(value);
};

export const isReadable = (stream) => {
	return stream instanceof ReadableStream || !!stream?.readable;
};

export const isWritable = (stream) => {
	return stream instanceof WritableStream || !!stream?.writable;
};

export const makeOptions = ({
	highWaterMark,
	signal,
	...streamOptions
} = {}) => {
	return {
		writableStrategy: { highWaterMark },
		readableStrategy: { highWaterMark },
		signal,
		...streamOptions,
	};
};

export const createReadableStream = (input, streamOptions = {}) => {
	const maxQueueSize = streamOptions.highWaterMark ?? 1024;
	const chunkSize = streamOptions.chunkSize ?? 16_384;
	if (chunkSize <= 0) throw new Error("chunkSize must be a positive number");
	const { signal } = streamOptions;
	const { readableStrategy } = makeOptions(streamOptions);
	let iterator;
	let controller;
	let closed = false;
	const cleanup = () => signal?.removeEventListener("abort", onAbort);
	const close = () => {
		closed = true;
		cleanup();
		controller.close();
	};
	const fail = (e) => {
		closed = true;
		cleanup();
		controller.error(e);
		return iterator?.return?.();
	};
	const onAbort = () => fail(signal.reason);
	// Returns whether the queue still wants more, so a sync pull() keeps going.
	// close() with chunks still queued only requests the close (desiredSize
	// stays positive), hence the explicit false.
	const settle = ({ value, done }) => {
		if (done) {
			close();
			return false;
		}
		controller.enqueue(value);
		return controller.desiredSize > 0;
	};
	const stream = new ReadableStream(
		{
			start(c) {
				controller = c;
				if (signal?.aborted) return onAbort();
				signal?.addEventListener("abort", onAbort);
				if (input === undefined) return;
				if (typeof input === "string") {
					let position = 0;
					const length = input.length;
					while (position < length) {
						const chunk = input.substring(position, position + chunkSize);
						controller.enqueue(chunk);
						position += chunkSize;
					}
					close();
				} else if (typeof input?.byteLength === "number") {
					// ArrayBuffer / SharedArrayBuffer / any view, zero-length included
					// (node parity): an empty ArrayBuffer or DataView is not iterable
					// and would otherwise error as unsupported.
					// Honor the view's byteOffset/byteLength; constructing
					// new Uint8Array(input.buffer) over the whole backing buffer would
					// leak adjacent heap (pooled Buffers / .subarray() views) and
					// diverge from the Node build.
					const bytes = ArrayBuffer.isView(input)
						? new Uint8Array(input.buffer, input.byteOffset, input.byteLength)
						: new Uint8Array(input);
					let position = 0;
					const length = bytes.byteLength;
					while (position < length) {
						controller.enqueue(bytes.subarray(position, position + chunkSize));
						position += chunkSize;
					}
					close();
				} else if (typeof input?.[Symbol.asyncIterator] === "function") {
					iterator = input[Symbol.asyncIterator]();
				} else if (typeof input?.[Symbol.iterator] === "function") {
					iterator = input[Symbol.iterator]();
				} else {
					fail(new TypeError("createReadableStream: unsupported input type"));
				}
			},
			pull() {
				if (!iterator) return;
				let next;
				do {
					try {
						next = iterator.next();
					} catch (e) {
						cleanup();
						throw e;
					}
					if (typeof next.then === "function") {
						return next.then(settle, (e) => {
							cleanup();
							throw e;
						});
					}
				} while (settle(next));
			},
			cancel() {
				closed = true;
				cleanup();
				return iterator?.return?.();
			},
		},
		readableStrategy,
	);
	stream.push = (chunk) => {
		if (closed) return;
		if (chunk === null) return close();
		const queueSize =
			(readableStrategy.highWaterMark ?? 1) - controller.desiredSize;
		if (queueSize >= maxQueueSize) {
			throw new Error(
				`createReadableStream queue size (${queueSize}) exceeds limit (${maxQueueSize})`,
			);
		}
		controller.enqueue(chunk);
	};
	return stream;
};

export const createPassThroughStream = (passThrough, flush, streamOptions) => {
	passThrough ??= (chunk) => chunk;
	if (typeof flush !== "function") {
		streamOptions = flush;
		flush = undefined;
	}
	const { signal } = streamOptions ?? {};
	const { writableStrategy, readableStrategy } = makeOptions(streamOptions);
	let onAbort;
	const cleanup = () => {
		if (onAbort) {
			signal.removeEventListener("abort", onAbort);
			onAbort = undefined;
		}
	};
	return new TransformStream(
		{
			start(controller) {
				if (signal) {
					onAbort = () => errorOnAbort(controller, signal.reason);
					if (signal.aborted) return onAbort();
					signal.addEventListener("abort", onAbort);
				}
			},
			async transform(chunk, controller) {
				try {
					signal?.throwIfAborted();
					await passThrough(chunk);
				} catch (e) {
					cleanup();
					throw e;
				}
				controller.enqueue(chunk);
			},
			async flush() {
				cleanup();
				signal?.throwIfAborted();
				if (flush) {
					await flush();
				}
			},
			cancel() {
				cleanup();
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
	let onAbort;
	const cleanup = () => {
		if (onAbort) {
			signal.removeEventListener("abort", onAbort);
			onAbort = undefined;
		}
	};
	return new TransformStream(
		{
			start(controller) {
				if (signal) {
					onAbort = () => errorOnAbort(controller, signal.reason);
					if (signal.aborted) return onAbort();
					signal.addEventListener("abort", onAbort);
				}
			},
			async transform(chunk, controller) {
				const enqueue = (chunk) => {
					controller.enqueue(chunk);
				};
				try {
					signal?.throwIfAborted();
					await transform(chunk, enqueue);
				} catch (e) {
					cleanup();
					throw e;
				}
			},
			async flush(controller) {
				cleanup();
				signal?.throwIfAborted();
				if (flush) {
					const enqueue = (chunk) => {
						controller.enqueue(chunk);
					};
					await flush(enqueue);
				}
			},
			cancel() {
				cleanup();
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
	const { signal, abort = () => {} } = streamOptions ?? {};
	const { writableStrategy } = makeOptions(streamOptions);
	let onAbort;
	const cleanup = () => {
		if (onAbort) {
			signal.removeEventListener("abort", onAbort);
			onAbort = undefined;
		}
	};
	return new WritableStream(
		{
			start(controller) {
				if (signal) {
					// controller.error() never reaches the sink's abort(), so the
					// caller's abort hook runs here too (node parity: the signal
					// destroys the node Writable, which runs it).
					onAbort = () => {
						errorOnAbort(controller, signal.reason);
						runAbort(abort, signal.reason);
					};
					if (signal.aborted) return onAbort();
					signal.addEventListener("abort", onAbort);
				}
			},
			async write(chunk) {
				try {
					signal?.throwIfAborted();
					await write(chunk);
				} catch (e) {
					cleanup();
					throw e;
				}
			},
			async close() {
				cleanup();
				signal?.throwIfAborted();
				if (close) {
					await close();
				}
			},
			// Called when the stream is aborted (writer.abort(), or pipeTo
			// propagating an upstream error / its signal), where close() would
			// never run. Not called when our own write/close threw.
			async abort(reason) {
				cleanup();
				if (!signal?.aborted) await runAbort(abort, reason);
			},
		},
		writableStrategy,
	);
};

export {
	concatBytes,
	createChunkDecoder,
	resolveLazy,
	timeout,
} from "./helpers.js";
