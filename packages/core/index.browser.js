// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
/* global ReadableStream, TransformStream, WritableStream */
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
	// Work on a copy so appending the terminal writable doesn't mutate the
	// caller's array.
	streams = [...streams];
	// Ensure stream ends with only writable
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
	// An initial value so the Promise check also runs for index 0 (a source
	// passed without await, e.g. an async stream factory).
	return streams.reduce((pipeline, stream, idx) => {
		if (typeof stream.then === "function") {
			throw new Error(`Promise instead of stream passed in at index ${idx}`);
		}
		if (idx === 0) {
			return stream;
		}
		// A WritableStream can only ever be the terminal; anywhere else
		// pipeThrough would reject it anyway.
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
	// undefined (the default) and null are both unlimited.
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
	// undefined (the default) and null are both unlimited.
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
	// undefined (the default) and null are both unlimited.
	const limit = maxBufferSize ?? Number.POSITIVE_INFINITY;
	const chunks = [];
	let size = 0;
	// A streaming decoder so multibyte sequences split across byte chunks
	// decode correctly. Mirrors Node's Buffer.concat(...).toString() semantics:
	// byte chunks are decoded as UTF-8, anything else is String()-ified.
	const decoder = createChunkDecoder();
	for await (const chunk of stream) {
		size += chunk?.length ?? chunk?.byteLength ?? 0;
		if (size > limit) {
			throw new RangeError(
				`streamToString buffer exceeds maxBufferSize (${maxBufferSize})`,
			);
		}
		if (ArrayBuffer.isView(chunk) || chunk instanceof ArrayBuffer) {
			// Decoded here: otherwise Array.join would coerce a typed array of
			// raw bytes into comma-separated decimal byte codes.
			chunks.push(decoder.decode(chunk));
		} else {
			// Array.prototype.join semantics (node parity): null/undefined -> "".
			chunks.push(`${chunk ?? ""}`);
		}
	}
	// Flush any bytes buffered by the streaming decoder.
	chunks.push(decoder.flush());
	return chunks.join("");
};

// Browser parity for the Node streamToBuffer. There is no Buffer in the web
// runtime, so this returns a concatenated Uint8Array (Node's Buffer is itself
// a Uint8Array, so byte consumers behave identically across builds).
export const streamToBuffer = async (stream, { maxBufferSize } = {}) => {
	// undefined (the default) and null are both unlimited.
	const limit = maxBufferSize ?? Number.POSITIVE_INFINITY;
	const value = [];
	let size = 0;
	for await (const chunk of stream) {
		// Bytes keep their raw byte window (any TypedArray/DataView/ArrayBuffer);
		// anything else is text. null/undefined -> empty (node parity).
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

// `?.` so null/undefined return false instead of throwing (Node's instanceof
// check safely returns false).
export const isReadable = (stream) => {
	return stream instanceof ReadableStream || !!stream?.readable;
};

export const isWritable = (stream) => {
	return stream instanceof WritableStream || !!stream?.writable;
};

// chunkSize is only createReadableStream's slice size, never a queuing
// strategy size(): highWaterMark counts chunks, as in the node build.
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
	const chunkSize = streamOptions.chunkSize ?? 16_384; // 16KB
	if (chunkSize <= 0) throw new Error("chunkSize must be a positive number");
	const { signal } = streamOptions;
	const { readableStrategy } = makeOptions(streamOptions);
	// Iterable inputs are pulled one next() per pull() (node's Readable.from
	// parity): upstream (a paginated fetch, a DB cursor) only advances on
	// demand, and cancel()/abort stop it via iterator.return().
	let iterator;
	let controller;
	const cleanup = () => signal?.removeEventListener("abort", onAbort);
	const close = () => {
		cleanup();
		controller.close();
	};
	const fail = (e) => {
		cleanup();
		controller.error(e);
		return iterator?.return?.();
	};
	const onAbort = () => fail(signal.reason);
	const stream = new ReadableStream(
		{
			start(c) {
				controller = c;
				if (signal?.aborted) return onAbort();
				// cleanup() removes the listener on every terminal path.
				signal?.addEventListener("abort", onAbort);
				// No input => manual-push mode (mirrors the node build's undefined
				// branch): leave the stream open for stream.push().
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
					// Arrays included: pulled lazily like node's Readable.from.
					iterator = input[Symbol.iterator]();
				} else {
					// number/boolean/symbol/null/non-iterable object: a missing branch
					// previously left the stream open forever (a silent hang). Error
					// promptly to match Node's immediate throw.
					fail(new TypeError("createReadableStream: unsupported input type"));
				}
			},
			async pull(controller) {
				// Manual-push mode has no iterator; push() enqueues directly.
				if (!iterator) return;
				let next;
				try {
					next = await iterator.next();
				} catch (e) {
					// A throwing source errors the stream without close()/cancel(), so
					// drop the abort listener here too.
					cleanup();
					throw e;
				}
				const { value, done } = next;
				if (done) close();
				else controller.enqueue(value);
			},
			cancel() {
				cleanup();
				return iterator?.return?.();
			},
		},
		readableStrategy,
	);
	stream.push = (chunk) => {
		if (chunk === null) return close();
		// desiredSize = highWaterMark - queued, so this mirrors the node build's
		// readableLength check. (Node closes the controller synchronously in
		// start(), so `controller` is always set here.)
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
	// Node parity: the default returns the chunk, so a thenable chunk's own
	// rejection surfaces when transform() awaits it.
	passThrough ??= (chunk) => chunk;
	if (typeof flush !== "function") {
		streamOptions = flush;
		flush = undefined;
	}
	const { signal } = streamOptions ?? {};
	const { writableStrategy, readableStrategy } = makeOptions(streamOptions);
	// Track the abort listener so we can remove it once the stream settles;
	// otherwise a shared signal accumulates a listener per constructed stream.
	// cleanup() is idempotent and is invoked from EVERY terminal path (clean
	// flush, downstream cancel, and a thrown transform/flush callback), not just
	// the clean flush — otherwise an errored/cancelled stream leaks its listener.
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
					// Already aborted: the event won't fire again (node parity).
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
			async flush(controller) {
				cleanup();
				signal?.throwIfAborted();
				if (flush) {
					await flush();
				}
				controller.terminate();
			},
			// Called when the readable side is cancelled or the writable side is
			// aborted/errored (e.g. a downstream sink errors).
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
	// See createPassThroughStream for why cleanup() is wired into every terminal
	// path rather than only the clean flush.
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
					// Already aborted: the event won't fire again (node parity).
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
				controller.terminate();
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
	// A no-op default keeps runAbort branch-free (identical in both builds).
	const { signal, abort = () => {} } = streamOptions ?? {};
	const { writableStrategy } = makeOptions(streamOptions);
	// See createPassThroughStream for why cleanup() is wired into every terminal
	// path (clean close, abort, and a thrown write/close callback).
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
					// Already aborted: the event won't fire again, so run the same
					// handler now (node parity: its Writable destroys immediately).
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
				// A fired signal already ran it from onAbort.
				if (!signal?.aborted) await runAbort(abort, reason);
			},
		},
		writableStrategy,
	);
};

// *** Shared helpers ***
export {
	concatBytes,
	createChunkDecoder,
	resolveLazy,
	timeout,
} from "./helpers.js";
