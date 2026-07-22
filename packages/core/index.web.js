// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
/* global ReadableStream, TransformStream, WritableStream */

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
	// A streaming TextDecoder so multibyte sequences split across byte chunks
	// decode correctly. Mirrors Node's Buffer.concat(...).toString() semantics:
	// byte chunks are decoded as UTF-8, anything else is String()-ified.
	let decoder;
	for await (const chunk of stream) {
		if (maxBufferSize != null) {
			size += chunk?.length ?? chunk?.byteLength ?? 0;
			if (size > maxBufferSize) {
				throw new Error(
					`streamToString buffer exceeds maxBufferSize (${maxBufferSize})`,
				);
			}
		}
		if (ArrayBuffer.isView(chunk) || chunk instanceof ArrayBuffer) {
			// Without { stream: true } a typed array of raw bytes would be
			// coerced by Array.join into comma-separated decimal byte codes.
			decoder ??= new TextDecoder();
			chunks.push(decoder.decode(chunk, { stream: true }));
		} else {
			chunks.push(`${chunk}`);
		}
	}
	// Flush any bytes buffered by the streaming decoder.
	if (decoder) chunks.push(decoder.decode());
	return chunks.join("");
};

// Browser parity for the Node streamToBuffer. There is no Buffer in the web
// runtime, so this returns a concatenated Uint8Array (Node's Buffer is itself
// a Uint8Array, so byte consumers behave identically across builds).
export const streamToBuffer = async (stream, { maxBufferSize } = {}) => {
	const value = [];
	let size = 0;
	for await (const chunk of stream) {
		const buf =
			chunk instanceof Uint8Array ? chunk : new TextEncoder().encode(chunk);
		if (maxBufferSize != null) {
			size += buf.byteLength;
			if (size > maxBufferSize) {
				throw new Error(
					`streamToBuffer buffer exceeds maxBufferSize (${maxBufferSize})`,
				);
			}
		}
		value.push(buf);
	}
	const total = value.reduce((n, b) => n + b.byteLength, 0);
	const out = new Uint8Array(total);
	let offset = 0;
	for (const b of value) {
		out.set(b, offset);
		offset += b.byteLength;
	}
	return out;
};

export const isReadable = (stream) => {
	// Short-circuit non-objects so the `.readable` access can't throw on
	// null/undefined/primitives (Node's instanceof check safely returns false).
	if (stream == null || typeof stream !== "object") return false;
	return stream instanceof ReadableStream || !!stream.readable;
};

export const isWritable = (stream) => {
	if (stream == null || typeof stream !== "object") return false;
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
	// Shared drain logic for start() and pull() so both paths treat a queued
	// null identically as EOF. Returns false once it has closed the stream.
	const drainQueued = (controller) => {
		while (queued.length) {
			const chunk = queued.shift();
			if (chunk === null) {
				controller.close();
				return false;
			}
			controller.enqueue(chunk);
		}
		return true;
	};
	const stream = new ReadableStream(
		{
			async start(controller) {
				// Drain pre-start pushes using the SAME null-as-EOF semantics as
				// pull(); the WHATWG spec allows an async start(), so a terminating
				// null queued before start runs must still close the stream.
				if (!drainQueued(controller)) return;
				// No input => manual-push mode (mirrors the node build's undefined
				// branch): leave the stream open for stream.push()/pull().
				if (input === undefined) return;
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
				} else if (
					input != null &&
					typeof input === "object" &&
					input.byteLength
				) {
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
					controller.close();
				} else if (
					input != null &&
					["function", "object"].includes(typeof input) &&
					typeof input[Symbol.asyncIterator] === "function"
				) {
					for await (const chunk of input) {
						controller.enqueue(chunk);
					}
					controller.close();
				} else if (
					input != null &&
					typeof input === "object" &&
					typeof input[Symbol.iterator] === "function"
				) {
					for (const chunk of input) {
						controller.enqueue(chunk);
					}
					controller.close();
				} else {
					// number/boolean/symbol/null/non-iterable object: a missing branch
					// previously left the stream open forever (a silent hang). Error
					// promptly to match Node's immediate throw.
					controller.error(
						new TypeError("createReadableStream: unsupported input type"),
					);
				}
			},
			pull(controller) {
				drainQueued(controller);
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
					onAbort = () =>
						controller.error(
							signal.reason ?? new DOMException("Aborted", "AbortError"),
						);
					signal.addEventListener("abort", onAbort, { once: true });
				}
			},
			async transform(chunk, controller) {
				try {
					await passThrough(chunk);
				} catch (e) {
					cleanup();
					throw e;
				}
				controller.enqueue(chunk);
			},
			async flush(controller) {
				cleanup();
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
					onAbort = () =>
						controller.error(
							signal.reason ?? new DOMException("Aborted", "AbortError"),
						);
					signal.addEventListener("abort", onAbort, { once: true });
				}
			},
			async transform(chunk, controller) {
				const enqueue = (chunk) => {
					controller.enqueue(chunk);
				};
				try {
					await transform(chunk, enqueue);
				} catch (e) {
					cleanup();
					throw e;
				}
			},
			async flush(controller) {
				cleanup();
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
	const { signal } = streamOptions ?? {};
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
					onAbort = () =>
						controller.error(
							signal.reason ?? new DOMException("Aborted", "AbortError"),
						);
					signal.addEventListener("abort", onAbort, { once: true });
				}
			},
			async write(chunk) {
				try {
					await write(chunk);
				} catch (e) {
					cleanup();
					throw e;
				}
			},
			async close() {
				cleanup();
				if (close) {
					await close();
				}
			},
			// Called when the stream is aborted (signal fires or a downstream error
			// propagates), where close() would never run.
			abort() {
				cleanup();
			},
		},
		writableStrategy,
	);
};

// *** Shared helpers ***
export const resolveLazy = (value) =>
	typeof value === "function" ? value() : value;

export const shallowClone = (obj) => ({ ...obj });

export const deepClone = (obj) => {
	try {
		return structuredClone(obj);
	} catch (e) {
		throw new Error("Failed to clone chunk, possibly circular reference", {
			cause: e,
		});
	}
};

export const shallowEqual = (a, b) => {
	if (a === b) return true;
	if (a == null || b == null) return false;
	const keysA = Object.keys(a);
	if (keysA.length !== Object.keys(b).length) return false;
	for (const key of keysA) {
		if (a[key] !== b[key]) return false;
	}
	return true;
};

export const deepEqual = (a, b) => {
	try {
		return JSON.stringify(a) === JSON.stringify(b);
	} catch (e) {
		throw new Error("Failed to stringify chunk, possibly circular reference", {
			cause: e,
		});
	}
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
