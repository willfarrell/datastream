// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
import { Readable, Transform, Writable } from "node:stream";
import { pipeline as pipelinePromise } from "node:stream/promises";
import { createChunkDecoder, runAbort, sanitizeObject } from "./helpers.js";

// Node.js streams interpret push(null) as EOF.
// Use a sentinel so null values flow through object-mode streams.
const NULL_SENTINEL = Symbol.for("@datastream/null");
const toSafe = (v) => (v === null ? NULL_SENTINEL : v);
const fromSafe = (v) => (v === NULL_SENTINEL ? null : v);

export const pipeline = async (streams, streamOptions = {}) => {
	for (let idx = 0, l = streams.length; idx < l; idx++) {
		if (typeof streams[idx].then === "function") {
			throw new Error(`Promise instead of stream passed in at index ${idx}`);
		}
	}
	// Work on copies so appending the terminal writable (and deriving its
	// objectMode) doesn't mutate the caller's array or options object.
	streams = [...streams];
	// Ensure stream ends with only writable
	const lastStream = streams[streams.length - 1];
	if (isReadable(lastStream)) {
		streamOptions = {
			...streamOptions,
			objectMode: lastStream._readableState.objectMode,
		};
		streams.push(createWritableStream(() => {}, streamOptions));
	}
	await pipelinePromise(streams, streamOptions);
	return result(streams);
};

export const pipejoin = (streams) => {
	for (let idx = 0, l = streams.length; idx < l; idx++) {
		if (typeof streams[idx].then === "function") {
			throw new Error(`Promise instead of stream passed in at index ${idx}`);
		}
	}
	// Destroy every stream on the first error (bare .pipe() would leave the
	// source producing into a dead chain). The error then surfaces on the
	// returned stream, like the browser build. Re-entry from the destroys'
	// own 'error' events is a no-op thanks to the destroyed check.
	const teardown = (error) => {
		for (const stream of streams) {
			if (!stream.destroyed) stream.destroy(error);
		}
	};
	let pipeline = streams[0];
	pipeline.on("error", teardown);
	for (let idx = 1, l = streams.length; idx < l; idx++) {
		pipeline = pipeline.pipe(streams[idx]);
		pipeline.on("error", teardown);
	}
	return pipeline;
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

// Not possible in WebStream
export const backpressureGauge = (streams) => {
	const keys = Object.keys(streams);
	const values = Object.values(streams);
	const metrics = {};
	for (let i = 0, l = values.length; i < l; i++) {
		const value = values[i];
		metrics[keys[i]] = { timeline: [], total: {} };
		let timestamp;
		const startTimestamp = Date.now();
		value.on("pause", () => {
			timestamp = Date.now(); // process.hrtime.bigint()
		});
		value.on("resume", () => {
			if (timestamp) {
				// Number.parseInt(  (process.hrtime.bigint() - pauseTimestamp).toString() , 10 ) / 1_000_000 // ms
				const duration = Date.now() - timestamp;
				metrics[keys[i]].timeline.push({ timestamp, duration });
				// Clear so a second resume without an intervening pause can't
				// record a phantom interval from the stale pause timestamp.
				timestamp = undefined;
			}
		});
		// Readable streams emit 'end'; writable (sink) streams emit 'finish'
		// and never 'end', so listen for both (plus 'close' as a backstop) to
		// record total duration for writable/duplex nodes too. Guard against
		// double-recording when more than one terminal event fires.
		const recordTotal = () => {
			if (metrics[keys[i]].total.timestamp !== undefined) return;
			const duration = Date.now() - startTimestamp;
			metrics[keys[i]].total = { timestamp: startTimestamp, duration };
		};
		value.on("end", recordTotal);
		value.on("finish", recordTotal);
		value.on("close", recordTotal);
	}
	return metrics;
};

// The .on("data") branches look redundant (Node Readables are async-iterable)
// but are kept on purpose: benchmarked on Node 26, collecting via for-await is
// 2-3x slower for object streams (e.g. 10k objects: ~1.3k -> ~0.45k ops/s).
// They also drain plain EventEmitters, which are not async-iterable.
export const streamToArray = (stream, { maxBufferSize } = {}) => {
	// undefined (the default) and null are both unlimited.
	const limit = maxBufferSize ?? Number.POSITIVE_INFINITY;
	if (typeof stream.on === "function") {
		return new Promise((resolve, reject) => {
			const value = [];
			let size = 0;
			stream.on("data", (chunk) => {
				size += chunk?.length ?? chunk?.byteLength ?? 1;
				if (size > limit) {
					stream.destroy(
						new RangeError(
							`streamToArray buffer exceeds maxBufferSize (${maxBufferSize})`,
						),
					);
					return;
				}
				value.push(fromSafe(chunk));
			});
			stream.on("end", () => {
				resolve(value);
			});
			stream.on("error", reject);
		});
	}
	return (async () => {
		const value = [];
		let size = 0;
		for await (const chunk of stream) {
			size += chunk?.length ?? chunk?.byteLength ?? 1;
			if (size > limit) {
				throw new RangeError(
					`streamToArray buffer exceeds maxBufferSize (${maxBufferSize})`,
				);
			}
			// Decode the null sentinel here too so both consumption paths agree.
			value.push(fromSafe(chunk));
		}
		return value;
	})();
};

export const streamToObject = (stream, { maxBufferSize } = {}) => {
	// undefined (the default) and null are both unlimited.
	const limit = maxBufferSize ?? Number.POSITIVE_INFINITY;
	if (typeof stream.on === "function") {
		return new Promise((resolve, reject) => {
			const value = Object.create(null);
			let size = 0;
			stream.on("data", (chunk) => {
				size += chunk?.length ?? chunk?.byteLength ?? 1;
				if (size > limit) {
					stream.destroy(
						new RangeError(
							`streamToObject buffer exceeds maxBufferSize (${maxBufferSize})`,
						),
					);
					return;
				}
				// Unwrap the null sentinel so a null-bearing object stream doesn't
				// leak the Symbol; Object.assign(value, null) is a safe no-op.
				Object.assign(value, fromSafe(chunk));
			});
			stream.on("end", () => {
				resolve(sanitizeObject(value));
			});
			stream.on("error", reject);
		});
	}
	return (async () => {
		const value = Object.create(null);
		let size = 0;
		for await (const chunk of stream) {
			size += chunk?.length ?? chunk?.byteLength ?? 1;
			if (size > limit) {
				throw new RangeError(
					`streamToObject buffer exceeds maxBufferSize (${maxBufferSize})`,
				);
			}
			Object.assign(value, fromSafe(chunk));
		}
		return sanitizeObject(value);
	})();
};

export const streamToString = (stream, { maxBufferSize } = {}) => {
	// undefined (the default) and null are both unlimited.
	const limit = maxBufferSize ?? Number.POSITIVE_INFINITY;
	// A streaming decoder so multibyte sequences split across byte chunks
	// decode correctly (browser parity); decoding each Buffer on its own turns a
	// split "é" into "\ufffd\ufffd". flush() emits any trailing partial bytes.
	const decoder = createChunkDecoder();
	const decode = (chunk) =>
		ArrayBuffer.isView(chunk) || chunk instanceof ArrayBuffer
			? decoder.decode(chunk)
			: // Array.prototype.join semantics: null/undefined -> "".
				`${chunk ?? ""}`;
	if (typeof stream.on === "function") {
		return new Promise((resolve, reject) => {
			const chunks = [];
			let size = 0;
			stream.on("data", (chunk) => {
				size += chunk?.length ?? chunk?.byteLength ?? 0;
				if (size > limit) {
					stream.destroy(
						new RangeError(
							`streamToString buffer exceeds maxBufferSize (${maxBufferSize})`,
						),
					);
					return;
				}
				// Unwrap the null sentinel; otherwise join("") throws
				// "Cannot convert a Symbol value to a string".
				chunks.push(decode(fromSafe(chunk)));
			});
			stream.on("end", () => {
				resolve(chunks.join("") + decoder.flush());
			});
			stream.on("error", reject);
		});
	}
	return (async () => {
		const chunks = [];
		let size = 0;
		for await (const chunk of stream) {
			size += chunk?.length ?? chunk?.byteLength ?? 0;
			if (size > limit) {
				throw new RangeError(
					`streamToString buffer exceeds maxBufferSize (${maxBufferSize})`,
				);
			}
			chunks.push(decode(fromSafe(chunk)));
		}
		return chunks.join("") + decoder.flush();
	})();
};

export const streamToBuffer = (stream, { maxBufferSize } = {}) => {
	// undefined (the default) and null are both unlimited.
	const limit = maxBufferSize ?? Number.POSITIVE_INFINITY;
	if (typeof stream.on === "function") {
		return new Promise((resolve, reject) => {
			const value = [];
			let size = 0;
			stream.on("data", (chunk) => {
				// Unwrap the null sentinel; Buffer.from(symbol) throws
				// ERR_INVALID_ARG_TYPE. fromSafe(null) -> null -> empty buffer.
				const buf = Buffer.from(fromSafe(chunk) ?? []);
				size += buf.length;
				if (size > limit) {
					stream.destroy(
						new RangeError(
							`streamToBuffer buffer exceeds maxBufferSize (${maxBufferSize})`,
						),
					);
					return;
				}
				value.push(buf);
			});
			stream.on("end", () => {
				resolve(Buffer.concat(value));
			});
			stream.on("error", reject);
		});
	}
	return (async () => {
		const value = [];
		let size = 0;
		for await (const chunk of stream) {
			const buf = Buffer.from(fromSafe(chunk) ?? []);
			size += buf.length;
			if (size > limit) {
				throw new RangeError(
					`streamToBuffer buffer exceeds maxBufferSize (${maxBufferSize})`,
				);
			}
			value.push(buf);
		}
		return Buffer.concat(value);
	})();
};

export const isReadable = (stream) => {
	return stream instanceof Readable;
};

export const isWritable = (stream) => {
	return stream instanceof Writable;
};

export const makeOptions = ({
	highWaterMark,
	chunkSize,
	objectMode,
	signal,
	...streamOptions
} = {}) => {
	objectMode ??= true;
	return {
		writableHighWaterMark: highWaterMark,
		writableObjectMode: objectMode,
		readableObjectMode: objectMode,
		readableHighWaterMark: highWaterMark,
		highWaterMark,
		chunkSize,
		objectMode,
		signal,
		...streamOptions,
	};
};

export const createReadableStream = (input, streamOptions = {}) => {
	if (input === undefined) {
		const maxQueueSize = streamOptions.highWaterMark ?? 1024;
		const stream = new Readable({
			objectMode: streamOptions.objectMode ?? true,
			highWaterMark: streamOptions.highWaterMark,
			// Browser parity: an abort destroys the push-mode stream (AbortError,
			// cause = signal.reason) instead of leaving consumers hanging.
			signal: streamOptions.signal,
			read() {},
		});
		const nativePush = Readable.prototype.push.bind(stream);
		stream.push = (chunk) => {
			if (chunk !== null && stream.readableLength >= maxQueueSize) {
				throw new Error(
					`createReadableStream queue size (${stream.readableLength}) exceeds limit (${maxQueueSize})`,
				);
			}
			return nativePush(chunk);
		};
		return stream;
	}
	// string doesn't chunk, and is slow
	if (typeof input === "string") {
		return Readable.from(chunkString(input, streamOptions), streamOptions);
	}
	// ArrayBuffer / SharedArrayBuffer / any view, even zero-length (which would
	// otherwise hit Readable.from and emit one empty Buffer, or throw for an
	// ArrayBuffer or DataView), streams its raw bytes.
	if (typeof input.byteLength === "number") {
		return Readable.from(chunkBytes(input, streamOptions), streamOptions);
	}
	if (Array.isArray(input)) {
		return Readable.from(input.map(toSafe), streamOptions);
	}
	return Readable.from(input, streamOptions);
};

// `?.`: createReadableStream(input, null) skips the `= {}` default.
const chunkSizeOf = (streamOptions) => {
	const size = streamOptions?.chunkSize ?? 16_384; // 16KB
	if (size <= 0) throw new Error("chunkSize must be a positive number");
	return size;
};

// Generators, but the size check runs eagerly (at createReadableStream time).
const chunkString = (input, streamOptions) => {
	const size = chunkSizeOf(streamOptions);
	return (function* () {
		let position = 0;
		const length = input.length;
		while (position < length) {
			yield input.substring(position, position + size);
			position += size;
		}
	})();
};

const chunkBytes = (input, streamOptions) => {
	const size = chunkSizeOf(streamOptions);
	// Honor the view's byteOffset/byteLength window over its raw bytes;
	// new Uint8Array(view) copies element values (Uint16 [0x0102] -> [2])
	// and yields nothing for a DataView.
	const bytes = ArrayBuffer.isView(input)
		? new Uint8Array(input.buffer, input.byteOffset, input.byteLength)
		: new Uint8Array(input);
	return (function* () {
		let position = 0;
		const length = bytes.byteLength;
		while (position < length) {
			yield bytes.subarray(position, position + size);
			position += size;
		}
	})();
};

export const createPassThroughStream = (passThrough, flush, streamOptions) => {
	passThrough ??= (chunk) => chunk;
	if (typeof flush !== "function") {
		streamOptions = flush;
		flush = undefined;
	}
	return new Transform({
		...makeOptions(streamOptions),
		transform(chunk, _encoding, callback) {
			try {
				const result = passThrough(fromSafe(chunk));
				if (typeof result?.then === "function") {
					result.then(() => {
						this.push(chunk);
						callback();
					}, callback);
				} else {
					this.push(chunk);
					callback();
				}
			} catch (e) {
				callback(e);
			}
		},
		flush(callback) {
			try {
				if (flush) {
					const result = flush();
					if (typeof result?.then === "function") {
						result.then(() => callback(), callback);
					} else {
						callback();
					}
				} else {
					callback();
				}
			} catch (e) {
				callback(e);
			}
		},
	});
};

export const createTransformStream = (transform, flush, streamOptions) => {
	transform ??= (chunk, enqueue) => enqueue(chunk);
	if (typeof flush !== "function") {
		streamOptions = flush;
		flush = undefined;
	}
	const stream = new Transform({
		...makeOptions(streamOptions),
		transform(chunk, _encoding, callback) {
			try {
				const result = transform(fromSafe(chunk), enqueue);
				if (typeof result?.then === "function") {
					result.then(() => callback(), callback);
				} else {
					callback();
				}
			} catch (e) {
				callback(e);
			}
		},
		flush(callback) {
			try {
				if (flush) {
					const result = flush(enqueue);
					if (typeof result?.then === "function") {
						result.then(() => callback(), callback);
					} else {
						callback();
					}
				} else {
					callback();
				}
			} catch (e) {
				callback(e);
			}
		},
	});
	const enqueue = (chunk, encoding) => {
		stream.push(toSafe(chunk), encoding);
	};
	return stream;
};

export const createWritableStream = (write, final, streamOptions) => {
	write ??= () => {};
	if (typeof final !== "function") {
		streamOptions = final;
		final = undefined;
	}
	const { abort, ...options } = streamOptions ?? {};
	// Set when our own write/final callback fails: that teardown is the stream
	// erroring, not an abort, so abort() is skipped (browser parity, where a
	// failing sink write/close never reaches the sink's abort()).
	let failed = false;
	const fail = (callback) => (e) => {
		failed = true;
		callback(e);
	};
	const settle = (result, callback) => {
		if (typeof result?.then === "function") {
			result.then(() => callback(), fail(callback));
		} else {
			callback();
		}
	};
	return new Writable({
		...makeOptions(options),
		write(chunk, _encoding, callback) {
			try {
				settle(write(fromSafe(chunk)), callback);
			} catch (e) {
				fail(callback)(e);
			}
		},
		final(callback) {
			try {
				settle(final?.(), callback);
			} catch (e) {
				fail(callback)(e);
			}
		},
		// Only installed with an abort hook, so a caller's own streamOptions
		// .destroy still passes through otherwise. destroy() also runs after a
		// clean finish (autoDestroy); only a teardown before 'finish' that we
		// didn't cause is an abort.
		...(abort && {
			destroy(error, callback) {
				if (failed || this.writableFinished) return callback(error);
				runAbort(abort, error ?? undefined).then(() => callback(error));
			},
		}),
	});
};

// *** Shared helpers ***
export {
	concatBytes,
	createChunkDecoder,
	resolveLazy,
	timeout,
} from "./helpers.js";
