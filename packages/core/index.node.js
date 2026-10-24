// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
import { Readable, Transform, Writable } from "node:stream";
import { finished, pipeline as pipelinePromise } from "node:stream/promises";
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
	// A stream built with an already-aborted signal is destroyed on creation.
	// Node's pipeline() then throws ERR_STREAM_UNABLE_TO_PIPE before listening
	// for 'error', so the stream's own AbortError went uncaught. Tear the node
	// streams down (web streams have no destroy()) and wait for each to close,
	// incl. async abort hooks, then reject with that error (browser parity).
	// A stream destroyed WITHOUT an error after push(null) but before "end"
	// (e.g. stop() then destroy() before flowing) is treated as finished by
	// pipe, which then never ends the destination and the pipeline hangs, so
	// reject it as a premature close like Node does for a destroyed source.
	const dead = streams.find((stream) => stream.destroyed);
	if (dead) {
		const error =
			dead.errored ??
			Object.assign(new Error("Premature close"), {
				code: "ERR_STREAM_PREMATURE_CLOSE",
			});
		await Promise.allSettled(
			streams
				.filter((stream) => stream.destroy)
				.map((stream) => finished(stream.destroy(error))),
		);
		throw error;
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

// A destroy() without an error emits 'close' but never 'end'/'error', so the
// .on("data") collectors would hang; reject like for-await / finished() do.
// 'close' after 'end' (or 'error') is a no-op: the promise already settled.
const prematureClose = () =>
	Object.assign(new Error("Premature close"), {
		code: "ERR_STREAM_PREMATURE_CLOSE",
	});

// The .on("data") branches look redundant (Node Readables are async-iterable)
// but are kept on purpose: benchmarked on Node 26, collecting via for-await is
// 2-3x slower for object streams (e.g. 10k objects: ~1.3k -> ~0.45k ops/s).
// They also drain plain EventEmitters, which are not async-iterable.
export const streamToArray = (stream, { maxBufferSize } = {}) => {
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
			stream.on("close", () => reject(prematureClose()));
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
			stream.on("close", () => reject(prematureClose()));
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
	const limit = maxBufferSize ?? Number.POSITIVE_INFINITY;
	const decoder = createChunkDecoder();
	const decode = (chunk) =>
		ArrayBuffer.isView(chunk) || chunk instanceof ArrayBuffer
			? decoder.decode(chunk)
			: `${chunk ?? ""}`;
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
			stream.on("close", () => reject(prematureClose()));
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

// Buffer.from(view) copies element values (Uint16 [0x6968] -> [0x68]) and
// yields nothing for a DataView: copy the view's raw byte window instead.
const toBuffer = (chunk) =>
	ArrayBuffer.isView(chunk)
		? Buffer.from(
				new Uint8Array(chunk.buffer, chunk.byteOffset, chunk.byteLength),
			)
		: Buffer.from(chunk ?? []);

export const streamToBuffer = (stream, { maxBufferSize } = {}) => {
	const limit = maxBufferSize ?? Number.POSITIVE_INFINITY;
	if (typeof stream.on === "function") {
		return new Promise((resolve, reject) => {
			const value = [];
			let size = 0;
			stream.on("data", (chunk) => {
				// Unwrap the null sentinel; Buffer.from(symbol) throws
				// ERR_INVALID_ARG_TYPE. fromSafe(null) -> null -> empty buffer.
				// A throw inside a 'data' listener escapes emit() as an uncaught
				// exception, so route it into the stream to reject the promise.
				let buf;
				try {
					buf = toBuffer(fromSafe(chunk));
				} catch (e) {
					stream.destroy(e);
					return;
				}
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
			stream.on("close", () => reject(prematureClose()));
		});
	}
	return (async () => {
		const value = [];
		let size = 0;
		for await (const chunk of stream) {
			const buf = toBuffer(fromSafe(chunk));
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
	// Iterables (arrays included) are pulled lazily so a yielded null flows
	// through as the sentinel instead of erroring ERR_STREAM_NULL_VALUES (browser
	// parity). Non-iterables fall through so Readable.from still throws
	// synchronously.
	if (typeof input[Symbol.asyncIterator] === "function") {
		return fromIterator(input[Symbol.asyncIterator](), streamOptions);
	}
	if (typeof input[Symbol.iterator] === "function") {
		return fromIterator(input[Symbol.iterator](), streamOptions);
	}
	return Readable.from(input, streamOptions);
};

// Pulls the iterator from read() directly instead of Readable.from(): its
// per-value try/catch, thenable probe and null check plus the null-safe
// generator wrapper cost ~40% on 10k-object arrays. Same contract as
// Readable.from: one next() in flight at a time, a promise value is awaited,
// a throwing/rejecting next() errors the stream, and destroy() runs the
// iterator's return() (generator finally blocks) before 'close'.
const fromIterator = (iterator, streamOptions) => {
	let reading = false;
	const stream = new Readable({
		objectMode: true,
		highWaterMark: 1,
		...streamOptions,
		read() {
			if (reading) return;
			reading = true;
			pull();
		},
		destroy(error, callback) {
			Promise.resolve()
				.then(() => iterator.return?.())
				.then(
					() => callback(error),
					(e) => callback(error ?? e),
				);
		},
	});
	const fail = (e) => stream.destroy(e);
	const push = (value) => {
		if (stream.push(toSafe(value))) return true;
		reading = false;
		return false;
	};
	const settle = ({ value, done }) => {
		if (done) {
			stream.push(null);
			return false;
		}
		if (typeof value?.then === "function") {
			value.then((v) => push(v) && pull(), fail);
			return false;
		}
		return push(value);
	};
	const pull = () => {
		for (;;) {
			let next;
			try {
				next = iterator.next();
			} catch (e) {
				return fail(e);
			}
			if (typeof next.then === "function") {
				return next.then((n) => settle(n) && pull(), fail);
			}
			if (!settle(next)) return;
		}
	};
	return stream;
};

const chunkSizeOf = (streamOptions) => {
	const size = streamOptions?.chunkSize ?? 16_384; // 16KB
	if (size <= 0) throw new Error("chunkSize must be a positive number");
	return size;
};

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
