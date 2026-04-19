// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
import { Readable, Transform, Writable } from "node:stream";
import { pipeline as pipelinePromise } from "node:stream/promises";

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
	// Ensure stream ends with only writable
	const lastStream = streams[streams.length - 1];
	if (isReadable(lastStream)) {
		streamOptions.objectMode = lastStream._readableState.objectMode;
		streams.push(createWritableStream(() => {}, streamOptions));
	}
	await pipelinePromise(streams, streamOptions);
	return result(streams);
};

export const pipejoin = (
	streams,
	onError = (e) => {
		process.nextTick(() => {
			throw e;
		});
	},
) => {
	const pipeline = streams.reduce((pipeline, stream, idx) => {
		if (typeof stream.then === "function") {
			throw new Error(`Promise instead of stream passed in at index ${idx}`);
		}
		return pipeline.pipe(stream).on("error", onError);
	});
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
			}
		});
		value.on("end", () => {
			const duration = Date.now() - startTimestamp;
			metrics[keys[i]].total = { timestamp: startTimestamp, duration };
		});
	}
	return metrics;
};

export const streamToArray = (stream, { maxBufferSize } = {}) => {
	if (typeof stream.on === "function") {
		return new Promise((resolve, reject) => {
			const value = [];
			let size = 0;
			stream.on("data", (chunk) => {
				if (maxBufferSize != null) {
					size += chunk?.length ?? chunk?.byteLength ?? 1;
					if (size > maxBufferSize) {
						stream.destroy(
							new Error(
								`streamToArray buffer exceeds maxBufferSize (${maxBufferSize})`,
							),
						);
						return;
					}
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
	})();
};

export const streamToObject = (stream, { maxBufferSize } = {}) => {
	if (typeof stream.on === "function") {
		return new Promise((resolve, reject) => {
			const value = Object.create(null);
			let size = 0;
			stream.on("data", (chunk) => {
				if (maxBufferSize != null) {
					size += chunk?.length ?? chunk?.byteLength ?? 1;
					if (size > maxBufferSize) {
						stream.destroy(
							new Error(
								`streamToObject buffer exceeds maxBufferSize (${maxBufferSize})`,
							),
						);
						return;
					}
				}
				Object.assign(value, chunk);
			});
			stream.on("end", () => {
				resolve({ ...value });
			});
			stream.on("error", reject);
		});
	}
	return (async () => {
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
	})();
};

export const streamToString = (stream, { maxBufferSize } = {}) => {
	if (typeof stream.on === "function") {
		return new Promise((resolve, reject) => {
			const chunks = [];
			let size = 0;
			stream.on("data", (chunk) => {
				if (maxBufferSize != null) {
					size += chunk?.length ?? chunk?.byteLength ?? 0;
					if (size > maxBufferSize) {
						stream.destroy(
							new Error(
								`streamToString buffer exceeds maxBufferSize (${maxBufferSize})`,
							),
						);
						return;
					}
				}
				chunks.push(chunk);
			});
			stream.on("end", () => {
				resolve(chunks.join(""));
			});
			stream.on("error", reject);
		});
	}
	return (async () => {
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
	})();
};

export const streamToBuffer = (stream, { maxBufferSize } = {}) => {
	if (typeof stream.on === "function") {
		return new Promise((resolve, reject) => {
			const value = [];
			let size = 0;
			stream.on("data", (chunk) => {
				const buf = Buffer.from(chunk);
				if (maxBufferSize != null) {
					size += buf.length;
					if (size > maxBufferSize) {
						stream.destroy(
							new Error(
								`streamToBuffer buffer exceeds maxBufferSize (${maxBufferSize})`,
							),
						);
						return;
					}
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
			const buf = Buffer.from(chunk);
			if (maxBufferSize != null) {
				size += buf.length;
				if (size > maxBufferSize) {
					throw new Error(
						`streamToBuffer buffer exceeds maxBufferSize (${maxBufferSize})`,
					);
				}
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
		return createReadableStreamFromString(input, streamOptions);
	}
	if (typeof input === "object" && input.byteLength) {
		return createReadableStreamFromArrayBuffer(input, streamOptions);
	}
	if (Array.isArray(input)) {
		return Readable.from(input.map(toSafe), streamOptions);
	}
	return Readable.from(input, streamOptions);
};

export const createReadableStreamFromString = (input, streamOptions = {}) => {
	const size = streamOptions?.chunkSize ?? 16_384; // 16KB
	if (size <= 0) throw new Error("chunkSize must be a positive number");
	function* iterator(input) {
		let position = 0;
		const length = input.length;
		while (position < length) {
			yield input.substring(position, position + size);
			position += size;
		}
	}
	return Readable.from(iterator(input), streamOptions);
};

export const createReadableStreamFromArrayBuffer = (
	input,
	streamOptions = {},
) => {
	const size = streamOptions?.chunkSize ?? 16_384; // 16KB
	if (size <= 0) throw new Error("chunkSize must be a positive number");
	function* iterator(input) {
		const bytes = new Uint8Array(input);
		let position = 0;
		const length = bytes.byteLength;
		while (position < length) {
			const nextPosition = position + size;
			yield bytes.subarray(position, nextPosition);
			position += size;
		}
	}
	return Readable.from(iterator(input), streamOptions);
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
				if (result != null && typeof result.then === "function") {
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
					if (result != null && typeof result.then === "function") {
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
				if (result != null && typeof result.then === "function") {
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
					if (result != null && typeof result.then === "function") {
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
	return new Writable({
		...makeOptions(streamOptions),
		write(chunk, _encoding, callback) {
			try {
				const result = write(fromSafe(chunk));
				if (result != null && typeof result.then === "function") {
					result.then(() => callback(), callback);
				} else {
					callback();
				}
			} catch (e) {
				callback(e);
			}
		},
		final(callback) {
			try {
				if (final) {
					const result = final();
					if (result != null && typeof result.then === "function") {
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
