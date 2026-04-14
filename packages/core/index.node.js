// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
import { Readable, Transform, Writable } from "node:stream";
import { pipeline as pipelinePromise } from "node:stream/promises";
import { setTimeout } from "node:timers/promises";

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
		throw e;
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
		let startTimestamp;
		value.on("pause", () => {
			timestamp = Date.now(); // process.hrtime.bigint()
		});
		value.on("resume", () => {
			if (timestamp) {
				// Number.parseInt(  (process.hrtime.bigint() - pauseTimestamp).toString() , 10 ) / 1_000_000 // ms
				const duration = Date.now() - timestamp;
				metrics[keys[i]].timeline.push({ timestamp, duration });
			} else {
				startTimestamp = Date.now();
			}
		});
		value.on("end", () => {
			const duration = Date.now() - startTimestamp;
			metrics[keys[i]].total = { timestamp: startTimestamp, duration };
		});
	}
	return metrics;
};

export const streamToArray = (stream) => {
	if (typeof stream.on === "function") {
		return new Promise((resolve, reject) => {
			const value = [];
			stream.on("data", (chunk) => {
				value.push(chunk);
			});
			stream.on("end", () => {
				resolve(value);
			});
			stream.on("error", reject);
		});
	}
	return (async () => {
		const value = [];
		for await (const chunk of stream) {
			value.push(chunk);
		}
		return value;
	})();
};

export const streamToObject = (stream) => {
	if (typeof stream.on === "function") {
		return new Promise((resolve, reject) => {
			const value = Object.create(null);
			stream.on("data", (chunk) => {
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
		for await (const chunk of stream) {
			Object.assign(value, chunk);
		}
		return { ...value };
	})();
};

export const streamToString = (stream) => {
	if (typeof stream.on === "function") {
		return new Promise((resolve, reject) => {
			const chunks = [];
			stream.on("data", (chunk) => {
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
		for await (const chunk of stream) {
			chunks.push(chunk);
		}
		return chunks.join("");
	})();
};

export const streamToBuffer = (stream) => {
	if (typeof stream.on === "function") {
		return new Promise((resolve, reject) => {
			const value = [];
			stream.on("data", (chunk) => {
				value.push(Buffer.from(chunk));
			});
			stream.on("end", () => {
				resolve(Buffer.concat(value));
			});
			stream.on("error", reject);
		});
	}
	return (async () => {
		const value = [];
		for await (const chunk of stream) {
			value.push(Buffer.from(chunk));
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

export const createReadableStream = (input = "", streamOptions = {}) => {
	// string doesn't chunk, and is slow
	if (typeof input === "string") {
		return createReadableStreamFromString(input, streamOptions);
	}
	if (typeof input === "object" && input.byteLength) {
		return createReadableStreamFromArrayBuffer(input, streamOptions);
	}
	return Readable.from(input, streamOptions);
};

export const createReadableStreamFromString = (input, streamOptions = {}) => {
	function* iterator(input) {
		const size = streamOptions?.chunkSize ?? 16_384; // 16KB
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
	function* iterator(input) {
		const size = streamOptions?.chunkSize ?? 16_384; // 16KB
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
				const result = passThrough(chunk);
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
				const result = transform(chunk, enqueue);
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
		stream.push(chunk, encoding);
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
				const result = write(chunk);
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

export const timeout = (ms, { signal } = {}) => {
	return setTimeout(ms, { signal });
};
