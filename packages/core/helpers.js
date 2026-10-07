// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
// Helpers shared verbatim by the node and web builds. Imported relatively so
// esbuild leaves the import in place; shipped via package.json `files`.
export const resolveLazy = (value) =>
	typeof value === "function" ? value() : value;

// streamToObject accumulates onto an Object.create(null). Returning a spread
// of that would copy any own `__proto__` key (e.g. from JSON.parse of untrusted
// input) as an own enumerable data property — a surprising contract that
// confuses prototype-based checks. Strip it and return a plain object.
export const sanitizeObject = (value) => {
	const out = {};
	for (const key of Object.keys(value)) {
		if (key === "__proto__") continue;
		out[key] = value[key];
	}
	return out;
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

// Runs a caller's createWritableStream abort(reason) hook (both builds). Its
// own failure is ignored: the stream is already failing with `reason`, which
// must not be replaced.
export const runAbort = async (abort, reason) => {
	try {
		await abort(reason);
	} catch {}
};

const streamDecodeOptions = { stream: true };

// Turns stream chunks into text. Byte chunks go through one streaming UTF-8
// TextDecoder so a multi-byte character split across chunks is reassembled
// (decoding each chunk on its own emits U+FFFD pairs); string chunks pass
// through untouched. flush() emits any incomplete trailing sequence as U+FFFD
// ("" otherwise). `options` are TextDecoder options (e.g. { ignoreBOM: true }).
export const createChunkDecoder = (options) => {
	const decoder = new TextDecoder("utf-8", options);
	return {
		decode: (chunk) =>
			typeof chunk === "string"
				? chunk
				: decoder.decode(chunk, streamDecodeOptions),
		flush: () => decoder.decode(),
	};
};

// Concatenates Uint8Arrays (views honoured) into a fresh Uint8Array without
// the node-only Buffer global, so both builds can share it.
export const concatBytes = (chunks) => {
	let total = 0;
	for (const chunk of chunks) total += chunk.length;
	const out = new Uint8Array(total);
	let offset = 0;
	for (const chunk of chunks) {
		out.set(chunk, offset);
		offset += chunk.length;
	}
	return out;
};
