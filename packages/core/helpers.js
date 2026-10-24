// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
// Helpers shared verbatim by the node and web builds. Imported relatively so
// esbuild leaves the import in place; shipped via package.json `files`.
export const resolveLazy = (value) =>
	typeof value === "function" ? value() : value;

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

export const runAbort = async (abort, reason) => {
	try {
		await abort(reason);
	} catch {}
};

const streamDecodeOptions = { stream: true };

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
