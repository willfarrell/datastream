// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
/* global CompressionStream, DecompressionStream, TransformStream */
// CompressionStream
// - https://caniuse.com/?search=CompressionStream
// - not supported on firefox - https://bugzilla.mozilla.org/show_bug.cgi?id=1586639
// - not supported in safari
// Internal: not a package export, inlined into each *.web.mjs bundle by
// bin/esbuild.
import { makeOptions } from "@datastream/core";

// Default decompression output ceiling (256MiB) so that untrusted compressed
// input is bounded by default (zip-bomb protection). Pass `maxOutputSize: null`
// to opt out of the limit entirely.
export const DEFAULT_DECOMPRESS_MAX_OUTPUT_SIZE = 256 * 1024 * 1024;

// Decompression is bounded by default; only an explicit null opts out.
export const resolveDecompressLimit = (maxOutputSize) =>
	maxOutputSize === null
		? undefined
		: (maxOutputSize ?? DEFAULT_DECOMPRESS_MAX_OUTPUT_SIZE);

const textEncoder = new TextEncoder();
// The WHATWG CompressionStream/DecompressionStream require each chunk to be a
// BufferSource; strings throw a TypeError in spec-compliant browsers. Node's
// implementation is lenient, so convert here to match Node + web brotli.
export const toBytes = (chunk) =>
	typeof chunk === "string" ? textEncoder.encode(chunk) : chunk;

// Input stage: convert string chunks to bytes (BufferSource) and honor an
// AbortSignal. The native CompressionStream/DecompressionStream accept neither a
// signal nor string chunks, so this stage provides both (parity with the Node
// build and web brotli). highWaterMark/chunkSize are threaded via makeOptions.
const inputStage = (streamOptions) => {
	const { signal } = streamOptions ?? {};
	const { writableStrategy, readableStrategy } = makeOptions(streamOptions);
	let onAbort;
	const abortError = () =>
		signal.reason ?? new DOMException("Aborted", "AbortError");
	return new TransformStream(
		{
			start(controller) {
				if (signal) {
					if (signal.aborted) {
						controller.error(abortError());
						return;
					}
					onAbort = () => controller.error(abortError());
					signal.addEventListener("abort", onAbort, { once: true });
				}
			},
			transform(chunk, controller) {
				controller.enqueue(toBytes(chunk));
			},
			flush() {
				if (onAbort) {
					signal.removeEventListener("abort", onAbort);
					onAbort = undefined;
				}
			},
		},
		writableStrategy,
		readableStrategy,
	);
};

// Output stage: enforce maxOutputSize and keep honoring the AbortSignal for the
// whole stream lifetime (a mid-flight abort errors this terminal readable).
const outputStage = (maxOutputSize, label, streamOptions) => {
	const { signal } = streamOptions ?? {};
	let onAbort;
	let outputSize = 0;
	const abortError = () =>
		signal.reason ?? new DOMException("Aborted", "AbortError");
	return new TransformStream({
		start(controller) {
			if (signal) {
				if (signal.aborted) {
					controller.error(abortError());
					return;
				}
				onAbort = () => controller.error(abortError());
				signal.addEventListener("abort", onAbort, { once: true });
			}
		},
		transform(chunk, controller) {
			if (maxOutputSize !== null && maxOutputSize !== undefined) {
				outputSize += chunk.byteLength;
				if (outputSize > maxOutputSize) {
					controller.error(
						new Error(
							`${label} output exceeds maxOutputSize (${maxOutputSize} bytes)`,
						),
					);
					return;
				}
			}
			controller.enqueue(chunk);
		},
		flush() {
			if (onAbort) {
				signal.removeEventListener("abort", onAbort);
				onAbort = undefined;
			}
		},
	});
};

const wrap = (compressor, maxOutputSize, streamOptions, label) => {
	const input = inputStage(streamOptions);
	const output = outputStage(maxOutputSize, label, streamOptions);
	const readable = input.readable.pipeThrough(compressor).pipeThrough(output);
	return { readable, writable: input.writable };
};

// gzip and deflate differ only by the format string the platform takes.
export const nativeCompressStreams = (format) => ({
	compressStream: (options = {}, streamOptions = {}) =>
		wrap(
			new CompressionStream(format),
			options.maxOutputSize,
			streamOptions,
			"Compression",
		),
	decompressStream: (options = {}, streamOptions = {}) =>
		wrap(
			new DecompressionStream(format),
			resolveDecompressLimit(options.maxOutputSize),
			streamOptions,
			"Decompression",
		),
});
