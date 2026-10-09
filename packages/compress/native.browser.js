// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
/* global CompressionStream, DecompressionStream, TransformStream */
// CompressionStream
// - https://caniuse.com/?search=CompressionStream
// - not supported on firefox - https://bugzilla.mozilla.org/show_bug.cgi?id=1586639
// - not supported in safari
// Internal: not a package export, inlined into each *.browser.mjs bundle by
// bin/esbuild.
import { makeOptions } from "@datastream/core";

// Default decompression output ceiling (256MiB) so that untrusted compressed
// input is bounded by default (zip-bomb protection). Pass `maxOutputSize: null`
// to opt out of the limit entirely.
export const DEFAULT_DECOMPRESS_MAX_OUTPUT_SIZE = 256 * 1024 * 1024;

// Decompression is bounded by default; only an explicit null opts out.
// null opts out of any ceiling; undefined gets the default one.
export const resolveDecompressLimit = (maxOutputSize) =>
	maxOutputSize === null
		? Number.POSITIVE_INFINITY
		: (maxOutputSize ?? DEFAULT_DECOMPRESS_MAX_OUTPUT_SIZE);

const textEncoder = new TextEncoder();
// The WHATWG CompressionStream/DecompressionStream require each chunk to be a
// BufferSource; strings throw a TypeError in spec-compliant browsers. Node's
// implementation is lenient, so convert here to match Node + web brotli.
export const toBytes = (chunk) =>
	typeof chunk === "string" ? textEncoder.encode(chunk) : chunk;

// Same Node pipeTo workaround as core's errorOnAbort: erroring a pipe
// destination synchronously leaks an unhandledRejection for an in-flight chunk.
// ponytail: drop once Node's webstreams pipeTo is fixed upstream.
const errorOnAbort = (controller, reason) =>
	setTimeout(() => controller.error(reason));

// Input stage: convert string chunks to bytes (BufferSource) and honor an
// AbortSignal. The native CompressionStream/DecompressionStream accept neither a
// signal nor string chunks, so this stage provides both (parity with the Node
// build and web brotli). highWaterMark/chunkSize are threaded via makeOptions.
const inputStage = (streamOptions) => {
	const { signal } = streamOptions;
	const { writableStrategy, readableStrategy } = makeOptions(streamOptions);
	let onAbort;
	// Called on every terminal path (flush, cancel), as in core's
	// createTransformStream: an errored stream never reaches flush(), so a
	// shared signal would otherwise keep one listener per failed stream.
	const cleanup = () => {
		if (onAbort) {
			signal.removeEventListener("abort", onAbort);
			onAbort = undefined;
		}
	};
	const abortError = () => signal.reason;
	return new TransformStream(
		{
			start(controller) {
				if (signal) {
					onAbort = () => errorOnAbort(controller, abortError());
					if (signal.aborted) return onAbort();
					signal.addEventListener("abort", onAbort);
				}
			},
			transform(chunk, controller) {
				signal?.throwIfAborted();
				controller.enqueue(toBytes(chunk));
			},
			flush() {
				cleanup();
				signal?.throwIfAborted();
			},
			// Readable side cancelled (e.g. a later stage errored).
			cancel: cleanup,
		},
		writableStrategy,
		readableStrategy,
	);
};

// Output stage: enforce maxOutputSize and keep honoring the AbortSignal for the
// whole stream lifetime (a mid-flight abort errors this terminal readable).
const outputStage = (maxOutputSize, label, streamOptions) => {
	const { signal } = streamOptions;
	let onAbort;
	let outputSize = 0;
	// See inputStage: also run when this stage errors itself.
	const cleanup = () => {
		if (onAbort) {
			signal.removeEventListener("abort", onAbort);
			onAbort = undefined;
		}
	};
	const abortError = () => signal.reason;
	return new TransformStream({
		start(controller) {
			if (signal) {
				onAbort = () => errorOnAbort(controller, abortError());
				if (signal.aborted) return onAbort();
				signal.addEventListener("abort", onAbort);
			}
		},
		transform(chunk, controller) {
			outputSize += chunk.byteLength;
			if (outputSize > maxOutputSize) {
				cleanup();
				controller.error(
					new RangeError(
						`${label} output exceeds maxOutputSize (${maxOutputSize} bytes)`,
					),
				);
				return;
			}
			controller.enqueue(chunk);
		},
		flush: cleanup,
		// Writable side aborted (e.g. corrupt input errored the decompressor).
		cancel: cleanup,
	});
};

const wrap = (compressor, maxOutputSize, streamOptions, label) => {
	// Explicit null streamOptions (default params only cover undefined) means
	// none, as in the node build.
	streamOptions ??= {};
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
			options.maxOutputSize ?? Number.POSITIVE_INFINITY,
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
