// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
/* global CompressionStream, DecompressionStream, TransformStream */
// CompressionStream
// - https://caniuse.com/?search=CompressionStream
// - not supported on firefox - https://bugzilla.mozilla.org/show_bug.cgi?id=1586639
// - not supported in safari
import { makeOptions } from "@datastream/core";

// Default decompression output ceiling (256MiB) so that untrusted compressed
// input is bounded by default (zip-bomb protection). Pass `maxOutputSize: null`
// to opt out of the limit entirely.
export const DEFAULT_DECOMPRESS_MAX_OUTPUT_SIZE = 256 * 1024 * 1024;

// Decompression is bounded by default; only an explicit null opts out.
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

const SLICE_SIZE = 16_384;
const toView = (bytes) => {
	if (ArrayBuffer.isView(bytes)) {
		return new Uint8Array(bytes.buffer, bytes.byteOffset, bytes.byteLength);
	}
	if (bytes instanceof ArrayBuffer) return new Uint8Array(bytes);
	throw new TypeError("chunk must be a string or BufferSource");
};

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
				const bytes = toView(toBytes(chunk));
				// Feed the (de)compressor in small slices: a DecompressionStream
				// inflates each chunk synchronously, so one large chunk of a zlib bomb
				// would allocate its whole output before outputStage could check
				// maxOutputSize. The pipe only moves the next slice once the previous
				// one's output was consumed, and stops once outputStage errors.
				for (let offset = 0; offset < bytes.byteLength; offset += SLICE_SIZE) {
					controller.enqueue(bytes.subarray(offset, offset + SLICE_SIZE));
				}
			},
			flush() {
				cleanup();
				signal?.throwIfAborted();
			},
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
		cancel: cleanup,
	});
};

const wrap = (compressor, maxOutputSize, streamOptions, label) => {
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
