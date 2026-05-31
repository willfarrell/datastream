// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
// CompressionStream
// - https://caniuse.com/?search=CompressionStream
// - doesn't support `br` - https://github.com/httptoolkit/brotli-wasm
// - not supported on firefox - https://bugzilla.mozilla.org/show_bug.cgi?id=1586639
// - not supported in safari
import { createTransformStream } from "@datastream/core";
import brotliPromise from "brotli-wasm"; // Import the default export

const { CompressStream, DecompressStream, BrotliStreamResultCode } =
	await brotliPromise; // Import is async in browsers due to wasm requirements!

// Fixed-size output buffer; the streaming loop drains NeedsMoreOutput so any
// chunk/output size is handled correctly.
const OUTPUT_SIZE = 16_384; // 16KB

// Default decompression output ceiling (256MiB) so that untrusted compressed
// input is bounded by default (zip-bomb protection). Pass `maxOutputSize: null`
// to opt out of the limit entirely.
const DEFAULT_DECOMPRESS_MAX_OUTPUT_SIZE = 256 * 1024 * 1024;

const textEncoder = new TextEncoder();
const toBytes = (chunk) =>
	typeof chunk === "string" ? textEncoder.encode(chunk) : chunk;

// https://github.com/httptoolkit/brotli-wasm/issues/14
export const brotliCompressStream = (options = {}, streamOptions = {}) => {
	const { quality, maxOutputSize } = options;
	const engine = new CompressStream(quality ?? 11);
	let outputSize = 0;
	const guard = (buf, enqueue) => {
		if (maxOutputSize != null) {
			outputSize += buf.byteLength;
			if (outputSize > maxOutputSize) {
				throw new Error(
					`Compression output exceeds maxOutputSize (${maxOutputSize} bytes)`,
				);
			}
		}
		enqueue(buf);
	};
	const transform = (chunk, enqueue) => {
		const input = toBytes(chunk);
		let inputOffset = 0;
		let code;
		do {
			const result = engine.compress(input.slice(inputOffset), OUTPUT_SIZE);
			guard(result.buf, enqueue);
			inputOffset += result.input_offset;
			code = result.code;
		} while (code === BrotliStreamResultCode.NeedsMoreOutput);
	};
	const flush = (enqueue) => {
		let code;
		do {
			const result = engine.compress(undefined, OUTPUT_SIZE);
			guard(result.buf, enqueue);
			code = result.code;
		} while (code === BrotliStreamResultCode.NeedsMoreOutput);
	};
	return createTransformStream(transform, flush, streamOptions);
};
export const brotliDecompressStream = (options = {}, streamOptions = {}) => {
	const { maxOutputSize } = options;
	const limit =
		maxOutputSize === null
			? undefined
			: (maxOutputSize ?? DEFAULT_DECOMPRESS_MAX_OUTPUT_SIZE);
	const engine = new DecompressStream();
	let outputSize = 0;
	const transform = (chunk, enqueue) => {
		const input = toBytes(chunk);
		let inputOffset = 0;
		let code;
		do {
			const result = engine.decompress(input.slice(inputOffset), OUTPUT_SIZE);
			if (limit !== undefined) {
				outputSize += result.buf.byteLength;
				if (outputSize > limit) {
					throw new Error(
						`Decompression output exceeds maxOutputSize (${limit} bytes)`,
					);
				}
			}
			enqueue(result.buf);
			inputOffset += result.input_offset;
			code = result.code;
		} while (code === BrotliStreamResultCode.NeedsMoreOutput);
		// A complete brotli stream (ResultSuccess) followed by leftover bytes in
		// the same chunk means trailing/garbage data; strict decoders reject it
		// rather than silently dropping it.
		if (
			code === BrotliStreamResultCode.ResultSuccess &&
			inputOffset < input.byteLength
		) {
			throw new Error(
				"Decompression has trailing bytes after end of brotli stream",
			);
		}
	};
	return createTransformStream(transform, undefined, streamOptions);
};

export default {
	compressStream: brotliCompressStream,
	decompressStream: brotliDecompressStream,
};
