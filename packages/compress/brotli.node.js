// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
import {
	constants,
	createBrotliCompress,
	createBrotliDecompress,
} from "node:zlib";

// Default decompression output ceiling (256MiB) so that untrusted compressed
// input is bounded by default (zip-bomb protection). Pass `maxOutputSize: null`
// to opt out of the limit entirely.
const DEFAULT_DECOMPRESS_MAX_OUTPUT_SIZE = 256 * 1024 * 1024;

const guardOutput = (stream, maxOutputSize, label) => {
	let outputSize = 0;
	const originalPush = stream.push.bind(stream);
	stream.push = (chunk, encoding) => {
		if (chunk !== null) {
			outputSize += chunk.byteLength ?? Buffer.byteLength(chunk);
			if (outputSize > maxOutputSize) {
				stream.push = originalPush;
				stream.destroy(
					new Error(
						`${label} output exceeds maxOutputSize (${maxOutputSize} bytes)`,
					),
				);
				return false;
			}
		}
		return originalPush(chunk, encoding);
	};
	const restore = () => {
		stream.push = originalPush;
	};
	stream.on("close", restore);
	stream.on("error", restore);
};

// quality: 0 - 11
export const brotliCompressStream = (options = {}, streamOptions = {}) => {
	const { quality, maxOutputSize } = options;
	const stream = createBrotliCompress({
		...streamOptions,
		params: {
			[constants.BROTLI_PARAM_QUALITY]:
				quality ?? constants.BROTLI_DEFAULT_QUALITY,
		},
	});
	if (maxOutputSize !== null && maxOutputSize !== undefined) {
		guardOutput(stream, maxOutputSize, "Compression");
	}
	return stream;
};
export const brotliDecompressStream = (options = {}, streamOptions = {}) => {
	const { maxOutputSize, params } = options;
	const zlibOptions = params ? { ...streamOptions, params } : streamOptions;
	const stream = createBrotliDecompress(zlibOptions);
	const limit =
		maxOutputSize === null
			? undefined
			: (maxOutputSize ?? DEFAULT_DECOMPRESS_MAX_OUTPUT_SIZE);
	if (limit !== undefined) {
		guardOutput(stream, limit, "Decompression");
	}
	return stream;
};

export default {
	compressStream: brotliCompressStream,
	decompressStream: brotliDecompressStream,
};
