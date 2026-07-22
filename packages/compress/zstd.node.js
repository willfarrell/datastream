// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
import { constants, createZstdCompress, createZstdDecompress } from "node:zlib";

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

export const zstdCompressStream = (options = {}, streamOptions = {}) => {
	const { quality, maxOutputSize, params } = options;
	const stream = createZstdCompress({
		...streamOptions,
		params: params ?? {
			[constants.ZSTD_c_compressionLevel]:
				quality ?? constants.ZSTD_CLEVEL_DEFAULT,
		},
	});
	if (maxOutputSize !== null && maxOutputSize !== undefined) {
		guardOutput(stream, maxOutputSize, "Compression");
	}
	return stream;
};
export const zstdDecompressStream = (options = {}, streamOptions = {}) => {
	const { maxOutputSize, params } = options;
	const stream = createZstdDecompress(
		params ? { ...streamOptions, params } : streamOptions,
	);
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
	compressStream: zstdCompressStream,
	decompressStream: zstdDecompressStream,
};
