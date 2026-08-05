// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
import { constants, createZstdCompress, createZstdDecompress } from "node:zlib";
import { guardCompress, guardDecompress } from "./guard.node.js";

export const zstdCompressStream = (options = {}, streamOptions = {}) => {
	const { quality, maxOutputSize, params } = options;
	const stream = createZstdCompress({
		...streamOptions,
		params: params ?? {
			[constants.ZSTD_c_compressionLevel]:
				quality ?? constants.ZSTD_CLEVEL_DEFAULT,
		},
	});
	return guardCompress(stream, maxOutputSize);
};
export const zstdDecompressStream = (options = {}, streamOptions = {}) => {
	const { maxOutputSize, params } = options;
	const stream = createZstdDecompress(
		params ? { ...streamOptions, params } : streamOptions,
	);
	return guardDecompress(stream, maxOutputSize);
};

export default {
	compressStream: zstdCompressStream,
	decompressStream: zstdDecompressStream,
};
