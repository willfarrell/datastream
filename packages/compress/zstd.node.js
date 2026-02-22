// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
import { constants, createZstdCompress, createZstdDecompress } from "node:zlib";

export const zstdCompressStream = (options = {}, _streamOptions = {}) => {
	const { quality } = options;
	options.params ??= {
		[constants.ZSTD_c_compressionLevel]:
			quality ?? constants.ZSTD_CLEVEL_DEFAULT,
	};
	return createZstdCompress(options);
};
export const zstdDecompressStream = (options, _streamOptions = {}) => {
	return createZstdDecompress(options);
};

export default {
	compressStream: zstdCompressStream,
	decompressStream: zstdDecompressStream,
};
