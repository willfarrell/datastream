// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
import {
	constants,
	createBrotliCompress,
	createBrotliDecompress,
} from "node:zlib";

// quality: 0 - 11
export const brotliCompressStream = ({ quality } = {}, streamOptions = {}) => {
	return createBrotliCompress({
		...streamOptions,
		params: {
			[constants.BROTLI_PARAM_QUALITY]:
				quality ?? constants.BROTLI_DEFAULT_QUALITY,
		},
	});
};
export const brotliDecompressStream = (params, streamOptions = {}) => {
	return createBrotliDecompress({ ...streamOptions, params });
};

export default {
	compressStream: brotliCompressStream,
	decompressStream: brotliDecompressStream,
};
