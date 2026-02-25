// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
import {
	constants,
	createBrotliCompress,
	createBrotliDecompress,
} from "node:zlib";

// quality: 0 - 11
export const brotliCompressStream = ({ quality } = {}, streamOptions = {}) => {
	const options = streamOptions;
	options.params = {
		[constants.BROTLI_PARAM_QUALITY]:
			quality ?? constants.BROTLI_DEFAULT_QUALITY,
	};
	return createBrotliCompress(options);
};
export const brotliDecompressStream = (params, streamOptions = {}) => {
	const options = streamOptions;
	options.params = params;
	return createBrotliDecompress(options);
};

export default {
	compressStream: brotliCompressStream,
	decompressStream: brotliDecompressStream,
};
