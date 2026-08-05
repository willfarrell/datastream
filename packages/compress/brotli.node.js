// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
import {
	constants,
	createBrotliCompress,
	createBrotliDecompress,
} from "node:zlib";
import { guardCompress, guardDecompress } from "./guard.node.js";

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
	return guardCompress(stream, maxOutputSize);
};
export const brotliDecompressStream = (options = {}, streamOptions = {}) => {
	const { maxOutputSize, params } = options;
	const zlibOptions = params ? { ...streamOptions, params } : streamOptions;
	return guardDecompress(createBrotliDecompress(zlibOptions), maxOutputSize);
};

export default {
	compressStream: brotliCompressStream,
	decompressStream: brotliDecompressStream,
};
