// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
import { createDeflate, createInflate } from "node:zlib";
import { guardCompress, guardDecompress } from "./guard.node.js";

// quality -1 - 9
export const deflateCompressStream = (options = {}, streamOptions = {}) => {
	const { quality, maxOutputSize, level } = options;
	return guardCompress(
		createDeflate({ ...streamOptions, level: level ?? quality }),
		maxOutputSize,
	);
};
export const deflateDecompressStream = (options = {}, streamOptions = {}) => {
	return guardDecompress(createInflate(streamOptions), options.maxOutputSize);
};

export default {
	compressStream: deflateCompressStream,
	decompressStream: deflateDecompressStream,
};
