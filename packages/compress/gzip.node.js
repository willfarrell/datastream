// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
import { createGunzip, createGzip } from "node:zlib";
import { guardCompress, guardDecompress } from "./guard.node.js";

// quality -1 - 9
export const gzipCompressStream = (options = {}, streamOptions = {}) => {
	const { quality, maxOutputSize } = options;
	return guardCompress(
		createGzip({ ...streamOptions, level: quality }),
		maxOutputSize,
	);
};
export const gzipDecompressStream = (options = {}, streamOptions = {}) => {
	return guardDecompress(createGunzip(streamOptions), options.maxOutputSize);
};

export default {
	compressStream: gzipCompressStream,
	decompressStream: gzipDecompressStream,
};
