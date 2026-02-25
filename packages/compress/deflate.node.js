// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
import { createDeflate, createInflate } from "node:zlib";

// TODO benchmark against `fflate`
// quality -1 - 9
export const deflateCompressStream = (options = {}, _streamOptions = {}) => {
	const { quality } = options;
	options.level ??= quality;
	return createDeflate(options);
};
export const deflateDecompressStream = (_options = {}, streamOptions = {}) => {
	return createInflate(streamOptions);
};

export default {
	compressStream: deflateCompressStream,
	decompressStream: deflateDecompressStream,
};
