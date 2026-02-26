// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
import { createGunzip, createGzip } from "node:zlib";

// quality -1 - 9
export const gzipCompressStream = ({ quality } = {}, streamOptions = {}) => {
	return createGzip({ ...streamOptions, level: quality });
};
export const gzipDecompressStream = (_options = {}, streamOptions = {}) => {
	return createGunzip(streamOptions);
};

export default {
	compressStream: gzipCompressStream,
	decompressStream: gzipDecompressStream,
};
