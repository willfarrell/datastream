// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
export const zstdCompressStream = (_options = {}, _streamOptions = {}) => {
	throw new Error("Not supported");
};
export const zstdDecompressStream = (_options, _streamOptions = {}) => {
	throw new Error("Not supported");
};

export default {
	compressStream: zstdCompressStream,
	decompressStream: zstdDecompressStream,
};
