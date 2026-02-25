// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
/* global CompressionStream, DecompressionStream */
// CompressionStream
// - https://caniuse.com/?search=CompressionStream
// - not supported on firefox - https://bugzilla.mozilla.org/show_bug.cgi?id=1586639
// - not supported in safari

export const deflateCompressStream = (_options = {}, _streamOptions = {}) => {
	return new CompressionStream("deflate");
};
export const deflateDecompressStream = (_options = {}, _streamOptions = {}) => {
	return new DecompressionStream("deflate");
};

export default {
	compressStream: deflateCompressStream,
	decompressStream: deflateDecompressStream,
};
