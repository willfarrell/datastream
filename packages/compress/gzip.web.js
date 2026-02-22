// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
/* global CompressionStream, DecompressionStream */
// CompressionStream
// - https://caniuse.com/?search=CompressionStream
// - not supported on firefox - https://bugzilla.mozilla.org/show_bug.cgi?id=1586639
// - not supported in safari

export const gzipCompressStream = (_options, _streamOptions = {}) => {
	return new CompressionStream("gzip");
};
export const gzipDecompressStream = (_options, _streamOptions = {}) => {
	return new DecompressionStream("gzip");
};

export default {
	compressStream: gzipCompressStream,
	decompressStream: gzipDecompressStream,
};
