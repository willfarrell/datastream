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
export const deflateDecompressStream = (options = {}, _streamOptions = {}) => {
	const { maxOutputSize } = options;
	const decompressor = new DecompressionStream("deflate");
	if (maxOutputSize != null) {
		let outputSize = 0;
		const limiter = new TransformStream({
			transform(chunk, controller) {
				outputSize += chunk.byteLength;
				if (outputSize > maxOutputSize) {
					controller.error(
						new Error(
							`Decompression output exceeds maxOutputSize (${maxOutputSize} bytes)`,
						),
					);
					return;
				}
				controller.enqueue(chunk);
			},
		});
		return {
			readable: decompressor.readable.pipeThrough(limiter),
			writable: decompressor.writable,
		};
	}
	return decompressor;
};

export default {
	compressStream: deflateCompressStream,
	decompressStream: deflateDecompressStream,
};
