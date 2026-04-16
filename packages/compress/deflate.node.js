// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
import { createDeflate, createInflate } from "node:zlib";

// quality -1 - 9
export const deflateCompressStream = (options = {}, _streamOptions = {}) => {
	const { quality, ...rest } = options;
	return createDeflate({ ...rest, level: rest.level ?? quality });
};
export const deflateDecompressStream = (options = {}, streamOptions = {}) => {
	const { maxOutputSize } = options;
	const stream = createInflate(streamOptions);
	if (maxOutputSize != null) {
		let outputSize = 0;
		const originalPush = stream.push.bind(stream);
		stream.push = (chunk) => {
			if (chunk !== null) {
				outputSize += chunk.length;
				if (outputSize > maxOutputSize) {
					stream.push = originalPush;
					stream.destroy(
						new Error(
							`Decompression output exceeds maxOutputSize (${maxOutputSize} bytes)`,
						),
					);
					return false;
				}
			}
			return originalPush(chunk);
		};
		stream.on("close", () => {
			stream.push = originalPush;
		});
	}
	return stream;
};

export default {
	compressStream: deflateCompressStream,
	decompressStream: deflateDecompressStream,
};
