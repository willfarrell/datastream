// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
import { createGunzip, createGzip } from "node:zlib";

// quality -1 - 9
export const gzipCompressStream = (options = {}, streamOptions = {}) => {
	const { quality, maxOutputSize } = options;
	const stream = createGzip({ ...streamOptions, level: quality });
	if (maxOutputSize !== null && maxOutputSize !== undefined) {
		let outputSize = 0;
		const originalPush = stream.push.bind(stream);
		stream.push = (chunk) => {
			if (chunk !== null) {
				outputSize += chunk.length;
				if (outputSize > maxOutputSize) {
					stream.push = originalPush;
					stream.destroy(
						new Error(
							`Compression output exceeds maxOutputSize (${maxOutputSize} bytes)`,
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
export const gzipDecompressStream = (options = {}, streamOptions = {}) => {
	const { maxOutputSize } = options;
	const stream = createGunzip(streamOptions);
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
	compressStream: gzipCompressStream,
	decompressStream: gzipDecompressStream,
};
