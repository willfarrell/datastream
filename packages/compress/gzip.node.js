// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
import { createGunzip, createGzip } from "node:zlib";

// quality -1 - 9
export const gzipCompressStream = ({ quality } = {}, streamOptions = {}) => {
	return createGzip({ ...streamOptions, level: quality });
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
	}
	return stream;
};

export default {
	compressStream: gzipCompressStream,
	decompressStream: gzipDecompressStream,
};
