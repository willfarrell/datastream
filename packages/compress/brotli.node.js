// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
import {
	constants,
	createBrotliCompress,
	createBrotliDecompress,
} from "node:zlib";

// quality: 0 - 11
export const brotliCompressStream = ({ quality } = {}, streamOptions = {}) => {
	return createBrotliCompress({
		...streamOptions,
		params: {
			[constants.BROTLI_PARAM_QUALITY]:
				quality ?? constants.BROTLI_DEFAULT_QUALITY,
		},
	});
};
export const brotliDecompressStream = (options = {}, streamOptions = {}) => {
	const { maxOutputSize, ...params } = options;
	const zlibOptions = Object.keys(params).length
		? { ...streamOptions, params }
		: streamOptions;
	const stream = createBrotliDecompress(zlibOptions);
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
	compressStream: brotliCompressStream,
	decompressStream: brotliDecompressStream,
};
