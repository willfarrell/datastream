// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
import { constants, createZstdCompress, createZstdDecompress } from "node:zlib";

export const zstdCompressStream = (options = {}, _streamOptions = {}) => {
	const { quality, ...rest } = options;
	return createZstdCompress({
		...rest,
		params: rest.params ?? {
			[constants.ZSTD_c_compressionLevel]:
				quality ?? constants.ZSTD_CLEVEL_DEFAULT,
		},
	});
};
export const zstdDecompressStream = (options = {}, _streamOptions = {}) => {
	const { maxOutputSize, ...rest } = options;
	const stream = createZstdDecompress(rest);
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
	compressStream: zstdCompressStream,
	decompressStream: zstdDecompressStream,
};
