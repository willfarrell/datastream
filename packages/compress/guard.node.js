// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
// Shared output-size guard for the node zlib wrappers. Internal: not a package
// export, inlined into each *.node.mjs bundle by bin/esbuild.

// Default decompression output ceiling (256MiB) so that untrusted compressed
// input is bounded by default (zip-bomb protection). Pass `maxOutputSize: null`
// to opt out of the limit entirely.
const DEFAULT_DECOMPRESS_MAX_OUTPUT_SIZE = 256 * 1024 * 1024;

const guardOutput = (stream, maxOutputSize, label) => {
	let outputSize = 0;
	const originalPush = stream.push.bind(stream);
	stream.push = (chunk, encoding) => {
		if (chunk !== null) {
			outputSize += chunk.byteLength ?? Buffer.byteLength(chunk);
			if (outputSize > maxOutputSize) {
				stream.push = originalPush;
				stream.destroy(
					new Error(
						`${label} output exceeds maxOutputSize (${maxOutputSize} bytes)`,
					),
				);
				return false;
			}
		}
		return originalPush(chunk, encoding);
	};
	const restore = () => {
		stream.push = originalPush;
	};
	stream.on("close", restore);
	stream.on("error", restore);
};

// Compression is bounded only when the caller asks for it.
export const guardCompress = (stream, maxOutputSize) => {
	if (maxOutputSize !== null && maxOutputSize !== undefined) {
		guardOutput(stream, maxOutputSize, "Compression");
	}
	return stream;
};

// Decompression is bounded by default; only an explicit null opts out.
export const guardDecompress = (stream, maxOutputSize) => {
	const limit =
		maxOutputSize === null
			? undefined
			: (maxOutputSize ?? DEFAULT_DECOMPRESS_MAX_OUTPUT_SIZE);
	if (limit !== undefined) {
		guardOutput(stream, limit, "Decompression");
	}
	return stream;
};
