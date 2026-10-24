// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT

export {
	brotliCompressStream,
	brotliDecompressStream,
} from "@datastream/compress/brotli";
export {
	deflateCompressStream,
	deflateDecompressStream,
} from "@datastream/compress/deflate";
export {
	gzipCompressStream,
	gzipDecompressStream,
} from "@datastream/compress/gzip";
// Node.js only: the browser build of the index does not export zstd.
export {
	zstdCompressStream,
	zstdDecompressStream,
} from "@datastream/compress/zstd";
