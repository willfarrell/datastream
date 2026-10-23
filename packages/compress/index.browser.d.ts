// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
// Types for the browser build: no zstd (Node.js only). Relative imports so the
// web codec types (and web core types) are used whatever conditions resolve.
export {
	brotliCompressStream,
	brotliDecompressStream,
} from "./brotli.browser.js";
export {
	deflateCompressStream,
	deflateDecompressStream,
} from "./deflate.browser.js";
export { gzipCompressStream, gzipDecompressStream } from "./gzip.browser.js";
