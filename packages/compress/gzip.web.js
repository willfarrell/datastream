// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
import { nativeCompressStreams } from "./native.web.js";

const { compressStream, decompressStream } = nativeCompressStreams("gzip");

export const gzipCompressStream = compressStream;
export const gzipDecompressStream = decompressStream;

export default {
	compressStream: gzipCompressStream,
	decompressStream: gzipDecompressStream,
};
