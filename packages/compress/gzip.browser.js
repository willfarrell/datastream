// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
import { nativeCompressStreams } from "./native.browser.js";

const { compressStream, decompressStream } = nativeCompressStreams("gzip");

export const gzipCompressStream = compressStream;
export const gzipDecompressStream = decompressStream;
