// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
import { nativeCompressStreams } from "./native.browser.js";

const { compressStream, decompressStream } = nativeCompressStreams("deflate");

export const deflateCompressStream = compressStream;
export const deflateDecompressStream = decompressStream;
