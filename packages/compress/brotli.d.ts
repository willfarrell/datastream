// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
import type { DatastreamTransform, StreamOptions } from "@datastream/core";

export interface BrotliCompressOptions {
	quality?: number;
	maxOutputSize?: number | null;
}

export interface BrotliDecompressOptions {
	maxOutputSize?: number | null;
	// Node.js only (node:zlib BROTLI_DECODER_PARAM_*); the browser build ignores it.
	params?: Record<number, number | boolean>;
}

export function brotliCompressStream(
	options?: BrotliCompressOptions,
	streamOptions?: StreamOptions,
): DatastreamTransform;
export function brotliDecompressStream(
	options?: BrotliDecompressOptions,
	streamOptions?: StreamOptions,
): DatastreamTransform;
