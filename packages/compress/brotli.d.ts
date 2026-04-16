// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
import type { DatastreamTransform, StreamOptions } from "@datastream/core";

export interface BrotliCompressOptions {
	quality?: number;
	maxOutputSize?: number;
}

export interface BrotliDecompressOptions {
	maxOutputSize?: number;
}

export function brotliCompressStream(
	options?: BrotliCompressOptions,
	streamOptions?: StreamOptions,
): DatastreamTransform;
export function brotliDecompressStream(
	options?: BrotliDecompressOptions,
	streamOptions?: StreamOptions,
): DatastreamTransform;
