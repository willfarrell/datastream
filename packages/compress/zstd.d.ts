// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
import type { DatastreamTransform, StreamOptions } from "@datastream/core";

// Node.js only (node:zlib): the zstd subpath has no browser export condition.

export interface ZstdCompressOptions {
	quality?: number;
	maxOutputSize?: number | null;
}

export interface ZstdDecompressOptions {
	maxOutputSize?: number | null;
}

export function zstdCompressStream(
	options?: ZstdCompressOptions,
	streamOptions?: StreamOptions,
): DatastreamTransform;
export function zstdDecompressStream(
	options?: ZstdDecompressOptions,
	streamOptions?: StreamOptions,
): DatastreamTransform;
