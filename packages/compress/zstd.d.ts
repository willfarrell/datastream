// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
import type { DatastreamTransform, StreamOptions } from "@datastream/core";

export interface ZstdCompressOptions {
	quality?: number;
	maxOutputSize?: number;
}

export interface ZstdDecompressOptions {
	maxOutputSize?: number;
}

export function zstdCompressStream(
	options?: ZstdCompressOptions,
	streamOptions?: StreamOptions,
): DatastreamTransform;
export function zstdDecompressStream(
	options?: ZstdDecompressOptions,
	streamOptions?: StreamOptions,
): DatastreamTransform;
