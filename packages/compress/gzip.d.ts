// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
import type { DatastreamTransform, StreamOptions } from "@datastream/core";

export interface GzipCompressOptions {
	quality?: number;
	maxOutputSize?: number;
}

export interface GzipDecompressOptions {
	maxOutputSize?: number;
}

export function gzipCompressStream(
	options?: GzipCompressOptions,
	streamOptions?: StreamOptions,
): DatastreamTransform;
export function gzipDecompressStream(
	options?: GzipDecompressOptions,
	streamOptions?: StreamOptions,
): DatastreamTransform;
