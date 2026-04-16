// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
import type { DatastreamTransform, StreamOptions } from "@datastream/core";

export interface DeflateCompressOptions {
	quality?: number;
	level?: number;
	maxOutputSize?: number;
}

export interface DeflateDecompressOptions {
	maxOutputSize?: number;
}

export function deflateCompressStream(
	options?: DeflateCompressOptions,
	streamOptions?: StreamOptions,
): DatastreamTransform;
export function deflateDecompressStream(
	options?: DeflateDecompressOptions,
	streamOptions?: StreamOptions,
): DatastreamTransform;
