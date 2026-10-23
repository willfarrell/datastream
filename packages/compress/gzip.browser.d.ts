// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
// Web build types (no @types/node). The option types are shared with
// gzip.d.ts so both builds accept the same options.
import type {
	DatastreamTransform,
	StreamOptions,
} from "@datastream/core/browser";

export interface GzipCompressOptions {
	// Node.js only (node:zlib); the browser build ignores quality and level.
	quality?: number;
	level?: number;
	maxOutputSize?: number | null;
}

export interface GzipDecompressOptions {
	maxOutputSize?: number | null;
}

export function gzipCompressStream(
	options?: GzipCompressOptions,
	streamOptions?: StreamOptions,
): DatastreamTransform;
export function gzipDecompressStream(
	options?: GzipDecompressOptions,
	streamOptions?: StreamOptions,
): DatastreamTransform;
