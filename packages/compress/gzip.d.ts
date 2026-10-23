// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
import type { DatastreamTransform, StreamOptions } from "@datastream/core";
import type {
	GzipCompressOptions,
	GzipDecompressOptions,
} from "./gzip.browser.js";

export type {
	GzipCompressOptions,
	GzipDecompressOptions,
} from "./gzip.browser.js";

export function gzipCompressStream(
	options?: GzipCompressOptions,
	streamOptions?: StreamOptions,
): DatastreamTransform;
export function gzipDecompressStream(
	options?: GzipDecompressOptions,
	streamOptions?: StreamOptions,
): DatastreamTransform;
