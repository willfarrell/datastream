// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
import type { DatastreamTransform, StreamOptions } from "@datastream/core";
import type {
	BrotliCompressOptions,
	BrotliDecompressOptions,
} from "./brotli.browser.js";

export type {
	BrotliCompressOptions,
	BrotliDecompressOptions,
} from "./brotli.browser.js";

export function brotliCompressStream(
	options?: BrotliCompressOptions,
	streamOptions?: StreamOptions,
): DatastreamTransform;
export function brotliDecompressStream(
	options?: BrotliDecompressOptions,
	streamOptions?: StreamOptions,
): DatastreamTransform;
