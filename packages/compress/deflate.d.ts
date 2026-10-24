// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
import type { DatastreamTransform, StreamOptions } from "@datastream/core";
import type {
	DeflateCompressOptions,
	DeflateDecompressOptions,
} from "./deflate.browser.js";

export type {
	DeflateCompressOptions,
	DeflateDecompressOptions,
} from "./deflate.browser.js";

export function deflateCompressStream(
	options?: DeflateCompressOptions,
	streamOptions?: StreamOptions,
): DatastreamTransform;
export function deflateDecompressStream(
	options?: DeflateDecompressOptions,
	streamOptions?: StreamOptions,
): DatastreamTransform;
