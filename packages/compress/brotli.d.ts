// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
import type { StreamOptions } from "@datastream/core";

export function brotliCompressStream(
	options?: { quality?: number },
	streamOptions?: StreamOptions,
): unknown;
export function brotliDecompressStream(
	options?: Record<string, unknown>,
	streamOptions?: StreamOptions,
): unknown;
