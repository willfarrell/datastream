// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
import type { StreamOptions } from "@datastream/core";

export function zstdCompressStream(
	options?: Record<string, unknown>,
	streamOptions?: StreamOptions,
): unknown;
export function zstdDecompressStream(
	options?: Record<string, unknown>,
	streamOptions?: StreamOptions,
): unknown;
