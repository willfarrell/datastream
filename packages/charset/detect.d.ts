// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
import type { StreamOptions, StreamResult } from "@datastream/core";

export function charsetDetectStream(
	options?: {
		resultKey?: string;
	},
	streamOptions?: StreamOptions,
): unknown & {
	result: () => StreamResult<{ charset: string; confidence: number }>;
};
