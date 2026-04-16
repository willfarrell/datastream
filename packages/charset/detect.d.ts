// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
import type {
	DatastreamPassThrough,
	StreamOptions,
	StreamResult,
} from "@datastream/core";

export function charsetDetectStream(
	options?: {
		resultKey?: string;
	},
	streamOptions?: StreamOptions,
): DatastreamPassThrough & {
	result: () => StreamResult<{ charset: string; confidence: number }>;
};
