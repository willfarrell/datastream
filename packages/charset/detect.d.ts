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
	// charset is undefined when the stream saw no bytes.
	result: () => StreamResult<{
		charset: string | undefined;
		confidence: number;
	}>;
};
