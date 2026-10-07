// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
import type {
	DatastreamTransform,
	StreamOptions,
	StreamResult,
} from "@datastream/core";

export interface JsonError {
	id: string;
	message: string;
	/** Row indexes, at most `maxErrorRows` of them. */
	idx: number[];
	/** True number of occurrences (may exceed `idx.length`). */
	count: number;
}

export function ndjsonParseStream(
	options?: {
		/** Default 16MB; null = unlimited. */
		maxBufferSize?: number | null;
		/** idx entries kept per error id. Default 1000; null = unlimited. */
		maxErrorRows?: number | null;
		/** Default "ndjsonErrors". */
		resultKey?: string;
	},
	streamOptions?: StreamOptions,
): DatastreamTransform<string, Record<string, unknown>> & {
	result: () => StreamResult<Record<string, JsonError>>;
};

export function ndjsonFormatStream(
	options?: {
		space?: number | string;
	},
	streamOptions?: StreamOptions,
): DatastreamTransform<Record<string, unknown>, string>;

export function jsonParseStream(
	options?: {
		/** Default 16MB; null = unlimited. */
		maxBufferSize?: number | null;
		/** Default 16MB; null = unlimited. */
		maxValueSize?: number | null;
		/** idx entries kept per error id. Default 1000; null = unlimited. */
		maxErrorRows?: number | null;
		/** Default "jsonErrors". */
		resultKey?: string;
	},
	streamOptions?: StreamOptions,
): DatastreamTransform<string, Record<string, unknown>> & {
	result: () => StreamResult<Record<string, JsonError>>;
};

export function jsonFormatStream(
	options?: {
		space?: number | string;
	},
	streamOptions?: StreamOptions,
): DatastreamTransform<Record<string, unknown>, string>;
