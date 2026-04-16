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
	idx: number[];
}

export function ndjsonParseStream(
	options?: {
		maxBufferSize?: number;
		resultKey?: string;
	},
	streamOptions?: StreamOptions,
): DatastreamTransform<string, Record<string, unknown>> & {
	result: () => StreamResult<Record<string, JsonError>>;
};

export function ndjsonFormatStream(
	options?: {
		space?: number | string;
		resultKey?: string;
	},
	streamOptions?: StreamOptions,
): DatastreamTransform<Record<string, unknown>, string>;

export function jsonParseStream(
	options?: {
		maxBufferSize?: number;
		maxValueSize?: number;
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
