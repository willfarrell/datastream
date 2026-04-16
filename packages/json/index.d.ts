// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
import type { StreamOptions, StreamResult } from "@datastream/core";

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
): unknown & {
	result: () => StreamResult<Record<string, JsonError>>;
};

export function ndjsonFormatStream(
	options?: {
		space?: number | string;
		resultKey?: string;
	},
	streamOptions?: StreamOptions,
): unknown;

export function jsonParseStream(
	options?: {
		maxBufferSize?: number;
		maxValueSize?: number;
		resultKey?: string;
	},
	streamOptions?: StreamOptions,
): unknown & {
	result: () => StreamResult<Record<string, JsonError>>;
};

export function jsonFormatStream(
	options?: {
		space?: number | string;
	},
	streamOptions?: StreamOptions,
): unknown;
