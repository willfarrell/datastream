// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
import type { StreamOptions, StreamResult } from "@datastream/core";

export function stringReadableStream(
	input: string | string[],
	streamOptions?: StreamOptions,
): unknown;

export function stringLengthStream(
	options?: {
		resultKey?: string;
	},
	streamOptions?: StreamOptions,
): unknown & {
	result: () => StreamResult<number>;
};

export function stringCountStream(
	options?: {
		substr?: string;
		resultKey?: string;
	},
	streamOptions?: StreamOptions,
): unknown & {
	result: () => StreamResult<number>;
};

export function stringMinimumFirstChunkSize(
	options?: {
		chunkSize?: number;
	},
	streamOptions?: StreamOptions,
): unknown;

export function stringMinimumChunkSize(
	options?: {
		chunkSize?: number;
	},
	streamOptions?: StreamOptions,
): unknown;

export function stringSkipConsecutiveDuplicates(
	options?: Record<string, unknown>,
	streamOptions?: StreamOptions,
): unknown;

export function stringReplaceStream(
	options: {
		pattern: string | RegExp;
		replacement: string;
	},
	streamOptions?: StreamOptions,
): unknown;

export function stringSplitStream(
	options: {
		separator: string;
	},
	streamOptions?: StreamOptions,
): unknown;

declare const _default: {
	readableStream: typeof stringReadableStream;
	lengthStream: typeof stringLengthStream;
	countStream: typeof stringCountStream;
	skipConsecutiveDuplicates: typeof stringSkipConsecutiveDuplicates;
	replaceStream: typeof stringReplaceStream;
	splitStream: typeof stringSplitStream;
};
export default _default;
