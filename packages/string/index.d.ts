// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
import type {
	DatastreamPassThrough,
	DatastreamReadable,
	DatastreamTransform,
	StreamOptions,
	StreamResult,
} from "@datastream/core";

export function stringReadableStream(
	input: string | string[],
	streamOptions?: StreamOptions,
): DatastreamReadable<string>;

export function stringLengthStream(
	options?: {
		resultKey?: string;
	},
	streamOptions?: StreamOptions,
): DatastreamPassThrough<string> & {
	result: () => StreamResult<number>;
};

export function stringCountStream(
	options?: {
		substr?: string;
		resultKey?: string;
	},
	streamOptions?: StreamOptions,
): DatastreamPassThrough<string> & {
	result: () => StreamResult<number>;
};

export function stringMinimumFirstChunkSizeStream(
	options?: {
		chunkSize?: number;
	},
	streamOptions?: StreamOptions,
): DatastreamTransform<string, string>;
/** @deprecated Use stringMinimumFirstChunkSizeStream */
export const stringMinimumFirstChunkSize: typeof stringMinimumFirstChunkSizeStream;

export function stringMinimumChunkSizeStream(
	options?: {
		chunkSize?: number;
	},
	streamOptions?: StreamOptions,
): DatastreamTransform<string, string>;
/** @deprecated Use stringMinimumChunkSizeStream */
export const stringMinimumChunkSize: typeof stringMinimumChunkSizeStream;

export function stringSkipConsecutiveDuplicatesStream(
	options?: Record<string, never>,
	streamOptions?: StreamOptions,
): DatastreamTransform<string, string>;
/** @deprecated Use stringSkipConsecutiveDuplicatesStream */
export const stringSkipConsecutiveDuplicates: typeof stringSkipConsecutiveDuplicatesStream;

export function stringReplaceStream(
	options: {
		pattern: string | RegExp;
		replacement: string;
		maxBufferSize?: number;
	},
	streamOptions?: StreamOptions,
): DatastreamTransform<string, string>;

export function stringSplitStream(
	options: {
		separator: string;
		maxBufferSize?: number;
	},
	streamOptions?: StreamOptions,
): DatastreamTransform<string, string>;

declare const _default: {
	readableStream: typeof stringReadableStream;
	lengthStream: typeof stringLengthStream;
	countStream: typeof stringCountStream;
	minimumFirstChunkSize: typeof stringMinimumFirstChunkSizeStream;
	minimumChunkSize: typeof stringMinimumChunkSizeStream;
	skipConsecutiveDuplicates: typeof stringSkipConsecutiveDuplicatesStream;
	replaceStream: typeof stringReplaceStream;
	splitStream: typeof stringSplitStream;
};
export default _default;
