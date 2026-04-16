// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
import type {
	DatastreamReadable,
	DatastreamTransform,
	DatastreamPassThrough,
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

export function stringMinimumFirstChunkSize(
	options?: {
		chunkSize?: number;
	},
	streamOptions?: StreamOptions,
): DatastreamTransform<string, string>;

export function stringMinimumChunkSize(
	options?: {
		chunkSize?: number;
	},
	streamOptions?: StreamOptions,
): DatastreamTransform<string, string>;

export function stringSkipConsecutiveDuplicates(
	options?: Record<string, never>,
	streamOptions?: StreamOptions,
): DatastreamTransform<string, string>;

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
	skipConsecutiveDuplicates: typeof stringSkipConsecutiveDuplicates;
	replaceStream: typeof stringReplaceStream;
	splitStream: typeof stringSplitStream;
};
export default _default;
