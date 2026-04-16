// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
import type {
	DatastreamReadable,
	DatastreamTransform,
	DatastreamPassThrough,
	StreamOptions,
	StreamResult,
} from "@datastream/core";

export function objectReadableStream<T = Record<string, unknown>>(
	input?: T[],
	streamOptions?: StreamOptions,
): DatastreamReadable<T>;

export function objectCountStream(
	options?: {
		resultKey?: string;
	},
	streamOptions?: StreamOptions,
): DatastreamPassThrough & {
	result: () => StreamResult<number>;
};

export function objectBatchStream<_T = Record<string, unknown>>(
	options: {
		keys: string[];
		maxBatchSize?: number;
	},
	streamOptions?: StreamOptions,
): DatastreamTransform;

export function objectPivotLongToWideStream(
	options: {
		keys: string[];
		valueParam: string;
		delimiter?: string;
	},
	streamOptions?: StreamOptions,
): DatastreamTransform;

export function objectPivotWideToLongStream(
	options: {
		keys: string[];
		keyParam?: string;
		valueParam?: string;
		isNestedObject?: boolean;
	},
	streamOptions?: StreamOptions,
): DatastreamTransform;

export function objectKeyValueStream(
	options: {
		key: string;
		value: string;
	},
	streamOptions?: StreamOptions,
): DatastreamTransform;

export function objectKeyValuesStream(
	options: {
		key: string;
		values?: string[];
	},
	streamOptions?: StreamOptions,
): DatastreamTransform;

export function objectKeyJoinStream(
	options: {
		keys: Record<string, string[]>;
		separator: string;
		isNestedObject?: boolean;
	},
	streamOptions?: StreamOptions,
): DatastreamTransform;

export function objectKeyMapStream(
	options: {
		keys: Record<string, string>;
	},
	streamOptions?: StreamOptions,
): DatastreamTransform;

export function objectValueMapStream(
	options: {
		key: string;
		values: Record<string, unknown>;
	},
	streamOptions?: StreamOptions,
): DatastreamTransform;

export function objectPickStream(
	options: {
		keys: string[];
	},
	streamOptions?: StreamOptions,
): DatastreamTransform;

export function objectOmitStream(
	options: {
		keys: string[];
	},
	streamOptions?: StreamOptions,
): DatastreamTransform;

export function objectFromEntriesStream(
	options: {
		keys: string[] | (() => string[]);
	},
	streamOptions?: StreamOptions,
): DatastreamTransform;

export function objectToEntriesStream(
	options: {
		keys: string[] | (() => string[]);
	},
	streamOptions?: StreamOptions,
): DatastreamTransform;

export function objectSkipConsecutiveDuplicatesStream(
	options?: Record<string, never>,
	streamOptions?: StreamOptions,
): DatastreamTransform;

declare const _default: {
	readableStream: typeof objectReadableStream;
	countStream: typeof objectCountStream;
	pickStream: typeof objectPickStream;
	omitStream: typeof objectOmitStream;
	batchStream: typeof objectBatchStream;
	pivotLongToWideStream: typeof objectPivotLongToWideStream;
	pivotWideToLongStream: typeof objectPivotWideToLongStream;
	keyValueStream: typeof objectKeyValueStream;
	keyValuesStream: typeof objectKeyValuesStream;
	keyJoinStream: typeof objectKeyJoinStream;
	keyMapStream: typeof objectKeyMapStream;
	valueMapStream: typeof objectValueMapStream;
	fromEntriesStream: typeof objectFromEntriesStream;
	toEntriesStream: typeof objectToEntriesStream;
	skipConsecutiveDuplicatesStream: typeof objectSkipConsecutiveDuplicatesStream;
};
export default _default;
