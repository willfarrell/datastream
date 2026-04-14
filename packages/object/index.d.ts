// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
import type { StreamOptions, StreamResult } from "@datastream/core";

export function objectReadableStream<T = Record<string, unknown>>(
	input?: T[],
	streamOptions?: StreamOptions,
): unknown;

export function objectCountStream(
	options?: {
		resultKey?: string;
	},
	streamOptions?: StreamOptions,
): unknown & {
	result: () => StreamResult<number>;
};

export function objectBatchStream<_T = Record<string, unknown>>(
	options: {
		keys: string[];
	},
	streamOptions?: StreamOptions,
): unknown;

export function objectPivotLongToWideStream(
	options: {
		keys: string[];
		valueParam: string;
		delimiter?: string;
	},
	streamOptions?: StreamOptions,
): unknown;

export function objectPivotWideToLongStream(
	options: {
		keys: string[];
		keyParam?: string;
		valueParam?: string;
		isNestedObject?: boolean;
	},
	streamOptions?: StreamOptions,
): unknown;

export function objectKeyValueStream(
	options: {
		key: string;
		value: string;
	},
	streamOptions?: StreamOptions,
): unknown;

export function objectKeyValuesStream(
	options: {
		key: string;
		values?: string[];
	},
	streamOptions?: StreamOptions,
): unknown;

export function objectKeyJoinStream(
	options: {
		keys: Record<string, string[]>;
		separator: string;
		isNestedObject?: boolean;
	},
	streamOptions?: StreamOptions,
): unknown;

export function objectKeyMapStream(
	options: {
		keys: Record<string, string>;
	},
	streamOptions?: StreamOptions,
): unknown;

export function objectValueMapStream(
	options: {
		key: string;
		values: Record<string, unknown>;
	},
	streamOptions?: StreamOptions,
): unknown;

export function objectPickStream(
	options: {
		keys: string[];
	},
	streamOptions?: StreamOptions,
): unknown;

export function objectOmitStream(
	options: {
		keys: string[];
	},
	streamOptions?: StreamOptions,
): unknown;

export function objectFromEntriesStream(
	options: {
		keys: string[] | (() => string[]);
	},
	streamOptions?: StreamOptions,
): unknown;

export function objectToEntriesStream(
	options: {
		keys: string[] | (() => string[]);
	},
	streamOptions?: StreamOptions,
): unknown;

export function objectSkipConsecutiveDuplicatesStream(
	options?: Record<string, unknown>,
	streamOptions?: StreamOptions,
): unknown;

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
