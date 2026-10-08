// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
import type {
	DatastreamPassThrough,
	DatastreamTransform,
	StreamOptions,
	StreamResult,
} from "@datastream/core";

export function stringLengthStream(
	options?: {
		resultKey?: string;
	},
	streamOptions?: StreamOptions,
): DatastreamPassThrough<string> & {
	result: () => StreamResult<number>;
};

export function stringCountStream(
	options: {
		/** Must be a non-empty string; the stream throws otherwise. */
		substr: string;
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

export function stringMinimumChunkSizeStream(
	options?: {
		chunkSize?: number;
	},
	streamOptions?: StreamOptions,
): DatastreamTransform<string, string>;

export function stringSkipConsecutiveDuplicatesStream(
	options?: Record<string, never>,
	streamOptions?: StreamOptions,
): DatastreamTransform<string, string>;

/**
 * Called once per match, like a String.prototype.replace function:
 * `(match, p1, ..., pN, offset, string, groups)`. `offset` is relative to the
 * start of the stream; `string` is only the text currently buffered (plus up
 * to `lookbehind` already-emitted chars), not the whole stream. `groups` is
 * always passed (undefined when the pattern has no named groups).
 */
export type StringReplaceFunction = (
	match: string,
	// biome-ignore lint/suspicious/noExplicitAny: captures, offset, string, groups
	...args: any[]
) => string;

export function stringReplaceStream(
	options: {
		pattern: string | RegExp;
		/**
		 * Output matches `input.replace(pattern, replacement)` (`replaceAll` for a
		 * string) as long as every match, including lookahead, fits in the
		 * held-back window. A template using $` or $' needs the whole stream, so
		 * the stream is buffered and replaced at flush.
		 */
		replacement: string | StringReplaceFunction;
		/**
		 * RegExp only: longest possible match (including lookahead). By default
		 * the latest chunk is held back, so a match longer than a chunk can be missed.
		 */
		maxMatchLength?: number;
		/** Already-emitted chars kept as context for lookbehind, ^ and \b (default 16) */
		lookbehind?: number;
		/** Default 16MB; null = unlimited. */
		maxBufferSize?: number | null;
	},
	streamOptions?: StreamOptions,
): DatastreamTransform<string, string>;

export function stringSplitStream(
	options: {
		separator: string;
		/** Default 16MB; null = unlimited. */
		maxBufferSize?: number | null;
	},
	streamOptions?: StreamOptions,
): DatastreamTransform<string, string>;
