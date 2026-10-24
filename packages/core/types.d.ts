// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT

export interface StreamOptions {
	highWaterMark?: number;
	chunkSize?: number;
	signal?: AbortSignal;
	[key: string]: unknown;
}

export interface StreamResult<T = unknown> {
	key: string;
	value: T;
}

export interface ResultStream<T = unknown> {
	result: () => StreamResult<T> | Promise<StreamResult<T>>;
}

export interface StreamCollectorOptions {
	/** Unlimited when undefined or null; exceeding it rejects with a RangeError. */
	maxBufferSize?: number | null;
}

export interface WritableStreamOptions extends StreamOptions {
	/**
	 * Runs once when the writable is torn down before it finishes by something
	 * other than its own write/close failing: an upstream/pipe error, a writer
	 * abort (browser) / destroy() (node), or `signal` firing. Receives the abort
	 * reason (undefined if none). May be async; its own failure is ignored.
	 */
	abort?: (reason: unknown) => void | Promise<void>;
}

export type ReadableInput<T> =
	| string
	| ArrayBufferLike
	| ArrayBufferView
	| T[]
	| Iterable<T>
	| AsyncIterable<T>;

export function makeOptions(options?: StreamOptions): Record<string, unknown>;

export function timeout(
	ms: number,
	options?: { signal?: AbortSignal },
): Promise<void>;

export function resolveLazy<T>(value: T | (() => T)): T;
// Streaming UTF-8 decode: byte chunks keep multi-byte chars split across
// chunks intact, string chunks pass through; flush() emits trailing bytes.
export function createChunkDecoder(options?: TextDecoderOptions): {
	decode: (chunk: string | AllowSharedBufferSource) => string;
	flush: () => string;
};
export function concatBytes(chunks: readonly Uint8Array[]): Uint8Array;
