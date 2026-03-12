// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
// Core types used across all packages
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

export interface ResultStream<_S, T = unknown> {
	result: () => StreamResult<T> | Promise<StreamResult<T>>;
}

// Pipeline & utilities
export function pipeline(
	streams: unknown[],
	streamOptions?: StreamOptions,
): Promise<Record<string, unknown>>;
export function pipejoin(
	streams: unknown[],
	onError?: (error: Error) => void,
): unknown;
export function result(streams: unknown[]): Promise<Record<string, unknown>>;

// Stream converters
export function streamToArray<T = unknown>(stream: unknown): Promise<T[]>;
export function streamToObject<T = Record<string, unknown>>(
	stream: unknown,
): Promise<T>;
export function streamToString(stream: unknown): Promise<string>;
export function streamToBuffer(stream: unknown): Promise<Buffer>;

// Type guards
export function isReadable(stream: unknown): boolean;
export function isWritable(stream: unknown): boolean;

// Options helper
export function makeOptions(options?: StreamOptions): Record<string, unknown>;

// Stream creators
export function createReadableStream<T = unknown>(
	input?: string | T[] | Iterable<T> | AsyncIterable<T>,
	streamOptions?: StreamOptions,
): unknown;
export function createReadableStreamFromString(
	input: string,
	streamOptions?: StreamOptions,
): unknown;
export function createReadableStreamFromArrayBuffer(
	input: ArrayBuffer | ArrayBufferLike,
	streamOptions?: StreamOptions,
): unknown;

export function createPassThroughStream<T = unknown>(
	passThrough?: ((chunk: T) => void | Promise<void>) | null,
	flush?: (() => void | Promise<void>) | StreamOptions,
	streamOptions?: StreamOptions,
): unknown & ResultStream<unknown>;

export function createTransformStream<I = unknown, O = unknown>(
	transform?:
		| ((
				chunk: I,
				enqueue: (chunk: O, encoding?: string) => void,
		  ) => void | Promise<void>)
		| null,
	flush?:
		| ((enqueue: (chunk: O, encoding?: string) => void) => void | Promise<void>)
		| StreamOptions,
	streamOptions?: StreamOptions,
): unknown;

export function createWritableStream<T = unknown>(
	write?: ((chunk: T) => void | Promise<void>) | null,
	close?: (() => void | Promise<void>) | StreamOptions,
	streamOptions?: StreamOptions,
): unknown;

// Backpressure (Node.js only)
export function backpressureGuage(streams: Record<string, unknown>): Record<
	string,
	{
		timeline: Array<{ timestamp: number; duration: number }>;
		total: { timestamp?: number; duration?: number };
	}
>;

// Timeout
export function timeout(
	ms: number,
	options?: { signal?: AbortSignal },
): Promise<void>;
