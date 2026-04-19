// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
import type { Readable, Transform, Writable } from "node:stream";

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

export interface ResultStream<T = unknown> {
	result: () => StreamResult<T> | Promise<StreamResult<T>>;
}

export type DatastreamReadable<T = unknown> = (ReadableStream<T> | Readable) & {
	push?: (chunk: T | null) => void;
};
export type DatastreamTransform<I = unknown, O = I> =
	| TransformStream<I, O>
	| Transform;
export type DatastreamWritable<T = unknown> = (WritableStream<T> | Writable) &
	Partial<ResultStream>;
export type DatastreamPassThrough<T = unknown> = (
	| TransformStream<T, T>
	| Transform
) &
	Partial<ResultStream>;
export type DatastreamStream =
	| DatastreamReadable
	| DatastreamTransform
	| DatastreamWritable
	| DatastreamPassThrough;

// Pipeline & utilities
export function pipeline(
	streams: DatastreamStream[],
	streamOptions?: StreamOptions,
): Promise<Record<string, unknown>>;
export function pipejoin(
	streams: DatastreamStream[],
	onError?: (error: Error) => void,
): DatastreamStream;
export function result(
	streams: DatastreamStream[],
): Promise<Record<string, unknown>>;

// Stream converters
export interface StreamCollectorOptions {
	maxBufferSize?: number;
}

export function streamToArray<T = unknown>(
	stream: DatastreamReadable<T>,
	options?: StreamCollectorOptions,
): Promise<T[]>;
export function streamToObject<T = Record<string, unknown>>(
	stream: DatastreamReadable,
	options?: StreamCollectorOptions,
): Promise<T>;
export function streamToString(
	stream: DatastreamReadable,
	options?: StreamCollectorOptions,
): Promise<string>;
export function streamToBuffer(
	stream: DatastreamReadable,
	options?: StreamCollectorOptions,
): Promise<Buffer>;

// Type guards
export function isReadable(stream: unknown): stream is ReadableStream;
export function isWritable(stream: unknown): stream is WritableStream;

// Options helper
export function makeOptions(options?: StreamOptions): Record<string, unknown>;

// Stream creators
export function createReadableStream<T = unknown>(
	input?: string | T[] | Iterable<T> | AsyncIterable<T>,
	streamOptions?: StreamOptions,
): DatastreamReadable<T>;
export function createReadableStreamFromString(
	input: string,
	streamOptions?: StreamOptions,
): DatastreamReadable<string>;
export function createReadableStreamFromArrayBuffer(
	input: ArrayBuffer | ArrayBufferLike,
	streamOptions?: StreamOptions,
): DatastreamReadable<Uint8Array>;

export function createPassThroughStream<T = unknown>(
	passThrough?: ((chunk: T) => void | Promise<void>) | null,
	flush?: (() => void | Promise<void>) | StreamOptions,
	streamOptions?: StreamOptions,
): DatastreamPassThrough<T> & ResultStream;

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
): DatastreamTransform<I, O>;

export function createWritableStream<T = unknown>(
	write?: ((chunk: T) => void | Promise<void>) | null,
	close?: (() => void | Promise<void>) | StreamOptions,
	streamOptions?: StreamOptions,
): DatastreamWritable<T>;

// Backpressure (Node.js only)
export function backpressureGauge(streams: Record<string, unknown>): Record<
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

// Shared helpers
export function resolveLazy<T>(value: T | (() => T)): T;
export function shallowClone<T extends object>(obj: T): T;
export function deepClone<T>(obj: T): T;
export function shallowEqual(a: unknown, b: unknown): boolean;
export function deepEqual(a: unknown, b: unknown): boolean;
