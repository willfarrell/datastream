// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
import type {
	ReadableInput,
	ResultStream,
	StreamCollectorOptions,
	StreamOptions,
	WritableStreamOptions,
} from "./types.js";

export type {
	ResultStream,
	StreamCollectorOptions,
	StreamOptions,
	StreamResult,
	WritableStreamOptions,
} from "./types.js";
export {
	concatBytes,
	createChunkDecoder,
	makeOptions,
	resolveLazy,
	timeout,
} from "./types.js";

// Method syntax keeps push bivariant, so a DatastreamReadable<string> is still
// assignable to DatastreamReadable<unknown>.
export type DatastreamReadable<T = unknown> = ReadableStream<T> & {
	push?(chunk: T | null): void;
};
export type DatastreamTransform<I = unknown, O = I> = TransformStream<I, O>;
export type DatastreamWritable<T = unknown> = WritableStream<T> &
	Partial<ResultStream>;
export type DatastreamPassThrough<T = unknown> = TransformStream<T, T> &
	Partial<ResultStream>;
export type DatastreamStream =
	| DatastreamReadable
	| DatastreamTransform
	| DatastreamWritable
	| DatastreamPassThrough;

export function pipeline(
	streams: DatastreamStream[],
	streamOptions?: StreamOptions,
): Promise<Record<string, unknown>>;
/** Errors tear the chain down and surface on the returned stream. */
export function pipejoin(streams: DatastreamStream[]): DatastreamStream;
export function result(
	streams: DatastreamStream[],
): Promise<Record<string, unknown>>;

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
): Promise<Uint8Array>;

export function isReadable(
	stream: unknown,
): stream is ReadableStream | { readable: unknown };
export function isWritable(
	stream: unknown,
): stream is WritableStream | { writable: unknown };

export function createReadableStream<T = unknown>(
	input?: ReadableInput<T>,
	streamOptions?: StreamOptions,
): DatastreamReadable<T>;

export function createPassThroughStream<T = unknown>(
	passThrough?: ((chunk: T) => void | Promise<void>) | null,
	flush?: (() => void | Promise<void>) | StreamOptions,
	streamOptions?: StreamOptions,
): DatastreamPassThrough<T>;

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
	close?: (() => void | Promise<void>) | WritableStreamOptions,
	streamOptions?: WritableStreamOptions,
): DatastreamWritable<T>;
