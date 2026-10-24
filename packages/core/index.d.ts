// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
import type { Readable, Transform, Writable } from "node:stream";
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
export type DatastreamReadable<T = unknown> = (ReadableStream<T> | Readable) & {
	push?(chunk: T | null): void;
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
/** Errors tear the chain down and surface on the returned stream. */
export function pipejoin(streams: DatastreamStream[]): DatastreamStream;
export function result(
	streams: DatastreamStream[],
): Promise<Record<string, unknown>>;

// Stream converters
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

export function isReadable(stream: unknown): stream is Readable;
export function isWritable(stream: unknown): stream is Writable;

// Stream creators
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

export function backpressureGauge(streams: Record<string, unknown>): Record<
	string,
	{
		timeline: Array<{ timestamp: number; duration: number }>;
		total: { timestamp?: number; duration?: number };
	}
>;
