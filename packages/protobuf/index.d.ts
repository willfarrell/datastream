// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
import type { DatastreamTransform, StreamOptions } from "@datastream/core";

export interface ProtobufType {
	encode(message: unknown): { finish(): Uint8Array };
	decode(buffer: Uint8Array): unknown;
	create(message: unknown): unknown;
}

// Type can be:
//   - a static ProtobufType value
//   - a sync function that takes the current chunk and returns a Type
//   - an async function returning a Promise<Type>
// When a static value is passed, it's cached so the hot path doesn't recompute.
export type ProtobufTypeInput =
	| ProtobufType
	| ((chunk: unknown) => ProtobufType | Promise<ProtobufType>);

export function protobufEncodeStream(
	options: { Type: ProtobufTypeInput },
	streamOptions?: StreamOptions,
): DatastreamTransform;

export function protobufDecodeStream<C = Uint8Array>(
	options: {
		Type: ProtobufTypeInput;
		/** Extract the protobuf payload bytes from each chunk. Default: identity. */
		payload?: (chunk: C) => Uint8Array;
		/**
		 * Caps the CUMULATIVE encoded INPUT bytes processed across the whole
		 * stream. This is a coarse input-volume guard, NOT a bound on decoded
		 * output memory: protobuf can expand a small encoded message into a much
		 * larger in-memory object graph (packed/repeated/nested fields), so the
		 * decoded objects may exceed this value.
		 */
		maxOutputSize?: number;
	},
	streamOptions?: StreamOptions,
): DatastreamTransform<C>;

export function protobufLengthPrefixFrameStream(
	options?: Record<string, never>,
	streamOptions?: StreamOptions,
): DatastreamTransform<Uint8Array, Uint8Array>;

export function protobufLengthPrefixUnframeStream(
	options?: { maxMessageSize?: number },
	streamOptions?: StreamOptions,
): DatastreamTransform<Uint8Array, Uint8Array>;
