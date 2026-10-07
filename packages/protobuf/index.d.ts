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
		 * Maximum encoded size of a single message in bytes (checked per message,
		 * before decoding). Default 64MiB; null = unlimited. Decoded objects can
		 * be larger than their encoded form.
		 */
		maxMessageSize?: number | null;
	},
	streamOptions?: StreamOptions,
): DatastreamTransform<C>;

export function protobufLengthPrefixFrameStream(
	options?: Record<string, never>,
	streamOptions?: StreamOptions,
): DatastreamTransform<Uint8Array, Uint8Array>;

export function protobufLengthPrefixUnframeStream(
	options?: { maxMessageSize?: number | null },
	streamOptions?: StreamOptions,
): DatastreamTransform<Uint8Array, Uint8Array>;
