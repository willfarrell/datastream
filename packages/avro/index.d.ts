// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
import type { DatastreamTransform, StreamOptions } from "@datastream/core";

export type AvroSchema = string | object;

export type AvroSchemaInput =
	| AvroSchema
	| (() => AvroSchema | Promise<AvroSchema>);

export type AvroResolvedSchema = AvroSchema | { schemaDefinition: string };

export interface AvroEnvelope {
	schemaId?: number;
	schemaVersionId?: string;
	payload: Uint8Array;
}

export function avroEncodeStream<T = unknown>(
	options: { schema: AvroSchemaInput },
	streamOptions?: StreamOptions,
): DatastreamTransform<T, Uint8Array>;

export function avroDecodeStream<T = unknown>(
	options: {
		schema: AvroSchemaInput;
		/** Max bytes per message (default 64MiB); null = unlimited. */
		maxMessageSize?: number | null;
	},
	streamOptions?: StreamOptions,
): DatastreamTransform<ArrayBufferView | ArrayBuffer, T>;

export function avroDecodeStream<
	T = unknown,
	E extends AvroEnvelope = { schemaId: number; payload: Uint8Array },
>(
	options: {
		schema?: never;
		// Method syntax: a resolver typed for one id kind (number or string)
		// must be assignable.
		resolveSchema(
			id: number | string,
		): AvroResolvedSchema | Promise<AvroResolvedSchema>;
		/** Max bytes per message (default 64MiB); null = unlimited. */
		maxMessageSize?: number | null;
	},
	streamOptions?: StreamOptions,
): DatastreamTransform<E, Omit<E, "payload"> & { value: T }>;
