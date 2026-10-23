// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
import type { DatastreamTransform, StreamOptions } from "@datastream/core";

// An Avro schema object (record/enum/array/union...), its JSON text (e.g. a
// schema registry's schemaDefinition), or a compiled avsc Type.
export type AvroSchema = string | object;

// A schema, or a zero-argument function returning one (or a Promise of one).
// A function is called once, when the first chunk arrives, so upstream can
// produce the schema (e.g. from a schema registry lookup) before it is needed.
export type AvroSchemaInput =
	| AvroSchema
	| (() => AvroSchema | Promise<AvroSchema>);

// What resolveSchema may return: a schema, or a schema registry resolver's
// result (confluentSchemaRegistryResolver, awsGlueSchemaRegistryResolver).
export type AvroResolvedSchema = AvroSchema | { schemaDefinition: string };

// An unframe stream's envelope (confluentUnframeStream, glueUnframeStream),
// possibly carrying the original message's fields (topic, partition, ...).
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

// Per-message schemas: each chunk is an envelope, its schema is looked up by
// schemaId ?? schemaVersionId, and { ...envelope, value } is emitted without
// payload.
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
