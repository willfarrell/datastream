// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
import type {
	DatastreamTransform,
	ResultStream,
	StreamOptions,
} from "@datastream/core";

export interface ConfluentSchemaIdResult {
	schemaId: number | null;
}

export interface ConfluentUnframeResult extends ConfluentSchemaIdResult {
	/** Distinct schema ids seen, in first-seen order, up to `maxSchemaIds`. */
	schemaIds: number[];
	/** Frames whose (new) id was not recorded because `maxSchemaIds` was reached. */
	untrackedSchemaIds: number;
}

export interface ConfluentEnvelope {
	schemaId: number;
	payload: Uint8Array;
}

export interface GlueSchemaResult {
	schemaVersionId: string | null;
	compression: "none" | "zlib" | null;
}

export interface GlueUnframeResult extends GlueSchemaResult {
	/** Distinct schema version ids seen, in first-seen order, up to `maxSchemaIds`. */
	schemaVersionIds: string[];
	/** Frames whose (new) id was not recorded because `maxSchemaIds` was reached. */
	untrackedSchemaVersionIds: number;
}

export interface GlueEnvelope {
	schemaVersionId: string;
	compression: "none" | "zlib";
	payload: Uint8Array;
}

/**
 * A message envelope carrying one frame in `value`, e.g. kafkaConsumeStream's
 * messages. A null `value` (a Kafka tombstone) errors the stream.
 */
export interface FramedMessage {
	value: Uint8Array | null;
}

/** An unframed envelope: the input message's fields plus the decoded ones. */
export type Unframed<I, E> = I extends FramedMessage ? Omit<I, keyof E> & E : E;

export function confluentFrameStream(
	options: { schemaId: number; resultKey?: string },
	streamOptions?: StreamOptions,
): DatastreamTransform<Uint8Array, Uint8Array> &
	ResultStream<ConfluentSchemaIdResult>;

export function confluentUnframeStream<
	I extends Uint8Array | FramedMessage = Uint8Array,
>(
	options?: { maxSchemaIds?: number | null; resultKey?: string },
	streamOptions?: StreamOptions,
): DatastreamTransform<I, Unframed<I, ConfluentEnvelope>> &
	ResultStream<ConfluentUnframeResult>;

export function glueFrameStream(
	options: {
		schemaVersionId: string;
		compression?: "none" | "zlib";
		maxOutputSize?: number | null;
		resultKey?: string;
	},
	streamOptions?: StreamOptions,
): DatastreamTransform<Uint8Array, Uint8Array> & ResultStream<GlueSchemaResult>;

export function glueUnframeStream<
	I extends Uint8Array | FramedMessage = Uint8Array,
>(
	options?: {
		maxOutputSize?: number | null;
		maxSchemaIds?: number | null;
		resultKey?: string;
	},
	streamOptions?: StreamOptions,
): DatastreamTransform<I, Unframed<I, GlueEnvelope>> &
	ResultStream<GlueUnframeResult>;

export interface ConfluentSchema {
	schemaId: number;
	/** Schema text as stored in the registry (Avro/JSON Schema JSON, or .proto source). */
	schemaDefinition: string;
	dataFormat: "AVRO" | "PROTOBUF" | "JSON";
	/** The registry's schema references, as returned (empty when it has none). */
	references: ConfluentSchemaReference[];
}

export interface ConfluentSchemaReference {
	name: string;
	subject: string;
	version: number;
}

export function confluentSchemaRegistryResolver(options: {
	/** Registry base URL, e.g. https://registry.example.com */
	url: string;
	/** Request headers, e.g. Authorization. */
	headers?: Record<string, string>;
	/** Cache TTL in ms; -1 (default) = never expires. */
	cacheExpiry?: number;
	maxCacheSize?: number | null;
	/** Per-lookup timeout in ms, covering the response body. Default 30000. */
	timeout?: number;
	/** Aborts in-flight lookups. */
	signal?: AbortSignal;
	/** Max response body bytes (default 1MiB); null = unlimited. */
	maxBodySize?: number | null;
	/** fetch implementation. Default: globalThis.fetch. */
	fetch?: typeof fetch;
}): (schemaId: number) => Promise<ConfluentSchema>;
