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

export function confluentFrameStream(
	options: { schemaId: number; resultKey?: string },
	streamOptions?: StreamOptions,
): DatastreamTransform<Uint8Array, Uint8Array> &
	ResultStream<ConfluentSchemaIdResult>;

export function confluentUnframeStream(
	options?: { maxSchemaIds?: number | null; resultKey?: string },
	streamOptions?: StreamOptions,
): DatastreamTransform<Uint8Array, ConfluentEnvelope> &
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

export function glueUnframeStream(
	options?: {
		maxOutputSize?: number | null;
		maxSchemaIds?: number | null;
		resultKey?: string;
	},
	streamOptions?: StreamOptions,
): DatastreamTransform<Uint8Array, GlueEnvelope> &
	ResultStream<GlueUnframeResult>;
