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

export interface ConfluentEnvelope {
	schemaId: number;
	payload: Uint8Array;
}

export interface GlueSchemaResult {
	schemaVersionId: string | null;
	compression: "none" | "zlib" | null;
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
	options?: { resultKey?: string },
	streamOptions?: StreamOptions,
): DatastreamTransform<Uint8Array, ConfluentEnvelope> &
	ResultStream<ConfluentSchemaIdResult>;

export function glueFrameStream(
	options: {
		schemaVersionId: string;
		compression?: "none" | "zlib";
		resultKey?: string;
	},
	streamOptions?: StreamOptions,
): DatastreamTransform<Uint8Array, Uint8Array> & ResultStream<GlueSchemaResult>;

export function glueUnframeStream(
	options?: { maxDecompressedBytes?: number; resultKey?: string },
	streamOptions?: StreamOptions,
): DatastreamTransform<Uint8Array, GlueEnvelope> &
	ResultStream<GlueSchemaResult>;
