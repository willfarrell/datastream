// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
import type {
	DatastreamPassThrough,
	DatastreamTransform,
	ResultStream,
	StreamOptions,
} from "@datastream/core";
import type { RecordBatch, Schema } from "apache-arrow";

export interface ArrowDetectSchemaResult {
	schema: Schema | null;
	fields: string[] | null;
}

export function arrowDetectSchemaStream(
	options?: { sampleSize?: number; resultKey?: string },
	streamOptions?: StreamOptions,
): DatastreamPassThrough & ResultStream<ArrowDetectSchemaResult>;

export function arrowBatchFromArrayStream(
	options: { schema: Schema | (() => Schema); batchSize?: number },
	streamOptions?: StreamOptions,
): DatastreamTransform<unknown[], RecordBatch>;

export function arrowBatchFromObjectStream(
	options: {
		schema: Schema | (() => Schema);
		batchSize?: number;
	},
	streamOptions?: StreamOptions,
): DatastreamTransform<Record<string, unknown>, RecordBatch>;

export function arrowToArrayStream(
	options?: Record<string, never>,
	streamOptions?: StreamOptions,
): DatastreamTransform<RecordBatch, unknown[]>;

export function arrowToObjectStream(
	options?: Record<string, never>,
	streamOptions?: StreamOptions,
): DatastreamTransform<RecordBatch, Record<string, unknown>>;
