// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
import type { DatastreamWritable, StreamOptions } from "@datastream/core";
import type { RecordBatch, Schema } from "apache-arrow";

export function duckdbConnect(
	path?: string,
	options?: Record<string, string>,
): Promise<unknown>;

export function duckdbAppenderStream(
	options: {
		db: unknown;
		table: string;
		schema?: Schema | (() => Schema);
	},
	streamOptions?: StreamOptions,
): Promise<DatastreamWritable<unknown[] | Record<string, unknown>>>;

export function duckdbArrowInsertStream(
	options: {
		db: unknown;
		table: string;
		schema?: Schema | (() => Schema);
	},
	streamOptions?: StreamOptions,
): Promise<DatastreamWritable<RecordBatch>>;
