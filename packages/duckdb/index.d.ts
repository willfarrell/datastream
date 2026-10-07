// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
import type { DatastreamWritable, StreamOptions } from "@datastream/core";
import type { RecordBatch, Schema } from "apache-arrow";

// browser: duckdb-wasm bundles (see `DuckDBBundles` in @duckdb/duckdb-wasm).
// Defaults to jsDelivr; pass self-hosted bundles in production.
export interface DuckDBWebConnectOptions {
	bundles?: Record<string, unknown>;
}

// node: `options` are DuckDB instance options; browser: DuckDBWebConnectOptions.
export function duckdbConnect(
	path?: string,
	options?: Record<string, string> | DuckDBWebConnectOptions,
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
		// browser: insert once this many rows are buffered (default 100_000).
		batchRows?: number;
	},
	streamOptions?: StreamOptions,
): Promise<DatastreamWritable<RecordBatch>>;
