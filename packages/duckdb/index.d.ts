// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
import type {
	DatastreamReadable,
	DatastreamWritable,
	StreamOptions,
} from "@datastream/core";
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

// Rows are plain objects. BIGINT is a bigint on both builds; DATE/TIMESTAMP
// are a Date (node) or epoch-ms number (browser). Destroying the stream while
// its query is running interrupts the whole connection, so give concurrent
// queries their own connection.
export function duckdbQueryStream(
	options: {
		db: unknown;
		sql: string;
		/** Values bound to positional `?` params in order. */
		params?: unknown[];
	},
	streamOptions?: StreamOptions,
): DatastreamReadable<Record<string, unknown>>;

// Both write streams run in their own transaction on `db` (BEGIN on the first
// write, COMMIT at the end, ROLLBACK on any error or abort), so a failed
// pipeline commits nothing. `db` must not already have a transaction open.
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
