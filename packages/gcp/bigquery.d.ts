// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
import type { Writable } from "node:stream";
import type {
	DatastreamReadable,
	DatastreamWritable,
	StreamOptions,
	StreamResult,
} from "@datastream/core";

/** A row as the SDK returns / accepts it. */
export type GcpBigQueryRow = Record<string, unknown>;

/** The parts of an @google-cloud/bigquery `BigQuery` the query stream uses. */
export interface GcpBigQueryClient {
	createQueryStream(
		query: Record<string, unknown>,
	): AsyncIterable<GcpBigQueryRow> & { end(): unknown };
}

/** The parts of an @google-cloud/bigquery `Table` the load stream uses. */
export interface GcpBigQueryLoadTable {
	createWriteStream(metadata: Record<string, unknown>): Writable;
}

/** The parts of an @google-cloud/bigquery `Table` the insert stream uses. */
export interface GcpBigQueryInsertTable {
	insert(
		rows: GcpBigQueryRow[],
		options: Record<string, unknown>,
	): Promise<unknown>;
}

export interface GcpBigQueryLoadResult {
	jobId: string;
	/** statistics.load.outputRows as a number. */
	outputRows: number;
	/** The job's statistics (statistics.load: inputFileBytes, badRecords, ...). */
	statistics: Record<string, unknown>;
}

export function gcpBigQueryQueryStream(
	options: {
		bigquery: GcpBigQueryClient;
		query: string;
		params?: unknown[] | Record<string, unknown>;
		/** INT64 as BigQueryInt (string-backed) instead of a lossy Number. */
		wrapIntegers?: boolean | Record<string, unknown>;
		/** JSON columns parsed instead of returned as strings. */
		parseJSON?: boolean;
		location?: string;
		[key: string]: unknown;
	},
	streamOptions?: StreamOptions,
): Promise<DatastreamReadable<GcpBigQueryRow>>;

export function gcpBigQueryLoadStream(
	options: {
		table: GcpBigQueryLoadTable;
		/** Defaults to "NEWLINE_DELIMITED_JSON". */
		sourceFormat?:
			| "NEWLINE_DELIMITED_JSON"
			| "CSV"
			| "AVRO"
			| "PARQUET"
			| "ORC"
			| (string & {});
		/** pipeline() result key. Defaults to "bigqueryLoad". */
		resultKey?: string;
		/** Other JobConfigurationLoad fields (schema, writeDisposition, ...). */
		[key: string]: unknown;
	},
	streamOptions?: StreamOptions,
): DatastreamWritable<string | Uint8Array> & {
	result: () => StreamResult<GcpBigQueryLoadResult>;
};

export function gcpBigQueryInsertStream(
	options: {
		table: GcpBigQueryInsertTable;
		/** Rows per insertAll request. Defaults to 500. */
		batchSize?: number;
		/** JSON bytes per request. Defaults to 9 MiB (limit: 10 MB). */
		maxBatchBytes?: number;
		/** Rows are already { insertId?, json }. */
		raw?: boolean;
		skipInvalidRows?: boolean;
		ignoreUnknownValues?: boolean;
		/** Defaults to true (a random insertId per row and attempt). */
		createInsertId?: boolean;
		/** SDK retries of partially failed rows. Defaults to 3. */
		partialRetries?: number;
		[key: string]: unknown;
	},
	streamOptions?: StreamOptions,
): DatastreamWritable<GcpBigQueryRow>;
