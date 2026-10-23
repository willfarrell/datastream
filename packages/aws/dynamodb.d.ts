// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
import type {
	DatastreamReadable,
	DatastreamWritable,
	StreamOptions,
} from "@datastream/core";

export function awsDynamoDBSetClient(ddbClient: unknown): void;

export function awsDynamoDBQueryStream(
	options: {
		client?: unknown;
		TableName?: string;
		[key: string]: unknown;
	},
	streamOptions?: StreamOptions,
): Promise<DatastreamReadable>;

export function awsDynamoDBScanStream(
	options: {
		client?: unknown;
		TableName?: string;
		[key: string]: unknown;
	},
	streamOptions?: StreamOptions,
): Promise<DatastreamReadable>;

export function awsDynamoDBExecuteStatementStream(
	options: {
		client?: unknown;
		Statement?: string;
		NextToken?: string;
		[key: string]: unknown;
	},
	streamOptions?: StreamOptions,
): Promise<DatastreamReadable>;

export function awsDynamoDBGetItemStream(
	options: {
		client?: unknown;
		Keys?: unknown[];
		TableName?: string;
		ConsistentRead?: boolean;
		ProjectionExpression?: string;
		ExpressionAttributeNames?: Record<string, string>;
		AttributesToGet?: string[];
		ReturnConsumedCapacity?: "INDEXES" | "TOTAL" | "NONE";
		// Max retries of failed entries (default 10); null = unlimited.
		retryMaxCount?: number | null;
	},
	streamOptions?: StreamOptions,
): Promise<DatastreamReadable>;

// On destroy/abort, entries still buffered are attached to the error as
// `failedMessages` (not sent).
export function awsDynamoDBPutItemStream(
	options: {
		client?: unknown;
		TableName?: string;
		// Request-level BatchWriteItem fields, sent on every request and retry.
		ReturnConsumedCapacity?: "INDEXES" | "TOTAL" | "NONE";
		ReturnItemCollectionMetrics?: "SIZE" | "NONE";
		// Max retries of failed entries (default 10); null = unlimited.
		retryMaxCount?: number | null;
		[key: string]: unknown;
	},
	streamOptions?: StreamOptions,
): DatastreamWritable;

// On destroy/abort, entries still buffered are attached to the error as
// `failedMessages` (not sent).
export function awsDynamoDBDeleteItemStream(
	options: {
		client?: unknown;
		TableName?: string;
		// Request-level BatchWriteItem fields, sent on every request and retry.
		ReturnConsumedCapacity?: "INDEXES" | "TOTAL" | "NONE";
		ReturnItemCollectionMetrics?: "SIZE" | "NONE";
		// Max retries of failed entries (default 10); null = unlimited.
		retryMaxCount?: number | null;
		[key: string]: unknown;
	},
	streamOptions?: StreamOptions,
): DatastreamWritable;
