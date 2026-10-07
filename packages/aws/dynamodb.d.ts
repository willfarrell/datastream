// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
import type {
	DatastreamReadable,
	DatastreamWritable,
	StreamOptions,
} from "@datastream/core";

export function awsDynamoDBSetClient(
	ddbClient: unknown,
	translateConfig?: unknown,
): void;

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
		retryCount?: number;
		// Max retries of failed entries (default 10); null = unlimited.
		retryMaxCount?: number | null;
	},
	streamOptions?: StreamOptions,
): Promise<DatastreamReadable>;

export function awsDynamoDBPutItemStream(
	options: {
		client?: unknown;
		TableName?: string;
		// Max retries of failed entries (default 10); null = unlimited.
		retryMaxCount?: number | null;
		[key: string]: unknown;
	},
	streamOptions?: StreamOptions,
): DatastreamWritable;

export function awsDynamoDBDeleteItemStream(
	options: {
		client?: unknown;
		TableName?: string;
		// Max retries of failed entries (default 10); null = unlimited.
		retryMaxCount?: number | null;
		[key: string]: unknown;
	},
	streamOptions?: StreamOptions,
): DatastreamWritable;
