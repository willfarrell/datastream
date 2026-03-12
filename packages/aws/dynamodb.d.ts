// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
import type { StreamOptions } from "@datastream/core";

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
): Promise<unknown>;

export function awsDynamoDBScanStream(
	options: {
		client?: unknown;
		TableName?: string;
		[key: string]: unknown;
	},
	streamOptions?: StreamOptions,
): Promise<unknown>;

export function awsDynamoDBGetItemStream(
	options: {
		client?: unknown;
		Keys?: unknown[];
		TableName?: string;
		retryCount?: number;
		retryMaxCount?: number;
		[key: string]: unknown;
	},
	streamOptions?: StreamOptions,
): Promise<unknown>;

export function awsDynamoDBPutItemStream(
	options: {
		client?: unknown;
		TableName?: string;
		[key: string]: unknown;
	},
	streamOptions?: StreamOptions,
): unknown;

export function awsDynamoDBDeleteItemStream(
	options: {
		client?: unknown;
		TableName?: string;
		[key: string]: unknown;
	},
	streamOptions?: StreamOptions,
): unknown;
