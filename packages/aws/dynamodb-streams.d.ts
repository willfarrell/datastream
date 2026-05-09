// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
import type { DatastreamReadable, StreamOptions } from "@datastream/core";

export function awsDynamoDBStreamsSetClient(
	dynamoDBStreamsClient: unknown,
): void;

export function awsDynamoDBStreamsGetRecordsStream(
	options: {
		client?: unknown;
		ShardIterator?: string;
		pollingActive?: boolean;
		pollingDelay?: number;
		[key: string]: unknown;
	},
	streamOptions?: StreamOptions,
): Promise<DatastreamReadable>;
