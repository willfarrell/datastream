// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
import type {
	DatastreamReadable,
	DatastreamWritable,
	StreamOptions,
} from "@datastream/core";

export function awsKinesisSetClient(kinesisClient: unknown): void;

export function awsKinesisGetRecordsStream(
	options: {
		client?: unknown;
		ShardIterator?: string;
		pollingActive?: boolean;
		pollingDelay?: number;
		[key: string]: unknown;
	},
	streamOptions?: StreamOptions,
): Promise<DatastreamReadable>;

export function awsKinesisPutRecordsStream(
	options: {
		client?: unknown;
		StreamName?: string;
		StreamARN?: string;
		[key: string]: unknown;
	},
	streamOptions?: StreamOptions,
): DatastreamWritable;
