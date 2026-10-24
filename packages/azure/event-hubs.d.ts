// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
import type {
	DatastreamReadable,
	DatastreamWritable,
	StreamOptions,
} from "@datastream/core";

export function azureEventHubsReceiveEventsStream(
	options: {
		client: unknown;
		partitionId?: string;
		pollingActive?: boolean;
		maxBatchSize?: number;
		maxWaitTimeInSeconds?: number;
		[key: string]: unknown;
	},
	streamOptions?: StreamOptions,
): Promise<DatastreamReadable>;

export function azureEventHubsSendEventsStream(
	options: {
		client: unknown;
		partitionKey?: string;
		partitionId?: string;
		maxSizeInBytes?: number;
		[key: string]: unknown;
	},
	streamOptions?: StreamOptions,
): DatastreamWritable;
