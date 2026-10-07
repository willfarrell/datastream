// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
import type {
	DatastreamReadable,
	DatastreamWritable,
	StreamOptions,
} from "@datastream/core";

export function azureQueueReceiveMessagesStream(
	options: {
		// QueueClient
		client: unknown;
		numberOfMessages?: number;
		visibilityTimeout?: number;
		pollingActive?: boolean;
		pollingDelay?: number;
		[key: string]: unknown;
	},
	streamOptions?: StreamOptions,
): Promise<DatastreamReadable>;

export function azureQueueSendMessageStream(
	options: {
		// QueueClient
		client: unknown;
		[key: string]: unknown;
	},
	streamOptions?: StreamOptions,
): DatastreamWritable;

export function azureQueueDeleteMessageStream(
	options: {
		// QueueClient
		client: unknown;
	},
	streamOptions?: StreamOptions,
): DatastreamWritable;
