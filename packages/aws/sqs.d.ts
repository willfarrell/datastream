// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
import type {
	DatastreamReadable,
	DatastreamWritable,
	StreamOptions,
} from "@datastream/core";

export function awsSQSSetClient(sqsClient: unknown): void;

export function awsSQSReceiveMessageStream(
	options: {
		client?: unknown;
		QueueUrl?: string;
		pollingActive?: boolean;
		pollingDelay?: number;
		[key: string]: unknown;
	},
	streamOptions?: StreamOptions,
): Promise<DatastreamReadable>;

export function awsSQSDeleteMessageStream(
	options: {
		client?: unknown;
		QueueUrl?: string;
		[key: string]: unknown;
	},
	streamOptions?: StreamOptions,
): DatastreamWritable;

export function awsSQSSendMessageStream(
	options: {
		client?: unknown;
		QueueUrl?: string;
		[key: string]: unknown;
	},
	streamOptions?: StreamOptions,
): DatastreamWritable;
