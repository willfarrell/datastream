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

// Batches by count only (DeleteMessageBatch carries no message bytes).
// On destroy/abort, entries still buffered are attached to the error as
// `failedMessages` (not sent).
export function awsSQSDeleteMessageStream(
	options: {
		client?: unknown;
		QueueUrl?: string;
		// Max retries of failed entries (default 10); null = unlimited.
		retryMaxCount?: number | null;
		[key: string]: unknown;
	},
	streamOptions?: StreamOptions,
): DatastreamWritable;

// Batches by the SQS message size (MessageBody + attribute names, types, values).
// On destroy/abort, entries still buffered are attached to the error as
// `failedMessages` (not sent).
export function awsSQSSendMessageStream(
	options: {
		client?: unknown;
		QueueUrl?: string;
		// Max retries of failed entries (default 10); null = unlimited.
		retryMaxCount?: number | null;
		[key: string]: unknown;
	},
	streamOptions?: StreamOptions,
): DatastreamWritable;
