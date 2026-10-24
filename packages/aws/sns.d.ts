// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
import type { DatastreamWritable, StreamOptions } from "@datastream/core";

export function awsSNSSetClient(snsClient: unknown): void;

// Batches by the SNS message size (Message + attribute names, types, values).
// On destroy/abort, entries still buffered are attached to the error as
// `failedMessages` (not sent).
export function awsSNSPublishMessageStream(
	options: {
		client?: unknown;
		TopicArn?: string;
		// Max retries of failed entries (default 10); null = unlimited.
		retryMaxCount?: number | null;
		[key: string]: unknown;
	},
	streamOptions?: StreamOptions,
): DatastreamWritable;
