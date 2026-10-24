// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
import type {
	DatastreamReadable,
	DatastreamWritable,
	StreamOptions,
} from "@datastream/core";

export function awsCloudWatchLogsSetClient(cwlClient: unknown): void;

export function awsCloudWatchLogsGetLogEventsStream(
	options: {
		client?: unknown;
		logGroupName?: string;
		logGroupIdentifier?: string;
		logStreamName?: string;
		startTime?: number;
		endTime?: number;
		startFromHead?: boolean;
		pollingActive?: boolean;
		pollingDelay?: number;
		[key: string]: unknown;
	},
	streamOptions?: StreamOptions,
): Promise<DatastreamReadable>;

export function awsCloudWatchLogsFilterLogEventsStream(
	options: {
		client?: unknown;
		logGroupName?: string;
		logGroupIdentifier?: string;
		filterPattern?: string;
		startTime?: number;
		endTime?: number;
		logStreamNames?: string[];
		logStreamNamePrefix?: string;
		[key: string]: unknown;
	},
	streamOptions?: StreamOptions,
): Promise<DatastreamReadable>;

// `cause` of the "awsCloudWatchLogsPutLogEvents has rejected events" error.
// The rest of that batch WAS accepted: re-send only the `rejected` events.
export interface AwsCloudWatchLogsRejectedEvents {
	rejectedLogEventsInfo: {
		tooNewLogEventStartIndex?: number;
		tooOldLogEventEndIndex?: number;
		expiredLogEventEndIndex?: number;
	};
	rejected: {
		tooNew: { timestamp: number; message: string }[];
		tooOld: { timestamp: number; message: string }[];
		expired: { timestamp: number; message: string }[];
	};
}

// Writable of `{ timestamp, message }` events (finite number / string, else a
// TypeError), batched into PutLogEvents requests within the service limits.
// On destroy/abort, events still buffered are attached to the error as
// `failedMessages` (not sent).
export function awsCloudWatchLogsPutLogEventsStream(
	options: {
		client?: unknown;
		logGroupName: string;
		logStreamName: string;
		[key: string]: unknown;
	},
	streamOptions?: StreamOptions,
): DatastreamWritable<{ timestamp: number; message: string }>;
