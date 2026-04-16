// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
import type { StreamOptions } from "@datastream/core";

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
): Promise<unknown>;

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
): Promise<unknown>;
