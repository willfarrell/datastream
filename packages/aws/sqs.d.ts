// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
import type { StreamOptions } from "@datastream/core";

export function awsSQSSetClient(sqsClient: unknown): void;

export function awsSQSReceiveMessageStream(
	options: {
		client?: unknown;
		QueueUrl?: string;
		[key: string]: unknown;
	},
	streamOptions?: StreamOptions,
): Promise<unknown>;

export function awsSQSDeleteMessageStream(
	options: {
		client?: unknown;
		QueueUrl?: string;
		[key: string]: unknown;
	},
	streamOptions?: StreamOptions,
): unknown;

export function awsSQSSendMessageStream(
	options: {
		client?: unknown;
		QueueUrl?: string;
		[key: string]: unknown;
	},
	streamOptions?: StreamOptions,
): unknown;
