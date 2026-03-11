// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
import type { StreamOptions } from "@datastream/core";

export function awsSNSSetClient(snsClient: unknown): void;

export function awsSNSPublishMessageStream(
	options: {
		client?: unknown;
		TopicArn?: string;
		[key: string]: unknown;
	},
	streamOptions?: StreamOptions,
): unknown;
