// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
import type {
	DatastreamReadable,
	DatastreamWritable,
	StreamOptions,
} from "@datastream/core";

export function azureServiceBusReceiveMessagesStream(
	options: {
		// ServiceBusReceiver
		client: unknown;
		maxMessageCount?: number;
		maxWaitTimeInMs?: number;
		pollingActive?: boolean;
		pollingDelay?: number;
		[key: string]: unknown;
	},
	streamOptions?: StreamOptions,
): Promise<DatastreamReadable>;

export function azureServiceBusSendMessagesStream(
	options: {
		// ServiceBusSender
		client: unknown;
		maxSizeInBytes?: number;
		[key: string]: unknown;
	},
	streamOptions?: StreamOptions,
): DatastreamWritable;

export function azureServiceBusCompleteMessageStream(
	options: {
		// ServiceBusReceiver
		client: unknown;
	},
	streamOptions?: StreamOptions,
): DatastreamWritable;

type ServiceBusPropertyValue = number | boolean | string | Date | null;

export function azureServiceBusAbandonMessageStream(
	options: {
		// ServiceBusReceiver
		client: unknown;
		propertiesToModify?: Record<string, ServiceBusPropertyValue>;
	},
	streamOptions?: StreamOptions,
): DatastreamWritable;

export function azureServiceBusDeadLetterMessageStream(
	options: {
		// ServiceBusReceiver
		client: unknown;
		deadLetterReason?: string;
		deadLetterErrorDescription?: string;
		[key: string]: unknown;
	},
	streamOptions?: StreamOptions,
): DatastreamWritable;
