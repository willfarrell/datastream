// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
import {
	DeleteMessageBatchCommand,
	ReceiveMessageCommand,
	SendMessageBatchCommand,
	SQSClient,
} from "@aws-sdk/client-sqs";
import { timeout } from "@datastream/core";
import { awsBatchEntriesStream, awsClientDefaults } from "./client.js";

// Created on first use, so importing the module (or always passing a per-call
// client) never constructs an unused SDK client.
let defaultClient;
const getDefaultClient = () =>
	(defaultClient ??= new SQSClient(awsClientDefaults));
export const awsSQSSetClient = (sqsClient) => {
	defaultClient = sqsClient;
};

export const awsSQSReceiveMessageStream = async (
	options,
	streamOptions = {},
) => {
	const { pollingActive, pollingDelay = 1000, client, ...sqsOptions } = options;
	async function* command(options) {
		let expectMore = true;
		while (expectMore) {
			const response = await (client ?? getDefaultClient()).send(
				new ReceiveMessageCommand(options),
				{
					abortSignal: streamOptions.signal,
				},
			);
			const messages = response.Messages ?? [];
			for (const item of messages) {
				yield item;
			}
			expectMore = pollingActive || messages.length > 0;
			if (pollingActive && messages.length === 0 && pollingDelay > 0) {
				// Abortable idle wait: rejects immediately and clears the timer
				// when streamOptions.signal aborts mid-delay.
				await timeout(pollingDelay, { signal: streamOptions.signal });
			}
		}
	}
	return command(sqsOptions);
};

const sqsBatchStream = (
	Command,
	errorMessage,
	oversizeMessage,
	options,
	streamOptions,
) => {
	const { retryMaxCount, client, ...sendOptions } = options;
	return awsBatchEntriesStream(
		(entries) =>
			(client ?? getDefaultClient()).send(
				new Command({ ...sendOptions, Entries: entries }),
				{ abortSignal: streamOptions.signal },
			),
		{ retryMaxCount, errorMessage, oversizeMessage },
		streamOptions,
	);
};

export const awsSQSDeleteMessageStream = (options, streamOptions = {}) =>
	sqsBatchStream(
		DeleteMessageBatchCommand,
		"awsSQSDeleteMessageBatch has failed entries",
		"awsSQSDeleteMessageBatch entry exceeds 256KiB limit",
		options,
		streamOptions,
	);

export const awsSQSSendMessageStream = (options, streamOptions = {}) =>
	sqsBatchStream(
		SendMessageBatchCommand,
		"awsSQSSendMessageBatch has failed entries",
		"awsSQSSendMessageBatch entry exceeds 256KiB limit",
		options,
		streamOptions,
	);
