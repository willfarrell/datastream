// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
import {
	DeleteMessageBatchCommand,
	ReceiveMessageCommand,
	SendMessageBatchCommand,
	SQSClient,
} from "@aws-sdk/client-sqs";
import { timeout } from "@datastream/core";
import {
	awsBatchEntriesStream,
	awsClientDefaults,
	awsReadableStream,
} from "./client.js";

// SQS SendMessageBatch aggregate payload limit (1MiB).
const SQS_MAX_BATCH_BYTES = 1_048_576;

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
	async function* command(options, signal) {
		let expectMore = true;
		while (expectMore) {
			const response = await (client ?? getDefaultClient()).send(
				new ReceiveMessageCommand(options),
				{
					abortSignal: signal,
				},
			);
			const messages = response.Messages ?? [];
			for (const item of messages) {
				yield item;
			}
			expectMore = pollingActive || messages.length > 0;
			if (pollingActive && messages.length === 0 && pollingDelay > 0) {
				// Abortable idle wait: rejects immediately and clears the timer
				// when the stream is destroyed or streamOptions.signal aborts mid-delay.
				await timeout(pollingDelay, { signal });
			}
		}
	}
	return awsReadableStream(
		(signal) => command(sqsOptions, signal),
		streamOptions,
	);
};

const sqsBatchStream = (Command, batchOptions, options, streamOptions) => {
	const { retryMaxCount, client, ...sendOptions } = options;
	return awsBatchEntriesStream(
		(entries) =>
			(client ?? getDefaultClient()).send(
				new Command({ ...sendOptions, Entries: entries }),
				{ abortSignal: streamOptions.signal },
			),
		{ retryMaxCount, ...batchOptions },
		streamOptions,
	);
};

export const awsSQSDeleteMessageStream = (options, streamOptions = {}) =>
	sqsBatchStream(
		DeleteMessageBatchCommand,
		{
			// DeleteMessageBatch carries no message bytes (only the 10-entry cap),
			// even when received messages with attributes are piped straight in.
			maxBatchBytes: Number.POSITIVE_INFINITY,
			errorMessage: "awsSQSDeleteMessageBatch has failed entries",
			destroyMessage:
				"awsSQSDeleteMessageBatch destroyed with buffered entries",
		},
		options,
		streamOptions,
	);

export const awsSQSSendMessageStream = (options, streamOptions = {}) =>
	sqsBatchStream(
		SendMessageBatchCommand,
		{
			maxBatchBytes: SQS_MAX_BATCH_BYTES,
			errorMessage: "awsSQSSendMessageBatch has failed entries",
			oversizeMessage: "awsSQSSendMessageBatch entry exceeds 1MiB limit",
			destroyMessage: "awsSQSSendMessageBatch destroyed with buffered entries",
		},
		options,
		streamOptions,
	);
