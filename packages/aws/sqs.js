// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
import {
	DeleteMessageBatchCommand,
	ReceiveMessageCommand,
	SendMessageBatchCommand,
	SQSClient,
} from "@aws-sdk/client-sqs";
import { createWritableStream, timeout } from "@datastream/core";
import { awsClientDefaults } from "./client.js";

// SendMessageBatch/DeleteMessageBatch: <=10 entries, <=256KB aggregate payload.
const SQS_MAX_ENTRIES = 10;
const SQS_MAX_BATCH_BYTES = 256 * 1024;

// Partial failures are overwhelmingly throttling-driven; a near-zero early
// delay (3^0 == 1ms) just hammers the throttled endpoint. Apply a floor so the
// first retries give capacity time to recover, while preserving the ~59sec cap.
const BACKOFF_FLOOR_MS = 50;
const BACKOFF_CAP_MS = 3 ** 10;
// streamOptions is always supplied by the exported stream functions (defaulting
// to {}), so it is never nullish here.
const backoff = (retryCount, streamOptions) =>
	timeout(
		Math.min(BACKOFF_CAP_MS, Math.max(BACKOFF_FLOOR_MS, 3 ** retryCount)),
		{
			signal: streamOptions.signal,
		},
	);

const _textEncoder = new TextEncoder();
const _byteLength = (chunk) =>
	_textEncoder.encode(JSON.stringify(chunk)).byteLength;

let client = new SQSClient(awsClientDefaults);
export const awsSQSSetClient = (sqsClient) => {
	client = sqsClient;
};

export const awsSQSReceiveMessageStream = async (
	options,
	streamOptions = {},
) => {
	const { pollingActive, pollingDelay = 1000, ...sqsOptions } = options;
	async function* command(options) {
		let expectMore = true;
		while (expectMore) {
			const response = await client.send(new ReceiveMessageCommand(options), {
				abortSignal: streamOptions.signal,
			});
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

// Shared batch writer that flushes on count or byte limits and retries the
// per-entry `Failed` subset (correlated by `Id`) with exponential backoff.
const sqsBatchStream = (
	Command,
	errorMessage,
	oversizeMessage,
	options,
	streamOptions,
) => {
	const { retryMaxCount = 10, ...sendOptions } = options;
	let batch = [];
	let batchBytes = 0;
	const send = async () => {
		if (!batch.length) {
			return;
		}
		let entries = batch;
		batch = [];
		batchBytes = 0;
		let retryCount = 0;
		while (true) {
			const response = await client.send(
				new Command({ ...sendOptions, Entries: entries }),
				{ abortSignal: streamOptions.signal },
			);
			const failed = response.Failed ?? [];
			if (!failed.length) {
				return;
			}
			const failedIds = new Set(failed.map((entry) => entry.Id));
			const failedEntries = entries.filter((entry) => failedIds.has(entry.Id));
			if (retryCount >= retryMaxCount) {
				throw new Error(errorMessage, { cause: failed });
			}
			await backoff(retryCount, streamOptions);
			retryCount++;
			entries = failedEntries;
		}
	};
	const write = async (chunk) => {
		const chunkBytes = _byteLength(chunk);
		// Surface an oversize single entry up front (mirroring Kinesis) instead of
		// letting SQS reject the whole batch with a BatchRequestTooLong error.
		if (chunkBytes > SQS_MAX_BATCH_BYTES) {
			throw new Error(oversizeMessage, {
				// Reached only for an oversize entry (a non-nullish object), so
				// reading chunk.Id directly is safe.
				cause: { Id: chunk.Id, bytes: chunkBytes, limit: SQS_MAX_BATCH_BYTES },
			});
		}
		if (
			batch.length === SQS_MAX_ENTRIES ||
			(batch.length && batchBytes + chunkBytes > SQS_MAX_BATCH_BYTES)
		) {
			await send();
		}
		batch.push(chunk);
		batchBytes += chunkBytes;
	};
	const final = () => send();
	return createWritableStream(write, final, streamOptions);
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

export default {
	setClient: awsSQSSetClient,
	sendMessageStream: awsSQSSendMessageStream,
	receiveMessageStream: awsSQSReceiveMessageStream,
	deleteMessageStream: awsSQSDeleteMessageStream,
};
