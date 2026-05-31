// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
import { PublishBatchCommand, SNSClient } from "@aws-sdk/client-sns";
import { createWritableStream, timeout } from "@datastream/core";
import { awsClientDefaults } from "./client.js";

// PublishBatch: <=10 entries, <=256KB aggregate payload.
const SNS_MAX_ENTRIES = 10;
const SNS_MAX_BATCH_BYTES = 256 * 1024;

// Partial failures are overwhelmingly throttling-driven; a near-zero early
// delay (3^0 == 1ms) just hammers the throttled endpoint. Apply a floor so the
// first retries give capacity time to recover, while preserving the ~59sec cap.
const BACKOFF_FLOOR_MS = 50;
const BACKOFF_CAP_MS = 3 ** 10;
// streamOptions is always supplied by the exported stream function (defaulting
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

let client = new SNSClient(awsClientDefaults);
export const awsSNSSetClient = (snsClient) => {
	client = snsClient;
};

export const awsSNSPublishMessageStream = (options, streamOptions = {}) => {
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
				new PublishBatchCommand({
					...sendOptions,
					PublishBatchRequestEntries: entries,
				}),
				{ abortSignal: streamOptions.signal },
			);
			const failed = response.Failed ?? [];
			if (!failed.length) {
				return;
			}
			const failedIds = new Set(failed.map((entry) => entry.Id));
			const failedEntries = entries.filter((entry) => failedIds.has(entry.Id));
			if (retryCount >= retryMaxCount) {
				throw new Error("awsSNSPublishBatch has failed entries", {
					cause: failed,
				});
			}
			await backoff(retryCount, streamOptions);
			retryCount++;
			entries = failedEntries;
		}
	};
	const write = async (chunk) => {
		const chunkBytes = _byteLength(chunk);
		// Surface an oversize single entry up front (mirroring Kinesis) instead of
		// letting SNS reject the whole batch with a BatchRequestTooLong error.
		if (chunkBytes > SNS_MAX_BATCH_BYTES) {
			throw new Error("awsSNSPublishBatch entry exceeds 256KiB limit", {
				// Reached only for an oversize entry (a non-nullish object), so
				// reading chunk.Id directly is safe.
				cause: { Id: chunk.Id, bytes: chunkBytes, limit: SNS_MAX_BATCH_BYTES },
			});
		}
		if (
			batch.length === SNS_MAX_ENTRIES ||
			(batch.length && batchBytes + chunkBytes > SNS_MAX_BATCH_BYTES)
		) {
			await send();
		}
		batch.push(chunk);
		batchBytes += chunkBytes;
	};
	const final = () => send();
	return createWritableStream(write, final, streamOptions);
};

export default {
	setClient: awsSNSSetClient,
	publishMessageStream: awsSNSPublishMessageStream,
};
