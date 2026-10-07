// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
import { createWritableStream, timeout } from "@datastream/core";

// AWS regions that expose FIPS 140-2/140-3 validated endpoints. This includes
// the US/Canada commercial regions and BOTH GovCloud regions (which most need
// FIPS and were previously, incorrectly, excluded).
const fipsRegions = new Set([
	"us-east-1",
	"us-east-2",
	"us-west-1",
	"us-west-2",
	"ca-central-1",
	"ca-west-1",
	"us-gov-east-1",
	"us-gov-west-1",
]);

export const awsRegionSupportsFips = (region) => fipsRegions.has(region);

export const awsClientDefaults = {
	// Lazy getter so AWS_REGION is resolved when a client is constructed rather
	// than frozen at module-import time (test harnesses / lazy config set it
	// after import).
	get useFipsEndpoint() {
		return awsRegionSupportsFips(process.env.AWS_REGION);
	},
};

// Partial failures (UnprocessedItems/Keys, Failed entries, failed records) are
// overwhelmingly throttling-driven; a near-zero early delay (3^0 == 1ms) just
// hammers the throttled endpoint. Apply a floor so the first retries give
// capacity time to recover, while preserving the ~59sec cap (3^10).
const BACKOFF_FLOOR_MS = 50;
const BACKOFF_CAP_MS = 3 ** 10;
// streamOptions is always supplied by the exported stream functions (defaulting
// to {}), so it is never nullish here.
export const awsBackoff = (retryCount, streamOptions) =>
	timeout(
		Math.min(BACKOFF_CAP_MS, Math.max(BACKOFF_FLOOR_MS, 3 ** retryCount)),
		{ signal: streamOptions.signal },
	);

// SNS PublishBatch and SQS SendMessageBatch/DeleteMessageBatch share limits:
// <=10 entries and <=256KiB aggregate payload.
const BATCH_MAX_ENTRIES = 10;
const BATCH_MAX_BYTES = 256 * 1024;

// Shared SNS/SQS batch writer: flushes on count or byte limits and retries the
// per-entry `Failed` subset (correlated by `Id`) with exponential backoff.
// `sendEntries(entries)` issues one batch request and resolves its response.
export const awsBatchEntriesStream = (
	sendEntries,
	{ retryMaxCount = 10, errorMessage, oversizeMessage },
	streamOptions,
) => {
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
			const response = await sendEntries(entries);
			const failed = response.Failed ?? [];
			if (!failed.length) {
				return;
			}
			const failedIds = new Set(failed.map((entry) => entry.Id));
			const failedEntries = entries.filter((entry) => failedIds.has(entry.Id));
			// SenderFault marks a permanent (caller-side) failure: retrying cannot
			// succeed, so fail the batch immediately instead of backing off.
			if (
				// null = retry without limit
				retryCount >= (retryMaxCount ?? Number.POSITIVE_INFINITY) ||
				failed.some((entry) => entry.SenderFault)
			) {
				throw new Error(errorMessage, { cause: failed });
			}
			await awsBackoff(retryCount, streamOptions);
			retryCount++;
			entries = failedEntries;
		}
	};
	const write = async (chunk) => {
		const chunkBytes = Buffer.byteLength(JSON.stringify(chunk));
		// Surface an oversize single entry up front (mirroring Kinesis) instead of
		// letting the service reject the whole batch with BatchRequestTooLong.
		if (chunkBytes > BATCH_MAX_BYTES) {
			throw new RangeError(oversizeMessage, {
				// Reached only for an oversize entry (a non-nullish object), so
				// reading chunk.Id directly is safe.
				cause: { Id: chunk.Id, bytes: chunkBytes, limit: BATCH_MAX_BYTES },
			});
		}
		if (
			batch.length === BATCH_MAX_ENTRIES ||
			(batch.length && batchBytes + chunkBytes > BATCH_MAX_BYTES)
		) {
			await send();
		}
		batch.push(chunk);
		batchBytes += chunkBytes;
	};
	const final = () => send();
	return createWritableStream(write, final, streamOptions);
};
