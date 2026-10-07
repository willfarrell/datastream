// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
import { createWritableStream, timeout } from "@datastream/core";

// Partial failures (Cosmos 429s) are throttling-driven: floor the first retries
// at 50ms so capacity can recover, and cap at 3^10ms (~59s).
const BACKOFF_FLOOR_MS = 50;
const BACKOFF_CAP_MS = 3 ** 10;
export const azureBackoff = (retryCount, streamOptions) =>
	timeout(
		Math.min(BACKOFF_CAP_MS, Math.max(BACKOFF_FLOOR_MS, 3 ** retryCount)),
		{ signal: streamOptions.signal },
	);

// Shared Service Bus / Event Hubs batch writer: the SDK batch object enforces
// the service size limit (tryAdd returns false when full), so flush when it is
// full and fail fast on a single entry that cannot fit an empty batch.
export const azureBatchStream = (
	{ createBatch, sendBatch, add, oversizeMessage },
	streamOptions,
) => {
	let batch;
	const write = async (chunk) => {
		batch ??= await createBatch();
		if (add(batch, chunk)) {
			return;
		}
		if (batch.count) {
			await sendBatch(batch);
			batch = await createBatch();
			if (add(batch, chunk)) {
				return;
			}
		}
		throw new RangeError(oversizeMessage, {
			cause: { limit: batch.maxSizeInBytes },
		});
	};
	const final = async () => {
		if (batch?.count) {
			await sendBatch(batch);
		}
	};
	return createWritableStream(write, final, streamOptions);
};
