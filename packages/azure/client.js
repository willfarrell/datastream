// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
import {
	createReadableStream,
	createWritableStream,
	timeout,
} from "@datastream/core";

// Partial failures (Cosmos 429s) are throttling-driven: floor the first retries
// at 50ms so capacity can recover, and cap at 3^10ms (~59s).
const BACKOFF_FLOOR_MS = 50;
const BACKOFF_CAP_MS = 3 ** 10;
export const azureBackoff = (retryCount, streamOptions) =>
	timeout(
		Math.min(BACKOFF_CAP_MS, Math.max(BACKOFF_FLOOR_MS, 3 ** retryCount)),
		{ signal: streamOptions.signal },
	);

// Readable over a paging/polling generator. Destroying a Readable.from stream
// only reaches the generator's return() after its next yield, so an idle poll
// (empty pages, no items) would keep running and the stream would never emit
// 'close'. Destroy also aborts the in-flight request / idle wait via the
// signal handed to `command(signal)` (combined with streamOptions.signal).
export const azureReadableStream = (command, streamOptions) => {
	const controller = new AbortController();
	const signal = streamOptions.signal
		? AbortSignal.any([streamOptions.signal, controller.signal])
		: controller.signal;
	const stream = createReadableStream(command(signal), streamOptions);
	const destroy = stream.destroy;
	stream.destroy = function (...args) {
		// Not after a natural end: the forwarded signal stays un-aborted.
		if (!this.readableEnded) controller.abort();
		return destroy.apply(this, args);
	};
	return stream;
};

// Node's final() runs only on a graceful end(), never on destroy() or an
// AbortSignal teardown, so a partly filled batch would be dropped silently.
// Like kafkaProduceStream, hand the unsent entries to the teardown error as
// err.failedMessages (nothing is sent on abort) so callers can retry them.
export const azureDestroyWithFailedMessages = (stream, pending, message) => {
	const destroy = stream._destroy.bind(stream);
	stream._destroy = (err, callback) => {
		const failedMessages = pending();
		if (failedMessages.length) {
			err ??= new Error(message);
			err.failedMessages = failedMessages;
		}
		destroy(err, callback);
	};
	return stream;
};

// Shared Service Bus / Event Hubs batch writer: the SDK batch object enforces
// the service size limit (tryAdd returns false when full), so flush when it is
// full and fail fast on a single entry that cannot fit an empty batch. The
// SDK batch does not expose its entries, so they are tracked in `pending`.
export const azureBatchStream = (
	{ createBatch, sendBatch, add, oversizeMessage, destroyMessage },
	streamOptions,
) => {
	let batch;
	let pending = [];
	const tryAdd = (chunk) => add(batch, chunk) && pending.push(chunk);
	// Cleared before the send: in-flight entries are no longer buffered.
	const send = () => {
		pending = [];
		return sendBatch(batch);
	};
	// Before the first chunk, or once one does not fit: send what is buffered
	// and open a new batch.
	const rotate = async (chunk) => {
		if (batch?.count) {
			await send();
		}
		batch = await createBatch();
		if (tryAdd(chunk)) {
			return;
		}
		throw new RangeError(oversizeMessage, {
			cause: { limit: batch.maxSizeInBytes },
		});
	};
	// Returns a promise only when rotating: an async write would allocate and
	// settle a promise per chunk.
	const write = (chunk) => {
		if (batch && tryAdd(chunk)) {
			return;
		}
		return rotate(chunk);
	};
	const final = async () => {
		if (batch?.count) {
			await send();
		}
	};
	return azureDestroyWithFailedMessages(
		createWritableStream(write, final, streamOptions),
		() => pending,
		destroyMessage,
	);
};
