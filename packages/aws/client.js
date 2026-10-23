// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
import {
	createReadableStream,
	createWritableStream,
	timeout,
} from "@datastream/core";

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

// Readable over a paging/polling generator. Destroying a Readable.from stream
// only reaches the generator's return() after its next yield, so an idle poll
// (empty pages, no items) would keep running and the stream would never emit
// 'close'. Destroy also aborts the in-flight request / idle wait via the
// signal handed to `command(signal)` (combined with streamOptions.signal).
export const awsReadableStream = (command, streamOptions) => {
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
// AbortSignal teardown, so entries still buffered below a batch limit would be
// dropped silently. Hand them to the teardown error as `failedMessages` (the
// kafkaProduceStream contract) so callers can recover/retry them; nothing is
// sent. `unsent()` returns the current buffer.
export const awsUnsentOnDestroy = (stream, unsent, errorMessage) => {
	const destroy = stream._destroy.bind(stream);
	stream._destroy = (error, callback) => {
		const entries = unsent();
		if (entries.length) {
			error ??= new Error(errorMessage);
			error.failedMessages = entries;
		}
		destroy(error, callback);
	};
	return stream;
};

// SNS PublishBatch and SQS SendMessageBatch/DeleteMessageBatch share the
// <=10 entries limit. The aggregate payload limit differs per service (SNS
// 256KiB, SQS 1MiB since Aug 2025), so callers pass it as `maxBatchBytes`.
const BATCH_MAX_ENTRIES = 10;

// The message size SNS/SQS check against those limits: the UTF-8 body (SNS
// Message / SQS MessageBody) plus each message attribute's name, data type and
// string or binary value. Id, receipt handles and system attributes do not
// count, so a delete entry measures 0. Buffer.byteLength takes strings and
// binary values alike.
const entryByteLength = (entry) => {
	let bytes = Buffer.byteLength(entry.MessageBody ?? entry.Message ?? "");
	// for-in: no per-entry [name, attribute] pair arrays (Object.entries) and no
	// throwaway `{}` when there are no attributes (for-in over undefined is a
	// no-op).
	const attributes = entry.MessageAttributes;
	for (const name in attributes) {
		const attribute = attributes[name];
		bytes +=
			Buffer.byteLength(name) +
			Buffer.byteLength(attribute.DataType) +
			Buffer.byteLength(attribute.StringValue ?? attribute.BinaryValue ?? "");
	}
	return bytes;
};

// Shared SNS/SQS batch writer: flushes on count or byte limits and retries the
// per-entry `Failed` subset (correlated by `Id`) with exponential backoff.
// `sendEntries(entries)` issues one batch request and resolves its response.
export const awsBatchEntriesStream = (
	sendEntries,
	{
		retryMaxCount = 10,
		maxBatchBytes,
		errorMessage,
		oversizeMessage,
		destroyMessage,
	},
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
		// SenderFault marks a permanent (caller-side) failure: retrying cannot
		// succeed, so set those aside and only back off on the retryable rest.
		// Throwing on the first SenderFault would drop the retryable entries.
		const permanent = [];
		while (true) {
			const response = await sendEntries(entries);
			const retryable = [];
			for (const entry of response.Failed ?? []) {
				(entry.SenderFault ? permanent : retryable).push(entry);
			}
			if (
				!retryable.length ||
				// null = retry without limit
				retryCount >= (retryMaxCount ?? Number.POSITIVE_INFINITY)
			) {
				const failed = permanent.concat(retryable);
				if (failed.length) {
					throw new Error(errorMessage, { cause: failed });
				}
				return;
			}
			const retryIds = new Set(retryable.map((entry) => entry.Id));
			await awsBackoff(retryCount, streamOptions);
			retryCount++;
			entries = entries.filter((entry) => retryIds.has(entry.Id));
		}
	};
	const write = async (chunk) => {
		const chunkBytes = entryByteLength(chunk);
		// Surface an oversize single entry up front (mirroring Kinesis) instead of
		// letting the service reject the whole batch with BatchRequestTooLong.
		if (chunkBytes > maxBatchBytes) {
			throw new RangeError(oversizeMessage, {
				// Reached only for an oversize entry (a non-nullish object), so
				// reading chunk.Id directly is safe.
				cause: { Id: chunk.Id, bytes: chunkBytes, limit: maxBatchBytes },
			});
		}
		if (
			batch.length === BATCH_MAX_ENTRIES ||
			(batch.length && batchBytes + chunkBytes > maxBatchBytes)
		) {
			await send();
		}
		batch.push(chunk);
		batchBytes += chunkBytes;
	};
	const final = () => send();
	return awsUnsentOnDestroy(
		createWritableStream(write, final, streamOptions),
		() => batch,
		destroyMessage,
	);
};
