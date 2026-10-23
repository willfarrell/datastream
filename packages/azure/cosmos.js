// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
import { createWritableStream } from "@datastream/core";
import {
	azureBackoff,
	azureDestroyWithFailedMessages,
	azureReadableStream,
} from "./client.js";

// executeBulkOperations accepts any count but this keeps per-request memory and
// retry sets bounded.
const COSMOS_BATCH_MAX = 100;
const THROTTLED = 429;

// client = Container
export const azureCosmosQueryStream = async (options, streamOptions = {}) => {
	const { client, query, ...feedOptions } = options;
	async function* command(signal) {
		const pages = client.items
			.query(query, { ...feedOptions, abortSignal: signal })
			.getAsyncIterator();
		for await (const page of pages) {
			for (const item of page.resources ?? []) {
				yield item;
			}
		}
	}
	return azureReadableStream(command, streamOptions);
};

const cosmosBulkStream = (toOperation, options, streamOptions) => {
	const { client, retryMaxCount = 10 } = options;
	// null = retry without limit
	const maxCount = retryMaxCount ?? Number.POSITIVE_INFINITY;
	// Written chunks, not operations: on destroy they are handed back as
	// err.failedMessages and can be re-written as-is.
	let batch = [];
	const send = async () => {
		let operations = batch.map(toOperation);
		batch = [];
		let retryCount = 0;
		while (true) {
			const results = await client.items.executeBulkOperations(operations, {
				abortSignal: streamOptions.signal,
			});
			// Status codes only: item bodies may be PII and must not leak into
			// logged error causes.
			const failed = [];
			const throttled = [];
			for (let index = 0; index < results.length; index++) {
				const { response, error } = results[index];
				const code = response?.statusCode ?? error?.code;
				if (code < 400) continue;
				failed.push(code);
				if (code === THROTTLED) throttled.push(operations[index]);
			}
			if (!failed.length) {
				return;
			}
			if (retryCount >= maxCount || throttled.length !== failed.length) {
				throw new Error("azureCosmosBulkOperations has failed operations", {
					cause: { statusCodes: failed },
				});
			}
			await azureBackoff(retryCount, streamOptions);
			retryCount++;
			operations = throttled;
		}
	};
	// Returns a promise only when a batch is sent: an async write would
	// allocate and settle a promise per chunk.
	const write = (chunk) => {
		batch.push(chunk);
		if (batch.length === COSMOS_BATCH_MAX) {
			return send();
		}
	};
	const final = () => (batch.length ? send() : undefined);
	return azureDestroyWithFailedMessages(
		createWritableStream(write, final, streamOptions),
		() => batch,
		"azureCosmosBulkOperations destroyed with a buffered batch",
	);
};

// chunk = item; partition key is read from the item by the SDK.
export const azureCosmosUpsertItemStream = (options, streamOptions = {}) =>
	cosmosBulkStream(
		(resourceBody) => ({ operationType: "Upsert", resourceBody }),
		options,
		streamOptions,
	);

// chunk = { id, partitionKey }
export const azureCosmosDeleteItemStream = (options, streamOptions = {}) =>
	cosmosBulkStream(
		({ id, partitionKey }) => ({ operationType: "Delete", id, partitionKey }),
		options,
		streamOptions,
	);
