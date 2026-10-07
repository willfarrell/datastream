// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
import { createWritableStream } from "@datastream/core";
import { azureBackoff } from "./client.js";

// executeBulkOperations accepts any count but this keeps per-request memory and
// retry sets bounded.
const COSMOS_BATCH_MAX = 100;
const THROTTLED = 429;

// client = Container
export const azureCosmosQueryStream = async (options, streamOptions = {}) => {
	const { client, query, ...feedOptions } = options;
	async function* command() {
		const pages = client.items
			.query(query, { ...feedOptions, abortSignal: streamOptions.signal })
			.getAsyncIterator();
		for await (const page of pages) {
			for (const item of page.resources ?? []) {
				yield item;
			}
		}
	}
	return command();
};

const cosmosBulkStream = (toOperation, options, streamOptions) => {
	const { client, retryMaxCount = 10 } = options;
	// null = retry without limit
	const maxCount = retryMaxCount ?? Number.POSITIVE_INFINITY;
	let batch = [];
	const send = async () => {
		let operations = batch;
		batch = [];
		let retryCount = 0;
		while (true) {
			const results = await client.items.executeBulkOperations(operations, {
				abortSignal: streamOptions.signal,
			});
			const statusCodes = results.map(
				(result) => result.response?.statusCode ?? result.error?.code,
			);
			// Status codes only: item bodies may be PII and must not leak into
			// logged error causes.
			const failed = statusCodes.filter((code) => !(code < 400));
			if (!failed.length) {
				return;
			}
			if (retryCount >= maxCount || failed.some((code) => code !== THROTTLED)) {
				throw new Error("azureCosmosBulkOperations has failed operations", {
					cause: { statusCodes: failed },
				});
			}
			await azureBackoff(retryCount, streamOptions);
			retryCount++;
			operations = operations.filter(
				(_operation, index) => statusCodes[index] === THROTTLED,
			);
		}
	};
	const write = async (chunk) => {
		if (batch.length === COSMOS_BATCH_MAX) {
			await send();
		}
		batch.push(toOperation(chunk));
	};
	const final = () => (batch.length ? send() : undefined);
	return createWritableStream(write, final, streamOptions);
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
