// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
import { createWritableStream } from "@datastream/core";
import {
	azureBackoff,
	azureDestroyWithFailedMessages,
	azureReadableStream,
} from "./client.js";

const COSMOS_BATCH_MAX = 100;
const THROTTLED = 429;

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
	const maxCount = retryMaxCount ?? Number.POSITIVE_INFINITY;
	let batch = [];
	const send = async () => {
		let operations = batch.map(toOperation);
		batch = [];
		let retryCount = 0;
		while (true) {
			const results = await client.items.executeBulkOperations(operations, {
				abortSignal: streamOptions.signal,
			});
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

export const azureCosmosUpsertItemStream = (options, streamOptions = {}) =>
	cosmosBulkStream(
		(resourceBody) => ({ operationType: "Upsert", resourceBody }),
		options,
		streamOptions,
	);

export const azureCosmosDeleteItemStream = (options, streamOptions = {}) =>
	cosmosBulkStream(
		({ id, partitionKey }) => ({ operationType: "Delete", id, partitionKey }),
		options,
		streamOptions,
	);
