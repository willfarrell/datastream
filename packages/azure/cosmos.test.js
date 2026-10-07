import { deepStrictEqual, rejects, strictEqual } from "node:assert";
import test, { describe } from "node:test";
import * as azureModule from "@datastream/azure/cosmos";
import {
	azureCosmosDeleteItemStream,
	azureCosmosQueryStream,
	azureCosmosUpsertItemStream,
} from "@datastream/azure/cosmos";
import {
	createReadableStream,
	pipeline,
	streamToArray,
} from "@datastream/core";
import { variant } from "../variant.js";

// Fake container whose bulk responses come from `respond(operations, call)`.
const container = (respond) => {
	const calls = [];
	return {
		calls,
		items: {
			executeBulkOperations: async (operations, options) => {
				calls.push(operations);
				deepStrictEqual(options, { abortSignal: undefined });
				return respond(operations, calls.length - 1);
			},
		},
	};
};
const ok = (operations) =>
	operations.map(() => ({ response: { statusCode: 200 } }));

describe(`@datastream/azure/cosmos (${variant})`, () => {
	test(`azureCosmosQueryStream yields every page's resources`, async () => {
		let args;
		const client = {
			items: {
				query: (...a) => {
					args = a;
					return {
						getAsyncIterator: async function* () {
							yield { resources: [{ id: "1" }] };
							yield {};
							yield { resources: [{ id: "2" }] };
						},
					};
				},
			},
		};
		const stream = await azureCosmosQueryStream({
			client,
			query: "SELECT * FROM c",
			maxItemCount: 5,
		});
		deepStrictEqual(await streamToArray(stream), [{ id: "1" }, { id: "2" }]);
		deepStrictEqual(args, [
			"SELECT * FROM c",
			{ maxItemCount: 5, abortSignal: undefined },
		]);
	});

	test(`azureCosmosUpsertItemStream batches by 100`, async () => {
		const client = container(ok);
		const input = Array.from({ length: 101 }, (_, i) => ({ id: `${i}` }));
		await pipeline([
			createReadableStream(input),
			azureCosmosUpsertItemStream({ client }),
		]);
		strictEqual(client.calls.length, 2);
		strictEqual(client.calls[0].length, 100);
		deepStrictEqual(client.calls[1], [
			{ operationType: "Upsert", resourceBody: { id: "100" } },
		]);
	});

	test(`azureCosmosUpsertItemStream sends nothing for empty input`, async () => {
		const client = container(ok);
		await pipeline([
			createReadableStream([]),
			azureCosmosUpsertItemStream({ client }),
		]);
		strictEqual(client.calls.length, 0);
	});

	test(`azureCosmosDeleteItemStream retries throttled operations`, async () => {
		const client = container((operations, call) =>
			call === 0
				? [{ response: { statusCode: 204 } }, { error: { code: 429 } }]
				: ok(operations),
		);
		await pipeline([
			createReadableStream([
				{ id: "a", partitionKey: "pa" },
				{ id: "b", partitionKey: "pb" },
			]),
			azureCosmosDeleteItemStream({ client }),
		]);
		deepStrictEqual(client.calls[1], [
			{ operationType: "Delete", id: "b", partitionKey: "pb" },
		]);
	});

	test(`azureCosmosUpsertItemStream throws once retries are exhausted`, async () => {
		const client = container(() => [{ response: { statusCode: 429 } }]);
		await rejects(
			pipeline([
				createReadableStream([{ id: "a" }]),
				azureCosmosUpsertItemStream({ client, retryMaxCount: 1 }),
			]),
			(e) => {
				deepStrictEqual(e.cause, { statusCodes: [429] });
				return true;
			},
		);
		strictEqual(client.calls.length, 2);
	});

	test(`azureCosmosUpsertItemStream fails fast on non-throttle errors`, async () => {
		const client = container(() => [
			{ response: { statusCode: 400 } },
			{ error: {} },
		]);
		await rejects(
			pipeline([
				createReadableStream([{ id: "a" }, { id: "b" }]),
				azureCosmosUpsertItemStream({ client, retryMaxCount: null }),
			]),
			{ message: "azureCosmosBulkOperations has failed operations" },
		);
		strictEqual(client.calls.length, 1);
	});

	test(`cosmos has no default export`, () => {
		strictEqual(Object.hasOwn(azureModule, "default"), false);
	});
});
