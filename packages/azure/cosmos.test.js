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
	isReadable,
	pipejoin,
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
		// An internal signal, aborted only if the stream is destroyed early.
		const { abortSignal } = args[1];
		strictEqual(abortSignal instanceof AbortSignal, true);
		strictEqual(abortSignal.aborted, false);
		deepStrictEqual(args, [
			"SELECT * FROM c",
			{ maxItemCount: 5, abortSignal },
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
		const client = container(() => [{ response: { statusCode: 400 } }]);
		await rejects(
			pipeline([
				createReadableStream([{ id: "a" }]),
				azureCosmosUpsertItemStream({ client, retryMaxCount: null }),
			]),
			(e) => {
				strictEqual(
					e.message,
					"azureCosmosBulkOperations has failed operations",
				);
				deepStrictEqual(e.cause, { statusCodes: [400] });
				return true;
			},
		);
		strictEqual(client.calls.length, 1);
	});

	// One non-throttle failure alongside throttled ones is not retried.
	test(`azureCosmosUpsertItemStream fails fast when any failure is not throttling`, async () => {
		const client = container((operations, call) =>
			call === 0
				? [{ error: { code: 429 } }, { error: { code: 409 } }]
				: ok(operations),
		);
		await rejects(
			pipeline([
				createReadableStream([{ id: "a" }, { id: "b" }]),
				azureCosmosUpsertItemStream({ client, retryMaxCount: null }),
			]),
			(e) => {
				deepStrictEqual(e.cause, { statusCodes: [429, 409] });
				return true;
			},
		);
		strictEqual(client.calls.length, 1);
	});

	// A result carrying neither a response nor an error has no status: failed.
	test(`azureCosmosUpsertItemStream treats a result without status as failed`, async () => {
		const client = container(() => [{}]);
		await rejects(
			pipeline([
				createReadableStream([{ id: "a" }]),
				azureCosmosUpsertItemStream({ client }),
			]),
			(e) => {
				deepStrictEqual(e.cause, { statusCodes: [undefined] });
				return true;
			},
		);
	});

	test(`azureCosmosQueryStream is a Readable that pipejoin accepts`, async () => {
		const client = {
			items: {
				query: () => ({
					getAsyncIterator: async function* () {
						yield { resources: [{ id: "1" }] };
					},
				}),
			},
		};
		const stream = await azureCosmosQueryStream({ client, query: "q" });
		strictEqual(isReadable(stream), true);
		deepStrictEqual(await streamToArray(pipejoin([stream])), [{ id: "1" }]);
	});

	const writeAll = async (stream, chunks) => {
		for (const chunk of chunks) {
			await new Promise((resolve) => stream.write(chunk, resolve));
		}
	};

	// final() never runs on destroy/abort: the unsent items must surface on the
	// error (kafkaProduceStream's err.failedMessages contract), not vanish.
	test(`azureCosmosUpsertItemStream destroy attaches unsent items`, async () => {
		const client = container(ok);
		const stream = azureCosmosUpsertItemStream({ client });
		const error = new Promise((resolve) => stream.once("error", resolve));
		const input = Array.from({ length: 101 }, (_, i) => ({ id: `${i}` }));
		await writeAll(stream, input);
		stream.destroy();
		const e = await error;
		strictEqual(
			e.message,
			"azureCosmosBulkOperations destroyed with a buffered batch",
		);
		// The written items (not operations), so they can be re-written as-is.
		deepStrictEqual(e.failedMessages, [{ id: "100" }]);
		strictEqual(client.calls.length, 1);
	});

	test(`azureCosmosDeleteItemStream abort attaches unsent items without sending`, async () => {
		const client = container(ok);
		const controller = new AbortController();
		const stream = azureCosmosDeleteItemStream(
			{ client },
			{ signal: controller.signal },
		);
		const error = new Promise((resolve) => stream.once("error", resolve));
		await writeAll(stream, [{ id: "a", partitionKey: "pa" }]);
		controller.abort();
		const e = await error;
		strictEqual(e.name, "AbortError");
		deepStrictEqual(e.failedMessages, [{ id: "a", partitionKey: "pa" }]);
		strictEqual(client.calls.length, 0);
	});

	test(`azureCosmosUpsertItemStream destroy before any write emits no error`, async () => {
		const stream = azureCosmosUpsertItemStream({ client: container(ok) });
		let errored = false;
		stream.on("error", () => {
			errored = true;
		});
		const closed = new Promise((resolve) => stream.once("close", resolve));
		stream.destroy();
		await closed;
		strictEqual(errored, false);
	});

	test(`cosmos has no default export`, () => {
		strictEqual(Object.hasOwn(azureModule, "default"), false);
	});
});
