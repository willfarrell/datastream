// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
import { deepStrictEqual, strictEqual } from "node:assert";
import test from "node:test";
import {
	azureCosmosQueryStream,
	azureCosmosUpsertItemStream,
	azureEventHubsKafkaMechanism,
	azureServiceBusDeadLetterMessageStream,
	azureServiceBusReceiveMessagesStream,
	azureServiceBusSendMessagesStream,
} from "@datastream/azure";
import {
	createReadableStream,
	pipeline,
	streamToArray,
} from "@datastream/core";
import fc from "fast-check";

const catchError = (input, e) => {
	const expectedErrors = [
		"azureEventHubsKafkaMechanism: namespace required",
		"azureEventHubsKafkaMechanism: namespace must be 6-50 letters, digits or hyphens, start with a letter and end with a letter or digit",
		"azureCosmosBulkOperations has failed operations",
	];
	if (expectedErrors.includes(e.message)) {
		return;
	}
	console.error(input, e);
	throw e;
};

const message = fc.record({ body: fc.anything() });

test("fuzz azureEventHubsKafkaMechanism w/ random namespace", async () => {
	await fc.assert(
		fc.asyncProperty(
			fc.oneof(
				fc.string({ maxLength: 60 }),
				fc.string({ unit: "binary", maxLength: 60 }),
				fc.stringMatching(/^[A-Za-z][A-Za-z0-9-]{4,48}[A-Za-z0-9]$/),
				fc.webUrl(),
			),
			async (namespace) => {
				try {
					let scope;
					const credential = {
						getToken: async (s) => {
							scope = s;
							return { token: "t", expiresOnTimestamp: 1 };
						},
					};
					await azureEventHubsKafkaMechanism({
						credential,
						namespace,
					}).oauthBearerProvider();
					const url = new URL(scope);
					strictEqual(
						url.hostname,
						`${namespace.toLowerCase()}.servicebus.windows.net`,
					);
					strictEqual(url.pathname, "/.default");
				} catch (e) {
					catchError(namespace, e);
				}
			},
		),
		{
			numRuns: 1_000,
			verbose: 2,
			examples: [["evil.example/"], ["a@evil.example"], ["abcdef#"]],
		},
	);
});

const fakeSender = (capacity) => {
	const sent = [];
	return {
		sent,
		createMessageBatch: async () => {
			const items = [];
			return {
				items,
				get count() {
					return items.length;
				},
				tryAddMessage: (m) => items.length < capacity && !!items.push(m),
			};
		},
		sendMessages: async (batch) => {
			sent.push(batch.items);
		},
	};
};

test("fuzz azureServiceBusSendMessagesStream w/ random messages and destroy", async () => {
	await fc.assert(
		fc.asyncProperty(
			fc.array(message, { maxLength: 30 }),
			fc.integer({ min: 1, max: 5 }),
			fc.option(fc.nat({ max: 30 })),
			async (messages, capacity, destroyAfter) => {
				const client = fakeSender(capacity);
				if (destroyAfter === null) {
					await pipeline([
						createReadableStream(messages),
						azureServiceBusSendMessagesStream({ client }),
					]);
					deepStrictEqual(client.sent.flat(), messages);
					return;
				}
				const written = messages.slice(0, destroyAfter);
				const stream = azureServiceBusSendMessagesStream({ client });
				let failedMessages = [];
				stream.on("error", (e) => {
					({ failedMessages } = e);
				});
				for (const m of written) {
					await new Promise((resolve) => stream.write(m, resolve));
				}
				const closed = new Promise((resolve) => stream.once("close", resolve));
				stream.destroy();
				await closed;
				deepStrictEqual([...client.sent.flat(), ...failedMessages], written);
			},
		),
		{
			numRuns: 1_000,
			verbose: 2,
			examples: [],
		},
	);
});

test("fuzz azureServiceBusReceiveMessagesStream w/ random pages", async () => {
	await fc.assert(
		fc.asyncProperty(
			fc.array(fc.array(message, { minLength: 1, maxLength: 10 }), {
				maxLength: 10,
			}),
			fc.integer({ min: 1, max: 100 }),
			async (pages, maxMessageCount) => {
				const queue = [...pages];
				const client = {
					receiveMessages: async () => queue.shift() ?? [],
				};
				const stream = await azureServiceBusReceiveMessagesStream({
					client,
					maxMessageCount,
				});
				deepStrictEqual(await streamToArray(stream), pages.flat());
			},
		),
		{
			numRuns: 500,
			verbose: 2,
			examples: [],
		},
	);
});

test("fuzz azureServiceBusDeadLetterMessageStream w/ random options", async () => {
	await fc.assert(
		fc.asyncProperty(
			fc.array(message, { maxLength: 10 }),
			fc.record(
				{
					deadLetterReason: fc.string(),
					deadLetterErrorDescription: fc.string(),
				},
				{ requiredKeys: [] },
			),
			async (messages, options) => {
				const calls = [];
				const client = {
					deadLetterMessage: async (...args) => calls.push(args),
				};
				await pipeline([
					createReadableStream(messages),
					azureServiceBusDeadLetterMessageStream({ client, ...options }),
				]);
				// fast-check may build a null-prototype record; the stream copies it.
				deepStrictEqual(
					calls,
					messages.map((m) => [m, { ...options }]),
				);
			},
		),
		{
			numRuns: 500,
			verbose: 2,
			examples: [],
		},
	);
});

const bulkResult = fc.oneof(
	fc.record({
		response: fc.record({ statusCode: fc.integer({ min: 100, max: 599 }) }),
	}),
	fc.record({ error: fc.record({ code: fc.integer({ min: 100, max: 599 }) }) }),
	fc.constant({}),
);

test("fuzz azureCosmosUpsertItemStream w/ random bulk results", async () => {
	await fc.assert(
		fc.asyncProperty(
			fc.array(fc.record({ id: fc.string() }), { maxLength: 250 }),
			fc.array(bulkResult, { minLength: 100, maxLength: 100 }),
			async (items, results) => {
				const client = {
					items: {
						executeBulkOperations: async (operations) =>
							operations.map((_operation, index) => results[index]),
					},
				};
				try {
					await pipeline([
						createReadableStream(items),
						azureCosmosUpsertItemStream({ client, retryMaxCount: 0 }),
					]);
				} catch (e) {
					catchError(items, e);
				}
			},
		),
		{
			numRuns: 500,
			verbose: 2,
			examples: [],
		},
	);
});

test("fuzz azureCosmosQueryStream w/ random pages", async () => {
	await fc.assert(
		fc.asyncProperty(
			fc.array(
				fc.option(fc.array(fc.record({ id: fc.string() }), { maxLength: 10 })),
				{ maxLength: 10 },
			),
			async (pages) => {
				const client = {
					items: {
						query: () => ({
							getAsyncIterator: async function* () {
								for (const resources of pages) {
									yield resources === null ? {} : { resources };
								}
							},
						}),
					},
				};
				const stream = await azureCosmosQueryStream({ client, query: "q" });
				deepStrictEqual(
					await streamToArray(stream),
					pages.flatMap((resources) => resources ?? []),
				);
			},
		),
		{
			numRuns: 500,
			verbose: 2,
			examples: [],
		},
	);
});
