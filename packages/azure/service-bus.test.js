import { deepStrictEqual, rejects, strictEqual } from "node:assert";
import test, { describe } from "node:test";
import * as azureBarrel from "@datastream/azure";
import * as azureModule from "@datastream/azure/service-bus";
import {
	azureServiceBusAbandonMessageStream,
	azureServiceBusCompleteMessageStream,
	azureServiceBusDeadLetterMessageStream,
	azureServiceBusReceiveMessagesStream,
	azureServiceBusSendMessagesStream,
} from "@datastream/azure/service-bus";
import {
	createReadableStream,
	isReadable,
	pipejoin,
	pipeline,
	streamToArray,
} from "@datastream/core";
import { variant } from "../variant.js";

const settle = async () => {
	for (let i = 0; i < 20; i++) {
		await new Promise((resolve) => setImmediate(resolve));
	}
};

const receiver = (pages) => {
	const calls = [];
	return {
		calls,
		receiveMessages: async (...args) => {
			calls.push(args);
			await new Promise((resolve) => setImmediate(resolve));
			args[1].abortSignal.throwIfAborted();
			return pages.shift() ?? [];
		},
	};
};

describe(`@datastream/azure/service-bus (${variant})`, () => {
	test(`azureServiceBusReceiveMessagesStream reads until an empty page`, async () => {
		const client = receiver([[{ body: 1 }, { body: 2 }]]);
		const stream = await azureServiceBusReceiveMessagesStream({
			client,
			maxWaitTimeInMs: 5,
		});
		deepStrictEqual(await streamToArray(stream), [{ body: 1 }, { body: 2 }]);
		const { abortSignal } = client.calls[0][1];
		strictEqual(abortSignal instanceof AbortSignal, true);
		strictEqual(abortSignal.aborted, false);
		deepStrictEqual(client.calls[0], [10, { maxWaitTimeInMs: 5, abortSignal }]);
	});

	test(`azureServiceBusReceiveMessagesStream keeps polling when active`, async () => {
		const client = receiver([[], [{ body: 1 }]]);
		const controller = new AbortController();
		const stream = await azureServiceBusReceiveMessagesStream(
			{ client, maxMessageCount: 5, pollingActive: true, pollingDelay: 1 },
			{ signal: controller.signal },
		);
		const messages = [];
		await rejects(async () => {
			for await (const message of stream) {
				messages.push(message);
				controller.abort();
			}
		});
		deepStrictEqual(messages, [{ body: 1 }]);
		strictEqual(client.calls[0][0], 5);
	});

	test(`azureServiceBusReceiveMessagesStream polls without delay`, async () => {
		const client = receiver([[], [{ body: 1 }]]);
		const stream = await azureServiceBusReceiveMessagesStream({
			client,
			pollingActive: true,
			pollingDelay: 0,
		});
		for await (const message of stream) {
			deepStrictEqual(message, { body: 1 });
			break;
		}
	});

	test(`azureServiceBusSendMessagesStream batches messages`, async () => {
		const sent = [];
		const client = {
			createMessageBatch: async (options) => {
				deepStrictEqual(options, {
					maxSizeInBytes: 10,
					abortSignal: undefined,
				});
				const items = [];
				return {
					items,
					get count() {
						return items.length;
					},
					tryAddMessage: (message) => items.length < 2 && !!items.push(message),
				};
			},
			sendMessages: async (batch, options) => {
				deepStrictEqual(options, { abortSignal: undefined });
				sent.push(batch.items);
			},
		};
		await pipeline([
			createReadableStream([{ body: 1 }, { body: 2 }, { body: 3 }]),
			azureServiceBusSendMessagesStream({ client, maxSizeInBytes: 10 }),
		]);
		deepStrictEqual(sent, [[{ body: 1 }, { body: 2 }], [{ body: 3 }]]);
	});

	test(`azureServiceBusCompleteMessageStream completes each message`, async () => {
		const completed = [];
		const client = { completeMessage: async (m) => completed.push(m) };
		await pipeline([
			createReadableStream([{ body: 1 }]),
			azureServiceBusCompleteMessageStream({ client }),
		]);
		deepStrictEqual(completed, [{ body: 1 }]);
	});

	test(`azureServiceBusAbandonMessageStream abandons each message`, async () => {
		const abandoned = [];
		const client = { abandonMessage: async (...args) => abandoned.push(args) };
		await pipeline([
			createReadableStream([{ body: 1 }, { body: 2 }]),
			azureServiceBusAbandonMessageStream({ client }),
		]);
		deepStrictEqual(abandoned, [
			[{ body: 1 }, undefined],
			[{ body: 2 }, undefined],
		]);
	});

	test(`azureServiceBusAbandonMessageStream passes propertiesToModify`, async () => {
		const abandoned = [];
		const client = { abandonMessage: async (...args) => abandoned.push(args) };
		await pipeline([
			createReadableStream([{ body: 1 }]),
			azureServiceBusAbandonMessageStream({
				client,
				propertiesToModify: { attempt: 2 },
			}),
		]);
		deepStrictEqual(abandoned, [[{ body: 1 }, { attempt: 2 }]]);
	});

	test(`azureServiceBusDeadLetterMessageStream dead-letters each message`, async () => {
		const deadLettered = [];
		const client = {
			deadLetterMessage: async (...args) => deadLettered.push(args),
		};
		await pipeline([
			createReadableStream([{ body: 1 }]),
			azureServiceBusDeadLetterMessageStream({ client }),
		]);
		deepStrictEqual(deadLettered, [[{ body: 1 }, {}]]);
	});

	test(`azureServiceBusDeadLetterMessageStream passes reason and description`, async () => {
		const deadLettered = [];
		const client = {
			deadLetterMessage: async (...args) => deadLettered.push(args),
		};
		await pipeline([
			createReadableStream([{ body: 1 }]),
			azureServiceBusDeadLetterMessageStream({
				client,
				deadLetterReason: "invalid",
				deadLetterErrorDescription: "schema mismatch",
			}),
		]);
		deepStrictEqual(deadLettered, [
			[
				{ body: 1 },
				{
					deadLetterReason: "invalid",
					deadLetterErrorDescription: "schema mismatch",
				},
			],
		]);
	});

	for (const [name, factory, method] of [
		["Abandon", azureServiceBusAbandonMessageStream, "abandonMessage"],
		["DeadLetter", azureServiceBusDeadLetterMessageStream, "deadLetterMessage"],
		["Complete", azureServiceBusCompleteMessageStream, "completeMessage"],
	]) {
		test(`azureServiceBus${name}MessageStream rejects when settling fails`, async () => {
			const client = {
				[method]: async () => {
					throw new Error("lock lost");
				},
			};
			await rejects(
				pipeline([createReadableStream([{ body: 1 }]), factory({ client })]),
				{ message: "lock lost" },
			);
		});
	}

	test(`azureServiceBusReceiveMessagesStream is a Readable that pipejoin accepts`, async () => {
		const client = receiver([[{ body: 1 }]]);
		const stream = await azureServiceBusReceiveMessagesStream({ client });
		strictEqual(isReadable(stream), true);
		deepStrictEqual(await streamToArray(pipejoin([stream])), [{ body: 1 }]);
	});

	test(`azureServiceBusReceiveMessagesStream destroy stops an idle poll`, {
		timeout: 1000,
	}, async () => {
		const client = receiver([]);
		const stream = await azureServiceBusReceiveMessagesStream({
			client,
			pollingActive: true,
			pollingDelay: 60_000,
		});
		stream.resume();
		await new Promise((resolve) => setTimeout(resolve, 5));
		const closed = new Promise((resolve) => stream.once("close", resolve));
		stream.destroy();
		await closed;
		strictEqual(client.calls.length, 1);
	});

	test(`azureServiceBusSendMessagesStream rejects a message too big for a batch`, async () => {
		const client = {
			createMessageBatch: async () => ({
				count: 0,
				maxSizeInBytes: 1024,
				tryAddMessage: () => false,
			}),
		};
		await rejects(
			pipeline([
				createReadableStream([{ body: 1 }]),
				azureServiceBusSendMessagesStream({ client }),
			]),
			{
				name: "RangeError",
				message: "azureServiceBusSendMessages message exceeds batch limit",
			},
		);
	});

	test(`azureServiceBusReceiveMessagesStream reads every page then ends without waiting`, async (t) => {
		t.mock.timers.enable({ apis: ["setTimeout"] });
		const client = receiver([[{ body: 1 }], [{ body: 2 }], []]);
		const stream = await azureServiceBusReceiveMessagesStream({
			client,
			pollingDelay: 60_000,
		});
		let ended = false;
		const result = streamToArray(stream).finally(() => {
			ended = true;
		});
		await settle();
		strictEqual(client.calls.length, 3);
		strictEqual(ended, true);
		deepStrictEqual(await result, [{ body: 1 }, { body: 2 }]);
	});

	for (const [label, pages, pollingDelay] of [
		["after a non-empty page", [[{ body: 1 }], [{ body: 2 }]], 60_000],
		["with pollingDelay 0", [[], [{ body: 1 }], [{ body: 2 }]], 0],
	]) {
		test(`azureServiceBusReceiveMessagesStream does not wait ${label}`, async (t) => {
			t.mock.timers.enable({ apis: ["setTimeout"] });
			const client = receiver(pages);
			const stream = await azureServiceBusReceiveMessagesStream({
				client,
				pollingActive: true,
				pollingDelay,
			});
			const messages = [];
			(async () => {
				for await (const message of stream) {
					messages.push(message);
					if (messages.length === 2) break;
				}
			})();
			await settle();
			deepStrictEqual(messages, [{ body: 1 }, { body: 2 }]);
		});
	}

	test(`azureServiceBusSendMessagesStream destroy attaches unsent messages`, async () => {
		const client = {
			createMessageBatch: async () => ({ count: 0, tryAddMessage: () => true }),
		};
		const stream = azureServiceBusSendMessagesStream({ client });
		const error = new Promise((resolve) => stream.once("error", resolve));
		await new Promise((resolve) => stream.write({ body: 1 }, resolve));
		stream.destroy();
		const e = await error;
		strictEqual(
			e.message,
			"azureServiceBusSendMessages destroyed with a buffered batch",
		);
		deepStrictEqual(e.failedMessages, [{ body: 1 }]);
	});

	test(`the barrel re-exports abandon and dead-letter`, () => {
		strictEqual(
			azureBarrel.azureServiceBusAbandonMessageStream,
			azureServiceBusAbandonMessageStream,
		);
		strictEqual(
			azureBarrel.azureServiceBusDeadLetterMessageStream,
			azureServiceBusDeadLetterMessageStream,
		);
	});

	test(`service-bus has no default export`, () => {
		strictEqual(Object.hasOwn(azureModule, "default"), false);
	});
});
