import { deepStrictEqual, rejects, strictEqual } from "node:assert";
import test, { describe } from "node:test";
import * as azureModule from "@datastream/azure/service-bus";
import {
	azureServiceBusCompleteMessageStream,
	azureServiceBusReceiveMessagesStream,
	azureServiceBusSendMessagesStream,
} from "@datastream/azure/service-bus";
import {
	createReadableStream,
	pipeline,
	streamToArray,
} from "@datastream/core";
import { variant } from "../variant.js";

const receiver = (pages) => {
	const calls = [];
	return {
		calls,
		receiveMessages: async (...args) => {
			calls.push(args);
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
		deepStrictEqual(client.calls[0], [
			10,
			{ maxWaitTimeInMs: 5, abortSignal: undefined },
		]);
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

	test(`service-bus has no default export`, () => {
		strictEqual(Object.hasOwn(azureModule, "default"), false);
	});
});
