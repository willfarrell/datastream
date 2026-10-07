import { deepStrictEqual, rejects, strictEqual } from "node:assert";
import test, { describe } from "node:test";
import * as azureModule from "@datastream/azure/queue";
import {
	azureQueueDeleteMessageStream,
	azureQueueReceiveMessagesStream,
	azureQueueSendMessageStream,
} from "@datastream/azure/queue";
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
		receiveMessages: async (options) => {
			calls.push(options);
			return pages.shift() ?? {};
		},
	};
};

describe(`@datastream/azure/queue (${variant})`, () => {
	test(`azureQueueReceiveMessagesStream reads until an empty page`, async () => {
		const client = receiver([
			{ receivedMessageItems: [{ messageId: "1" }, { messageId: "2" }] },
			{ receivedMessageItems: [] },
		]);
		const stream = await azureQueueReceiveMessagesStream({
			client,
			numberOfMessages: 32,
		});
		deepStrictEqual(await streamToArray(stream), [
			{ messageId: "1" },
			{ messageId: "2" },
		]);
		deepStrictEqual(client.calls[0], {
			numberOfMessages: 32,
			abortSignal: undefined,
		});
	});

	test(`azureQueueReceiveMessagesStream keeps polling when active`, async () => {
		const client = receiver([
			{},
			{ receivedMessageItems: [{ messageId: "1" }] },
		]);
		const controller = new AbortController();
		const stream = await azureQueueReceiveMessagesStream(
			{ client, pollingActive: true, pollingDelay: 1 },
			{ signal: controller.signal },
		);
		const messages = [];
		await rejects(async () => {
			for await (const message of stream) {
				messages.push(message);
				controller.abort();
			}
		});
		deepStrictEqual(messages, [{ messageId: "1" }]);
	});

	test(`azureQueueReceiveMessagesStream polls without delay`, async () => {
		const client = receiver([
			{},
			{ receivedMessageItems: [{ messageId: "1" }] },
		]);
		const stream = await azureQueueReceiveMessagesStream({
			client,
			pollingActive: true,
			pollingDelay: 0,
		});
		for await (const message of stream) {
			deepStrictEqual(message, { messageId: "1" });
			break;
		}
		strictEqual(client.calls.length, 2);
	});

	test(`azureQueueSendMessageStream sends each chunk`, async () => {
		const sent = [];
		const client = { sendMessage: async (...args) => sent.push(args) };
		await pipeline([
			createReadableStream(["a", "b"]),
			azureQueueSendMessageStream({ client, messageTimeToLive: 60 }),
		]);
		deepStrictEqual(sent, [
			["a", { messageTimeToLive: 60, abortSignal: undefined }],
			["b", { messageTimeToLive: 60, abortSignal: undefined }],
		]);
	});

	test(`azureQueueDeleteMessageStream deletes each chunk`, async () => {
		const deleted = [];
		const client = { deleteMessage: async (...args) => deleted.push(args) };
		await pipeline([
			createReadableStream([{ messageId: "1", popReceipt: "p" }]),
			azureQueueDeleteMessageStream({ client }),
		]);
		deepStrictEqual(deleted, [["1", "p", { abortSignal: undefined }]]);
	});

	test(`queue has no default export`, () => {
		strictEqual(Object.hasOwn(azureModule, "default"), false);
	});
});
