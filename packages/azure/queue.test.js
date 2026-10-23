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
		receiveMessages: async (options) => {
			calls.push(options);
			await new Promise((resolve) => setImmediate(resolve));
			options.abortSignal.throwIfAborted();
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
		const { abortSignal } = client.calls[0];
		strictEqual(abortSignal instanceof AbortSignal, true);
		strictEqual(abortSignal.aborted, false);
		deepStrictEqual(client.calls[0], {
			numberOfMessages: 32,
			abortSignal,
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
		strictEqual(client.calls.length, 3);
		await new Promise((resolve) => setTimeout(resolve, 5));
		strictEqual(client.calls.length, 3);
		strictEqual(client.calls[2].abortSignal.aborted, true);
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

	test(`azureQueueReceiveMessagesStream is a Readable that pipejoin accepts`, async () => {
		const client = receiver([{ receivedMessageItems: [{ messageId: "1" }] }]);
		const stream = await azureQueueReceiveMessagesStream({ client });
		strictEqual(isReadable(stream), true);
		deepStrictEqual(await streamToArray(pipejoin([stream])), [
			{ messageId: "1" },
		]);
	});

	test(`azureQueueReceiveMessagesStream destroy stops an idle poll`, {
		timeout: 1000,
	}, async () => {
		const client = receiver([]);
		const stream = await azureQueueReceiveMessagesStream({
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

	test(`azureQueueReceiveMessagesStream reads every page then ends without waiting`, async (t) => {
		t.mock.timers.enable({ apis: ["setTimeout"] });
		const client = receiver([
			{ receivedMessageItems: [{ messageId: "1" }] },
			{ receivedMessageItems: [{ messageId: "2" }] },
			{},
		]);
		const stream = await azureQueueReceiveMessagesStream({
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
		deepStrictEqual(await result, [{ messageId: "1" }, { messageId: "2" }]);
	});

	for (const [label, pages, pollingDelay] of [
		[
			"after a non-empty page",
			[
				{ receivedMessageItems: [{ messageId: "1" }] },
				{ receivedMessageItems: [{ messageId: "2" }] },
			],
			60_000,
		],
		[
			"with pollingDelay 0",
			[
				{},
				{ receivedMessageItems: [{ messageId: "1" }] },
				{ receivedMessageItems: [{ messageId: "2" }] },
			],
			0,
		],
	]) {
		test(`azureQueueReceiveMessagesStream does not wait ${label}`, async (t) => {
			t.mock.timers.enable({ apis: ["setTimeout"] });
			const client = receiver(pages);
			const stream = await azureQueueReceiveMessagesStream({
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
			deepStrictEqual(messages, [{ messageId: "1" }, { messageId: "2" }]);
		});
	}

	test(`azureQueueReceiveMessagesStream waits pollingDelay after an empty page`, async (t) => {
		t.mock.timers.enable({ apis: ["setTimeout"] });
		const client = receiver([
			{},
			{ receivedMessageItems: [{ messageId: "1" }] },
			{},
		]);
		const controller = new AbortController();
		t.after(() => controller.abort());
		const stream = await azureQueueReceiveMessagesStream(
			{ client, pollingActive: true, pollingDelay: 60_000 },
			{ signal: controller.signal },
		);
		const messages = [];
		let error;
		const done = (async () => {
			for await (const message of stream) {
				messages.push(message);
			}
		})().catch((e) => {
			error = e;
		});
		await settle();
		strictEqual(client.calls.length, 1);
		t.mock.timers.tick(59_999);
		await settle();
		strictEqual(client.calls.length, 1);
		t.mock.timers.tick(1);
		await settle();
		strictEqual(client.calls.length, 3);
		deepStrictEqual(messages, [{ messageId: "1" }]);
		strictEqual(error, undefined);
		controller.abort();
		await settle();
		await done;
		strictEqual(error instanceof Error, true);
		strictEqual(client.calls.length, 3);
	});

	test(`queue has no default export`, () => {
		strictEqual(Object.hasOwn(azureModule, "default"), false);
	});
});
