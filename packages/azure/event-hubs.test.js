import { deepStrictEqual, ok, rejects, strictEqual } from "node:assert";
import test, { describe } from "node:test";
import * as azureModule from "@datastream/azure/event-hubs";
import {
	azureEventHubsReceiveEventsStream,
	azureEventHubsSendEventsStream,
} from "@datastream/azure/event-hubs";
import {
	createReadableStream,
	pipeline,
	streamToArray,
} from "@datastream/core";
import { variant } from "../variant.js";

// Fake EventHubConsumerClient: `drive(handlers)` runs once subscribed.
const consumer = (drive) => {
	const state = { args: undefined, closed: false };
	return {
		state,
		subscribe: (...args) => {
			state.args = args;
			setImmediate(() => drive(args.find((a) => a.processEvents)));
			return {
				close: async () => {
					state.closed = true;
				},
			};
		},
	};
};

describe(`@datastream/azure/event-hubs (${variant})`, () => {
	test(`azureEventHubsReceiveEventsStream yields until an empty page`, async () => {
		const client = consumer(async (handlers) => {
			await handlers.processEvents([{ body: 1 }, { body: 2 }]);
			await handlers.processEvents([{ body: 3 }]);
			await handlers.processEvents([]);
		});
		const stream = await azureEventHubsReceiveEventsStream({
			client,
			maxWaitTimeInSeconds: 1,
		});
		deepStrictEqual(await streamToArray(stream), [
			{ body: 1 },
			{ body: 2 },
			{ body: 3 },
		]);
		ok(client.state.closed);
		strictEqual(client.state.args.length, 2);
		deepStrictEqual(client.state.args[1], { maxWaitTimeInSeconds: 1 });
	});

	test(`azureEventHubsReceiveEventsStream subscribes to one partition`, async () => {
		const client = consumer(async (handlers) => {
			await handlers.processEvents([]);
			await handlers.processEvents([{ body: 1 }]);
		});
		const stream = await azureEventHubsReceiveEventsStream({
			client,
			partitionId: "0",
			pollingActive: true,
		});
		for await (const event of stream) {
			deepStrictEqual(event, { body: 1 });
			break;
		}
		strictEqual(client.state.args[0], "0");
		ok(client.state.closed);
	});

	// subscribe() without a partitionId runs one pump per partition; every pump
	// waiting in processEvents must resume once the queue drains, not just the last.
	test(`azureEventHubsReceiveEventsStream resumes every concurrent partition pump`, {
		timeout: 1000,
	}, async () => {
		const pump = async (handlers, p) => {
			await handlers.processEvents([{ body: `${p}0` }]);
			await handlers.processEvents([{ body: `${p}1` }]);
		};
		const client = consumer((handlers) => {
			pump(handlers, "a");
			pump(handlers, "b");
		});
		const stream = await azureEventHubsReceiveEventsStream({
			client,
			pollingActive: true,
		});
		const bodies = [];
		for await (const event of stream) {
			bodies.push(event.body);
			if (bodies.length === 4) break;
		}
		deepStrictEqual(bodies.sort(), ["a0", "a1", "b0", "b1"]);
	});

	test(`azureEventHubsReceiveEventsStream ignores retryable processError`, async () => {
		const client = consumer(async (handlers) => {
			// The SDK reports errors it is already retrying, then carries on.
			await handlers.processError(
				Object.assign(new Error("transient"), { retryable: true }),
			);
			await handlers.processEvents([{ body: "a" }]);
			await handlers.processEvents([]);
		});
		const stream = await azureEventHubsReceiveEventsStream({ client });
		deepStrictEqual(await streamToArray(stream), [{ body: "a" }]);
	});

	test(`azureEventHubsReceiveEventsStream throws processError`, async () => {
		const client = consumer(async (handlers) => {
			await handlers.processError(new Error("boom"));
		});
		const stream = await azureEventHubsReceiveEventsStream({ client });
		await rejects(streamToArray(stream), { message: "boom" });
	});

	test(`azureEventHubsReceiveEventsStream stops on abort`, async () => {
		const controller = new AbortController();
		const client = consumer(() => controller.abort());
		const stream = await azureEventHubsReceiveEventsStream(
			{ client, pollingActive: true },
			{ signal: controller.signal },
		);
		await rejects(streamToArray(stream));
		ok(client.state.closed);
	});

	test(`azureEventHubsReceiveEventsStream rejects when already aborted`, async () => {
		const controller = new AbortController();
		controller.abort();
		const client = consumer(() => {});
		const stream = await azureEventHubsReceiveEventsStream(
			{ client },
			{ signal: controller.signal },
		);
		await rejects(streamToArray(stream));
	});

	test(`azureEventHubsSendEventsStream batches events`, async () => {
		const sent = [];
		const client = {
			createBatch: async (options) => {
				deepStrictEqual(options, { partitionKey: "k", abortSignal: undefined });
				const items = [];
				return {
					items,
					get count() {
						return items.length;
					},
					tryAdd: (event) => items.length < 2 && !!items.push(event),
				};
			},
			sendBatch: async (batch) => sent.push(batch.items),
		};
		await pipeline([
			createReadableStream([{ body: 1 }, { body: 2 }, { body: 3 }]),
			azureEventHubsSendEventsStream({ client, partitionKey: "k" }),
		]);
		deepStrictEqual(sent, [[{ body: 1 }, { body: 2 }], [{ body: 3 }]]);
	});

	test(`event-hubs has no default export`, () => {
		strictEqual(Object.hasOwn(azureModule, "default"), false);
	});
});
