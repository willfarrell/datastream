import { deepStrictEqual, ok, rejects, strictEqual } from "node:assert";
import test, { describe } from "node:test";
import * as azureModule from "@datastream/azure/event-hubs";
import {
	azureEventHubsReceiveEventsStream,
	azureEventHubsSendEventsStream,
} from "@datastream/azure/event-hubs";
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
		// Never subscribed.
		strictEqual(client.state.args, undefined);
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
			sendBatch: async (batch, options) => {
				deepStrictEqual(options, { abortSignal: undefined });
				sent.push(batch.items);
			},
		};
		await pipeline([
			createReadableStream([{ body: 1 }, { body: 2 }, { body: 3 }]),
			azureEventHubsSendEventsStream({ client, partitionKey: "k" }),
		]);
		deepStrictEqual(sent, [[{ body: 1 }, { body: 2 }], [{ body: 3 }]]);
	});

	test(`azureEventHubsReceiveEventsStream is a Readable that pipejoin accepts`, async () => {
		const client = consumer(async (handlers) => {
			await handlers.processEvents([{ body: 1 }]);
			await handlers.processEvents([]);
		});
		const stream = await azureEventHubsReceiveEventsStream({ client });
		strictEqual(isReadable(stream), true);
		deepStrictEqual(await streamToArray(pipejoin([stream])), [{ body: 1 }]);
	});

	// A generator's return() only lands at its next yield; destroying the stream
	// while no events arrive must still close the subscription and emit 'close'.
	test(`azureEventHubsReceiveEventsStream destroy closes an idle subscription`, {
		timeout: 1000,
	}, async () => {
		const client = consumer(() => {});
		const stream = await azureEventHubsReceiveEventsStream({
			client,
			pollingActive: true,
		});
		stream.resume();
		await new Promise((resolve) => setImmediate(resolve));
		const closed = new Promise((resolve) => stream.once("close", resolve));
		stream.destroy();
		await closed;
		ok(client.state.closed);
	});

	// processEvents holds the SDK pump until the consumer drains the page (so it
	// cannot read ahead of a slow pipeline), and ending the stream releases it.
	test(`azureEventHubsReceiveEventsStream holds processEvents until the page drains`, {
		timeout: 1000,
	}, async () => {
		let delivered;
		const events = Array.from({ length: 100 }, (_, body) => ({ body }));
		const client = consumer((handlers) => {
			delivered = handlers.processEvents(events);
		});
		const stream = await azureEventHubsReceiveEventsStream({
			client,
			pollingActive: true,
		});
		const iterator = stream[Symbol.asyncIterator]();
		deepStrictEqual((await iterator.next()).value, { body: 0 });
		let resolved = false;
		delivered.then(() => {
			resolved = true;
		});
		await settle();
		// Read-ahead buffers only highWaterMark events, not the whole page.
		strictEqual(resolved, false);
		await iterator.return();
		await delivered;
		ok(client.state.closed);
	});

	// The SDK may deliver before the generator first waits for events.
	test(`azureEventHubsReceiveEventsStream accepts callbacks fired inside subscribe`, async () => {
		const delivered = [];
		const client = {
			subscribe: (handlers) => {
				delivered.push(
					handlers.processEvents([]),
					handlers.processError(new Error("boom")),
				);
				return { close: async () => {} };
			},
		};
		const stream = await azureEventHubsReceiveEventsStream({ client });
		await rejects(streamToArray(stream), { message: "boom" });
		await Promise.all(delivered);
	});

	test(`azureEventHubsReceiveEventsStream ignores an empty processError`, {
		timeout: 1000,
	}, async () => {
		const client = consumer(async (handlers) => {
			await handlers.processError(undefined);
			await handlers.processEvents([{ body: "a" }]);
			await handlers.processEvents([]);
		});
		const stream = await azureEventHubsReceiveEventsStream({ client });
		deepStrictEqual(await streamToArray(stream), [{ body: "a" }]);
	});

	// A long-lived signal must not keep a finished stream's abort handler.
	test(`azureEventHubsReceiveEventsStream drops its abort listener once done`, async () => {
		const controller = new AbortController();
		const client = consumer(async (handlers) => {
			await handlers.processEvents([{ body: 1 }]);
			await handlers.processEvents([]);
		});
		const stream = await azureEventHubsReceiveEventsStream(
			{ client },
			{ signal: controller.signal },
		);
		deepStrictEqual(await streamToArray(stream), [{ body: 1 }]);
		let inspected = false;
		controller.abort({
			get retryable() {
				inspected = true;
				return true;
			},
		});
		strictEqual(inspected, false);
	});

	test(`azureEventHubsSendEventsStream rejects an event too big for a batch`, async () => {
		const client = {
			createBatch: async () => ({
				count: 0,
				maxSizeInBytes: 1024,
				tryAdd: () => false,
			}),
		};
		await rejects(
			pipeline([
				createReadableStream([{ body: 1 }]),
				azureEventHubsSendEventsStream({ client }),
			]),
			{
				name: "RangeError",
				message: "azureEventHubsSendEvents event exceeds batch limit",
			},
		);
	});

	test(`azureEventHubsSendEventsStream destroy attaches unsent events`, async () => {
		const client = {
			createBatch: async () => ({ count: 0, tryAdd: () => true }),
		};
		const stream = azureEventHubsSendEventsStream({ client });
		const error = new Promise((resolve) => stream.once("error", resolve));
		await new Promise((resolve) => stream.write({ body: 1 }, resolve));
		stream.destroy();
		const e = await error;
		strictEqual(
			e.message,
			"azureEventHubsSendEvents destroyed with a buffered batch",
		);
		deepStrictEqual(e.failedMessages, [{ body: 1 }]);
	});

	test(`event-hubs has no default export`, () => {
		strictEqual(Object.hasOwn(azureModule, "default"), false);
	});
});
