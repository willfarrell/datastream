import { deepStrictEqual, rejects, strictEqual, throws } from "node:assert";
import { EventEmitter } from "node:events";
import test, { describe } from "node:test";
import {
	createReadableStream,
	pipeline,
	streamToArray,
} from "@datastream/core";
import * as gcpModule from "@datastream/gcp/pubsub";
import {
	gcpPubSubAckStream,
	gcpPubSubNackStream,
	gcpPubSubPublishStream,
	gcpPubSubSubscribeStream,
} from "@datastream/gcp/pubsub";
import { variant } from "../variant.js";

const settle = async () => {
	for (let i = 0; i < 20; i++) {
		await new Promise((resolve) => setImmediate(resolve));
	}
};

const write = (stream, chunk) =>
	new Promise((resolve) => stream.write(chunk, resolve));

// Topic whose publishes stay outstanding until the test settles them, like the
// SDK's batched publishMessage (a Promise<messageId> per message).
const deferredTopic = () => {
	const topic = {
		published: [],
		deferreds: [],
		flushes: 0,
		publishMessage(message) {
			topic.published.push(message);
			const deferred = Promise.withResolvers();
			topic.deferreds.push(deferred);
			return deferred.promise;
		},
		flush: async () => {
			topic.flushes++;
		},
	};
	return topic;
};

const instantTopic = () => {
	const topic = {
		published: [],
		flushes: 0,
		publishMessage: async (message) => {
			topic.published.push(message);
			return `id-${topic.published.length}`;
		},
		flush: async () => {
			topic.flushes++;
		},
	};
	return topic;
};

// Mirrors Subscription._listen (googleapis/nodejs-pubsub src/subscription.ts):
// the first 'message' listener opens the subscriber, removing the last one
// closes it; close() is a no-op once closed and emits 'close' before it
// resolves (src/subscriber.ts).
class FakeSubscription extends EventEmitter {
	isOpen = false;
	opens = 0;
	closes = 0;
	constructor({ close, log } = {}) {
		super();
		this.closeImpl = close;
		this.log = log;
		this.on("newListener", (event) => {
			if (!this.isOpen && event === "message") {
				this.isOpen = true;
				this.opens++;
			}
		});
		this.on("removeListener", () => {
			if (this.isOpen && this.listenerCount("message") === 0) {
				void this.close();
			}
		});
	}
	async close() {
		this.closes++;
		this.log?.push(["close"]);
		if (!this.isOpen) return;
		this.isOpen = false;
		await this.closeImpl?.();
		this.emit("close");
	}
	listeners3() {
		return ["message", "error", "close"].map((event) =>
			this.listenerCount(event),
		);
	}
}

const fakeMessage = (id, log) => ({
	id,
	data: Buffer.from(id),
	ack: () => log.push(["ack", id]),
	nack: () => log.push(["nack", id]),
	ackWithResponse: async () => {
		log.push(["ackWithResponse", id]);
		return "SUCCESS";
	},
	nackWithResponse: async () => {
		log.push(["nackWithResponse", id]);
		return "SUCCESS";
	},
});

describe(`@datastream/gcp/pubsub (${variant})`, () => {
	test("exports", () => {
		deepStrictEqual(Object.keys(gcpModule).sort(), [
			"gcpPubSubAckStream",
			"gcpPubSubNackStream",
			"gcpPubSubPublishStream",
			"gcpPubSubSubscribeStream",
		]);
	});

	// *** gcpPubSubPublishStream *** //
	test("gcpPubSubPublishStream requires a topic", () => {
		throws(() => gcpPubSubPublishStream(), {
			name: "TypeError",
			message: "gcpPubSubPublishStream: topic required",
		});
		throws(() => gcpPubSubPublishStream({}), {
			name: "TypeError",
			message: "gcpPubSubPublishStream: topic required",
		});
	});

	test("gcpPubSubPublishStream validates maxInFlight", () => {
		for (const maxInFlight of [0, 0.5, -1, Number.NaN, "x"]) {
			throws(
				() => gcpPubSubPublishStream({ topic: instantTopic(), maxInFlight }),
				{
					name: "TypeError",
					message: "gcpPubSubPublishStream: maxInFlight must be >= 1",
				},
			);
		}
		gcpPubSubPublishStream({ topic: instantTopic(), maxInFlight: 1 });
	});

	test("gcpPubSubPublishStream publishes each message and flushes at the end", async () => {
		const topic = instantTopic();
		const a = { data: Buffer.from("a"), attributes: { k: "v" } };
		const b = { data: new Uint8Array([1]), orderingKey: "o" };
		const c = { json: { x: 1 } };
		await pipeline([
			createReadableStream([a, b, c]),
			gcpPubSubPublishStream({ topic }),
		]);
		strictEqual(topic.published.length, 3);
		// Passed through untouched (the SDK copies it).
		strictEqual(topic.published[0], a);
		strictEqual(topic.published[1], b);
		strictEqual(topic.published[2], c);
		strictEqual(topic.flushes, 1);
	});

	test("gcpPubSubPublishStream encodes string data as UTF-8", async () => {
		const topic = instantTopic();
		const chunk = { data: "héllo", attributes: { k: "v" } };
		await pipeline([
			createReadableStream([chunk]),
			gcpPubSubPublishStream({ topic }),
		]);
		deepStrictEqual(topic.published, [
			{ data: Buffer.from("héllo"), attributes: { k: "v" } },
		]);
		strictEqual(Buffer.isBuffer(topic.published[0].data), true);
		// The caller's chunk is not mutated.
		strictEqual(chunk.data, "héllo");
	});

	test("gcpPubSubPublishStream keeps up to maxInFlight outstanding and waits for the oldest", async () => {
		const topic = deferredTopic();
		const stream = gcpPubSubPublishStream({ topic, maxInFlight: 2 });
		stream.on("error", () => {});
		const done = Promise.all(
			[1, 2, 3, 4].map((i) => write(stream, { data: Buffer.from(`${i}`) })),
		);
		await settle();
		strictEqual(topic.published.length, 2);
		// A newer publish confirming does not free the slot the oldest holds.
		topic.deferreds[1].resolve("id-2");
		await settle();
		strictEqual(topic.published.length, 2);
		topic.deferreds[0].resolve("id-1");
		await settle();
		strictEqual(topic.published.length, 4);
		for (const deferred of topic.deferreds) deferred.resolve();
		await done;
	});

	test("gcpPubSubPublishStream does not wait below maxInFlight", async () => {
		const topic = deferredTopic();
		const stream = gcpPubSubPublishStream({ topic });
		for (let i = 0; i < 100; i++) stream.write({ data: Buffer.from(`${i}`) });
		await settle();
		strictEqual(topic.published.length, 100);
		stream.write({ data: Buffer.from("100") });
		await settle();
		strictEqual(topic.published.length, 100);
		topic.deferreds[0].resolve();
		await settle();
		strictEqual(topic.published.length, 101);
		for (const deferred of topic.deferreds) deferred.resolve();
	});

	test("gcpPubSubPublishStream final() flushes then waits for every publish", async () => {
		const topic = deferredTopic();
		const stream = gcpPubSubPublishStream({ topic });
		let finished = false;
		stream.on("finish", () => {
			finished = true;
		});
		stream.write({ data: Buffer.from("a") });
		stream.write({ data: Buffer.from("b") });
		stream.end();
		await settle();
		strictEqual(topic.flushes, 1);
		strictEqual(finished, false);
		topic.deferreds[0].resolve("id-1");
		await settle();
		strictEqual(finished, false);
		topic.deferreds[1].resolve("id-2");
		await settle();
		strictEqual(finished, true);
	});

	test("gcpPubSubPublishStream errors on a rejected publish with every unconfirmed message", async () => {
		const topic = deferredTopic();
		const stream = gcpPubSubPublishStream({ topic });
		const chunks = ["a", "b", "c"].map((id) => ({ data: Buffer.from(id) }));
		for (const chunk of chunks) stream.write(chunk);
		await settle();
		topic.deferreds[0].resolve("id-a");
		const error = new Error("publish failed");
		// Not the oldest outstanding: the stream must not wait for it to be.
		topic.deferreds[2].reject(error);
		const err = await new Promise((resolve) => stream.on("error", resolve));
		strictEqual(err, error);
		deepStrictEqual(err.failedMessages, [chunks[1], chunks[2]]);
		strictEqual(err.failedMessages[0], chunks[1]);
		strictEqual(stream.destroyed, true);
	});

	test("gcpPubSubPublishStream errors when publishMessage throws synchronously", async () => {
		const error = new TypeError(
			"Data must be in the form of a Buffer or Uint8Array.",
		);
		const topic = {
			publishMessage: () => {
				throw error;
			},
			flush: async () => {},
		};
		const chunk = { data: 1 };
		await rejects(
			pipeline([
				createReadableStream([chunk]),
				gcpPubSubPublishStream({ topic }),
			]),
			(err) => {
				strictEqual(err, error);
				deepStrictEqual(err.failedMessages, [chunk]);
				return true;
			},
		);
	});

	test("gcpPubSubPublishStream leaves non-object chunks to the SDK to reject", async () => {
		const published = [];
		const topic = {
			publishMessage: async (message) => {
				published.push(message);
				throw new Error("at least one attribute must be present");
			},
			flush: async () => {},
		};
		const stream = gcpPubSubPublishStream({ topic });
		stream.write(undefined);
		const err = await new Promise((resolve) => stream.on("error", resolve));
		strictEqual(err.message, "at least one attribute must be present");
		deepStrictEqual(published, [undefined]);
		deepStrictEqual(err.failedMessages, [undefined]);
	});

	test("gcpPubSubPublishStream errors when flush rejects", async () => {
		const error = new Error("flush failed");
		const topic = instantTopic();
		topic.flush = async () => {
			throw error;
		};
		await rejects(
			pipeline([
				createReadableStream([{ data: Buffer.from("a") }]),
				gcpPubSubPublishStream({ topic }),
			]),
			(err) => err === error,
		);
	});

	test("gcpPubSubPublishStream destroy() attaches the unconfirmed messages", async () => {
		const topic = deferredTopic();
		const stream = gcpPubSubPublishStream({ topic });
		const chunks = ["a", "b"].map((id) => ({ data: Buffer.from(id) }));
		for (const chunk of chunks) stream.write(chunk);
		await settle();
		topic.deferreds[0].resolve("id-a");
		await settle();
		stream.destroy();
		const err = await new Promise((resolve) => stream.on("error", resolve));
		strictEqual(
			err.message,
			"gcpPubSubPublishStream: destroyed with unconfirmed publishes",
		);
		deepStrictEqual(err.failedMessages, [chunks[1]]);
		// Can't be cancelled: a late outcome is ignored, not unhandled.
		topic.deferreds[1].reject(new Error("late"));
		await settle();
	});

	test("gcpPubSubPublishStream destroy() with nothing outstanding is clean", async () => {
		const topic = instantTopic();
		const stream = gcpPubSubPublishStream({ topic });
		await write(stream, { data: Buffer.from("a") });
		await settle();
		let errored = false;
		stream.on("error", () => {
			errored = true;
		});
		stream.destroy();
		await settle();
		strictEqual(errored, false);
	});

	test("gcpPubSubPublishStream abort attaches the unconfirmed messages and publishes nothing more", async () => {
		const topic = deferredTopic();
		const controller = new AbortController();
		const stream = gcpPubSubPublishStream(
			{ topic, maxInFlight: 1 },
			{ signal: controller.signal },
		);
		const a = { data: Buffer.from("a") };
		stream.write(a);
		stream.write({ data: Buffer.from("b") });
		await settle();
		controller.abort();
		const err = await new Promise((resolve) => stream.on("error", resolve));
		strictEqual(err.name, "AbortError");
		deepStrictEqual(err.failedMessages, [a]);
		topic.deferreds[0].resolve("id-a");
		await settle();
		strictEqual(topic.published.length, 1);
		strictEqual(topic.flushes, 0);
	});

	test("gcpPubSubPublishStream with an already-aborted signal publishes nothing", async () => {
		const topic = instantTopic();
		await rejects(
			pipeline([
				createReadableStream([{ data: Buffer.from("a") }]),
				gcpPubSubPublishStream({ topic }, { signal: AbortSignal.abort() }),
			]),
			{ name: "AbortError" },
		);
		strictEqual(topic.published.length, 0);
		strictEqual(topic.flushes, 0);
	});

	// *** gcpPubSubSubscribeStream *** //
	test("gcpPubSubSubscribeStream requires a subscription", () => {
		throws(() => gcpPubSubSubscribeStream(), {
			name: "TypeError",
			message: "gcpPubSubSubscribeStream: subscription required",
		});
		throws(() => gcpPubSubSubscribeStream({}), {
			name: "TypeError",
			message: "gcpPubSubSubscribeStream: subscription required",
		});
	});

	test("gcpPubSubSubscribeStream streams received messages until stop()", async () => {
		const log = [];
		const subscription = new FakeSubscription({ log });
		const stream = gcpPubSubSubscribeStream({ subscription });
		// Already closed: stop() must not wait for it.
		gcpPubSubAckStream({ subscription }).destroy();
		await settle();
		strictEqual(subscription.opens, 1);
		deepStrictEqual(subscription.listeners3(), [1, 1, 1]);
		const a = fakeMessage("a", log);
		const b = fakeMessage("b", log);
		subscription.emit("message", a);
		subscription.emit("message", b);
		const output = streamToArray(stream);
		await stream.stop();
		const messages = await output;
		strictEqual(messages.length, 2);
		strictEqual(messages[0], a);
		strictEqual(messages[1], b);
		strictEqual(subscription.closes, 1);
		strictEqual(subscription.isOpen, false);
		deepStrictEqual(subscription.listeners3(), [0, 0, 0]);
		deepStrictEqual(log, [["close"]]);
		// Idempotent.
		await stream.stop();
		strictEqual(subscription.closes, 1);
	});

	test("gcpPubSubSubscribeStream stop() nacks messages that arrive while draining", async () => {
		const log = [];
		const close = Promise.withResolvers();
		const subscription = new FakeSubscription({
			log,
			close: () => close.promise,
		});
		const stream = gcpPubSubSubscribeStream({ subscription });
		const ackStream = gcpPubSubAckStream({ subscription });
		const done = pipeline([stream, ackStream]);
		subscription.emit("message", fakeMessage("a", log));
		await settle();
		const stopped = stream.stop();
		subscription.emit("message", fakeMessage("late", log));
		await settle();
		// Still listening (removing the last 'message' listener would make the
		// SDK close and nack everything) until close() has resolved.
		deepStrictEqual(subscription.listeners3(), [1, 1, 1]);
		close.resolve();
		await stopped;
		await done;
		deepStrictEqual(log, [["ack", "a"], ["nack", "late"], ["close"]]);
		deepStrictEqual(subscription.listeners3(), [0, 0, 0]);
	});

	test("gcpPubSubSubscribeStream stop() waits for this subscription's ack/nack streams to close", async () => {
		const log = [];
		const subscription = new FakeSubscription({ log });
		const other = new FakeSubscription();
		const stream = gcpPubSubSubscribeStream({ subscription });
		const ackStream = gcpPubSubAckStream({ subscription });
		const nackStream = gcpPubSubNackStream({ subscription });
		const otherStream = gcpPubSubAckStream({ subscription: other });
		const done = pipeline([stream, ackStream]);
		subscription.emit("message", fakeMessage("a", log));
		let resolved = false;
		const stopped = stream.stop().then(() => {
			resolved = true;
		});
		await done;
		await settle();
		strictEqual(subscription.closes, 0);
		nackStream.write(fakeMessage("b", log));
		nackStream.end();
		await stopped;
		strictEqual(resolved, true);
		deepStrictEqual(log, [["ack", "a"], ["nack", "b"], ["close"]]);
		// otherStream (another subscription) never closed and did not block.
		strictEqual(otherStream.closed, false);
		otherStream.destroy();
	});

	test("gcpPubSubSubscribeStream stop() rejects when close() fails", async () => {
		const error = new Error("close failed");
		const subscription = new FakeSubscription({
			close: async () => {
				throw error;
			},
		});
		const stream = gcpPubSubSubscribeStream({ subscription });
		stream.resume();
		await rejects(stream.stop(), (err) => err === error);
		deepStrictEqual(subscription.listeners3(), [0, 0, 0]);
	});

	test("gcpPubSubSubscribeStream errors on a subscription error and closes it", async () => {
		const log = [];
		const close = Promise.withResolvers();
		const subscription = new FakeSubscription({
			log,
			close: () => close.promise,
		});
		const stream = gcpPubSubSubscribeStream({ subscription });
		let closed = false;
		stream.on("close", () => {
			closed = true;
		});
		const error = new Error("stream failed");
		const received = new Promise((resolve) => stream.on("error", resolve));
		subscription.emit("error", error);
		await settle();
		strictEqual(subscription.closes, 1);
		// Torn down only once the subscription has closed.
		strictEqual(closed, false);
		deepStrictEqual(subscription.listeners3(), [1, 1, 1]);
		subscription.emit("message", fakeMessage("late", log));
		close.resolve();
		strictEqual(await received, error);
		await settle();
		strictEqual(closed, true);
		deepStrictEqual(subscription.listeners3(), [0, 0, 0]);
		deepStrictEqual(log, [["close"], ["nack", "late"]]);
	});

	test("gcpPubSubSubscribeStream destroy() closes the subscription", async () => {
		const subscription = new FakeSubscription();
		const stream = gcpPubSubSubscribeStream({ subscription });
		let errored = false;
		stream.on("error", () => {
			errored = true;
		});
		stream.destroy();
		await new Promise((resolve) => stream.on("close", resolve));
		strictEqual(errored, false);
		strictEqual(subscription.closes, 1);
		deepStrictEqual(subscription.listeners3(), [0, 0, 0]);
	});

	test("gcpPubSubSubscribeStream destroy() surfaces a close() failure", async () => {
		const error = new Error("close failed");
		const subscription = new FakeSubscription({
			close: async () => {
				throw error;
			},
		});
		const stream = gcpPubSubSubscribeStream({ subscription });
		stream.destroy();
		strictEqual(
			await new Promise((resolve) => stream.on("error", resolve)),
			error,
		);
	});

	test("gcpPubSubSubscribeStream destroy(err) keeps err over a close() failure", async () => {
		const subscription = new FakeSubscription({
			close: async () => {
				throw new Error("close failed");
			},
		});
		const stream = gcpPubSubSubscribeStream({ subscription });
		const error = new Error("boom");
		stream.destroy(error);
		strictEqual(
			await new Promise((resolve) => stream.on("error", resolve)),
			error,
		);
	});

	test("gcpPubSubSubscribeStream destroy() during a stop() drain is a hard stop", async () => {
		const log = [];
		const subscription = new FakeSubscription({ log });
		const stream = gcpPubSubSubscribeStream({ subscription });
		const ackStream = gcpPubSubAckStream({ subscription });
		subscription.emit("message", fakeMessage("a", log));
		const stopped = stream.stop();
		await settle();
		strictEqual(subscription.closes, 0);
		stream.destroy();
		await settle();
		strictEqual(subscription.closes, 1);
		ackStream.destroy();
		await stopped;
		strictEqual(subscription.closes, 1);
		deepStrictEqual(log, [["close"]]);
	});

	test("gcpPubSubSubscribeStream abort closes the subscription without acking", async () => {
		const log = [];
		const subscription = new FakeSubscription({ log });
		const controller = new AbortController();
		const stream = gcpPubSubSubscribeStream(
			{ subscription },
			{ signal: controller.signal },
		);
		const ackStream = gcpPubSubAckStream(
			{ subscription },
			{ signal: controller.signal },
		);
		const done = pipeline([stream, ackStream]);
		subscription.emit("message", fakeMessage("a", log));
		await settle();
		controller.abort();
		subscription.emit("message", fakeMessage("b", log));
		await rejects(done, { name: "AbortError" });
		await settle();
		deepStrictEqual(log, [["ack", "a"], ["close"], ["nack", "b"]]);
		deepStrictEqual(subscription.listeners3(), [0, 0, 0]);
	});

	test("gcpPubSubSubscribeStream with an already-aborted signal never opens the subscription", async () => {
		const subscription = new FakeSubscription();
		const stream = gcpPubSubSubscribeStream(
			{ subscription },
			{ signal: AbortSignal.abort() },
		);
		await rejects(streamToArray(stream), { name: "AbortError" });
		strictEqual(subscription.opens, 0);
		strictEqual(subscription.closes, 0);
		deepStrictEqual(subscription.listeners3(), [0, 0, 0]);
	});

	test("gcpPubSubSubscribeStream errors (does not throw) when its buffer overflows", async () => {
		const log = [];
		const subscription = new FakeSubscription({ log });
		const stream = gcpPubSubSubscribeStream(
			{ subscription },
			{ highWaterMark: 2 },
		);
		const received = new Promise((resolve) => stream.on("error", resolve));
		subscription.emit("message", fakeMessage("a", log));
		subscription.emit("message", fakeMessage("b", log));
		subscription.emit("message", fakeMessage("c", log));
		const err = await received;
		strictEqual(
			err.message,
			"createReadableStream queue size (2) exceeds limit (2)",
		);
		await settle();
		strictEqual(subscription.closes, 1);
		deepStrictEqual(subscription.listeners3(), [0, 0, 0]);
	});

	test("gcpPubSubSubscribeStream ends when the subscription is closed elsewhere", async () => {
		const log = [];
		const subscription = new FakeSubscription({ log });
		const stream = gcpPubSubSubscribeStream({ subscription });
		const a = fakeMessage("a", log);
		subscription.emit("message", a);
		const output = streamToArray(stream);
		await subscription.close();
		const messages = await output;
		strictEqual(messages.length, 1);
		strictEqual(messages[0], a);
		subscription.emit("message", fakeMessage("late", log));
		await settle();
		deepStrictEqual(subscription.listeners3(), [0, 0, 0]);
		strictEqual(subscription.closes, 2);
		deepStrictEqual(log, [["close"], ["close"]]);
	});

	// push(null) on a destroyed readable marks it ended, so finished() would
	// treat the teardown as a clean end and the pipeline would never settle.
	test("gcpPubSubSubscribeStream stop() after destroy() still rejects the pipeline", async () => {
		const subscription = new FakeSubscription();
		const stream = gcpPubSubSubscribeStream({ subscription });
		const done = pipeline([stream, gcpPubSubAckStream({ subscription })]);
		stream.destroy();
		const stopped = stream.stop();
		await rejects(done, { code: "ERR_STREAM_PREMATURE_CLOSE" });
		await stopped;
		strictEqual(subscription.closes, 1);
	});

	// Node's finished() counts an ended readable with an empty buffer as done,
	// but pipe only ends the destination on 'end': destroying in between would
	// leave the pipeline pending forever.
	test("gcpPubSubSubscribeStream destroy() right after stop() lets the drained readable end", async () => {
		const log = [];
		const subscription = new FakeSubscription({ log });
		// Already closed elsewhere: close() resolves at once.
		subscription.close = () => {
			subscription.isOpen = false;
			log.push(["close"]);
			return Promise.resolve();
		};
		const stream = gcpPubSubSubscribeStream({ subscription });
		const done = pipeline([stream, gcpPubSubAckStream({ subscription })]);
		subscription.emit("message", fakeMessage("a", log));
		// Before the pipeline has started flowing.
		const stopped = stream.stop();
		stream.destroy();
		await done;
		await stopped;
		// Closed at once (hard stop); the SDK ignores the later ack.
		deepStrictEqual(log, [["close"], ["ack", "a"]]);
	});

	test("gcpPubSubSubscribeStream external close after destroy() still rejects the pipeline", async () => {
		const subscription = new FakeSubscription();
		const stream = gcpPubSubSubscribeStream({ subscription });
		const done = pipeline([stream, gcpPubSubAckStream({ subscription })]);
		stream.destroy();
		subscription.emit("close");
		await rejects(done, { code: "ERR_STREAM_PREMATURE_CLOSE" });
	});

	test("gcpPubSubSubscribeStream ends on an external close even if close() then fails", async () => {
		const subscription = new FakeSubscription({
			close: async () => {
				throw new Error("close failed");
			},
		});
		const log = [];
		const stream = gcpPubSubSubscribeStream({ subscription });
		const output = streamToArray(stream);
		// The SDK emitted 'close' itself (a failed stream start closes it).
		subscription.emit("close");
		subscription.emit("message", fakeMessage("late", log));
		deepStrictEqual(await output, []);
		deepStrictEqual(log, [["nack", "late"]]);
		await settle();
		strictEqual(subscription.closes, 1);
		deepStrictEqual(subscription.listeners3(), [0, 0, 0]);
	});

	// *** gcpPubSubAckStream / gcpPubSubNackStream *** //
	test("gcpPubSubAckStream / gcpPubSubNackStream require a subscription", () => {
		throws(() => gcpPubSubAckStream(), {
			name: "TypeError",
			message: "gcpPubSubAckStream: subscription required",
		});
		throws(() => gcpPubSubNackStream({}), {
			name: "TypeError",
			message: "gcpPubSubNackStream: subscription required",
		});
		throws(() => gcpPubSubNackStream(), {
			name: "TypeError",
			message: "gcpPubSubNackStream: subscription required",
		});
	});

	test("gcpPubSubAckStream acks and gcpPubSubNackStream nacks each message", async () => {
		const log = [];
		const subscription = new FakeSubscription();
		await pipeline([
			createReadableStream([fakeMessage("a", log), fakeMessage("b", log)]),
			gcpPubSubAckStream({ subscription }),
		]);
		await pipeline([
			createReadableStream([fakeMessage("c", log)]),
			gcpPubSubNackStream({ subscription }),
		]);
		deepStrictEqual(log, [
			["ack", "a"],
			["ack", "b"],
			["nack", "c"],
		]);
	});

	test("gcpPubSubAckStream / gcpPubSubNackStream withResponse await the *WithResponse result", async () => {
		const log = [];
		const subscription = new FakeSubscription();
		const message = fakeMessage("a", log);
		const deferred = Promise.withResolvers();
		message.ackWithResponse = () => {
			log.push(["ackWithResponse", "a"]);
			return deferred.promise;
		};
		const ackStream = gcpPubSubAckStream({ subscription, withResponse: true });
		let written = false;
		ackStream.write(message, () => {
			written = true;
		});
		await settle();
		strictEqual(written, false);
		deferred.resolve("SUCCESS");
		await settle();
		strictEqual(written, true);
		ackStream.end();
		await pipeline([
			createReadableStream([fakeMessage("b", log)]),
			gcpPubSubNackStream({ subscription, withResponse: true }),
		]);
		deepStrictEqual(log, [
			["ackWithResponse", "a"],
			["nackWithResponse", "b"],
		]);
	});

	test("gcpPubSubAckStream withResponse errors when the ack fails", async () => {
		const subscription = new FakeSubscription();
		const error = Object.assign(new Error("Permission denied"), {
			errorCode: "PERMISSION_DENIED",
		});
		const message = fakeMessage("a", []);
		message.ackWithResponse = async () => {
			throw error;
		};
		await rejects(
			pipeline([
				createReadableStream([message]),
				gcpPubSubAckStream({ subscription, withResponse: true }),
			]),
			(err) => err === error,
		);
	});

	test("gcpPubSubAckStream acks nothing after abort", async () => {
		const log = [];
		const subscription = new FakeSubscription();
		const controller = new AbortController();
		const ackStream = gcpPubSubAckStream(
			{ subscription },
			{ signal: controller.signal },
		);
		ackStream.on("error", () => {});
		await write(ackStream, fakeMessage("a", log));
		controller.abort();
		ackStream.write(fakeMessage("b", log));
		await settle();
		deepStrictEqual(log, [["ack", "a"]]);
	});
});
