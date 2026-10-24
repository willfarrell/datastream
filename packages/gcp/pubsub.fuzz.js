// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
import { deepStrictEqual, strictEqual } from "node:assert";
import { EventEmitter } from "node:events";
import test from "node:test";
import { createReadableStream, pipeline } from "@datastream/core";
import {
	gcpPubSubAckStream,
	gcpPubSubPublishStream,
	gcpPubSubSubscribeStream,
} from "@datastream/gcp/pubsub";
import fc from "fast-check";

const catchError = (input, e) => {
	const expectedErrors = [
		"Data must be in the form of a Buffer or Uint8Array.",
		"All attributes must be in the form of a string.",
		"If data is undefined, at least one attribute must be present.",
		"publish failed",
		"boom",
		"gcpPubSubPublishStream: destroyed with unconfirmed publishes",
		"stream failed",
	];
	if (
		expectedErrors.includes(e.message) ||
		e.name === "AbortError" ||
		e.code === "ERR_STREAM_PREMATURE_CLOSE" ||
		e.message.startsWith("createReadableStream queue size")
	) {
		return;
	}
	console.error(input, e);
	throw e;
};

const tick = () => new Promise((resolve) => setImmediate(resolve));

// The SDK's synchronous validation (googleapis/nodejs-pubsub
// src/publisher/index.ts publishMessage).
const validate = (message) => {
	const { data, attributes = {} } = Object.assign({}, message);
	if (data === undefined && !Object.keys(attributes).length) {
		throw new TypeError(
			"If data is undefined, at least one attribute must be present.",
		);
	}
	if (data && !(data instanceof Uint8Array)) {
		throw new TypeError("Data must be in the form of a Buffer or Uint8Array.");
	}
	for (const value of Object.values(attributes)) {
		if (typeof value !== "string") {
			throw new TypeError("All attributes must be in the form of a string.");
		}
	}
};

test("fuzz gcpPubSubPublishStream w/ random message shapes", async () => {
	await fc.assert(
		fc.asyncProperty(
			fc.array(
				fc.oneof(
					fc.record(
						{
							data: fc.oneof(
								fc.string(),
								fc.string({ unit: "binary" }),
								fc.uint8Array(),
								fc.anything(),
							),
							attributes: fc.dictionary(
								fc.string(),
								fc.oneof(fc.string(), fc.anything()),
							),
							orderingKey: fc.string(),
							json: fc.jsonValue(),
						},
						{ requiredKeys: [] },
					),
					fc.anything(),
				),
				{ maxLength: 10 },
			),
			async (chunks) => {
				const published = [];
				const topic = {
					publishMessage: async (message) => {
						validate(message);
						published.push(message);
						return `${published.length}`;
					},
					flush: async () => {},
				};
				try {
					await pipeline([
						createReadableStream(chunks),
						gcpPubSubPublishStream({ topic }),
					]);
					strictEqual(published.length, chunks.length);
					for (let i = 0; i < chunks.length; i++) {
						const { data } = chunks[i];
						if (typeof data === "string") {
							deepStrictEqual(published[i].data, Buffer.from(data));
						} else {
							strictEqual(published[i], chunks[i]);
						}
					}
				} catch (e) {
					catchError(chunks, e);
				}
			},
		),
		{ numRuns: 1_000, verbose: 2 },
	);
});

test("fuzz gcpPubSubPublishStream w/ random delays, failures and destroys", async () => {
	await fc.assert(
		fc.asyncProperty(
			fc.record({
				maxInFlight: fc.integer({ min: 1, max: 5 }),
				ops: fc.array(
					fc.record({
						delay: fc.integer({ min: 0, max: 3 }),
						fail: fc.boolean(),
						pause: fc.boolean(),
					}),
					{ maxLength: 12 },
				),
				teardown: fc.constantFrom("end", "destroy", "destroyError", "abort"),
				teardownAt: fc.nat({ max: 12 }),
			}),
			async (input) => {
				const { maxInFlight, ops, teardown, teardownAt } = input;
				const published = [];
				const confirmed = new Set();
				const failed = new Set();
				const settled = [];
				const topic = {
					publishMessage: (message) => {
						published.push(message);
						const { delay, fail } = ops[message.i];
						const promise = (async () => {
							for (let i = 0; i < delay; i++) await tick();
							if (fail) {
								failed.add(message);
								throw new Error("publish failed");
							}
							confirmed.add(message);
							return `${message.i}`;
						})();
						settled.push(promise.catch(() => {}));
						return promise;
					},
					flush: async () => {},
				};
				const controller = new AbortController();
				const stream = gcpPubSubPublishStream(
					{ topic, maxInFlight },
					{ signal: controller.signal },
				);
				let error;
				stream.on("error", (e) => {
					error ??= e;
				});
				const closed = new Promise((resolve) => stream.on("close", resolve));
				const written = [];
				const tearDown = () => {
					if (teardown === "destroy") stream.destroy();
					if (teardown === "destroyError") stream.destroy(new Error("boom"));
					if (teardown === "abort") controller.abort();
				};
				for (let i = 0; i < ops.length; i++) {
					if (i === teardownAt) tearDown();
					const chunk = { data: Buffer.from(`${i}`), i };
					written.push(chunk);
					stream.write(chunk, () => {});
					if (ops[i].pause) await tick();
				}
				if (teardownAt >= ops.length) tearDown();
				if (!stream.destroyed) stream.end();
				await closed;
				await Promise.all(settled);
				try {
					if (error) throw error;
				} catch (e) {
					catchError(input, e);
				}
				const failedMessages = error?.failedMessages ?? [];
				const reported = new Set(failedMessages);
				const isPublished = new Set(published);
				strictEqual(reported.size, failedMessages.length);
				deepStrictEqual(
					failedMessages,
					written.filter((chunk) => reported.has(chunk)),
				);
				for (const chunk of failedMessages) {
					strictEqual(isPublished.has(chunk), true);
				}
				for (const chunk of failed) strictEqual(reported.has(chunk), true);
				strictEqual(isPublished.size, published.length);
				const confirmedOnly = [...confirmed].filter((c) => !reported.has(c));
				const rejected = written.filter((chunk) => !isPublished.has(chunk));
				strictEqual(
					confirmedOnly.length + failedMessages.length + rejected.length,
					written.length,
				);
				if (teardown === "end" && !error) {
					strictEqual(confirmed.size, written.length);
				}
			},
		),
		{ numRuns: 500, verbose: 2, timeout: 2_000 },
	);
});

class FakeSubscription extends EventEmitter {
	isOpen = false;
	closes = 0;
	constructor() {
		super();
		this.on("newListener", (event) => {
			if (event === "message") this.isOpen = true;
		});
	}
	async close() {
		this.closes++;
		if (!this.isOpen) return;
		this.isOpen = false;
		await tick();
		this.emit("close");
	}
}

test("fuzz gcpPubSubSubscribeStream w/ random event sequences", async () => {
	await fc.assert(
		fc.asyncProperty(
			fc.record({
				highWaterMark: fc.integer({ min: 1, max: 4 }),
				events: fc.array(
					fc.constantFrom(
						"message",
						"message",
						"message",
						"tick",
						"error",
						"externalClose",
						"stop",
						"destroy",
						"abort",
					),
					{ maxLength: 20 },
				),
			}),
			async (input) => {
				const { highWaterMark, events } = input;
				const subscription = new FakeSubscription();
				const controller = new AbortController();
				const streamOptions = { highWaterMark, signal: controller.signal };
				const stream = gcpPubSubSubscribeStream(
					{ subscription },
					streamOptions,
				);
				const ackStream = gcpPubSubAckStream({ subscription }, streamOptions);
				const handled = new Map();
				let aborted = false;
				let ackedAfterAbort = false;
				const message = (id) => ({
					id,
					ack: () => {
						if (aborted) ackedAfterAbort = true;
						strictEqual(handled.has(id), false);
						handled.set(id, "ack");
					},
					nack: () => {
						strictEqual(handled.has(id), false);
						handled.set(id, "nack");
					},
				});
				const done = pipeline([stream, ackStream]).catch((e) =>
					catchError(input, e),
				);
				const stops = [];
				let id = 0;
				for (const event of events) {
					if (event === "message") subscription.emit("message", message(id++));
					if (event === "tick") await tick();
					if (event === "error" && subscription.listenerCount("error")) {
						subscription.emit("error", new Error("stream failed"));
					}
					if (event === "externalClose") subscription.close();
					if (event === "stop") stops.push(stream.stop());
					if (event === "destroy") stream.destroy();
					if (event === "abort") {
						aborted = true;
						controller.abort();
					}
				}
				stops.push(stream.stop());
				await Promise.all(stops);
				await done;
				for (let i = 0; i < 5; i++) await tick();
				strictEqual(ackedAfterAbort, false);
				strictEqual(stream.destroyed, true);
				strictEqual(subscription.isOpen, false);
				for (const event of ["message", "error", "close"]) {
					strictEqual(subscription.listenerCount(event), 0);
				}
			},
		),
		{ numRuns: 500, verbose: 2, timeout: 2_000 },
	);
});
