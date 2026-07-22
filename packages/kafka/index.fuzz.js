// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
import test from "node:test";
import { streamToArray } from "@datastream/core";
import {
	kafkaConnect,
	kafkaConsumeStream,
	kafkaProduceStream,
} from "@datastream/kafka";
import fc from "fast-check";

const catchError = (input, e) => {
	const expectedErrors = [
		"kafkaProduceStream: producer required",
		"kafkaProduceStream: topic required",
		"kafkaProduceStream: batchSize must be >= 1",
		"kafkaConsumeStream: consumer required",
		"kafkaConsumeStream: topics required",
		// Node.js stream protocol: null is the EOF sentinel and cannot be written
		"May not write null values to stream",
	];
	// Also swallow normalizeMessage TypeErrors (chunk type errors)
	if (expectedErrors.includes(e.message)) {
		return;
	}
	if (e instanceof TypeError && e.message.startsWith("kafkaProduceStream:")) {
		return;
	}
	// Node.js writable stream rejects null with ERR_STREAM_NULL_VALUES
	if (e.code === "ERR_STREAM_NULL_VALUES") {
		return;
	}
	console.error(input, e);
	throw e;
};

// ---------------------------------------------------------------------------
// Minimal fake Kafka factory
// ---------------------------------------------------------------------------

const makeFakeKafka = () => {
	const producerMock = {
		connect: async () => {},
		disconnect: async () => {},
		send: async () => {},
	};

	const consumerMock = {
		_runResolve: null,
		connect: async () => {},
		disconnect: async () => {},
		subscribe: async () => {},
		stop: async () => {
			if (consumerMock._runResolve) consumerMock._runResolve();
		},
		run: async () =>
			new Promise((resolve) => {
				consumerMock._runResolve = resolve;
			}),
	};

	const Kafka = function (opts) {
		this.opts = opts;
		this.producer = () => producerMock;
		this.consumer = () => consumerMock;
	};

	return { Kafka, producerMock, consumerMock };
};

// ---------------------------------------------------------------------------
// fuzz: kafkaConnect
// ---------------------------------------------------------------------------

test("fuzz kafkaConnect w/ random options", async () => {
	await fc.assert(
		fc.asyncProperty(
			fc.record({
				brokers: fc.option(
					fc.array(fc.string({ minLength: 1, maxLength: 50 }), {
						minLength: 1,
						maxLength: 3,
					}),
				),
				clientId: fc.option(fc.string({ minLength: 0, maxLength: 50 })),
				groupId: fc.option(fc.string({ minLength: 0, maxLength: 50 })),
				ssl: fc.option(fc.boolean()),
				producer: fc.option(fc.oneof(fc.constant(false), fc.constant({}))),
			}),
			async (options) => {
				try {
					const { Kafka } = makeFakeKafka();
					const conn = await kafkaConnect({ ...options, Kafka });
					await conn.disconnect();
				} catch (e) {
					catchError(options, e);
				}
			},
		),
		{
			numRuns: 1_000,
			verbose: 2,
			examples: [],
		},
	);
});

// ---------------------------------------------------------------------------
// fuzz: kafkaProduceStream options
// ---------------------------------------------------------------------------

test("fuzz kafkaProduceStream w/ random options", async () => {
	await fc.assert(
		fc.asyncProperty(
			fc.record({
				topic: fc.option(fc.string({ minLength: 0, maxLength: 50 })),
				batchSize: fc.option(fc.integer({ min: -5, max: 200 })),
				acks: fc.option(fc.integer({ min: -1, max: 1 })),
				compression: fc.option(fc.integer({ min: 0, max: 4 })),
				timeout: fc.option(fc.integer({ min: 0, max: 30_000 })),
			}),
			async (options) => {
				try {
					const { producerMock } = makeFakeKafka();
					const stream = await kafkaProduceStream({
						producer: producerMock,
						...options,
					});
					stream.end();
					await new Promise((resolve, reject) => {
						stream.on("finish", resolve);
						stream.on("error", reject);
					});
				} catch (e) {
					catchError(options, e);
				}
			},
		),
		{
			numRuns: 1_000,
			verbose: 2,
			examples: [],
		},
	);
});

// ---------------------------------------------------------------------------
// fuzz: kafkaProduceStream w/ random messages
// ---------------------------------------------------------------------------

test("fuzz kafkaProduceStream w/ random messages", async () => {
	await fc.assert(
		fc.asyncProperty(
			fc.array(
				fc.oneof(
					fc.string({ minLength: 0, maxLength: 100 }),
					fc.record({
						value: fc.string({ minLength: 0, maxLength: 100 }),
						key: fc.option(fc.string({ minLength: 0, maxLength: 50 })),
					}),
					// invalid types to exercise normalizeMessage error path
					fc.integer(),
					fc.boolean(),
					// null is excluded: it is the Node.js stream EOF sentinel and
					// writing null to a Writable is a stream protocol error, not
					// something kafkaProduceStream is expected to handle.
				),
				{ minLength: 1, maxLength: 50 },
			),
			async (messages) => {
				try {
					const { producerMock } = makeFakeKafka();
					const stream = await kafkaProduceStream({
						producer: producerMock,
						topic: "fuzz-topic",
					});
					await new Promise((resolve, reject) => {
						stream.on("error", (e) => {
							try {
								catchError(messages, e);
								resolve();
							} catch (err) {
								reject(err);
							}
						});
						stream.on("finish", resolve);
						for (const msg of messages) {
							stream.write(msg);
						}
						stream.end();
					});
				} catch (e) {
					catchError(messages, e);
				}
			},
		),
		{
			numRuns: 1_000,
			verbose: 2,
			examples: [],
		},
	);
});

// ---------------------------------------------------------------------------
// fuzz: kafkaConsumeStream options
// ---------------------------------------------------------------------------

test("fuzz kafkaConsumeStream w/ random options", async () => {
	await fc.assert(
		fc.asyncProperty(
			fc.record({
				topics: fc.option(
					fc.oneof(
						fc.string({ minLength: 1, maxLength: 50 }),
						fc.array(fc.string({ minLength: 1, maxLength: 50 }), {
							minLength: 1,
							maxLength: 3,
						}),
						fc.constant([]),
					),
				),
				fromBeginning: fc.option(fc.boolean()),
				autoCommit: fc.option(fc.boolean()),
				partitionsConsumedConcurrently: fc.option(
					fc.integer({ min: 1, max: 8 }),
				),
			}),
			async (options) => {
				try {
					const { consumerMock } = makeFakeKafka();
					const stream = await kafkaConsumeStream({
						consumer: consumerMock,
						...options,
					});
					const closePromise = new Promise((resolve) =>
						stream.once("close", resolve),
					);
					stream.destroy();
					await closePromise;
				} catch (e) {
					catchError(options, e);
				}
			},
		),
		{
			numRuns: 1_000,
			verbose: 2,
			examples: [],
		},
	);
});

// ---------------------------------------------------------------------------
// fuzz: kafkaConsumeStream message delivery w/ random message payloads
// ---------------------------------------------------------------------------

test("fuzz kafkaConsumeStream w/ random message payloads", async () => {
	await fc.assert(
		fc.asyncProperty(
			fc.array(
				fc.record({
					topic: fc.string({ minLength: 1, maxLength: 50 }),
					partition: fc.integer({ min: 0, max: 10 }),
					offset: fc.nat().map(String),
					key: fc.option(fc.string({ minLength: 0, maxLength: 50 })),
					value: fc.option(fc.string({ minLength: 0, maxLength: 200 })),
					timestamp: fc.nat().map(String),
				}),
				{ minLength: 1, maxLength: 100 },
			),
			async (messages) => {
				try {
					let eachMessageFn;
					const consumerMock = {
						_runResolve: null,
						connect: async () => {},
						disconnect: async () => {},
						subscribe: async () => {},
						stop: async () => {
							if (consumerMock._runResolve) consumerMock._runResolve();
						},
						run: async ({ eachMessage }) => {
							eachMessageFn = eachMessage;
							return new Promise((resolve) => {
								consumerMock._runResolve = resolve;
							});
						},
					};

					const stream = await kafkaConsumeStream(
						{ consumer: consumerMock, topics: "fuzz-topic" },
						{ highWaterMark: messages.length + 1 },
					);

					const delivery = (async () => {
						for (const msg of messages) {
							await eachMessageFn({
								topic: msg.topic,
								partition: msg.partition,
								message: {
									offset: msg.offset,
									key: msg.key,
									value: msg.value,
									headers: {},
									timestamp: msg.timestamp,
								},
							});
						}
						await stream.stop();
					})();

					await streamToArray(stream);
					await delivery;
				} catch (e) {
					catchError(messages, e);
				}
			},
		),
		{
			numRuns: 1_000,
			verbose: 2,
			examples: [],
		},
	);
});
