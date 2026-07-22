// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
import test from "node:test";
import { createReadableStream, streamToArray } from "@datastream/core";
import {
	kafkaConnect,
	kafkaConsumeStream,
	kafkaProduceStream,
} from "@datastream/kafka";
import { Bench } from "tinybench";

const time = Number(process.env.BENCH_TIME ?? 5_000);

// ---------------------------------------------------------------------------
// Fake Kafka factory (trimmed — no spy counters needed for perf)
// ---------------------------------------------------------------------------

const makeFakeKafka = ({ run } = {}) => {
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
		run:
			run ??
			(async () =>
				new Promise((resolve) => {
					consumerMock._runResolve = resolve;
				})),
	};

	const Kafka = function (opts) {
		this.opts = opts;
		this.producer = () => producerMock;
		this.consumer = () => consumerMock;
	};

	return { Kafka, producerMock, consumerMock };
};

// ---------------------------------------------------------------------------
// perf: kafkaProduceStream
// ---------------------------------------------------------------------------

test("perf: kafkaProduceStream", async () => {
	const bench = new Bench({ name: "kafkaProduceStream", time });

	const MSG_COUNT = 10_000;
	const messages = Array.from({ length: MSG_COUNT }, (_, i) => ({
		value: `message-${i}`,
		key: `key-${i}`,
	}));

	bench.add(`produce ${MSG_COUNT} messages (batchSize default)`, async () => {
		const { Kafka } = makeFakeKafka();
		const { producer } = await kafkaConnect({ Kafka });
		const stream = await kafkaProduceStream({ producer, topic: "perf-topic" });
		const readable = createReadableStream(messages);
		const writeAll = new Promise((resolve, reject) => {
			readable.on("data", (chunk) => {
				stream.write(chunk);
			});
			readable.on("end", () => {
				stream.end();
			});
			stream.on("finish", resolve);
			stream.on("error", reject);
		});
		await writeAll;
	});

	bench.add(`produce ${MSG_COUNT} messages (batchSize 1000)`, async () => {
		const { Kafka } = makeFakeKafka();
		const { producer } = await kafkaConnect({ Kafka });
		const stream = await kafkaProduceStream({
			producer,
			topic: "perf-topic",
			batchSize: 1_000,
		});
		const readable = createReadableStream(messages);
		const writeAll = new Promise((resolve, reject) => {
			readable.on("data", (chunk) => {
				stream.write(chunk);
			});
			readable.on("end", () => {
				stream.end();
			});
			stream.on("finish", resolve);
			stream.on("error", reject);
		});
		await writeAll;
	});

	bench.add(`produce ${MSG_COUNT} string messages`, async () => {
		const { Kafka } = makeFakeKafka();
		const { producer } = await kafkaConnect({ Kafka });
		const stream = await kafkaProduceStream({ producer, topic: "perf-topic" });
		const strMessages = Array.from({ length: MSG_COUNT }, (_, i) => `msg-${i}`);
		const readable = createReadableStream(strMessages);
		const writeAll = new Promise((resolve, reject) => {
			readable.on("data", (chunk) => {
				stream.write(chunk);
			});
			readable.on("end", () => {
				stream.end();
			});
			stream.on("finish", resolve);
			stream.on("error", reject);
		});
		await writeAll;
	});

	await bench.run();
	console.log(`\n${bench.name}`);
	console.table(bench.table());
});

// ---------------------------------------------------------------------------
// perf: kafkaConsumeStream
// ---------------------------------------------------------------------------

test("perf: kafkaConsumeStream", async () => {
	const bench = new Bench({ name: "kafkaConsumeStream", time });

	const MSG_COUNT = 10_000;

	bench.add(`consume ${MSG_COUNT} messages`, async () => {
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
			{ consumer: consumerMock, topics: "perf-topic" },
			{ highWaterMark: MSG_COUNT },
		);

		// Drive messages then stop
		const delivery = (async () => {
			for (let i = 0; i < MSG_COUNT; i++) {
				await eachMessageFn({
					topic: "perf-topic",
					partition: 0,
					message: {
						offset: String(i),
						key: `key-${i}`,
						value: `value-${i}`,
						headers: {},
						timestamp: String(Date.now()),
					},
				});
			}
			await stream.stop();
		})();

		await streamToArray(stream);
		await delivery;
	});

	bench.add(`consume ${MSG_COUNT} messages (highWaterMark 100)`, async () => {
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
			{ consumer: consumerMock, topics: "perf-topic" },
			{ highWaterMark: 100 },
		);

		const delivery = (async () => {
			for (let i = 0; i < MSG_COUNT; i++) {
				await eachMessageFn({
					topic: "perf-topic",
					partition: 0,
					message: {
						offset: String(i),
						key: `key-${i}`,
						value: `value-${i}`,
						headers: {},
						timestamp: String(Date.now()),
					},
				});
			}
			await stream.stop();
		})();

		await streamToArray(stream);
		await delivery;
	});

	await bench.run();
	console.log(`\n${bench.name}`);
	console.table(bench.table());
});
