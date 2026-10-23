// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
import { deepStrictEqual, ok, rejects, strictEqual } from "node:assert";
import { EventEmitter } from "node:events";
import test, { describe } from "node:test";
import {
	createReadableStream,
	pipeline,
	streamToArray,
} from "@datastream/core";
import {
	kafkaCommitStream,
	kafkaConnect,
	kafkaConsumeStream,
	kafkaProduceStream,
} from "@datastream/kafka";

describe("@datastream/kafka", () => {
	const makeFakeKafka = ({
		connectProducer = async () => {},
		disconnectProducer = async () => {},
		sendMessages = async () => {},
		connectConsumer = async () => {},
		disconnectConsumer = async () => {},
		subscribe = async () => {},
		run,
	} = {}) => {
		const producerMock = {
			connectCalled: 0,
			disconnectCalled: 0,
			sendArgs: [],
			connect: async () => {
				producerMock.connectCalled++;
				return connectProducer();
			},
			disconnect: async () => {
				producerMock.disconnectCalled++;
				return disconnectProducer();
			},
			send: async (opts) => {
				producerMock.sendArgs.push(opts);
				return sendMessages(opts);
			},
		};

		const consumerMock = {
			connectCalled: 0,
			disconnectCalled: 0,
			subscribeCalls: [],
			stopCalled: 0,
			_runResolve: null,
			connect: async () => {
				consumerMock.connectCalled++;
				return connectConsumer();
			},
			disconnect: async () => {
				consumerMock.disconnectCalled++;
				return disconnectConsumer();
			},
			subscribe: async (opts) => {
				consumerMock.subscribeCalls.push(opts);
				return subscribe(opts);
			},
			stop: async () => {
				consumerMock.stopCalled++;
				if (consumerMock._runResolve) consumerMock._runResolve();
			},
			run:
				run ??
				(async () => {
					return new Promise((resolve) => {
						consumerMock._runResolve = resolve;
					});
				}),
		};

		const Kafka = function (opts) {
			this.opts = opts;
			this.producer = (opts) => {
				producerMock.producerOpts = opts;
				return producerMock;
			};
			this.consumer = (opts) => {
				consumerMock.consumerOpts = opts;
				return consumerMock;
			};
		};

		return { Kafka, producerMock, consumerMock };
	};

	test("kafkaConnect: returns kafka, producer, consumer, disconnect", async () => {
		const { Kafka, producerMock, consumerMock } = makeFakeKafka();
		const result = await kafkaConnect({ groupId: "g1", Kafka });
		ok(result.kafka, "has kafka");
		ok(result.producer === producerMock, "has producer");
		ok(result.consumer === consumerMock, "has consumer");
		ok(typeof result.disconnect === "function", "has disconnect");
	});

	test("kafkaConnect: passes brokers/clientId/ssl/sasl to Kafka constructor", async () => {
		let capturedOpts;
		const FakeKafka = function (opts) {
			capturedOpts = opts;
			this.producer = () => ({
				connect: async () => {},
				disconnect: async () => {},
			});
			this.consumer = () => ({
				connect: async () => {},
				disconnect: async () => {},
			});
		};
		await kafkaConnect({
			brokers: ["b1"],
			clientId: "cid",
			ssl: true,
			sasl: { mechanism: "plain" },
			groupId: "g1",
			Kafka: FakeKafka,
		});
		deepStrictEqual(capturedOpts.brokers, ["b1"]);
		strictEqual(capturedOpts.clientId, "cid");
		strictEqual(capturedOpts.ssl, true);
		deepStrictEqual(capturedOpts.sasl, { mechanism: "plain" });
	});

	test("kafkaConnect: producer is null when producerOptions === false", async () => {
		const { Kafka } = makeFakeKafka();
		const result = await kafkaConnect({ producer: false, Kafka });
		strictEqual(result.producer, null, "producer should be null");
	});

	test("kafkaConnect: consumer is null when groupId is not provided", async () => {
		const { Kafka } = makeFakeKafka();
		const result = await kafkaConnect({ Kafka });
		strictEqual(result.consumer, null, "consumer should be null");
	});

	test("kafkaConnect: producer connects when producerOptions is truthy", async () => {
		const { Kafka, producerMock } = makeFakeKafka();
		await kafkaConnect({ Kafka });
		strictEqual(producerMock.connectCalled, 1, "producer.connect called once");
	});

	test("kafkaConnect: consumer connects when groupId is provided", async () => {
		const { Kafka, consumerMock } = makeFakeKafka();
		await kafkaConnect({ groupId: "g1", Kafka });
		strictEqual(consumerMock.connectCalled, 1, "consumer.connect called once");
	});

	test("kafkaConnect: producer not connected when producer === false", async () => {
		const { Kafka, producerMock } = makeFakeKafka();
		await kafkaConnect({ producer: false, Kafka });
		strictEqual(producerMock.connectCalled, 0, "producer.connect not called");
	});

	test("kafkaConnect: consumer not connected when no groupId", async () => {
		const { Kafka, consumerMock } = makeFakeKafka();
		await kafkaConnect({ Kafka });
		strictEqual(consumerMock.connectCalled, 0, "consumer.connect not called");
	});

	test("kafkaConnect: disconnect() calls producer.disconnect when producer exists", async () => {
		const { Kafka, producerMock } = makeFakeKafka();
		const conn = await kafkaConnect({ Kafka });
		await conn.disconnect();
		strictEqual(producerMock.disconnectCalled, 1);
	});

	test("kafkaConnect: disconnect() calls consumer.disconnect when consumer exists", async () => {
		const { Kafka, consumerMock } = makeFakeKafka();
		const conn = await kafkaConnect({ groupId: "g1", Kafka });
		await conn.disconnect();
		strictEqual(consumerMock.disconnectCalled, 1);
	});

	test("kafkaConnect: disconnect() does not call producer.disconnect when producer is null", async () => {
		const { Kafka, producerMock } = makeFakeKafka();
		const conn = await kafkaConnect({ producer: false, Kafka });
		await conn.disconnect();
		strictEqual(producerMock.disconnectCalled, 0);
	});

	test("kafkaConnect: disconnect() does not call consumer.disconnect when consumer is null", async () => {
		const { Kafka, consumerMock } = makeFakeKafka();
		const conn = await kafkaConnect({ Kafka });
		await conn.disconnect();
		strictEqual(consumerMock.disconnectCalled, 0);
	});

	test("kafkaConnect: disconnect() still disconnects consumer when producer.disconnect throws", async () => {
		const { Kafka, producerMock, consumerMock } = makeFakeKafka({
			disconnectProducer: async () => {
				throw new Error("producer disconnect failed");
			},
		});
		const conn = await kafkaConnect({ groupId: "g1", Kafka });
		await rejects(() => conn.disconnect(), {
			message: "producer disconnect failed",
		});
		strictEqual(producerMock.disconnectCalled, 1);
		strictEqual(
			consumerMock.disconnectCalled,
			1,
			"consumer still disconnected",
		);
	});

	test("kafkaConnect: disconnect() rethrows the first rejection when both fail", async () => {
		const { Kafka } = makeFakeKafka({
			disconnectProducer: async () => {
				throw new Error("producer disconnect failed");
			},
			disconnectConsumer: async () => {
				throw new Error("consumer disconnect failed");
			},
		});
		const conn = await kafkaConnect({ groupId: "g1", Kafka });
		await rejects(() => conn.disconnect(), {
			message: "producer disconnect failed",
		});
	});

	test("kafkaConnect: disconnect() rethrows consumer error when only consumer fails", async () => {
		const { Kafka, producerMock } = makeFakeKafka({
			disconnectConsumer: async () => {
				throw new Error("consumer disconnect failed");
			},
		});
		const conn = await kafkaConnect({ groupId: "g1", Kafka });
		await rejects(() => conn.disconnect(), {
			message: "consumer disconnect failed",
		});
		strictEqual(producerMock.disconnectCalled, 1);
	});

	test("kafkaConnect: producer connect error triggers teardown and rethrows", async () => {
		const { Kafka, producerMock } = makeFakeKafka({
			connectProducer: async () => {
				throw new Error("producer connect failed");
			},
		});
		await rejects(
			async () => kafkaConnect({ Kafka }),
			/producer connect failed/,
		);
		strictEqual(producerMock.disconnectCalled, 1);
	});

	test("kafkaConnect: consumer connect error triggers teardown and rethrows", async () => {
		const { Kafka, producerMock, consumerMock } = makeFakeKafka({
			connectConsumer: async () => {
				throw new Error("consumer connect failed");
			},
		});
		await rejects(
			async () => kafkaConnect({ groupId: "g1", Kafka }),
			/consumer connect failed/,
		);
		strictEqual(producerMock.disconnectCalled, 1);
		strictEqual(consumerMock.disconnectCalled, 1);
	});

	test("kafkaConnect: passes consumerOptions alongside groupId", async () => {
		const { Kafka, consumerMock } = makeFakeKafka();
		await kafkaConnect({
			groupId: "g1",
			consumer: { sessionTimeout: 5000 },
			Kafka,
		});
		deepStrictEqual(consumerMock.consumerOpts, {
			groupId: "g1",
			sessionTimeout: 5000,
		});
	});

	test("module has only named exports", async () => {
		const mod = await import("@datastream/kafka");
		deepStrictEqual(Object.keys(mod).sort(), [
			"kafkaCommitStream",
			"kafkaConnect",
			"kafkaConsumeStream",
			"kafkaProduceStream",
		]);
	});

	test("kafkaProduceStream: throws when producer is missing", async () => {
		await rejects(
			async () => kafkaProduceStream({ topic: "t" }),
			/kafkaProduceStream: producer required/,
		);
	});

	test("kafkaProduceStream: throws TypeError when producer is missing", async () => {
		await rejects(async () => kafkaProduceStream({ topic: "t" }), TypeError);
	});

	test("kafkaProduceStream: throws when topic is missing", async () => {
		const { producerMock } = makeFakeKafka();
		await rejects(
			async () => kafkaProduceStream({ producer: producerMock }),
			/kafkaProduceStream: topic required/,
		);
	});

	test("kafkaProduceStream: throws TypeError when topic is missing", async () => {
		const { producerMock } = makeFakeKafka();
		await rejects(
			async () => kafkaProduceStream({ producer: producerMock }),
			TypeError,
		);
	});

	test("kafkaProduceStream: throws when batchSize is 0", async () => {
		const { producerMock } = makeFakeKafka();
		await rejects(
			async () =>
				kafkaProduceStream({
					producer: producerMock,
					topic: "t",
					batchSize: 0,
				}),
			/kafkaProduceStream: batchSize must be >= 1/,
		);
	});

	test("kafkaProduceStream: throws TypeError when batchSize < 1", async () => {
		const { producerMock } = makeFakeKafka();
		await rejects(
			async () =>
				kafkaProduceStream({
					producer: producerMock,
					topic: "t",
					batchSize: 0,
				}),
			TypeError,
		);
	});

	test("kafkaProduceStream: accepts batchSize === 1 without throwing", async () => {
		const { producerMock } = makeFakeKafka();
		const stream = await kafkaProduceStream({
			producer: producerMock,
			topic: "t",
			batchSize: 1,
		});
		ok(stream, "stream created");
		stream.end();
		await new Promise((resolve) => stream.on("finish", resolve));
	});

	test("kafkaProduceStream: sends string chunk wrapped as { value }", async () => {
		const { producerMock } = makeFakeKafka();
		const stream = await kafkaProduceStream({
			producer: producerMock,
			topic: "t",
		});
		stream.write("hello");
		stream.end();
		await new Promise((resolve) => stream.on("finish", resolve));
		strictEqual(producerMock.sendArgs.length, 1);
		deepStrictEqual(producerMock.sendArgs[0].messages[0], { value: "hello" });
	});

	test("kafkaProduceStream: sends Uint8Array chunk wrapped as { value }", async () => {
		const { producerMock } = makeFakeKafka();
		const stream = await kafkaProduceStream({
			producer: producerMock,
			topic: "t",
		});
		const buf = new Uint8Array([1, 2, 3]);
		stream.write(buf);
		stream.end();
		await new Promise((resolve) => stream.on("finish", resolve));
		strictEqual(producerMock.sendArgs.length, 1);
		deepStrictEqual(producerMock.sendArgs[0].messages[0], { value: buf });
	});

	test("kafkaProduceStream: passes through { value, key, headers } object unchanged", async () => {
		const { producerMock } = makeFakeKafka();
		const stream = await kafkaProduceStream({
			producer: producerMock,
			topic: "t",
		});
		const msg = { value: "v", key: "k", headers: { h: "1" } };
		stream.write(msg);
		stream.end();
		await new Promise((resolve) => stream.on("finish", resolve));
		deepStrictEqual(producerMock.sendArgs[0].messages[0], msg);
	});

	test("kafkaProduceStream: emits error on invalid chunk (boolean false — not Uint8Array/string/object-with-value)", async () => {
		const { producerMock } = makeFakeKafka();
		const stream = await kafkaProduceStream({
			producer: producerMock,
			topic: "t",
		});
		const errorPromise = new Promise((resolve) =>
			stream.once("error", resolve),
		);
		stream.write(false);
		const err = await errorPromise;
		ok(err instanceof TypeError, "error is TypeError");
		ok(err.message.includes("boolean"), "error message includes actual type");
	});

	test("kafkaProduceStream: error message includes actual type for non-null invalid chunk", async () => {
		const { producerMock } = makeFakeKafka();
		const stream = await kafkaProduceStream({
			producer: producerMock,
			topic: "t",
		});
		const errorPromise = new Promise((resolve) =>
			stream.once("error", resolve),
		);
		stream.write(42);
		const err = await errorPromise;
		ok(err.message.includes("number"), "error message includes 'number'");
	});

	test("kafkaProduceStream: batches messages and flushes at batchSize", async () => {
		const { producerMock } = makeFakeKafka();
		const stream = await kafkaProduceStream({
			producer: producerMock,
			topic: "t",
			batchSize: 3,
		});
		for (let i = 0; i < 6; i++) stream.write(`msg${i}`);
		stream.end();
		await new Promise((resolve) => stream.on("finish", resolve));
		strictEqual(producerMock.sendArgs.length, 2);
		strictEqual(producerMock.sendArgs[0].messages.length, 3);
		strictEqual(producerMock.sendArgs[1].messages.length, 3);
	});

	test("kafkaProduceStream: partial batch flushed on end", async () => {
		const { producerMock } = makeFakeKafka();
		const stream = await kafkaProduceStream({
			producer: producerMock,
			topic: "t",
			batchSize: 10,
		});
		stream.write("a");
		stream.write("b");
		stream.end();
		await new Promise((resolve) => stream.on("finish", resolve));
		strictEqual(producerMock.sendArgs.length, 1);
		strictEqual(producerMock.sendArgs[0].messages.length, 2);
	});

	test("kafkaProduceStream: empty stream sends no messages", async () => {
		const { producerMock } = makeFakeKafka();
		const stream = await kafkaProduceStream({
			producer: producerMock,
			topic: "t",
		});
		stream.end();
		await new Promise((resolve) => stream.on("finish", resolve));
		strictEqual(producerMock.sendArgs.length, 0);
	});

	test("kafkaProduceStream: sends with correct topic", async () => {
		const { producerMock } = makeFakeKafka();
		const stream = await kafkaProduceStream({
			producer: producerMock,
			topic: "my-topic",
		});
		stream.write("msg");
		stream.end();
		await new Promise((resolve) => stream.on("finish", resolve));
		strictEqual(producerMock.sendArgs[0].topic, "my-topic");
	});

	test("kafkaProduceStream: sends with acks default -1", async () => {
		const { producerMock } = makeFakeKafka();
		const stream = await kafkaProduceStream({
			producer: producerMock,
			topic: "t",
		});
		stream.write("msg");
		stream.end();
		await new Promise((resolve) => stream.on("finish", resolve));
		strictEqual(producerMock.sendArgs[0].acks, -1);
	});

	test("kafkaProduceStream: sends with custom acks value", async () => {
		const { producerMock } = makeFakeKafka();
		const stream = await kafkaProduceStream({
			producer: producerMock,
			topic: "t",
			acks: 1,
		});
		stream.write("msg");
		stream.end();
		await new Promise((resolve) => stream.on("finish", resolve));
		strictEqual(producerMock.sendArgs[0].acks, 1);
	});

	test("kafkaProduceStream: forwards compression to producer.send", async () => {
		const { producerMock } = makeFakeKafka();
		const stream = await kafkaProduceStream({
			producer: producerMock,
			topic: "t",
			compression: 2,
		});
		stream.write("msg");
		stream.end();
		await new Promise((resolve) => stream.on("finish", resolve));
		strictEqual(producerMock.sendArgs[0].compression, 2);
	});

	test("kafkaProduceStream: forwards timeout to producer.send", async () => {
		const { producerMock } = makeFakeKafka();
		const stream = await kafkaProduceStream({
			producer: producerMock,
			topic: "t",
			timeout: 5000,
		});
		stream.write("msg");
		stream.end();
		await new Promise((resolve) => stream.on("finish", resolve));
		strictEqual(producerMock.sendArgs[0].timeout, 5000);
	});

	test("kafkaProduceStream: send error attaches failedMessages", async () => {
		const sendErr = new Error("send failed");
		const { producerMock } = makeFakeKafka({
			sendMessages: async () => {
				throw sendErr;
			},
		});
		const stream = await kafkaProduceStream({
			producer: producerMock,
			topic: "t",
		});
		const errPromise = new Promise((resolve) => stream.once("error", resolve));
		stream.write("msg");
		stream.end();
		const err = await errPromise;
		ok(Array.isArray(err.failedMessages), "failedMessages is array");
		strictEqual(err.failedMessages.length, 1);
		strictEqual(err.failedMessages[0].value, "msg");
	});

	test("kafkaProduceStream: destroy with buffered batch attaches failedMessages to error", async () => {
		const { producerMock } = makeFakeKafka();
		const stream = await kafkaProduceStream({
			producer: producerMock,
			topic: "t",
			batchSize: 100,
		});
		const errPromise = new Promise((resolve) => stream.once("error", resolve));
		const w1Done = new Promise((resolve) => {
			stream.write("msg1", resolve);
		});
		const w2Done = new Promise((resolve) => {
			stream.write("msg2", resolve);
		});
		await w1Done;
		await w2Done;
		stream.destroy();
		const err = await errPromise;
		ok(
			err.message.includes("destroyed with a buffered batch"),
			`expected destroy error, got: ${err.message}`,
		);
		ok(Array.isArray(err.failedMessages), "failedMessages is array");
		strictEqual(err.failedMessages.length, 2);
	});

	test("kafkaProduceStream: destroy with existing error attaches failedMessages", async () => {
		const { producerMock } = makeFakeKafka();
		const stream = await kafkaProduceStream({
			producer: producerMock,
			topic: "t",
			batchSize: 100,
		});
		const errPromise = new Promise((resolve) => stream.once("error", resolve));
		const wDone = new Promise((resolve) => {
			stream.write("msg", resolve);
		});
		await wDone;
		const existingErr = new Error("existing error");
		stream.destroy(existingErr);
		const err = await errPromise;
		strictEqual(err.message, "existing error");
		ok(Array.isArray(err.failedMessages));
		strictEqual(err.failedMessages.length, 1);
	});

	test("kafkaProduceStream: destroy with empty batch does not set failedMessages", async () => {
		const { producerMock } = makeFakeKafka();
		const stream = await kafkaProduceStream({
			producer: producerMock,
			topic: "t",
			batchSize: 100,
		});
		const closePromise = new Promise((resolve) => stream.on("close", resolve));
		stream.destroy();
		await closePromise;
	});

	test("kafkaConsumeStream: throws when consumer is missing", async () => {
		await rejects(
			async () => kafkaConsumeStream({ topics: "t" }),
			/kafkaConsumeStream: consumer required/,
		);
	});

	test("kafkaConsumeStream: throws TypeError when consumer is missing", async () => {
		await rejects(async () => kafkaConsumeStream({ topics: "t" }), TypeError);
	});

	test("kafkaConsumeStream: throws when topics is missing", async () => {
		const { consumerMock } = makeFakeKafka();
		await rejects(
			async () => kafkaConsumeStream({ consumer: consumerMock }),
			/kafkaConsumeStream: topics required/,
		);
	});

	test("kafkaConsumeStream: throws TypeError when topics is missing", async () => {
		const { consumerMock } = makeFakeKafka();
		await rejects(
			async () => kafkaConsumeStream({ consumer: consumerMock }),
			TypeError,
		);
	});

	test("kafkaConsumeStream: throws when topics is an empty array", async () => {
		const { consumerMock } = makeFakeKafka();
		await rejects(
			async () => kafkaConsumeStream({ consumer: consumerMock, topics: [] }),
			/kafkaConsumeStream: topics required/,
		);
	});

	test("kafkaConsumeStream: subscribes to a single string topic", async () => {
		const { consumerMock } = makeFakeKafka();
		const stream = await kafkaConsumeStream({
			consumer: consumerMock,
			topics: "my-topic",
		});
		deepStrictEqual(consumerMock.subscribeCalls, [
			{ topic: "my-topic", fromBeginning: false },
		]);
		const closePromise = new Promise((r) => stream.once("close", r));
		stream.destroy();
		await closePromise;
	});

	test("kafkaConsumeStream: subscribes to multiple topics in array", async () => {
		const { consumerMock } = makeFakeKafka();
		const stream = await kafkaConsumeStream({
			consumer: consumerMock,
			topics: ["topic-a", "topic-b"],
		});
		deepStrictEqual(consumerMock.subscribeCalls, [
			{ topic: "topic-a", fromBeginning: false },
			{ topic: "topic-b", fromBeginning: false },
		]);
		const closePromise = new Promise((r) => stream.once("close", r));
		stream.destroy();
		await closePromise;
	});

	test("kafkaConsumeStream: passes fromBeginning=true to subscribe", async () => {
		const { consumerMock } = makeFakeKafka();
		const stream = await kafkaConsumeStream({
			consumer: consumerMock,
			topics: "t",
			fromBeginning: true,
		});
		strictEqual(consumerMock.subscribeCalls[0].fromBeginning, true);
		const closePromise = new Promise((r) => stream.once("close", r));
		stream.destroy();
		await closePromise;
	});

	const makeMessageConsumer = (_messages) => {
		const consumerMock = {
			subscribeCalls: [],
			stopCalled: 0,
			_eachMessage: null,
			_runResolve: null,
			connect: async () => {},
			disconnect: async () => {},
			subscribe: async (opts) => {
				consumerMock.subscribeCalls.push(opts);
			},
			stop: async () => {
				consumerMock.stopCalled++;
				if (consumerMock._runResolve) consumerMock._runResolve();
			},
			run: async ({ eachMessage }) => {
				consumerMock._eachMessage = eachMessage;
				return new Promise((resolve) => {
					consumerMock._runResolve = resolve;
				});
			},
		};
		return consumerMock;
	};

	test("kafkaConsumeStream: delivers messages from eachMessage to stream", async () => {
		const msgs = [
			{
				topic: "t",
				partition: 0,
				message: {
					offset: "0",
					key: "k",
					value: "v",
					headers: {},
					timestamp: "0",
				},
			},
			{
				topic: "t",
				partition: 0,
				message: {
					offset: "1",
					key: "k2",
					value: "v2",
					headers: {},
					timestamp: "1",
				},
			},
		];
		const consumer = makeMessageConsumer(msgs);
		const stream = await kafkaConsumeStream({ consumer, topics: "t" });

		setTimeout(async () => {
			for (const m of msgs) await consumer._eachMessage(m);
			await stream.stop();
		}, 0);

		const received = await streamToArray(stream);
		strictEqual(received.length, 2);
		strictEqual(received[0].value, "v");
		strictEqual(received[1].value, "v2");
	});

	test("kafkaConsumeStream: message object includes topic, partition, offset, key, value, headers, timestamp", async () => {
		const consumer = makeMessageConsumer([]);
		const stream = await kafkaConsumeStream({ consumer, topics: "t" });

		setTimeout(async () => {
			await consumer._eachMessage({
				topic: "my-topic",
				partition: 3,
				message: {
					offset: "42",
					key: "mykey",
					value: "myval",
					headers: { x: "1" },
					timestamp: "9999",
				},
			});
			await stream.stop();
		}, 0);

		const [msg] = await streamToArray(stream);
		strictEqual(msg.topic, "my-topic");
		strictEqual(msg.partition, 3);
		strictEqual(msg.offset, "42");
		strictEqual(msg.key, "mykey");
		strictEqual(msg.value, "myval");
		deepStrictEqual(msg.headers, { x: "1" });
		strictEqual(msg.timestamp, "9999");
	});

	test("kafkaConsumeStream: stop() ends the readable stream", async () => {
		const consumer = makeMessageConsumer([]);
		const stream = await kafkaConsumeStream({ consumer, topics: "t" });

		const endedPromise = new Promise((resolve) => stream.once("end", resolve));
		stream.resume();
		setTimeout(() => stream.stop(), 0);
		await endedPromise;
	});

	test("kafkaConsumeStream: stop() is idempotent (calling twice doesn't call consumer.stop twice)", async () => {
		const consumer = makeMessageConsumer([]);
		const stream = await kafkaConsumeStream({ consumer, topics: "t" });

		await stream.stop();
		await stream.stop();
		strictEqual(consumer.stopCalled, 1, "consumer.stop called only once");
	});

	test("kafkaConsumeStream: eachMessage after stop() rejects (so kafkajs does not commit) and does not push", async () => {
		const consumer = makeMessageConsumer([]);
		const stream = await kafkaConsumeStream({ consumer, topics: "t" });

		const receivedChunks = [];
		stream.on("data", (chunk) => receivedChunks.push(chunk));

		await stream.stop();
		await rejects(
			consumer._eachMessage({
				topic: "t",
				partition: 0,
				message: {
					offset: "0",
					key: null,
					value: "dropped",
					headers: {},
					timestamp: "0",
				},
			}),
			{
				message:
					"kafkaConsumeStream: stream stopped; message at t[0]@0 not delivered",
			},
		);

		await new Promise((resolve) => setTimeout(resolve, 10));
		strictEqual(receivedChunks.length, 0, "no chunks after stop");
	});

	test("kafkaConsumeStream: passes autoCommit to consumer.run", async () => {
		let capturedOpts;
		const consumer = {
			connect: async () => {},
			disconnect: async () => {},
			subscribe: async () => {},
			stop: async () => {
				if (consumer._runResolve) consumer._runResolve();
			},
			run: async (opts) => {
				capturedOpts = opts;
				return new Promise((resolve) => {
					consumer._runResolve = resolve;
				});
			},
		};
		const stream = await kafkaConsumeStream({
			consumer,
			topics: "t",
			autoCommit: true,
		});
		await stream.stop();
		strictEqual(capturedOpts.autoCommit, true);
		stream.destroy();
	});

	test("kafkaConsumeStream: passes partitionsConsumedConcurrently to consumer.run", async () => {
		let capturedOpts;
		const consumer = {
			connect: async () => {},
			disconnect: async () => {},
			subscribe: async () => {},
			stop: async () => {
				if (consumer._runResolve) consumer._runResolve();
			},
			run: async (opts) => {
				capturedOpts = opts;
				return new Promise((resolve) => {
					consumer._runResolve = resolve;
				});
			},
		};
		const stream = await kafkaConsumeStream({
			consumer,
			topics: "t",
			partitionsConsumedConcurrently: 4,
		});
		await stream.stop();
		strictEqual(capturedOpts.partitionsConsumedConcurrently, 4);
		stream.destroy();
	});

	test("kafkaConsumeStream: already-aborted signal calls stop immediately", async () => {
		const consumer = makeMessageConsumer([]);
		const ac = new AbortController();
		ac.abort();

		const stream = await kafkaConsumeStream(
			{ consumer, topics: "t" },
			{ signal: ac.signal },
		);
		const endedPromise = new Promise((resolve) => {
			if (stream.readableEnded) return resolve();
			stream.once("end", resolve);
		});
		stream.resume();
		await endedPromise;
	});

	test("kafkaConsumeStream: abort signal triggers stop", async () => {
		const consumer = makeMessageConsumer([]);
		const ac = new AbortController();

		const stream = await kafkaConsumeStream(
			{ consumer, topics: "t" },
			{ signal: ac.signal },
		);
		const endedPromise = new Promise((resolve) => stream.once("end", resolve));
		stream.resume();
		ac.abort();
		await endedPromise;
		strictEqual(consumer.stopCalled, 1);
	});

	test("kafkaConsumeStream: signal is read from streamOptions", async () => {
		const consumer = makeMessageConsumer([]);
		const ac = new AbortController();
		const stream = await kafkaConsumeStream(
			{ consumer, topics: "t" },
			{ signal: ac.signal },
		);
		const endedPromise = new Promise((resolve) => stream.once("end", resolve));
		stream.resume();
		ac.abort();
		await endedPromise;
		strictEqual(consumer.stopCalled, 1);
	});

	test("kafkaConsumeStream: signal in options is ignored", async () => {
		const consumer = makeMessageConsumer([]);
		const ac = new AbortController();
		ac.abort();
		const stream = await kafkaConsumeStream({
			consumer,
			topics: "t",
			signal: ac.signal,
		});
		strictEqual(consumer.stopCalled, 0);
		await stream.stop();
		stream.destroy();
	});

	test("kafkaConsumeStream: destroy() calls consumer.stop", async () => {
		const consumer = makeMessageConsumer([]);
		const stream = await kafkaConsumeStream({ consumer, topics: "t" });

		const closePromise = new Promise((resolve) =>
			stream.once("close", resolve),
		);
		stream.destroy();
		await closePromise;
		strictEqual(consumer.stopCalled, 1);
	});

	test("kafkaConsumeStream: destroy() removes abort listener (signal guard)", async () => {
		const consumer = makeMessageConsumer([]);
		const ac = new AbortController();

		const stream = await kafkaConsumeStream(
			{ consumer, topics: "t" },
			{ signal: ac.signal },
		);
		const closePromise = new Promise((resolve) =>
			stream.once("close", resolve),
		);
		stream.destroy();
		await closePromise;

		ac.abort();
		await new Promise((resolve) => setTimeout(resolve, 10));
		strictEqual(consumer.stopCalled, 1, "consumer.stop called only once");
	});

	test("kafkaConsumeStream: stop() removes abort listener so abort after stop is a no-op", async () => {
		const consumer = makeMessageConsumer([]);
		const ac = new AbortController();

		const stream = await kafkaConsumeStream(
			{ consumer, topics: "t" },
			{ signal: ac.signal },
		);
		await stream.stop();

		ac.abort();
		await new Promise((resolve) => setTimeout(resolve, 10));
		strictEqual(consumer.stopCalled, 1);
	});

	test("kafkaConsumeStream: respects highWaterMark streamOption", async () => {
		const consumer = makeMessageConsumer([]);
		const stream = await kafkaConsumeStream(
			{ consumer, topics: "t" },
			{ highWaterMark: 5 },
		);
		strictEqual(stream.readableHighWaterMark, 5);
		await stream.stop();
		stream.destroy();
	});

	test("kafkaConsumeStream: backpressure suspends eachMessage until _read is called", async () => {
		const consumer = makeMessageConsumer([]);
		const stream = await kafkaConsumeStream(
			{ consumer, topics: "t" },
			{ highWaterMark: 1 },
		);

		let msg2Sent = false;
		const deliveryDone = (async () => {
			await consumer._eachMessage({
				topic: "t",
				partition: 0,
				message: {
					offset: "0",
					key: null,
					value: "v1",
					headers: {},
					timestamp: "0",
				},
			});
			await consumer._eachMessage({
				topic: "t",
				partition: 0,
				message: {
					offset: "1",
					key: null,
					value: "v2",
					headers: {},
					timestamp: "1",
				},
			});
			msg2Sent = true;
			await stream.stop();
		})();

		const received = await streamToArray(stream);
		await deliveryDone;
		ok(msg2Sent, "second message was eventually delivered");
		strictEqual(received.length, 2);
	});

	test("kafkaConsumeStream: stop() does not deadlock when consumer.stop waits for a parked eachMessage", async () => {
		let inFlight = Promise.resolve();
		const consumer = makeMessageConsumer([]);
		const baseStop = consumer.stop;
		consumer.stop = async () => {
			await inFlight;
			await baseStop();
		};
		const stream = await kafkaConsumeStream(
			{ consumer, topics: "t" },
			{ highWaterMark: 1 },
		);
		inFlight = consumer._eachMessage({
			topic: "t",
			partition: 0,
			message: {
				offset: "0",
				key: null,
				value: "v0",
				headers: {},
				timestamp: "0",
			},
		});
		let parked = true;
		inFlight.then(() => {
			parked = false;
		});
		await new Promise((resolve) => setImmediate(resolve));
		ok(parked, "eachMessage is parked on backpressure");

		let timer;
		const timeout = new Promise((_resolve, reject) => {
			timer = setTimeout(() => reject(new Error("stop() deadlocked")), 1000);
		});
		try {
			await Promise.race([stream.stop(), timeout]);
		} finally {
			clearTimeout(timer);
		}
		strictEqual(parked, false, "parked eachMessage released");
		strictEqual(consumer.stopCalled, 1);
		stream.destroy();
	});

	test("kafkaConsumeStream: destroy() does not deadlock when consumer.stop waits for a parked eachMessage", async () => {
		let inFlight = Promise.resolve();
		const consumer = makeMessageConsumer([]);
		const baseStop = consumer.stop;
		consumer.stop = async () => {
			await inFlight;
			await baseStop();
		};
		const stream = await kafkaConsumeStream(
			{ consumer, topics: "t" },
			{ highWaterMark: 1 },
		);
		inFlight = consumer._eachMessage({
			topic: "t",
			partition: 0,
			message: {
				offset: "0",
				key: null,
				value: "v0",
				headers: {},
				timestamp: "0",
			},
		});
		await new Promise((resolve) => setImmediate(resolve));

		const closed = new Promise((resolve) => stream.once("close", resolve));
		stream.destroy();
		let timer;
		const timeout = new Promise((_resolve, reject) => {
			timer = setTimeout(() => reject(new Error("destroy deadlocked")), 1000);
		});
		try {
			await Promise.race([closed, timeout]);
		} finally {
			clearTimeout(timer);
		}
		await inFlight;
		strictEqual(consumer.stopCalled, 1);
	});

	test("kafkaConsumeStream: abort does not deadlock when consumer.stop waits for a parked eachMessage", async () => {
		let inFlight = Promise.resolve();
		const consumer = makeMessageConsumer([]);
		const baseStop = consumer.stop;
		consumer.stop = async () => {
			await inFlight;
			await baseStop();
		};
		const controller = new AbortController();
		const stream = await kafkaConsumeStream(
			{ consumer, topics: "t" },
			{ highWaterMark: 1, signal: controller.signal },
		);
		inFlight = consumer._eachMessage({
			topic: "t",
			partition: 0,
			message: {
				offset: "0",
				key: null,
				value: "v0",
				headers: {},
				timestamp: "0",
			},
		});
		await new Promise((resolve) => setImmediate(resolve));
		controller.abort();

		let timer;
		const timeout = new Promise((_resolve, reject) => {
			timer = setTimeout(() => reject(new Error("abort deadlocked")), 1000);
		});
		try {
			await Promise.race([inFlight, timeout]);
		} finally {
			clearTimeout(timer);
		}
		await new Promise((resolve) => setImmediate(resolve));
		strictEqual(consumer.stopCalled, 1);
		stream.destroy();
	});

	test("kafkaConsumeStream: stopConsumerOnce is memoized (stop + destroy only calls consumer.stop once)", async () => {
		const consumer = makeMessageConsumer([]);
		const stream = await kafkaConsumeStream({ consumer, topics: "t" });

		const closePromise = new Promise((resolve) =>
			stream.once("close", resolve),
		);
		await stream.stop();
		stream.destroy();
		await closePromise;

		strictEqual(consumer.stopCalled, 1, "consumer.stop called exactly once");
	});

	test("kafkaConnect: producerOptions are passed through to kafka.producer()", async () => {
		let capturedProducerOpts;
		const FakeKafka = function () {
			this.producer = (opts) => {
				capturedProducerOpts = opts;
				return { connect: async () => {}, disconnect: async () => {} };
			};
			this.consumer = () => ({
				connect: async () => {},
				disconnect: async () => {},
			});
		};
		const producerOptions = { transactionalId: "tx-1", idempotent: true };
		await kafkaConnect({ producer: producerOptions, Kafka: FakeKafka });
		deepStrictEqual(capturedProducerOpts, producerOptions);
	});

	test("kafkaConnect: producerOptions ?? {} default passes empty object to kafka.producer()", async () => {
		let capturedProducerOpts;
		const FakeKafka = function () {
			this.producer = (opts) => {
				capturedProducerOpts = opts;
				return { connect: async () => {}, disconnect: async () => {} };
			};
			this.consumer = () => ({
				connect: async () => {},
				disconnect: async () => {},
			});
		};
		await kafkaConnect({ Kafka: FakeKafka });
		deepStrictEqual(capturedProducerOpts, {});
	});

	test("kafkaConnect: consumer connect error with producer===false does not throw in teardown", async () => {
		const FakeKafka = function () {
			this.producer = () => ({
				connect: async () => {},
				disconnect: async () => {},
			});
			this.consumer = () => ({
				connect: async () => {
					throw new Error("consumer connect failed");
				},
				disconnect: async () => {},
			});
		};
		await rejects(
			async () =>
				kafkaConnect({ producer: false, groupId: "g1", Kafka: FakeKafka }),
			/consumer connect failed/,
			"should rethrow consumer connect error",
		);
	});

	test("kafkaProduceStream: error message includes type 'number' for numeric chunk", async () => {
		const { producerMock } = makeFakeKafka();
		const stream = await kafkaProduceStream({
			producer: producerMock,
			topic: "t",
		});
		const errPromise = new Promise((resolve) => stream.once("error", resolve));
		stream.write(123);
		const err = await errPromise;
		ok(
			err.message.includes("number"),
			"error message includes 'number' (not empty string)",
		);
		ok(err.message.length > 10, "error message is not empty");
	});

	test("kafkaProduceStream: normalizeMessage: object without value property throws TypeError with type 'object'", async () => {
		const { producerMock } = makeFakeKafka();
		const stream = await kafkaProduceStream({
			producer: producerMock,
			topic: "t",
		});
		const errPromise = new Promise((resolve) => stream.once("error", resolve));
		stream.write({ noValue: "here" });
		const err = await errPromise;
		ok(err instanceof TypeError, "should be TypeError");
		ok(err.message.includes("object"), "message includes type 'object'");
	});

	test("kafkaProduceStream: after batch flush, next messages go into a fresh batch", async () => {
		const { producerMock } = makeFakeKafka();
		const stream = await kafkaProduceStream({
			producer: producerMock,
			topic: "t",
			batchSize: 2,
		});
		stream.write("a");
		stream.write("b");
		stream.write("c");
		stream.write("d");
		stream.end();
		await new Promise((resolve) => stream.on("finish", resolve));
		strictEqual(producerMock.sendArgs.length, 2, "two separate flushes");
		strictEqual(producerMock.sendArgs[0].messages[0].value, "a");
		strictEqual(producerMock.sendArgs[0].messages[1].value, "b");
		strictEqual(producerMock.sendArgs[1].messages.length, 2);
		strictEqual(producerMock.sendArgs[1].messages[0].value, "c");
		strictEqual(producerMock.sendArgs[1].messages[1].value, "d");
	});

	test("kafkaConsumeStream: autoCommit defaults to false and is passed to consumer.run", async () => {
		let capturedOpts;
		const consumer = {
			connect: async () => {},
			disconnect: async () => {},
			subscribe: async () => {},
			stop: async () => {
				if (consumer._runResolve) consumer._runResolve();
			},
			run: async (opts) => {
				capturedOpts = opts;
				return new Promise((resolve) => {
					consumer._runResolve = resolve;
				});
			},
		};
		const stream = await kafkaConsumeStream({ consumer, topics: "t" });
		strictEqual(capturedOpts.autoCommit, false);
		const closePromise = new Promise((r) => stream.once("close", r));
		stream.destroy();
		await closePromise;
	});

	test("kafkaConsumeStream: destroy() sets stopped=true so eachMessage after destroy rejects", async () => {
		const consumer = makeMessageConsumer([]);
		const stream = await kafkaConsumeStream({ consumer, topics: "t" });

		const closePromise = new Promise((resolve) =>
			stream.once("close", resolve),
		);
		stream.destroy();
		await closePromise;

		await rejects(
			consumer._eachMessage({
				topic: "late",
				partition: 3,
				message: {
					offset: "99",
					key: null,
					value: "late",
					headers: {},
					timestamp: "0",
				},
			}),
			{
				message:
					"kafkaConsumeStream: stream stopped; message at late[3]@99 not delivered",
			},
		);
	});

	test("kafkaConsumeStream: abort event listener is on 'abort' event (not '')", async () => {
		const consumer = makeMessageConsumer([]);
		const listeners = [];
		const mockSignal = {
			aborted: false,
			addEventListener: (event, fn) => {
				listeners.push({ event, fn });
			},
			removeEventListener: (event, fn) => {
				const idx = listeners.findIndex(
					(l) => l.event === event && l.fn === fn,
				);
				if (idx !== -1) listeners.splice(idx, 1);
			},
		};

		const stream = await kafkaConsumeStream(
			{ consumer, topics: "t" },
			{ signal: mockSignal },
		);
		strictEqual(listeners.length, 1, "one listener added");
		strictEqual(listeners[0].event, "abort", "listener is on 'abort' event");

		await stream.stop();
		strictEqual(listeners.length, 0, "listener removed after stop");
		stream.destroy();
	});

	test("kafkaConsumeStream: destroy() removes 'abort' listener (correct event name)", async () => {
		const consumer = makeMessageConsumer([]);
		const listeners = [];
		const mockSignal = {
			aborted: false,
			addEventListener: (event, fn) => {
				listeners.push({ event, fn });
			},
			removeEventListener: (event, fn) => {
				const idx = listeners.findIndex(
					(l) => l.event === event && l.fn === fn,
				);
				if (idx !== -1) listeners.splice(idx, 1);
			},
		};

		const stream = await kafkaConsumeStream(
			{ consumer, topics: "t" },
			{ signal: mockSignal },
		);
		strictEqual(listeners.length, 1);

		const closePromise = new Promise((resolve) =>
			stream.once("close", resolve),
		);
		stream.destroy();
		await closePromise;

		strictEqual(listeners.length, 0, "listener removed on destroy");
	});

	test("kafkaConsumeStream: stop() when not stopped pushes null to end stream", async () => {
		const consumer = makeMessageConsumer([]);
		const stream = await kafkaConsumeStream({ consumer, topics: "t" });
		stream.resume();

		const endPromise = new Promise((resolve) => stream.once("end", resolve));
		await stream.stop();
		await endPromise;
		await stream.stop();
		strictEqual(consumer.stopCalled, 1);
	});

	test("kafkaConsumeStream: wantsMore=true means eachMessage does NOT call waitForRead", async () => {
		const consumer = makeMessageConsumer([]);
		const stream = await kafkaConsumeStream(
			{ consumer, topics: "t" },
			{ highWaterMark: 100 },
		);

		let delivered = false;
		const delivery = (async () => {
			await consumer._eachMessage({
				topic: "t",
				partition: 0,
				message: {
					offset: "0",
					key: null,
					value: "v",
					headers: {},
					timestamp: "0",
				},
			});
			delivered = true;
		})();

		await new Promise((resolve) => setTimeout(resolve, 5));
		ok(delivered, "eachMessage completed without waiting for read");

		await stream.stop();
		await delivery;
		stream.destroy();
	});

	test("kafkaConsumeStream: non-aborted signal does NOT call stop immediately", async () => {
		const consumer = makeMessageConsumer([]);
		const ac = new AbortController();

		const stream = await kafkaConsumeStream(
			{ consumer, topics: "t" },
			{ signal: ac.signal },
		);
		strictEqual(
			consumer.stopCalled,
			0,
			"stop not called for non-aborted signal",
		);

		await stream.stop();
		stream.destroy();
	});

	test("kafkaConsumeStream: non-aborted signal registers abort listener (not stop immediately)", async () => {
		const consumer = makeMessageConsumer([]);
		const listeners = [];
		const mockSignal = {
			aborted: false,
			addEventListener: (event, fn) => listeners.push({ event, fn }),
			removeEventListener: (_event, fn) => {
				const idx = listeners.findIndex((l) => l.fn === fn);
				if (idx !== -1) listeners.splice(idx, 1);
			},
		};

		const stream = await kafkaConsumeStream(
			{ consumer, topics: "t" },
			{ signal: mockSignal },
		);
		strictEqual(consumer.stopCalled, 0);
		strictEqual(listeners.length, 1);
		strictEqual(listeners[0].event, "abort");
		await stream.stop();
		stream.destroy();
	});

	test("kafkaConnect: uses dynamic import when Kafka is not provided (no connect)", async () => {
		const result = await kafkaConnect({ producer: false });
		strictEqual(result.producer, null, "producer is null");
		strictEqual(result.consumer, null, "consumer is null");
		ok(result.kafka, "kafka instance created via dynamic import");
		ok(typeof result.disconnect === "function");
		await result.disconnect();
	});

	test("kafkaConnect: producer.disconnect() throwing in catch block is swallowed", async () => {
		const { Kafka } = makeFakeKafka({
			connectProducer: async () => {
				throw new Error("producer connect failed");
			},
			disconnectProducer: async () => {
				throw new Error("producer disconnect also failed");
			},
		});
		await rejects(
			async () => kafkaConnect({ Kafka }),
			/producer connect failed/,
		);
	});

	test("kafkaConnect: consumer.disconnect() throwing in catch block is swallowed", async () => {
		const { Kafka } = makeFakeKafka({
			connectConsumer: async () => {
				throw new Error("consumer connect failed");
			},
			disconnectConsumer: async () => {
				throw new Error("consumer disconnect also failed");
			},
		});
		await rejects(
			async () => kafkaConnect({ groupId: "g1", Kafka }),
			/consumer connect failed/,
		);
	});

	test("kafkaConsumeStream: consumer.stop() throwing is swallowed (stopConsumerOnce catch)", async () => {
		const consumer = {
			connect: async () => {},
			disconnect: async () => {},
			subscribe: async () => {},
			stopCalled: 0,
			_runResolve: null,
			stop: async () => {
				consumer.stopCalled++;
				if (consumer._runResolve) consumer._runResolve();
				throw new Error("consumer.stop exploded");
			},
			run: async () =>
				new Promise((resolve) => {
					consumer._runResolve = resolve;
				}),
		};

		const stream = await kafkaConsumeStream({ consumer, topics: "t" });
		stream.resume();
		await stream.stop();
		strictEqual(consumer.stopCalled, 1);
	});

	const microtick = () => new Promise((resolve) => setImmediate(resolve));

	test("kafkaConsumeStream: eachMessage SUSPENDS while buffer is full, resumes on read", async () => {
		const consumer = makeMessageConsumer([]);
		const stream = await kafkaConsumeStream(
			{ consumer, topics: "t" },
			{ highWaterMark: 1 },
		);

		let settled = false;
		const p = consumer
			._eachMessage({
				topic: "t",
				partition: 0,
				message: {
					offset: "0",
					key: null,
					value: "v1",
					headers: {},
					timestamp: "0",
				},
			})
			.then(() => {
				settled = true;
			});

		await microtick();
		strictEqual(
			settled,
			false,
			"eachMessage should be suspended while buffer full",
		);

		const item = stream.read();
		strictEqual(item.value, "v1");
		await p;
		strictEqual(settled, true, "eachMessage resumes after _read");

		await stream.stop();
		stream.destroy();
	});

	test("kafkaConsumeStream: eachMessage does NOT suspend while buffer has room", async () => {
		const consumer = makeMessageConsumer([]);
		const stream = await kafkaConsumeStream(
			{ consumer, topics: "t" },
			{ highWaterMark: 100 },
		);

		let settled = false;
		const p = consumer
			._eachMessage({
				topic: "t",
				partition: 0,
				message: {
					offset: "0",
					key: null,
					value: "v1",
					headers: {},
					timestamp: "0",
				},
			})
			.then(() => {
				settled = true;
			});

		for (let i = 0; i < 20; i++) await Promise.resolve();
		strictEqual(
			settled,
			true,
			"eachMessage should complete via microtasks (no waitForRead) when buffer has room",
		);
		await p;

		await stream.stop();
		stream.destroy();
	});

	test("kafkaConsumeStream: second stop() does not push null again / emit an error", async () => {
		const consumer = makeMessageConsumer([]);
		const stream = await kafkaConsumeStream({ consumer, topics: "t" });
		stream.resume();

		let errored = null;
		stream.on("error", (e) => {
			errored = e;
		});

		const endPromise = new Promise((resolve) => stream.once("end", resolve));
		await stream.stop();
		await endPromise;
		await stream.stop();
		await new Promise((resolve) => setTimeout(resolve, 10));
		strictEqual(errored, null, "no error from a redundant second stop()");
		strictEqual(consumer.stopCalled, 1);
	});

	test("kafkaConsumeStream: runPromise rejection is swallowed by stop()", async () => {
		const consumer = {
			connect: async () => {},
			disconnect: async () => {},
			subscribe: async () => {},
			stopCalled: 0,
			_runResolve: null,
			_runReject: null,
			stop: async () => {
				consumer.stopCalled++;
				if (consumer._runReject) consumer._runReject(new Error("run rejected"));
			},
			run: async () =>
				new Promise((_resolve, reject) => {
					consumer._runReject = reject;
				}),
		};

		const stream = await kafkaConsumeStream({ consumer, topics: "t" });
		stream.resume();
		await stream.stop();
		strictEqual(consumer.stopCalled, 1);
	});

	test("kafkaConsumeStream: runPromise rejection before stop() errors the stream", async () => {
		const consumer = {
			connect: async () => {},
			disconnect: async () => {},
			subscribe: async () => {},
			stop: async () => {},
			run: async () => {
				throw new Error("run rejected");
			},
		};

		const stream = await kafkaConsumeStream({ consumer, topics: "t" });
		const err = await new Promise((resolve) => stream.once("error", resolve));
		strictEqual(err.message, "run rejected");
		strictEqual(stream.destroyed, true);
	});

	const makeCommitConsumer = ({ commitOffsets = async () => {} } = {}) => {
		const consumer = {
			commits: [],
			listeners: new Map(),
			removed: [],
			events: { STOP: "consumer.stop", GROUP_JOIN: "consumer.group_join" },
			on: (eventName, listener) => {
				consumer.listeners.set(eventName, listener);
				return () => {
					consumer.removed.push(eventName);
					consumer.listeners.delete(eventName);
				};
			},
			commitOffsets: async (offsets) => {
				consumer.commits.push(offsets);
				return commitOffsets(offsets);
			},
		};
		return consumer;
	};

	const msg = (topic, partition, offset) => ({
		topic,
		partition,
		offset,
		key: null,
		value: "v",
		headers: {},
		timestamp: "0",
	});

	test("kafkaCommitStream: throws TypeError when consumer is missing", async () => {
		await rejects(() => kafkaCommitStream(), {
			name: "TypeError",
			message: "kafkaCommitStream: consumer required",
		});
		await rejects(() => kafkaCommitStream({}), {
			name: "TypeError",
			message: "kafkaCommitStream: consumer required",
		});
	});

	test("kafkaCommitStream: throws TypeError when batchSize < 1", async () => {
		const consumer = makeCommitConsumer();
		await rejects(() => kafkaCommitStream({ consumer, batchSize: 0 }), {
			name: "TypeError",
			message: "kafkaCommitStream: batchSize must be >= 1",
		});
		await rejects(
			() => kafkaCommitStream({ consumer, batchSize: Number.NaN }),
			{
				name: "TypeError",
				message: "kafkaCommitStream: batchSize must be >= 1",
			},
		);
	});

	test("kafkaCommitStream: commits highest offset + 1 per topic-partition in final()", async () => {
		const consumer = makeCommitConsumer();
		const stream = await kafkaCommitStream({ consumer });
		await pipeline([
			createReadableStream([
				msg("a", 0, "4"),
				msg("a", 1, "7"),
				msg("a", 0, "5"),
				msg("b", 0, "0"),
				msg("a", 1, "8"),
			]),
			stream,
		]);
		deepStrictEqual(consumer.commits, [
			[
				{ topic: "a", partition: 0, offset: "6" },
				{ topic: "a", partition: 1, offset: "9" },
				{ topic: "b", partition: 0, offset: "1" },
			],
		]);
	});

	test("kafkaCommitStream: commits every batchSize messages and the rest in final()", async () => {
		const consumer = makeCommitConsumer();
		const stream = await kafkaCommitStream({ consumer, batchSize: 2 });
		await pipeline([
			createReadableStream([
				msg("t", 0, "0"),
				msg("t", 0, "1"),
				msg("t", 0, "2"),
				msg("t", 0, "3"),
				msg("t", 0, "4"),
			]),
			stream,
		]);
		deepStrictEqual(consumer.commits, [
			[{ topic: "t", partition: 0, offset: "2" }],
			[{ topic: "t", partition: 0, offset: "4" }],
			[{ topic: "t", partition: 0, offset: "5" }],
		]);
	});

	test("kafkaCommitStream: batchSize defaults to 100", async () => {
		const consumer = makeCommitConsumer();
		const stream = await kafkaCommitStream({ consumer });
		const chunks = [];
		for (let i = 0; i < 101; i++) chunks.push(msg("t", 0, String(i)));
		await pipeline([createReadableStream(chunks), stream]);
		deepStrictEqual(consumer.commits, [
			[{ topic: "t", partition: 0, offset: "100" }],
			[{ topic: "t", partition: 0, offset: "101" }],
		]);
	});

	test("kafkaCommitStream: never commits an offset lower than one already committed", async () => {
		const consumer = makeCommitConsumer();
		const stream = await kafkaCommitStream({ consumer, batchSize: 1 });
		await pipeline([
			createReadableStream([
				msg("t", 0, "10"),
				msg("t", 0, "5"),
				msg("t", 0, "10"),
			]),
			stream,
		]);
		deepStrictEqual(consumer.commits, [
			[{ topic: "t", partition: 0, offset: "11" }],
		]);
	});

	test("kafkaCommitStream: computes offset + 1 beyond Number precision", async () => {
		const consumer = makeCommitConsumer();
		const stream = await kafkaCommitStream({ consumer });
		await pipeline([
			createReadableStream([msg("t", 0, "9007199254740993")]),
			stream,
		]);
		deepStrictEqual(consumer.commits, [
			[{ topic: "t", partition: 0, offset: "9007199254740994" }],
		]);
	});

	test("kafkaCommitStream: empty stream commits nothing", async () => {
		const consumer = makeCommitConsumer();
		const stream = await kafkaCommitStream({ consumer });
		await pipeline([createReadableStream([]), stream]);
		deepStrictEqual(consumer.commits, []);
	});

	test("kafkaCommitStream: listens for GROUP_JOIN only and removes it when the stream closes", async () => {
		const consumer = makeCommitConsumer();
		const stream = await kafkaCommitStream({ consumer });
		strictEqual(
			typeof consumer.listeners.get("consumer.group_join"),
			"function",
		);
		strictEqual(consumer.listeners.has("consumer.stop"), false);
		await pipeline([createReadableStream([]), stream]);
		await new Promise((resolve) => setImmediate(resolve));
		deepStrictEqual(consumer.removed, ["consumer.group_join"]);
	});

	test("kafkaCommitStream: destroy() drops pending offsets and removes the GROUP_JOIN listener", async () => {
		const consumer = makeCommitConsumer();
		const stream = await kafkaCommitStream({ consumer });
		await new Promise((resolve) => stream.write(msg("t", 0, "0"), resolve));
		const closed = new Promise((resolve) => stream.once("close", resolve));
		stream.destroy();
		await closed;
		deepStrictEqual(consumer.commits, []);
		deepStrictEqual(consumer.removed, ["consumer.group_join"]);
	});

	test("kafkaCommitStream: commitOffsets rejection errors the stream", async () => {
		const consumer = makeCommitConsumer({
			commitOffsets: async () => {
				throw new Error("commit failed");
			},
		});
		const stream = await kafkaCommitStream({ consumer });
		await rejects(
			() => pipeline([createReadableStream([msg("t", 0, "0")]), stream]),
			{ message: "commit failed" },
		);
	});

	test("kafkaCommitStream: commits what kafkaConsumeStream emitted", async () => {
		const commitConsumer = makeCommitConsumer();
		const consumer = makeMessageConsumer([]);
		consumer.commitOffsets = commitConsumer.commitOffsets;
		consumer.events = commitConsumer.events;
		consumer.on = commitConsumer.on;
		const source = await kafkaConsumeStream({ consumer, topics: "t" });
		const sink = await kafkaCommitStream({ consumer, batchSize: 1 });
		setTimeout(async () => {
			await consumer._eachMessage({
				topic: "t",
				partition: 3,
				message: { offset: "41", key: null, value: "v", headers: {} },
			});
			await source.stop();
		}, 0);
		await pipeline([source, sink]);
		deepStrictEqual(commitConsumer.commits, [
			[{ topic: "t", partition: 3, offset: "42" }],
		]);
	});

	const NOT_INITIALIZED =
		"Consumer group was not initialized, consumer#run must be called first";
	const kafkajsError = (message) =>
		Object.assign(new Error(message), { name: "KafkaJSNonRetriableError" });

	const makeKafkajsConsumer = () => {
		const emitter = new EventEmitter();
		const consumer = {
			log: [],
			group: false,
			eachMessage: null,
			events: { STOP: "consumer.stop", GROUP_JOIN: "consumer.group_join" },
			on: (eventName, listener) => {
				emitter.on(eventName, listener);
				return () => emitter.off(eventName, listener);
			},
			subscribe: async () => {},
			run: async ({ eachMessage }) => {
				consumer.eachMessage = eachMessage;
				consumer.join();
			},
			join: () => {
				consumer.group = true;
				emitter.emit("consumer.group_join");
			},
			leave: () => {
				consumer.group = false;
				emitter.emit("consumer.stop");
			},
			stop: async () => {
				consumer.log.push("stop");
				consumer.leave();
			},
			commitOffsets: async (offsets) => {
				if (!consumer.group) {
					consumer.log.push(["refused", offsets]);
					throw kafkajsError(NOT_INITIALIZED);
				}
				consumer.log.push(["commit", offsets]);
			},
		};
		return consumer;
	};
	const deliver = (consumer, offset, partition = 0) =>
		consumer.eachMessage({
			topic: "t",
			partition,
			message: { offset, key: null, value: "v", headers: {}, timestamp: "0" },
		});
	const write = (stream, chunk) =>
		new Promise((resolve, reject) =>
			stream.write(chunk, (err) => (err ? reject(err) : resolve())),
		);
	const commits = (consumer) =>
		consumer.log.filter((entry) => entry[0] === "commit").map(([, o]) => o);

	test("kafkaCommitStream: keeps committing after a kafkajs crash-restart (STOP then GROUP_JOIN)", async () => {
		const consumer = makeKafkajsConsumer();
		await consumer.run({});
		const stream = await kafkaCommitStream({ consumer, batchSize: 1 });
		await write(stream, msg("t", 0, "0"));
		consumer.leave();
		await write(stream, msg("t", 0, "1"));
		consumer.join();
		await write(stream, msg("t", 0, "2"));
		await write(stream, msg("t", 0, "3"));
		await new Promise((resolve) => stream.end(resolve));
		deepStrictEqual(consumer.log, [
			["commit", [{ topic: "t", partition: 0, offset: "1" }]],
			["refused", [{ topic: "t", partition: 0, offset: "2" }]],
			["commit", [{ topic: "t", partition: 0, offset: "3" }]],
			["commit", [{ topic: "t", partition: 0, offset: "4" }]],
		]);
	});

	test("kafkaCommitStream: keeps refused offsets pending for the next commit", async () => {
		const consumer = makeKafkajsConsumer();
		const stream = await kafkaCommitStream({ consumer, batchSize: 2 });
		await write(stream, msg("t", 0, "0"));
		await write(stream, msg("t", 1, "0"));
		consumer.group = true;
		await write(stream, msg("t", 0, "1"));
		await new Promise((resolve) => stream.end(resolve));
		deepStrictEqual(consumer.log, [
			[
				"refused",
				[
					{ topic: "t", partition: 0, offset: "1" },
					{ topic: "t", partition: 1, offset: "1" },
				],
			],
			[
				"commit",
				[
					{ topic: "t", partition: 0, offset: "2" },
					{ topic: "t", partition: 1, offset: "1" },
				],
			],
		]);
	});

	test("kafkaCommitStream: GROUP_JOIN resets high-water marks so a replay commits", async () => {
		const consumer = makeKafkajsConsumer();
		await consumer.run({});
		const stream = await kafkaCommitStream({ consumer, batchSize: 1 });
		await write(stream, msg("t", 0, "10"));
		consumer.join();
		await write(stream, msg("t", 0, "5"));
		await write(stream, msg("t", 0, "6"));
		await new Promise((resolve) => stream.end(resolve));
		deepStrictEqual(commits(consumer), [
			[{ topic: "t", partition: 0, offset: "11" }],
			[{ topic: "t", partition: 0, offset: "6" }],
			[{ topic: "t", partition: 0, offset: "7" }],
		]);
	});

	test("kafkaCommitStream: GROUP_JOIN drops offsets pending from the previous generation", async () => {
		const consumer = makeKafkajsConsumer();
		await consumer.run({});
		const stream = await kafkaCommitStream({ consumer });
		await write(stream, msg("t", 0, "0"));
		await write(stream, msg("t", 1, "0"));
		consumer.join();
		await write(stream, msg("t", 1, "1"));
		await new Promise((resolve) => stream.end(resolve));
		deepStrictEqual(commits(consumer), [
			[{ topic: "t", partition: 1, offset: "2" }],
		]);
	});

	test("kafkaCommitStream: other KafkaJSNonRetriableError rejections error the stream", async () => {
		const consumer = makeCommitConsumer({
			commitOffsets: async () => {
				throw kafkajsError("Invalid topic t");
			},
		});
		const stream = await kafkaCommitStream({ consumer });
		await rejects(
			() => pipeline([createReadableStream([msg("t", 0, "0")]), stream]),
			{ name: "KafkaJSNonRetriableError", message: "Invalid topic t" },
		);
	});

	test("kafkaCommitStream: a non-kafkajs error with the not-initialized message errors the stream", async () => {
		const consumer = makeCommitConsumer({
			commitOffsets: async () => {
				throw new Error(NOT_INITIALIZED);
			},
		});
		const stream = await kafkaCommitStream({ consumer });
		await rejects(
			() => pipeline([createReadableStream([msg("t", 0, "0")]), stream]),
			{ name: "Error", message: NOT_INITIALIZED },
		);
	});

	test("kafkaConsumeStream: graceful stop() commits the tail before consumer.stop()", async () => {
		const consumer = makeKafkajsConsumer();
		const source = await kafkaConsumeStream({ consumer, topics: "t" });
		const sink = await kafkaCommitStream({ consumer, batchSize: 100 });
		const done = pipeline([source, sink]);
		for (let i = 0; i < 50; i++) await deliver(consumer, String(i));
		await source.stop();
		await done;
		deepStrictEqual(consumer.log, [
			["commit", [{ topic: "t", partition: 0, offset: "50" }]],
			"stop",
		]);
	});

	// An ended-then-destroyed readable that closes before 'end' is seen as
	// finished by pipe, which then never ends the destination: the pipeline
	// must still settle rather than hang.
	test("kafkaConsumeStream: stop() then destroy() before flowing still settles the pipeline", async () => {
		const consumer = makeKafkajsConsumer();
		const source = await kafkaConsumeStream({ consumer, topics: "t" });
		const sink = await kafkaCommitStream({ consumer });
		const stopping = source.stop();
		source.destroy();
		const settled = await Promise.race([
			pipeline([source, sink]).then(
				() => "resolved",
				() => "rejected",
			),
			new Promise((resolve) => setTimeout(resolve, 1000, "hung")),
		]);
		await stopping;
		strictEqual(settled === "hung", false, "pipeline hung");
		strictEqual(sink.destroyed, true);
	});

	test("kafkaConsumeStream: stop() waits for every open commit stream on the consumer", async () => {
		const consumer = makeKafkajsConsumer();
		const source = await kafkaConsumeStream({ consumer, topics: "t" });
		const sink = await kafkaCommitStream({ consumer });
		const other = await kafkaCommitStream({ consumer });
		const done = pipeline([source, sink]);
		await deliver(consumer, "0");
		let stopped = false;
		const stopping = source.stop().then(() => {
			stopped = true;
		});
		await done;
		await new Promise((resolve) => setImmediate(resolve));
		strictEqual(stopped, false, "still waiting for the other commit stream");
		deepStrictEqual(consumer.log, [
			["commit", [{ topic: "t", partition: 0, offset: "1" }]],
		]);
		other.destroy();
		await stopping;
		deepStrictEqual(consumer.log.at(-1), "stop");
	});

	test("kafkaConsumeStream: stop() does not wait for commit streams of another consumer", async () => {
		const consumer = makeKafkajsConsumer();
		const source = await kafkaConsumeStream({ consumer, topics: "t" });
		const other = await kafkaCommitStream({ consumer: makeKafkajsConsumer() });
		source.resume();
		await source.stop();
		deepStrictEqual(consumer.log, ["stop"]);
		other.destroy();
	});

	test("kafkaConsumeStream: a message arriving while stop() drains is held, then rejected after consumer.stop()", async () => {
		const consumer = makeKafkajsConsumer();
		const source = await kafkaConsumeStream({ consumer, topics: "t" });
		const sink = await kafkaCommitStream({ consumer });
		const received = [];
		source.on("data", (chunk) => received.push(chunk.offset));
		await deliver(consumer, "0");
		const stopping = source.stop();
		let settled = false;
		const late = deliver(consumer, "1").finally(() => {
			settled = true;
		});
		late.catch(() => {});
		await new Promise((resolve) => setImmediate(resolve));
		strictEqual(settled, false, "held while draining");
		sink.end();
		await stopping;
		await rejects(late, {
			message:
				"kafkaConsumeStream: stream stopped; message at t[0]@1 not delivered",
		});
		deepStrictEqual(received, ["0"]);
		deepStrictEqual(consumer.log, ["stop"]);
	});

	test("kafkaConsumeStream: abort leaves the group first, so pending offsets are not committed", async () => {
		const consumer = makeKafkajsConsumer();
		const controller = new AbortController();
		const source = await kafkaConsumeStream(
			{ consumer, topics: "t" },
			{ signal: controller.signal },
		);
		const sink = await kafkaCommitStream({ consumer });
		const done = pipeline([source, sink]);
		await deliver(consumer, "0");
		await deliver(consumer, "1");
		controller.abort();
		await done;
		deepStrictEqual(consumer.log, [
			"stop",
			["refused", [{ topic: "t", partition: 0, offset: "2" }]],
		]);
	});

	test("kafkaConsumeStream: destroy() mid-drain stops the consumer without waiting for commit streams", async () => {
		const consumer = makeKafkajsConsumer();
		const source = await kafkaConsumeStream({ consumer, topics: "t" });
		const sink = await kafkaCommitStream({ consumer });
		await deliver(consumer, "0");
		const stopping = source.stop();
		const closed = new Promise((resolve) => source.once("close", resolve));
		source.destroy();
		await closed;
		deepStrictEqual(consumer.log, ["stop"]);
		sink.destroy();
		await stopping;
	});

	test("kafkaConsumeStream: runPromise rejection is swallowed by abort", async () => {
		const consumer = {
			subscribe: async () => {},
			stopCalled: 0,
			stop: async () => {
				consumer.stopCalled++;
				consumer._runReject(new Error("run rejected"));
			},
			run: async () =>
				new Promise((_resolve, reject) => {
					consumer._runReject = reject;
				}),
		};
		const controller = new AbortController();
		const stream = await kafkaConsumeStream(
			{ consumer, topics: "t" },
			{ signal: controller.signal },
		);
		let errored = null;
		stream.on("error", (e) => {
			errored = e;
		});
		const ended = new Promise((resolve) => stream.once("end", resolve));
		stream.resume();
		controller.abort();
		await ended;
		await new Promise((resolve) => setImmediate(resolve));
		strictEqual(errored, null);
		strictEqual(consumer.stopCalled, 1);
	});

	test("kafkaConsumeStream: stop() does not wait for an already-closed commit stream", async () => {
		const consumer = makeKafkajsConsumer();
		const earlier = await kafkaCommitStream({ consumer });
		const closed = new Promise((resolve) => earlier.once("close", resolve));
		earlier.destroy();
		await closed;
		const source = await kafkaConsumeStream({ consumer, topics: "t" });
		source.resume();
		await source.stop();
		deepStrictEqual(consumer.log, ["stop"]);
	});
});
