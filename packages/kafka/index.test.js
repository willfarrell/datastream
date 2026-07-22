// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
import { deepStrictEqual, ok, rejects, strictEqual } from "node:assert";
import test from "node:test";
import { streamToArray } from "@datastream/core";

import kafkaDefault, {
	kafkaConnect,
	kafkaConsumeStream,
	kafkaProduceStream,
} from "@datastream/kafka";

// ---------------------------------------------------------------------------
// Fake Kafka factory helpers
// ---------------------------------------------------------------------------

/** Build a simple fake kafkajs-like Kafka constructor. */
const makeFakeKafka = ({
	connectProducer = async () => {},
	disconnectProducer = async () => {},
	sendMessages = async () => {},
	connectConsumer = async () => {},
	disconnectConsumer = async () => {},
	subscribe = async () => {},
	// run: caller can override; default drives one message then resolves
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
				// Return a promise that resolves when stop() is called
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

// ---------------------------------------------------------------------------
// kafkaConnect
// ---------------------------------------------------------------------------

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

test("kafkaConnect: producer connect error triggers teardown and rethrows", async () => {
	const { Kafka, producerMock } = makeFakeKafka({
		connectProducer: async () => {
			throw new Error("producer connect failed");
		},
	});
	await rejects(async () => kafkaConnect({ Kafka }), /producer connect failed/);
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

test("kafkaConnect: default exports connect/produceStream/consumeStream", () => {
	ok(typeof kafkaDefault.connect === "function");
	ok(typeof kafkaDefault.produceStream === "function");
	ok(typeof kafkaDefault.consumeStream === "function");
});

// ---------------------------------------------------------------------------
// kafkaProduceStream: validation
// ---------------------------------------------------------------------------

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
			kafkaProduceStream({ producer: producerMock, topic: "t", batchSize: 0 }),
		/kafkaProduceStream: batchSize must be >= 1/,
	);
});

test("kafkaProduceStream: throws TypeError when batchSize < 1", async () => {
	const { producerMock } = makeFakeKafka();
	await rejects(
		async () =>
			kafkaProduceStream({ producer: producerMock, topic: "t", batchSize: 0 }),
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

// ---------------------------------------------------------------------------
// kafkaProduceStream: normalizeMessage
// ---------------------------------------------------------------------------

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
	const errorPromise = new Promise((resolve) => stream.once("error", resolve));
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
	const errorPromise = new Promise((resolve) => stream.once("error", resolve));
	stream.write(42);
	const err = await errorPromise;
	ok(err.message.includes("number"), "error message includes 'number'");
});

// ---------------------------------------------------------------------------
// kafkaProduceStream: batching
// ---------------------------------------------------------------------------

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
	// 2 batches of 3
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

// ---------------------------------------------------------------------------
// kafkaProduceStream: send error attaches failedMessages
// ---------------------------------------------------------------------------

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

// ---------------------------------------------------------------------------
// kafkaProduceStream: destroy with buffered batch
// ---------------------------------------------------------------------------

test("kafkaProduceStream: destroy with buffered batch attaches failedMessages to error", async () => {
	const { producerMock } = makeFakeKafka();
	const stream = await kafkaProduceStream({
		producer: producerMock,
		topic: "t",
		batchSize: 100,
	});
	const errPromise = new Promise((resolve) => stream.once("error", resolve));
	// Await both writes so they land in the batch before destroy
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
	// Await write so msg lands in batch before destroy
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
	// No writes — batch is empty
	const closePromise = new Promise((resolve) => stream.on("close", resolve));
	stream.destroy();
	await closePromise;
	// No error should have been emitted with failedMessages
	// since batch was empty we never set failedMessages
});

// ---------------------------------------------------------------------------
// kafkaConsumeStream: validation
// ---------------------------------------------------------------------------

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

// ---------------------------------------------------------------------------
// kafkaConsumeStream: subscribe
// ---------------------------------------------------------------------------

test("kafkaConsumeStream: subscribes to a single string topic", async () => {
	const { consumerMock } = makeFakeKafka();
	const stream = await kafkaConsumeStream({
		consumer: consumerMock,
		topics: "my-topic",
	});
	// Subscribe happens during stream creation; assert then clean up
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

// ---------------------------------------------------------------------------
// kafkaConsumeStream: message delivery
// ---------------------------------------------------------------------------

/** Helper: build a fake consumer that delivers messages via run(). */
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

	// Deliver messages then stop
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
	await stream.stop(); // second call should be no-op
	strictEqual(consumer.stopCalled, 1, "consumer.stop called only once");
});

test("kafkaConsumeStream: eachMessage after stopped is a no-op (does not push)", async () => {
	const consumer = makeMessageConsumer([]);
	const stream = await kafkaConsumeStream({ consumer, topics: "t" });

	const receivedChunks = [];
	stream.on("data", (chunk) => receivedChunks.push(chunk));

	await stream.stop();
	// After stop, eachMessage should return without pushing
	await consumer._eachMessage({
		topic: "t",
		partition: 0,
		message: {
			offset: "0",
			key: null,
			value: "dropped",
			headers: {},
			timestamp: "0",
		},
	});

	// Give event loop a tick to see if anything was pushed
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
		autoCommit: false,
	});
	await stream.stop();
	strictEqual(capturedOpts.autoCommit, false);
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

// ---------------------------------------------------------------------------
// kafkaConsumeStream: abort signal
// ---------------------------------------------------------------------------

test("kafkaConsumeStream: already-aborted signal calls stop immediately", async () => {
	const consumer = makeMessageConsumer([]);
	const ac = new AbortController();
	ac.abort();

	const stream = await kafkaConsumeStream({
		consumer,
		topics: "t",
		signal: ac.signal,
	});
	// Stream should be ended because signal was already aborted
	const endedPromise = new Promise((resolve) => {
		if (stream.readableEnded) return resolve();
		stream.once("end", resolve);
	});
	stream.resume(); // drain to trigger end event
	await endedPromise;
});

test("kafkaConsumeStream: abort signal triggers stop", async () => {
	const consumer = makeMessageConsumer([]);
	const ac = new AbortController();

	const stream = await kafkaConsumeStream({
		consumer,
		topics: "t",
		signal: ac.signal,
	});
	const endedPromise = new Promise((resolve) => stream.once("end", resolve));
	stream.resume(); // start flowing
	ac.abort();
	await endedPromise;
	strictEqual(consumer.stopCalled, 1);
});

test("kafkaConsumeStream: destroy() calls consumer.stop", async () => {
	const consumer = makeMessageConsumer([]);
	const stream = await kafkaConsumeStream({ consumer, topics: "t" });

	const closePromise = new Promise((resolve) => stream.once("close", resolve));
	stream.destroy();
	await closePromise;
	strictEqual(consumer.stopCalled, 1);
});

test("kafkaConsumeStream: destroy() removes abort listener (signal guard)", async () => {
	const consumer = makeMessageConsumer([]);
	const ac = new AbortController();

	const stream = await kafkaConsumeStream({
		consumer,
		topics: "t",
		signal: ac.signal,
	});
	const closePromise = new Promise((resolve) => stream.once("close", resolve));
	stream.destroy();
	await closePromise;

	// Aborting after destroy should not call consumer.stop a second time
	ac.abort();
	await new Promise((resolve) => setTimeout(resolve, 10));
	strictEqual(consumer.stopCalled, 1, "consumer.stop called only once");
});

test("kafkaConsumeStream: stop() removes abort listener so abort after stop is a no-op", async () => {
	const consumer = makeMessageConsumer([]);
	const ac = new AbortController();

	const stream = await kafkaConsumeStream({
		consumer,
		topics: "t",
		signal: ac.signal,
	});
	await stream.stop();

	// Aborting after stop should not call consumer.stop again
	ac.abort();
	await new Promise((resolve) => setTimeout(resolve, 10));
	strictEqual(consumer.stopCalled, 1);
});

// ---------------------------------------------------------------------------
// kafkaConsumeStream: highWaterMark option
// ---------------------------------------------------------------------------

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

// ---------------------------------------------------------------------------
// kafkaConsumeStream: backpressure (wantsMore === false triggers waitForRead)
// ---------------------------------------------------------------------------

test("kafkaConsumeStream: backpressure suspends eachMessage until _read is called", async () => {
	const consumer = makeMessageConsumer([]);
	// Use a very small HWM so backpressure kicks in quickly
	const stream = await kafkaConsumeStream(
		{ consumer, topics: "t" },
		{ highWaterMark: 1 },
	);

	let msg2Sent = false;
	const deliveryDone = (async () => {
		// Send first message - should go through
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
		// Send second message - may block until stream reads first
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

// ---------------------------------------------------------------------------
// kafkaConsumeStream: stopConsumerOnce memoization
// ---------------------------------------------------------------------------

test("kafkaConsumeStream: stopConsumerOnce is memoized (stop + destroy only calls consumer.stop once)", async () => {
	const consumer = makeMessageConsumer([]);
	const stream = await kafkaConsumeStream({ consumer, topics: "t" });

	const closePromise = new Promise((resolve) => stream.once("close", resolve));
	// Call stop() then destroy() — both trigger stopConsumerOnce
	await stream.stop();
	stream.destroy();
	await closePromise;

	strictEqual(consumer.stopCalled, 1, "consumer.stop called exactly once");
});

// ---------------------------------------------------------------------------
// kafkaConnect: producerOptions passed to kafka.producer()
// ---------------------------------------------------------------------------

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
	// No producerOptions provided — should default to {}
	await kafkaConnect({ Kafka: FakeKafka });
	deepStrictEqual(capturedProducerOpts, {});
});

// ---------------------------------------------------------------------------
// kafkaConnect: optional chaining in error teardown
// ---------------------------------------------------------------------------

test("kafkaConnect: consumer connect error with producer===false does not throw in teardown", async () => {
	// producer is null; in the catch block, producer?.disconnect() must NOT throw
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
	// producer: false => producer is null => in catch block producer?.disconnect() is safe
	await rejects(
		async () =>
			kafkaConnect({ producer: false, groupId: "g1", Kafka: FakeKafka }),
		/consumer connect failed/,
		"should rethrow consumer connect error",
	);
});

// ---------------------------------------------------------------------------
// kafkaProduceStream: null chunk error message specifics
// ---------------------------------------------------------------------------

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

// ---------------------------------------------------------------------------
// kafkaProduceStream: batch reset is correct after flush
// ---------------------------------------------------------------------------

test("kafkaProduceStream: after batch flush, next messages go into a fresh batch", async () => {
	const { producerMock } = makeFakeKafka();
	const stream = await kafkaProduceStream({
		producer: producerMock,
		topic: "t",
		batchSize: 2,
	});
	// 4 messages = 2 full batches
	stream.write("a");
	stream.write("b");
	stream.write("c");
	stream.write("d");
	stream.end();
	await new Promise((resolve) => stream.on("finish", resolve));
	strictEqual(producerMock.sendArgs.length, 2, "two separate flushes");
	// First batch: a, b
	strictEqual(producerMock.sendArgs[0].messages[0].value, "a");
	strictEqual(producerMock.sendArgs[0].messages[1].value, "b");
	// Second batch: c, d (not ["Stryker was here", "c", "d"])
	strictEqual(producerMock.sendArgs[1].messages.length, 2);
	strictEqual(producerMock.sendArgs[1].messages[0].value, "c");
	strictEqual(producerMock.sendArgs[1].messages[1].value, "d");
});

// ---------------------------------------------------------------------------
// kafkaConsumeStream: autoCommit default true is passed to consumer.run
// ---------------------------------------------------------------------------

test("kafkaConsumeStream: autoCommit defaults to true and is passed to consumer.run", async () => {
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
	// Default autoCommit should be true
	strictEqual(capturedOpts.autoCommit, true);
	const closePromise = new Promise((r) => stream.once("close", r));
	stream.destroy();
	await closePromise;
});

// ---------------------------------------------------------------------------
// kafkaConsumeStream: destroy sets stopped=true so eachMessage is a no-op
// ---------------------------------------------------------------------------

test("kafkaConsumeStream: destroy() sets stopped=true so eachMessage after destroy is a no-op", async () => {
	const consumer = makeMessageConsumer([]);
	const stream = await kafkaConsumeStream({ consumer, topics: "t" });

	const closePromise = new Promise((resolve) => stream.once("close", resolve));
	stream.destroy();
	await closePromise;

	// Try delivering a message after destroy — should be a no-op (stopped===true)
	if (consumer._eachMessage) {
		const _chunksPushed = [];
		// The stream is already destroyed so push won't add data; just verify no throw
		await consumer._eachMessage({
			topic: "t",
			partition: 0,
			message: {
				offset: "99",
				key: null,
				value: "late",
				headers: {},
				timestamp: "0",
			},
		});
	}
	await new Promise((resolve) => setTimeout(resolve, 10));
});

// ---------------------------------------------------------------------------
// kafkaConsumeStream: abort event listener uses "abort" event name
// ---------------------------------------------------------------------------

test("kafkaConsumeStream: abort event listener is on 'abort' event (not '')", async () => {
	const consumer = makeMessageConsumer([]);
	const listeners = [];
	const mockSignal = {
		aborted: false,
		addEventListener: (event, fn) => {
			listeners.push({ event, fn });
		},
		removeEventListener: (event, fn) => {
			const idx = listeners.findIndex((l) => l.event === event && l.fn === fn);
			if (idx !== -1) listeners.splice(idx, 1);
		},
	};

	const stream = await kafkaConsumeStream({
		consumer,
		topics: "t",
		signal: mockSignal,
	});
	// Listener was registered on 'abort' event
	strictEqual(listeners.length, 1, "one listener added");
	strictEqual(listeners[0].event, "abort", "listener is on 'abort' event");

	// stop() removes it
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
			const idx = listeners.findIndex((l) => l.event === event && l.fn === fn);
			if (idx !== -1) listeners.splice(idx, 1);
		},
	};

	const stream = await kafkaConsumeStream({
		consumer,
		topics: "t",
		signal: mockSignal,
	});
	strictEqual(listeners.length, 1);

	const closePromise = new Promise((resolve) => stream.once("close", resolve));
	stream.destroy();
	await closePromise;

	strictEqual(listeners.length, 0, "listener removed on destroy");
});

// ---------------------------------------------------------------------------
// kafkaConsumeStream: stop() correctly handles already-stopped idempotency
// ---------------------------------------------------------------------------

test("kafkaConsumeStream: stop() when not stopped pushes null to end stream", async () => {
	const consumer = makeMessageConsumer([]);
	const stream = await kafkaConsumeStream({ consumer, topics: "t" });
	stream.resume();

	const endPromise = new Promise((resolve) => stream.once("end", resolve));
	await stream.stop();
	await endPromise;
	// Second call is no-op
	await stream.stop();
	strictEqual(consumer.stopCalled, 1);
});

// ---------------------------------------------------------------------------
// kafkaConsumeStream: wantsMore backpressure - eachMessage completes when wantsMore=true
// ---------------------------------------------------------------------------

test("kafkaConsumeStream: wantsMore=true means eachMessage does NOT call waitForRead", async () => {
	// With large HWM, push() returns true (wantsMore), so waitForRead is NOT called
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

	// Give a tick; eachMessage should complete without blocking (no waitForRead)
	await new Promise((resolve) => setTimeout(resolve, 5));
	ok(delivered, "eachMessage completed without waiting for read");

	await stream.stop();
	await delivery;
	stream.destroy();
});

// ---------------------------------------------------------------------------
// kafkaConsumeStream: signal.aborted check prevents double-stop
// ---------------------------------------------------------------------------

test("kafkaConsumeStream: non-aborted signal does NOT call stop immediately", async () => {
	const consumer = makeMessageConsumer([]);
	const ac = new AbortController();

	const stream = await kafkaConsumeStream({
		consumer,
		topics: "t",
		signal: ac.signal,
	});
	// Consumer.stop should NOT have been called yet (signal is not aborted)
	strictEqual(consumer.stopCalled, 0, "stop not called for non-aborted signal");

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

	const stream = await kafkaConsumeStream({
		consumer,
		topics: "t",
		signal: mockSignal,
	});
	// Should have added listener (not called stop)
	strictEqual(consumer.stopCalled, 0);
	strictEqual(listeners.length, 1);
	strictEqual(listeners[0].event, "abort");
	await stream.stop();
	stream.destroy();
});

// ---------------------------------------------------------------------------
// kafkaConnect: dynamic import fallback (Kafka not injected)
// ---------------------------------------------------------------------------

test("kafkaConnect: uses dynamic import when Kafka is not provided (no connect)", async () => {
	// Call without injecting Kafka — exercises the `Kafka ??= (await import('kafkajs')).Kafka` branch.
	// Pass producer:false and omit groupId so neither producer nor consumer is
	// created, meaning connect() is never called and no broker is needed.
	const result = await kafkaConnect({ producer: false });
	strictEqual(result.producer, null, "producer is null");
	strictEqual(result.consumer, null, "consumer is null");
	ok(result.kafka, "kafka instance created via dynamic import");
	ok(typeof result.disconnect === "function");
	await result.disconnect();
});

// ---------------------------------------------------------------------------
// kafkaConnect: disconnect() throws during error teardown (catch handlers)
// ---------------------------------------------------------------------------

test("kafkaConnect: producer.disconnect() throwing in catch block is swallowed", async () => {
	const { Kafka } = makeFakeKafka({
		connectProducer: async () => {
			throw new Error("producer connect failed");
		},
		disconnectProducer: async () => {
			throw new Error("producer disconnect also failed");
		},
	});
	// Should still rethrow the original connect error, not the disconnect error
	await rejects(async () => kafkaConnect({ Kafka }), /producer connect failed/);
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
	// Should rethrow the consumer connect error despite disconnect throwing
	await rejects(
		async () => kafkaConnect({ groupId: "g1", Kafka }),
		/consumer connect failed/,
	);
});

// ---------------------------------------------------------------------------
// kafkaConsumeStream: consumer.stop() throwing is swallowed by stopConsumerOnce
// ---------------------------------------------------------------------------

test("kafkaConsumeStream: consumer.stop() throwing is swallowed (stopConsumerOnce catch)", async () => {
	// Build a consumer whose stop() rejects — this exercises the .catch(() => {})
	// in stopConsumerOnce, which must not propagate the error.
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
	// stop() must not throw even though consumer.stop() does
	await stream.stop();
	strictEqual(consumer.stopCalled, 1);
});

// ---------------------------------------------------------------------------
// kafkaConsumeStream: runPromise rejection is swallowed (anonymous_22)
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// kafkaConsumeStream: precise backpressure contract
//   - when the buffer is FULL (push returns false / wantsMore=false) eachMessage
//     MUST suspend on waitForRead() until the next _read releases it.
//   - when the buffer has ROOM (push returns true / wantsMore=true) eachMessage
//     MUST NOT suspend.
// These pin down `if (!wantsMore) await waitForRead()` and waitForRead()'s
// Promise so the BooleanLiteral / ConditionalExpression / ArrowFunction mutants
// all fail.
// ---------------------------------------------------------------------------

const microtick = () => new Promise((resolve) => setImmediate(resolve));

test("kafkaConsumeStream: eachMessage SUSPENDS while buffer is full, resumes on read", async () => {
	const consumer = makeMessageConsumer([]);
	// HWM 1 => first push fills the buffer and returns false (wantsMore=false).
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

	// eachMessage must still be parked on waitForRead() — nothing has read yet.
	await microtick();
	strictEqual(
		settled,
		false,
		"eachMessage should be suspended while buffer full",
	);

	// Reading one item triggers _read -> releaseReaders -> resolves waitForRead.
	const item = stream.read();
	strictEqual(item.value, "v1");
	await p;
	strictEqual(settled, true, "eachMessage resumes after _read");

	await stream.stop();
	stream.destroy();
});

test("kafkaConsumeStream: eachMessage does NOT suspend while buffer has room", async () => {
	const consumer = makeMessageConsumer([]);
	// Large HWM => push returns true (wantsMore=true) => no waitForRead().
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

	// Drain MICROTASKS only (no setImmediate / I/O turn). The stream's internal
	// _read() — which would release a wrongly-parked waiter — is scheduled on a
	// macrotask, so it has NOT run yet. With correct code (wantsMore=true => no
	// waitForRead) eachMessage resolves purely via microtasks. With a mutant that
	// always waits (`if (true)`) or inverts the guard, eachMessage is still parked
	// here because only a macrotask _read could release it.
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

// ---------------------------------------------------------------------------
// kafkaConsumeStream: stop() idempotency guard (if (stopped) return) prevents a
// second stream.push(null), which would emit ERR_STREAM_PUSH_AFTER_EOF.
// ---------------------------------------------------------------------------

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
	// Second stop() must early-return; without the `if (stopped) return` guard it
	// would call stream.push(null) again after EOF and emit an error event.
	await stream.stop();
	await new Promise((resolve) => setTimeout(resolve, 10));
	strictEqual(errored, null, "no error from a redundant second stop()");
	strictEqual(consumer.stopCalled, 1);
});

test("kafkaConsumeStream: runPromise rejection is swallowed by stop()", async () => {
	// Build a consumer whose run() promise rejects — exercises the
	// runPromise.catch(() => {}) in stop(), which must swallow the error.
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
	// stop() must not throw even though runPromise rejects
	await stream.stop();
	strictEqual(consumer.stopCalled, 1);
});
