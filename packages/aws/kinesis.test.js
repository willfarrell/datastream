import { deepStrictEqual, rejects, strictEqual } from "node:assert";
import test, { describe } from "node:test";
import {
	GetRecordsCommand,
	KinesisClient,
	PutRecordsCommand,
} from "@aws-sdk/client-kinesis";
import * as awsModule from "@datastream/aws/kinesis";
import {
	awsKinesisGetRecordsStream,
	awsKinesisPutRecordsStream,
	awsKinesisSetClient,
} from "@datastream/aws/kinesis";
import {
	createReadableStream,
	isReadable,
	pipejoin,
	pipeline,
	streamToArray,
} from "@datastream/core";
import { mockClient } from "aws-sdk-client-mock";
import { variant } from "../variant.js";

// The stream reads ahead of the consumer, so a mock that keeps resolving empty
// pages with no poll delay would spin forever (starving the consumer): end
// such sequences with a poll that never answers, like an idle long poll.
const neverResolves = () => new Promise(() => {});

describe(`@datastream/aws/kinesis (${variant})`, () => {
	test(`awsKinesisGetRecordsStream should get chunk`, async (_t) => {
		const client = mockClient(KinesisClient);
		awsKinesisSetClient(client);

		client
			.on(GetRecordsCommand)
			.resolvesOnce({
				Records: [{ Data: "a" }],
				NextShardIterator: "iter2",
			})
			.resolvesOnce({
				Records: [],
				NextShardIterator: "iter3",
			});

		const options = { ShardIterator: "iter1" };
		const stream = await awsKinesisGetRecordsStream(options);
		const output = await streamToArray(stream);

		deepStrictEqual(output, [{ Data: "a" }]);
	});

	test(`awsKinesisGetRecordsStream should handle empty Records (undefined)`, async (_t) => {
		const client = mockClient(KinesisClient);
		awsKinesisSetClient(client);

		client.on(GetRecordsCommand).resolves({ NextShardIterator: "iter2" });

		const options = { ShardIterator: "iter1" };
		const stream = await awsKinesisGetRecordsStream(options);
		const output = await streamToArray(stream);

		deepStrictEqual(output, []);
	});

	test(`awsKinesisGetRecordsStream should track NextShardIterator across calls`, async (_t) => {
		const client = mockClient(KinesisClient);
		awsKinesisSetClient(client);

		client
			.on(GetRecordsCommand)
			.resolvesOnce({
				Records: [{ Data: "a" }],
				NextShardIterator: "iter2",
			})
			.resolvesOnce({
				Records: [{ Data: "b" }],
				NextShardIterator: "iter3",
			})
			.resolvesOnce({
				Records: [],
				NextShardIterator: "iter4",
			});

		const options = { ShardIterator: "iter1" };
		const stream = await awsKinesisGetRecordsStream(options);
		const output = await streamToArray(stream);

		deepStrictEqual(output, [{ Data: "a" }, { Data: "b" }]);
	});

	test(`awsKinesisGetRecordsStream should keep polling with pollingActive option`, async (_t) => {
		const client = mockClient(KinesisClient);
		awsKinesisSetClient(client);

		client
			.on(GetRecordsCommand)
			.resolvesOnce({ Records: [{ Data: "a" }], NextShardIterator: "iter2" })
			.resolvesOnce({ Records: [], NextShardIterator: "iter3" })
			.resolvesOnce({ Records: [{ Data: "b" }], NextShardIterator: "iter4" })
			.callsFake(neverResolves);

		const options = {
			ShardIterator: "iter1",
			pollingActive: true,
			pollingDelay: 0,
		};
		const stream = await awsKinesisGetRecordsStream(options);

		const output = [];
		for await (const item of stream) {
			output.push(item);
			if (output.length >= 2) break;
		}

		deepStrictEqual(output, [{ Data: "a" }, { Data: "b" }]);
	});

	test(`awsKinesisGetRecordsStream should delay polling when pollingActive and no records`, async (t) => {
		t.mock.timers.enable({ apis: ["setTimeout"] });

		const client = mockClient(KinesisClient);
		awsKinesisSetClient(client);

		client
			.on(GetRecordsCommand)
			.resolvesOnce({ Records: [], NextShardIterator: "iter2" })
			.resolvesOnce({ Records: [{ Data: "a" }], NextShardIterator: "iter3" })
			.resolves({ Records: [], NextShardIterator: "iter4" });

		const options = {
			ShardIterator: "iter1",
			pollingActive: true,
			pollingDelay: 1000,
		};
		const stream = await awsKinesisGetRecordsStream(options);

		const output = [];
		const consuming = (async () => {
			for await (const item of stream) {
				output.push(item);
				if (output.length >= 1) break;
			}
		})();

		await new Promise((resolve) => setImmediate(resolve));
		t.mock.timers.tick(1000);

		await consuming;

		deepStrictEqual(output, [{ Data: "a" }]);
	});

	test(`awsKinesisPutRecordsStream should put chunk`, async (_t) => {
		const client = mockClient(KinesisClient);
		awsKinesisSetClient(client);

		const input = "abcdefghijk"
			.split("")
			.map((id) => ({ Data: id, PartitionKey: id }));
		const options = {
			StreamName: "test-stream",
		};

		client.on(PutRecordsCommand).resolves({});

		const stream = [
			createReadableStream(input),
			awsKinesisPutRecordsStream(options),
		];
		const result = await pipeline(stream);

		deepStrictEqual(result, {});
	});

	test(`awsKinesisPutRecordsStream should handle empty input`, async (_t) => {
		const client = mockClient(KinesisClient);
		awsKinesisSetClient(client);

		const input = [];
		const options = {
			StreamName: "test-stream",
		};

		const stream = [
			createReadableStream(input),
			awsKinesisPutRecordsStream(options),
		];
		const result = await pipeline(stream);

		deepStrictEqual(result, {});
	});

	test(`awsKinesisPutRecordsStream should retry failed records on partial failure`, async (_t) => {
		const client = mockClient(KinesisClient);
		awsKinesisSetClient(client);

		const input = [
			{ Data: "a", PartitionKey: "a" },
			{ Data: "b", PartitionKey: "b" },
		];
		const options = { StreamName: "test-stream" };

		client
			.on(PutRecordsCommand)
			.resolvesOnce({
				FailedRecordCount: 1,
				Records: [
					{ SequenceNumber: "1" },
					{ ErrorCode: "ProvisionedThroughputExceededException" },
				],
			})
			.resolvesOnce({
				FailedRecordCount: 0,
				Records: [{ SequenceNumber: "2" }],
			});

		const stream = [
			createReadableStream(input),
			awsKinesisPutRecordsStream(options),
		];
		await pipeline(stream);

		const calls = client.commandCalls(PutRecordsCommand);
		deepStrictEqual(calls.length, 2);
		deepStrictEqual(calls[1].args[0].input.Records, [
			{ Data: "b", PartitionKey: "b" },
		]);
	});

	test(`awsKinesisPutRecordsStream should throw when records keep failing`, async (_t) => {
		const client = mockClient(KinesisClient);
		awsKinesisSetClient(client);

		const input = [{ Data: "a", PartitionKey: "a" }];
		const options = { StreamName: "test-stream", retryMaxCount: 0 };

		client.on(PutRecordsCommand).resolves({
			FailedRecordCount: 1,
			Records: [{ ErrorCode: "InternalFailure", ErrorMessage: "boom" }],
		});

		const stream = [
			createReadableStream(input),
			awsKinesisPutRecordsStream(options),
		];
		await rejects(() => pipeline(stream), {
			message: "awsKinesisPutRecords has failed records",
		});
	});

	test(`awsKinesisPutRecordsStream should flush when aggregate bytes exceed 5MiB`, async (_t) => {
		const client = mockClient(KinesisClient);
		awsKinesisSetClient(client);

		client.on(PutRecordsCommand).resolves({ FailedRecordCount: 0 });

		const big = "x".repeat(1024 * 1024 - 1024);
		const input = Array.from({ length: 8 }, (_v, i) => ({
			Data: big,
			PartitionKey: `${i}`,
		}));
		const options = { StreamName: "test-stream" };

		const stream = [
			createReadableStream(input),
			awsKinesisPutRecordsStream(options),
		];
		await pipeline(stream);

		const calls = client.commandCalls(PutRecordsCommand);
		deepStrictEqual(calls.length > 1, true);
		for (const call of calls) {
			deepStrictEqual(call.args[0].input.Records.length <= 5, true);
		}
	});

	test(`awsKinesisPutRecordsStream should count ExplicitHashKey toward record size`, async (_t) => {
		const client = mockClient(KinesisClient);
		awsKinesisSetClient(client);
		client.on(PutRecordsCommand).resolves({ FailedRecordCount: 0 });

		const input = [
			{ Data: "a", PartitionKey: "p", ExplicitHashKey: "123456789" },
		];
		const options = { StreamName: "test-stream" };
		const stream = [
			createReadableStream(input),
			awsKinesisPutRecordsStream(options),
		];
		await pipeline(stream);

		deepStrictEqual(client.commandCalls(PutRecordsCommand).length, 1);
	});

	test(`awsKinesisPutRecordsStream should not retry when FailedRecordCount has no ErrorCode entries`, async (_t) => {
		const client = mockClient(KinesisClient);
		awsKinesisSetClient(client);
		client.on(PutRecordsCommand).resolves({
			FailedRecordCount: 1,
			Records: [{ SequenceNumber: "1" }],
		});

		const input = [{ Data: "a", PartitionKey: "a" }];
		const options = { StreamName: "test-stream" };
		const stream = [
			createReadableStream(input),
			awsKinesisPutRecordsStream(options),
		];
		await pipeline(stream);

		deepStrictEqual(client.commandCalls(PutRecordsCommand).length, 1);
	});

	test(`awsKinesisPutRecordsStream should reject a single record over 1MiB`, async (_t) => {
		const client = mockClient(KinesisClient);
		awsKinesisSetClient(client);

		client.on(PutRecordsCommand).resolves({ FailedRecordCount: 0 });

		const tooBig = "x".repeat(1024 * 1024 + 1);
		const input = [{ Data: tooBig, PartitionKey: "k" }];
		const options = { StreamName: "test-stream" };

		const stream = [
			createReadableStream(input),
			awsKinesisPutRecordsStream(options),
		];
		await rejects(() => pipeline(stream), {
			name: "RangeError",
			message: "awsKinesisPutRecords record exceeds 1MiB limit",
		});
	});

	test(`awsKinesisPutRecordsStream should not mutate caller options`, async (_t) => {
		const client = mockClient(KinesisClient);
		awsKinesisSetClient(client);
		client.on(PutRecordsCommand).resolves({ FailedRecordCount: 0 });

		const options = { StreamName: "test-stream" };
		const optionsCopy = { ...options };
		const input = [{ Data: "a", PartitionKey: "a" }];

		const stream = [
			createReadableStream(input),
			awsKinesisPutRecordsStream(options),
		];
		await pipeline(stream);

		deepStrictEqual(options, optionsCopy);
	});

	test(`awsKinesisPutRecordsStream should forward abort signal to client.send`, async (_t) => {
		const client = mockClient(KinesisClient);
		awsKinesisSetClient(client);
		client.on(PutRecordsCommand).resolves({ FailedRecordCount: 0 });

		const controller = new AbortController();
		const input = [{ Data: "a", PartitionKey: "a" }];
		const stream = [
			createReadableStream(input),
			awsKinesisPutRecordsStream(
				{ StreamName: "test-stream" },
				{ signal: controller.signal },
			),
		];
		await pipeline(stream);

		const calls = client.commandCalls(PutRecordsCommand);
		deepStrictEqual(calls[0].args[1]?.abortSignal, controller.signal);
	});

	test(`awsKinesisGetRecordsStream should abort the idle poll delay on signal`, async (_t) => {
		const client = mockClient(KinesisClient);
		awsKinesisSetClient(client);

		client
			.on(GetRecordsCommand)
			.resolves({ Records: [], NextShardIterator: "i" });

		const controller = new AbortController();
		const options = {
			ShardIterator: "iter1",
			pollingActive: true,
			pollingDelay: 60_000,
		};
		const stream = await awsKinesisGetRecordsStream(options, {
			signal: controller.signal,
		});

		const consuming = (async () => {
			for await (const _item of stream) {
			}
		})();

		await new Promise((resolve) => setImmediate(resolve));
		controller.abort();

		await rejects(consuming, (error) => {
			return error.cause?.code === "AbortError" || /Abort/i.test(error.message);
		});
	});

	test(`awsKinesisGetRecordsStream should pass signal to client`, async (_t) => {
		const client = mockClient(KinesisClient);
		awsKinesisSetClient(client);
		client.on(GetRecordsCommand).resolves({
			Records: [{ Data: "a" }],
			NextShardIterator: null,
		});

		const controller = new AbortController();
		const options = { ShardIterator: "iter-1" };
		const stream = await awsKinesisGetRecordsStream(options, {
			signal: controller.signal,
		});
		await streamToArray(stream);

		const calls = client.commandCalls(GetRecordsCommand);
		const sent = calls[0].args[1]?.abortSignal;
		deepStrictEqual(sent.aborted, false);
		controller.abort();
		deepStrictEqual(sent.aborted, true);
	});

	test(`awsKinesisPutRecordsStream splits at the 5MiB-64KiB aggregate cap, not 5MiB`, async (_t) => {
		const client = mockClient(KinesisClient);
		awsKinesisSetClient(client);
		client.on(PutRecordsCommand).resolves({ FailedRecordCount: 0 });

		const each = 128 * 1024;
		const data = "x".repeat(each);
		const input = Array.from({ length: 41 }, (_v, i) => ({
			Data: data,
			PartitionKey: `${i % 10}`,
		}));
		const options = { StreamName: "test-stream" };
		await pipeline([
			createReadableStream(input),
			awsKinesisPutRecordsStream(options),
		]);

		const calls = client.commandCalls(PutRecordsCommand);
		deepStrictEqual(calls.length, 2);
		deepStrictEqual(calls[0].args[0].input.Records.length, 39);
		deepStrictEqual(calls[1].args[0].input.Records.length, 2);
	});

	test(`awsKinesisPutRecordsStream keeps two small records under the cap in one batch`, async (_t) => {
		const client = mockClient(KinesisClient);
		awsKinesisSetClient(client);
		client.on(PutRecordsCommand).resolves({ FailedRecordCount: 0 });

		const input = [
			{ Data: "a", PartitionKey: "1" },
			{ Data: "b", PartitionKey: "2" },
		];
		await pipeline([
			createReadableStream(input),
			awsKinesisPutRecordsStream({ StreamName: "test-stream" }),
		]);

		const calls = client.commandCalls(PutRecordsCommand);
		deepStrictEqual(calls.length, 1);
		deepStrictEqual(calls[0].args[0].input.Records.length, 2);
	});

	test(`awsKinesisPutRecordsStream accepts a record exactly at the 1MiB limit`, async (_t) => {
		const client = mockClient(KinesisClient);
		awsKinesisSetClient(client);
		client.on(PutRecordsCommand).resolves({ FailedRecordCount: 0 });

		const exact = "x".repeat(1024 * 1024);
		const input = [{ Data: exact }];
		await pipeline([
			createReadableStream(input),
			awsKinesisPutRecordsStream({ StreamName: "test-stream" }),
		]);

		deepStrictEqual(client.commandCalls(PutRecordsCommand).length, 1);
	});

	test(`awsKinesisPutRecordsStream counts a missing Data as zero bytes`, async (_t) => {
		const client = mockClient(KinesisClient);
		awsKinesisSetClient(client);
		client.on(PutRecordsCommand).resolves({ FailedRecordCount: 0 });

		const input = [{ PartitionKey: "x".repeat(1024 * 1024) }];
		await pipeline([
			createReadableStream(input),
			awsKinesisPutRecordsStream({ StreamName: "test-stream" }),
		]);

		deepStrictEqual(client.commandCalls(PutRecordsCommand).length, 1);
	});

	test(`awsKinesisPutRecordsStream counts PartitionKey bytes toward the 1MiB limit`, async (_t) => {
		const client = mockClient(KinesisClient);
		awsKinesisSetClient(client);
		client.on(PutRecordsCommand).resolves({ FailedRecordCount: 0 });

		const data = "x".repeat(1024 * 1024);
		const input = [{ Data: data, PartitionKey: "pk" }];
		await rejects(
			() =>
				pipeline([
					createReadableStream(input),
					awsKinesisPutRecordsStream({ StreamName: "test-stream" }),
				]),
			{ message: "awsKinesisPutRecords record exceeds 1MiB limit" },
		);
	});

	test(`awsKinesisPutRecordsStream counts ExplicitHashKey bytes toward the 1MiB limit`, async (_t) => {
		const client = mockClient(KinesisClient);
		awsKinesisSetClient(client);
		client.on(PutRecordsCommand).resolves({ FailedRecordCount: 0 });

		const data = "x".repeat(1024 * 1024);
		const input = [{ Data: data, PartitionKey: "p", ExplicitHashKey: "h" }];
		await rejects(
			() =>
				pipeline([
					createReadableStream(input),
					awsKinesisPutRecordsStream({ StreamName: "test-stream" }),
				]),
			{ message: "awsKinesisPutRecords record exceeds 1MiB limit" },
		);
	});

	test(`awsKinesisPutRecordsStream error cause lists only failed (ErrorCode) records`, async (_t) => {
		const client = mockClient(KinesisClient);
		awsKinesisSetClient(client);

		client.on(PutRecordsCommand).resolves({
			FailedRecordCount: 1,
			Records: [
				{ SequenceNumber: "ok-1" },
				{ ErrorCode: "InternalFailure", ErrorMessage: "boom" },
			],
		});

		const input = [
			{ Data: "a", PartitionKey: "a" },
			{ Data: "b", PartitionKey: "b" },
		];
		const options = { StreamName: "test-stream", retryMaxCount: 0 };
		await rejects(
			() =>
				pipeline([
					createReadableStream(input),
					awsKinesisPutRecordsStream(options),
				]),
			(error) => {
				deepStrictEqual(
					error.message,
					"awsKinesisPutRecords has failed records",
				);
				deepStrictEqual(error.cause, [
					{ ErrorCode: "InternalFailure", ErrorMessage: "boom" },
				]);
				return true;
			},
		);
	});

	test(`awsKinesisPutRecordsStream flushes at exactly 500 records`, async (_t) => {
		const client = mockClient(KinesisClient);
		awsKinesisSetClient(client);
		client.on(PutRecordsCommand).resolves({ FailedRecordCount: 0 });

		const input = Array.from({ length: 501 }, (_v, i) => ({
			Data: "x",
			PartitionKey: `${i}`,
		}));
		await pipeline([
			createReadableStream(input),
			awsKinesisPutRecordsStream({ StreamName: "test-stream" }),
		]);

		const calls = client.commandCalls(PutRecordsCommand);
		deepStrictEqual(calls.length, 2);
		deepStrictEqual(calls[0].args[0].input.Records.length, 500);
		deepStrictEqual(calls[1].args[0].input.Records.length, 1);
	});

	test(`awsKinesisPutRecordsStream first retry backoff is floored at 50ms`, async (t) => {
		t.mock.timers.enable({ apis: ["setTimeout"] });

		const client = mockClient(KinesisClient);
		awsKinesisSetClient(client);
		client
			.on(PutRecordsCommand)
			.resolvesOnce({
				FailedRecordCount: 1,
				Records: [{ ErrorCode: "ProvisionedThroughputExceededException" }],
			})
			.resolves({ FailedRecordCount: 0 });

		const input = [{ Data: "a", PartitionKey: "a" }];
		const consuming = pipeline([
			createReadableStream(input),
			awsKinesisPutRecordsStream({ StreamName: "test-stream" }),
		]);

		await new Promise((resolve) => setImmediate(resolve));
		t.mock.timers.tick(1);
		await new Promise((resolve) => setImmediate(resolve));
		deepStrictEqual(client.commandCalls(PutRecordsCommand).length, 1);
		t.mock.timers.tick(49);
		await consuming;
		deepStrictEqual(client.commandCalls(PutRecordsCommand).length, 2);
	});

	test(`awsKinesis streams use the per-call client option and do not forward it`, async (_t) => {
		awsKinesisSetClient({
			send: async () => {
				throw new Error("module client used");
			},
		});
		const inputs = [];
		const client = {
			send: async (command) => {
				inputs.push({ ...command.input });
				return command.input.Records
					? { FailedRecordCount: 0 }
					: { NextShardIterator: null };
			},
		};

		const output = await streamToArray(
			await awsKinesisGetRecordsStream({ ShardIterator: "s", client }),
		);
		await pipeline([
			createReadableStream([{ Data: "a", PartitionKey: "p" }]),
			awsKinesisPutRecordsStream({ StreamName: "k", client }),
		]);

		deepStrictEqual(output, []);
		deepStrictEqual(inputs, [
			{ ShardIterator: "s" },
			{ StreamName: "k", Records: [{ Data: "a", PartitionKey: "p" }] },
		]);
	});

	test(`awsKinesisSetClient routes subsequent sends to the new client`, async (_t) => {
		const first = mockClient(KinesisClient);
		awsKinesisSetClient(first);
		first.on(PutRecordsCommand).resolves({ FailedRecordCount: 0 });

		const second = mockClient(KinesisClient);
		second.on(PutRecordsCommand).resolves({ FailedRecordCount: 0 });
		awsKinesisSetClient(second);

		await pipeline([
			createReadableStream([{ Data: "a", PartitionKey: "a" }]),
			awsKinesisPutRecordsStream({ StreamName: "test-stream" }),
		]);

		deepStrictEqual(second.commandCalls(PutRecordsCommand).length, 1);
		deepStrictEqual(first.commandCalls(PutRecordsCommand).length, 0);
	});

	test(`awsKinesisGetRecordsStream stops polling when NextShardIterator is null even with pollingActive`, async (_t) => {
		const client = mockClient(KinesisClient);
		awsKinesisSetClient(client);

		client.on(GetRecordsCommand).resolves({
			Records: [{ Data: "a" }],
			NextShardIterator: null,
		});

		const options = {
			ShardIterator: "iter1",
			pollingActive: true,
			pollingDelay: 0,
		};
		const stream = await awsKinesisGetRecordsStream(options);
		const output = await streamToArray(stream);

		deepStrictEqual(output, [{ Data: "a" }]);
		deepStrictEqual(client.commandCalls(GetRecordsCommand).length, 1);
	});

	test(`awsKinesisGetRecordsStream stops polling when NextShardIterator is undefined`, async (_t) => {
		let calls = 0;
		awsKinesisSetClient({
			send: async () => {
				calls++;
				if (calls > 1) {
					throw new Error("GetRecords called after shard closed");
				}
				return { Records: [{ Data: "a" }] };
			},
		});

		const stream = await awsKinesisGetRecordsStream({
			ShardIterator: "iter1",
			pollingActive: true,
			pollingDelay: 0,
		});
		const output = await streamToArray(stream);

		deepStrictEqual(output, [{ Data: "a" }]);
		deepStrictEqual(calls, 1);
	});

	test(`awsKinesisGetRecordsStream without pollingActive keeps reading empty pages while MillisBehindLatest > 0`, async (_t) => {
		const responses = [
			{ Records: [], NextShardIterator: "iter2", MillisBehindLatest: 5000 },
			{
				Records: [{ Data: "a" }],
				NextShardIterator: "iter3",
				MillisBehindLatest: 1,
			},
			{ Records: [], NextShardIterator: "iter4", MillisBehindLatest: 0 },
		];
		const iterators = [];
		awsKinesisSetClient({
			send: async (command) => {
				iterators.push(command.input.ShardIterator);
				const response = responses.shift();
				if (!response) {
					throw new Error("GetRecords called after caught up");
				}
				return response;
			},
		});

		const stream = await awsKinesisGetRecordsStream({
			ShardIterator: "iter1",
			pollingDelay: 0,
		});
		const output = await streamToArray(stream);

		deepStrictEqual(output, [{ Data: "a" }]);
		deepStrictEqual(iterators, ["iter1", "iter2", "iter3"]);
	});

	test(`awsKinesisGetRecordsStream without pollingActive waits pollingDelay between empty catch-up pages`, async (t) => {
		t.mock.timers.enable({ apis: ["setTimeout"] });
		const responses = [
			{ Records: [], NextShardIterator: "iter2", MillisBehindLatest: 5000 },
			{ Records: [], NextShardIterator: "iter3", MillisBehindLatest: 0 },
		];
		let calls = 0;
		awsKinesisSetClient({
			send: async () => {
				calls++;
				return responses.shift();
			},
		});

		const stream = await awsKinesisGetRecordsStream({
			ShardIterator: "iter1",
			pollingDelay: 250,
		});
		let output;
		streamToArray(stream).then((result) => {
			output = result;
		});
		await new Promise((resolve) => setImmediate(resolve));
		deepStrictEqual(calls, 1);
		t.mock.timers.tick(249);
		await new Promise((resolve) => setImmediate(resolve));
		deepStrictEqual(calls, 1);
		t.mock.timers.tick(1);
		await new Promise((resolve) => setImmediate(resolve));
		deepStrictEqual(calls, 2);
		deepStrictEqual(output, []);
	});

	test(`awsKinesisGetRecordsStream closed shard with an empty page ends without an idle wait`, async (t) => {
		t.mock.timers.enable({ apis: ["setTimeout"] });
		awsKinesisSetClient({
			send: async () => ({ Records: [], NextShardIterator: null }),
		});

		const stream = await awsKinesisGetRecordsStream({
			ShardIterator: "iter1",
			pollingActive: true,
			pollingDelay: 1000,
		});
		let output;
		streamToArray(stream).then((result) => {
			output = result;
		});
		await new Promise((resolve) => setImmediate(resolve));
		deepStrictEqual(output, []);
	});

	test(`awsKinesisGetRecordsStream without pollingActive stops once records run out`, async (_t) => {
		const client = mockClient(KinesisClient);
		awsKinesisSetClient(client);

		client
			.on(GetRecordsCommand)
			.resolvesOnce({ Records: [{ Data: "a" }], NextShardIterator: "iter2" })
			.resolves({ Records: [], NextShardIterator: "iter3" });

		const options = { ShardIterator: "iter1" };
		const stream = await awsKinesisGetRecordsStream(options);
		const output = await streamToArray(stream);

		deepStrictEqual(output, [{ Data: "a" }]);
		deepStrictEqual(client.commandCalls(GetRecordsCommand).length, 2);
	});

	test(`awsKinesisSetClient stores the passed client reference`, async (_t) => {
		// mockClient patches the SDK prototype, hiding instance identity; a plain
		// stub proves the stored reference is the one actually used to send. A
		// `setClient(){}` mutant would leave the previous client in place and the
		// stub's send would never run.
		let calls = 0;
		const stub = {
			send: async () => {
				calls++;
				return { Records: [{ Data: "stub" }], NextShardIterator: null };
			},
		};
		awsKinesisSetClient(stub);

		const stream = await awsKinesisGetRecordsStream({ ShardIterator: "iter1" });
		const output = await streamToArray(stream);

		deepStrictEqual(output, [{ Data: "stub" }]);
		deepStrictEqual(calls, 1);
	});

	test(`awsKinesisGetRecordsStream forwards ShardIterator to GetRecords`, async (_t) => {
		let firstShardIterator = "unset";
		const stub = {
			send: (command) => {
				if (firstShardIterator === "unset") {
					firstShardIterator = command.input.ShardIterator;
				}
				return Promise.resolve({
					Records: [{ Data: "a" }],
					NextShardIterator: null,
				});
			},
		};
		awsKinesisSetClient(stub);

		const stream = await awsKinesisGetRecordsStream({
			ShardIterator: "iter-xyz",
		});
		await streamToArray(stream);

		deepStrictEqual(firstShardIterator, "iter-xyz");
	});

	test(`awsKinesisGetRecordsStream does not delay when records are present (pollingActive)`, async (t) => {
		t.mock.timers.enable({ apis: ["setTimeout"] });
		const client = mockClient(KinesisClient);
		awsKinesisSetClient(client);
		client.on(GetRecordsCommand).resolves({
			Records: [{ Data: "a" }],
			NextShardIterator: "iter-next",
		});

		const options = {
			ShardIterator: "iter1",
			pollingActive: true,
			pollingDelay: 60_000,
		};
		const stream = await awsKinesisGetRecordsStream(options);

		const output = [];
		for await (const item of stream) {
			output.push(item);
			if (output.length >= 3) break;
		}
		deepStrictEqual(output.length, 3);
	});

	test(`awsKinesisGetRecordsStream pollingDelay 0 schedules no timer`, async (t) => {
		t.mock.timers.enable({ apis: ["setTimeout"] });
		const client = mockClient(KinesisClient);
		awsKinesisSetClient(client);
		client
			.on(GetRecordsCommand)
			.resolvesOnce({ Records: [], NextShardIterator: "iter2" })
			.resolves({ Records: [{ Data: "a" }], NextShardIterator: "iter3" });

		const options = {
			ShardIterator: "iter1",
			pollingActive: true,
			pollingDelay: 0,
		};
		const stream = await awsKinesisGetRecordsStream(options);

		const output = [];
		(async () => {
			for await (const item of stream) {
				output.push(item);
				if (output.length >= 1) break;
			}
		})();
		await new Promise((resolve) => setImmediate(resolve));
		deepStrictEqual(output, [{ Data: "a" }]);
	});

	test(`awsKinesisGetRecordsStream non-polling empty page ends without an idle wait`, async (t) => {
		t.mock.timers.enable({ apis: ["setTimeout"] });
		const client = mockClient(KinesisClient);
		awsKinesisSetClient(client);
		client
			.on(GetRecordsCommand)
			.resolvesOnce({ Records: [{ Data: "a" }], NextShardIterator: "iter2" })
			.resolves({ Records: [], NextShardIterator: "iter3" });

		const stream = await awsKinesisGetRecordsStream({ ShardIterator: "iter1" });
		const output = await streamToArray(stream);

		deepStrictEqual(output, [{ Data: "a" }]);
	});

	test(`awsKinesisPutRecordsStream accepts a record with no Data field`, async (_t) => {
		const client = mockClient(KinesisClient);
		awsKinesisSetClient(client);
		client.on(PutRecordsCommand).resolves({ FailedRecordCount: 0 });

		const input = [{ PartitionKey: "p" }];
		await pipeline([
			createReadableStream(input),
			awsKinesisPutRecordsStream({ StreamName: "test-stream" }),
		]);
		deepStrictEqual(client.commandCalls(PutRecordsCommand).length, 1);
	});

	test(`awsKinesisPutRecordsStream counts ExplicitHashKey as the deciding byte`, async (_t) => {
		const client = mockClient(KinesisClient);
		awsKinesisSetClient(client);
		client.on(PutRecordsCommand).resolves({ FailedRecordCount: 0 });

		const data = "x".repeat(1024 * 1024);
		const input = [{ Data: data, ExplicitHashKey: "h" }];
		await rejects(
			() =>
				pipeline([
					createReadableStream(input),
					awsKinesisPutRecordsStream({ StreamName: "test-stream" }),
				]),
			{ message: "awsKinesisPutRecords record exceeds 1MiB limit" },
		);
	});

	test(`awsKinesisPutRecordsStream measures non-string Data by byteLength`, async (_t) => {
		const client = mockClient(KinesisClient);
		awsKinesisSetClient(client);
		client.on(PutRecordsCommand).resolves({ FailedRecordCount: 0 });

		const exact = new Uint8Array(1024 * 1024);
		await pipeline([
			createReadableStream([{ Data: exact }]),
			awsKinesisPutRecordsStream({ StreamName: "test-stream" }),
		]);
		deepStrictEqual(client.commandCalls(PutRecordsCommand).length, 1);

		const over = new Uint8Array(1024 * 1024 + 1);
		await rejects(
			() =>
				pipeline([
					createReadableStream([{ Data: over }]),
					awsKinesisPutRecordsStream({ StreamName: "test-stream" }),
				]),
			{ message: "awsKinesisPutRecords record exceeds 1MiB limit" },
		);
	});

	test(`awsKinesisPutRecordsStream oversize error cause carries bytes and limit`, async (_t) => {
		const client = mockClient(KinesisClient);
		awsKinesisSetClient(client);
		client.on(PutRecordsCommand).resolves({ FailedRecordCount: 0 });

		const tooBig = "x".repeat(1024 * 1024 + 1);
		await rejects(
			() =>
				pipeline([
					createReadableStream([{ Data: tooBig, PartitionKey: "k" }]),
					awsKinesisPutRecordsStream({ StreamName: "test-stream" }),
				]),
			(error) => {
				deepStrictEqual(
					error.message,
					"awsKinesisPutRecords record exceeds 1MiB limit",
				);
				deepStrictEqual(error.cause.limit, 1024 * 1024);
				deepStrictEqual(error.cause.bytes, 1024 * 1024 + 2);
				return true;
			},
		);
	});

	test(`awsKinesisPutRecordsStream does not retry when FailedRecordCount is 0`, async (_t) => {
		const client = mockClient(KinesisClient);
		awsKinesisSetClient(client);
		client.on(PutRecordsCommand).resolves({
			FailedRecordCount: 0,
			Records: [{ ErrorCode: "ShouldBeIgnored" }],
		});

		await pipeline([
			createReadableStream([{ Data: "a", PartitionKey: "a" }]),
			awsKinesisPutRecordsStream({ StreamName: "test-stream" }),
		]);
		deepStrictEqual(client.commandCalls(PutRecordsCommand).length, 1);
	});

	test(`awsKinesisPutRecordsStream tolerates a results array shorter than records`, async (_t) => {
		const client = mockClient(KinesisClient);
		awsKinesisSetClient(client);
		client
			.on(PutRecordsCommand)
			.resolvesOnce({
				FailedRecordCount: 1,
				Records: [{ ErrorCode: "ProvisionedThroughputExceededException" }],
			})
			.resolves({ FailedRecordCount: 0 });

		await pipeline([
			createReadableStream([
				{ Data: "a", PartitionKey: "a" },
				{ Data: "b", PartitionKey: "b" },
			]),
			awsKinesisPutRecordsStream({ StreamName: "test-stream" }),
		]);

		const calls = client.commandCalls(PutRecordsCommand);
		deepStrictEqual(calls.length, 2);
		deepStrictEqual(calls[1].args[0].input.Records, [
			{ Data: "a", PartitionKey: "a" },
		]);
	});

	test(`awsKinesisPutRecordsStream stops retrying after retryMaxCount`, async (_t) => {
		const client = mockClient(KinesisClient);
		awsKinesisSetClient(client);
		client.on(PutRecordsCommand).resolves({
			FailedRecordCount: 1,
			Records: [{ ErrorCode: "InternalFailure" }],
		});

		await rejects(
			() =>
				pipeline([
					createReadableStream([{ Data: "a", PartitionKey: "a" }]),
					awsKinesisPutRecordsStream({
						StreamName: "test-stream",
						retryMaxCount: 2,
					}),
				]),
			{ message: "awsKinesisPutRecords has failed records" },
		);
		deepStrictEqual(client.commandCalls(PutRecordsCommand).length, 3);
	});

	test(`awsKinesisPutRecordsStream retries without limit when retryMaxCount is null`, async (t) => {
		const client = mockClient(KinesisClient);
		awsKinesisSetClient(client);
		const failed = {
			FailedRecordCount: 1,
			Records: [{ ErrorCode: "InternalFailure" }],
		};
		t.mock.method(globalThis, "setTimeout", (fn) => setImmediate(fn));
		const mock = client.on(PutRecordsCommand);
		for (let i = 0; i < 11; i++) mock.resolvesOnce(failed);
		mock.resolves({ FailedRecordCount: 0 });

		await pipeline([
			createReadableStream([{ Data: "a", PartitionKey: "a" }]),
			awsKinesisPutRecordsStream({
				StreamName: "test-stream",
				retryMaxCount: null,
			}),
		]);
		deepStrictEqual(client.commandCalls(PutRecordsCommand).length, 12);
	});

	test(`awsKinesisPutRecordsStream cap subtracts 64KiB (not 64/1024)`, async (_t) => {
		const client = mockClient(KinesisClient);
		awsKinesisSetClient(client);
		client.on(PutRecordsCommand).resolves({ FailedRecordCount: 0 });

		const data = "x".repeat(1_048_000);
		const input = Array.from({ length: 5 }, (_v, i) => ({
			Data: data,
			PartitionKey: `${i % 10}`,
		}));
		await pipeline([
			createReadableStream(input),
			awsKinesisPutRecordsStream({ StreamName: "test-stream" }),
		]);

		const calls = client.commandCalls(PutRecordsCommand);
		deepStrictEqual(calls.length, 2);
		deepStrictEqual(calls[0].args[0].input.Records.length, 4);
		deepStrictEqual(calls[1].args[0].input.Records.length, 1);
	});

	test(`awsKinesisPutRecordsStream keeps a batch summing exactly to the cap`, async (_t) => {
		const client = mockClient(KinesisClient);
		awsKinesisSetClient(client);
		client.on(PutRecordsCommand).resolves({ FailedRecordCount: 0 });

		const data = "x".repeat(65_535);
		const input = Array.from({ length: 80 }, (_v, i) => ({
			Data: data,
			PartitionKey: `${i % 10}`,
		}));
		await pipeline([
			createReadableStream(input),
			awsKinesisPutRecordsStream({ StreamName: "test-stream" }),
		]);

		const calls = client.commandCalls(PutRecordsCommand);
		deepStrictEqual(calls.length, 2);
		deepStrictEqual(calls[0].args[0].input.Records.length, 79);
		deepStrictEqual(calls[1].args[0].input.Records.length, 1);
	});

	test(`awsKinesisPutRecordsStream falls back to empty array when response.Records is undefined`, async (_t) => {
		const client = mockClient(KinesisClient);
		awsKinesisSetClient(client);
		client
			.on(PutRecordsCommand)
			.resolvesOnce({
				FailedRecordCount: 1,
			})
			.resolves({ FailedRecordCount: 0 });

		const input = [{ Data: "a", PartitionKey: "a" }];
		const stream = [
			createReadableStream(input),
			awsKinesisPutRecordsStream({ StreamName: "test-stream" }),
		];
		await pipeline(stream);

		deepStrictEqual(client.commandCalls(PutRecordsCommand).length, 1);
	});

	test(`awsKinesisPutRecordsStream aborts during retry backoff`, async (t) => {
		t.mock.timers.enable({ apis: ["setTimeout"] });
		const client = mockClient(KinesisClient);
		awsKinesisSetClient(client);
		client.on(PutRecordsCommand).resolves({
			FailedRecordCount: 1,
			Records: [{ ErrorCode: "ProvisionedThroughputExceededException" }],
		});

		let sends = 0;
		client.on(PutRecordsCommand).callsFake(() => {
			sends++;
			return Promise.resolve({
				FailedRecordCount: 1,
				Records: [{ ErrorCode: "ProvisionedThroughputExceededException" }],
			});
		});

		const controller = new AbortController();
		const consuming = pipeline([
			createReadableStream([{ Data: "a", PartitionKey: "a" }]),
			awsKinesisPutRecordsStream(
				{ StreamName: "test-stream" },
				{ signal: controller.signal },
			),
		]);

		await new Promise((resolve) => setImmediate(resolve));
		controller.abort();

		await rejects(consuming, (error) => {
			return error.cause?.code === "AbortError" || /Abort/i.test(error.message);
		});

		t.mock.timers.tick(60_000);
		await new Promise((resolve) => setImmediate(resolve));
		deepStrictEqual(sends, 1);
	});

	test(`awsKinesisGetRecordsStream is a Readable that pipejoin accepts`, async (_t) => {
		const client = mockClient(KinesisClient);
		awsKinesisSetClient(client);
		client.on(GetRecordsCommand).resolves({ Records: [{ Data: "a" }] });

		const stream = await awsKinesisGetRecordsStream({ ShardIterator: "iter1" });
		strictEqual(isReadable(stream), true);
		deepStrictEqual(await streamToArray(pipejoin([stream])), [{ Data: "a" }]);
	});

	test(`awsKinesisGetRecordsStream destroy stops an idle poll`, {
		timeout: 1000,
	}, async (_t) => {
		const client = mockClient(KinesisClient);
		awsKinesisSetClient(client);
		client
			.on(GetRecordsCommand)
			.resolves({ Records: [], NextShardIterator: "iter" });

		const stream = await awsKinesisGetRecordsStream({
			ShardIterator: "iter1",
			pollingActive: true,
			pollingDelay: 60_000,
		});
		stream.resume();
		await new Promise((resolve) => setImmediate(resolve));
		const closed = new Promise((resolve) => stream.once("close", resolve));
		stream.destroy();
		await closed;
		deepStrictEqual(client.commandCalls(GetRecordsCommand).length, 1);
	});

	test(`kinesis has no default export`, (_t) => {
		deepStrictEqual(Object.hasOwn(awsModule, "default"), false);
	});

	const writeEach = async (stream, chunks) => {
		for (const chunk of chunks) {
			await new Promise((resolve, reject) =>
				stream.write(chunk, (error) => (error ? reject(error) : resolve())),
			);
		}
	};
	const destroyedWith = async (stream, error) => {
		const closed = new Promise((resolve) => stream.once("close", resolve));
		stream.once("error", () => {});
		stream.destroy(error);
		await closed;
		return stream.errored;
	};

	test(`awsKinesisPutRecordsStream destroy(error) attaches the buffered entries`, async (_t) => {
		const client = mockClient(KinesisClient);
		awsKinesisSetClient(client);
		client.on(PutRecordsCommand).resolves({});
		const stream = awsKinesisPutRecordsStream({ StreamName: "s" });
		const chunks = Array.from({ length: 2 }, (_v, i) => ({
			Data: `${i}`,
			PartitionKey: "p",
		}));
		await writeEach(stream, chunks);
		const error = new Error("boom");
		strictEqual(await destroyedWith(stream, error), error);
		deepStrictEqual(error.message, "boom");
		deepStrictEqual(error.failedMessages, chunks);
		deepStrictEqual(client.commandCalls(PutRecordsCommand).length, 0);
	});

	test(`awsKinesisPutRecordsStream destroy() raises an error carrying the buffered entries`, async (_t) => {
		const client = mockClient(KinesisClient);
		awsKinesisSetClient(client);
		client.on(PutRecordsCommand).resolves({});
		const stream = awsKinesisPutRecordsStream({ StreamName: "s" });
		const chunks = Array.from({ length: 1 }, (_v, i) => ({
			Data: `${i}`,
			PartitionKey: "p",
		}));
		await writeEach(stream, chunks);
		const error = await destroyedWith(stream);
		deepStrictEqual(
			error.message,
			"awsKinesisPutRecords destroyed with buffered records",
		);
		deepStrictEqual(error.failedMessages, chunks);
		deepStrictEqual(client.commandCalls(PutRecordsCommand).length, 0);
	});

	test(`awsKinesisPutRecordsStream destroy() with nothing buffered raises no error`, async (_t) => {
		const client = mockClient(KinesisClient);
		awsKinesisSetClient(client);
		client.on(PutRecordsCommand).resolves({});
		const stream = awsKinesisPutRecordsStream({ StreamName: "s" });
		const closed = new Promise((resolve) => stream.once("close", resolve));
		stream.destroy();
		await closed;
		strictEqual(stream.errored, null);
		deepStrictEqual(client.commandCalls(PutRecordsCommand).length, 0);
	});

	test(`awsKinesisPutRecordsStream attaches only the entries buffered after the last flush`, async (_t) => {
		const client = mockClient(KinesisClient);
		awsKinesisSetClient(client);
		client.on(PutRecordsCommand).resolves({});
		const stream = awsKinesisPutRecordsStream({ StreamName: "s" });
		const chunks = Array.from({ length: 501 }, (_v, i) => ({
			Data: `${i}`,
			PartitionKey: "p",
		}));
		await writeEach(stream, chunks);
		const error = await destroyedWith(stream);
		deepStrictEqual(error.failedMessages, chunks.slice(500));
		deepStrictEqual(client.commandCalls(PutRecordsCommand).length, 1);
	});

	test(`awsKinesisPutRecordsStream abort attaches the buffered entries without sending`, async (_t) => {
		const client = mockClient(KinesisClient);
		awsKinesisSetClient(client);
		client.on(PutRecordsCommand).resolves({});
		const controller = new AbortController();
		const stream = awsKinesisPutRecordsStream(
			{ StreamName: "s" },
			{
				signal: controller.signal,
			},
		);
		const chunks = Array.from({ length: 2 }, (_v, i) => ({
			Data: `${i}`,
			PartitionKey: "p",
		}));
		await writeEach(stream, chunks);
		const closed = new Promise((resolve) => stream.once("close", resolve));
		stream.once("error", () => {});
		controller.abort();
		await closed;
		const error = stream.errored;
		deepStrictEqual(error.name, "AbortError");
		deepStrictEqual(error.failedMessages, chunks);
		deepStrictEqual(client.commandCalls(PutRecordsCommand).length, 0);
	});
});
