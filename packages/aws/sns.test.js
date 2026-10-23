import { deepStrictEqual, rejects, strictEqual } from "node:assert";
import test, { describe } from "node:test";
import { PublishBatchCommand, SNSClient } from "@aws-sdk/client-sns";
import * as awsModule from "@datastream/aws/sns";
import {
	awsSNSPublishMessageStream,
	awsSNSSetClient,
} from "@datastream/aws/sns";
import { createReadableStream, pipeline } from "@datastream/core";
import { mockClient } from "aws-sdk-client-mock";
import { variant } from "../variant.js";

describe(`@datastream/aws/sns (${variant})`, () => {
	test(`awsSNSPublishMessageStream should put chunk`, async (_t) => {
		const client = mockClient(SNSClient);
		awsSNSSetClient(client);

		const input = "abcdefghijk".split("").map((id) => ({ id }));
		const options = {
			TopicArn: "arn:aws:sns:us-east-1:000000000000:test",
		};

		client
			.on(PublishBatchCommand, {
				PublishBatchRequestEntries: "abcdefghij"
					.split("")
					.map((id) => ({ id })),
			})
			.resolves({})
			.on(PublishBatchCommand, {
				PublishBatchRequestEntries: "k".split("").map((id) => ({ id })),
			})
			.resolves({});

		const stream = [
			createReadableStream(input),
			awsSNSPublishMessageStream(options),
		];
		const result = await pipeline(stream);

		deepStrictEqual(result, {});
	});

	test(`awsSNSPublishMessageStream should handle empty input`, async (_t) => {
		const client = mockClient(SNSClient);
		awsSNSSetClient(client);

		const input = [];
		const options = {
			TopicArn: "arn:aws:sns:us-east-1:000000000000:test",
		};

		const stream = [
			createReadableStream(input),
			awsSNSPublishMessageStream(options),
		];
		const result = await pipeline(stream);

		deepStrictEqual(result, {});
	});

	test(`awsSNSPublishMessageStream should retry failed entries`, async (_t) => {
		const client = mockClient(SNSClient);
		awsSNSSetClient(client);

		const input = [
			{ Id: "0", Message: "a" },
			{ Id: "1", Message: "b" },
		];
		const options = {
			TopicArn: "arn:aws:sns:us-east-1:000000000000:test",
		};

		client
			.on(PublishBatchCommand)
			.resolvesOnce({
				Successful: [{ Id: "0" }],
				Failed: [{ Id: "1", Code: "ThrottlingException", SenderFault: false }],
			})
			.resolvesOnce({
				Successful: [{ Id: "1" }],
				Failed: [],
			});

		const stream = [
			createReadableStream(input),
			awsSNSPublishMessageStream(options),
		];
		await pipeline(stream);

		const calls = client.commandCalls(PublishBatchCommand);
		deepStrictEqual(calls.length, 2);
		deepStrictEqual(calls[1].args[0].input.PublishBatchRequestEntries, [
			{ Id: "1", Message: "b" },
		]);
	});

	test(`awsSNSPublishMessageStream should throw when entries keep failing`, async (_t) => {
		const client = mockClient(SNSClient);
		awsSNSSetClient(client);

		const input = [{ Id: "0", Message: "a" }];
		const options = {
			TopicArn: "arn:aws:sns:us-east-1:000000000000:test",
			retryMaxCount: 0,
		};

		client.on(PublishBatchCommand).resolves({
			Successful: [],
			Failed: [{ Id: "0", Code: "InternalError", SenderFault: false }],
		});

		const stream = [
			createReadableStream(input),
			awsSNSPublishMessageStream(options),
		];
		await rejects(() => pipeline(stream), {
			message: "awsSNSPublishBatch has failed entries",
		});
	});

	test(`awsSNSPublishMessageStream retries retryable failures alongside a SenderFault entry, then throws`, async (_t) => {
		const permanent = { Id: "1", Code: "InvalidParameter", SenderFault: true };
		const sent = [];
		awsSNSSetClient({
			send: async (command) => {
				sent.push(command.input.PublishBatchRequestEntries);
				return sent.length === 1
					? {
							Successful: [],
							Failed: [
								{ Id: "0", Code: "Throttling", SenderFault: false },
								permanent,
							],
						}
					: { Successful: [{ Id: "0" }], Failed: [] };
			},
		});

		await rejects(
			() =>
				pipeline([
					createReadableStream([
						{ Id: "0", Message: "a" },
						{ Id: "1", Message: "b" },
					]),
					awsSNSPublishMessageStream({ TopicArn: "t", retryMaxCount: 1 }),
				]),
			(error) => {
				deepStrictEqual(error.message, "awsSNSPublishBatch has failed entries");
				deepStrictEqual(error.cause, [permanent]);
				return true;
			},
		);
		deepStrictEqual(sent, [
			[
				{ Id: "0", Message: "a" },
				{ Id: "1", Message: "b" },
			],
			[{ Id: "0", Message: "a" }],
		]);
	});

	test(`awsSNSPublishMessageStream error cause holds SenderFault and still-failing entries once retries run out`, async (_t) => {
		const permanent = { Id: "1", Code: "InvalidParameter", SenderFault: true };
		const stillFailing = { Id: "0", Code: "Throttling", SenderFault: false };
		let calls = 0;
		awsSNSSetClient({
			send: async () => {
				calls++;
				return calls === 1
					? { Successful: [], Failed: [{ ...stillFailing }, permanent] }
					: { Successful: [], Failed: [stillFailing] };
			},
		});

		await rejects(
			() =>
				pipeline([
					createReadableStream([
						{ Id: "0", Message: "a" },
						{ Id: "1", Message: "b" },
					]),
					awsSNSPublishMessageStream({ TopicArn: "t", retryMaxCount: 1 }),
				]),
			(error) => {
				deepStrictEqual(error.message, "awsSNSPublishBatch has failed entries");
				deepStrictEqual(error.cause, [permanent, stillFailing]);
				return true;
			},
		);
		deepStrictEqual(calls, 2);
	});

	test(`awsSNSPublishMessageStream throws without retrying when every failed entry has SenderFault`, async (_t) => {
		const failed = [{ Id: "1", Code: "InvalidParameter", SenderFault: true }];
		let calls = 0;
		awsSNSSetClient({
			send: async () => {
				calls++;
				return { Successful: [{ Id: "0" }], Failed: failed };
			},
		});

		await rejects(
			() =>
				pipeline([
					createReadableStream([
						{ Id: "0", Message: "a" },
						{ Id: "1", Message: "b" },
					]),
					awsSNSPublishMessageStream({ TopicArn: "t", retryMaxCount: 1 }),
				]),
			(error) => {
				deepStrictEqual(error.message, "awsSNSPublishBatch has failed entries");
				deepStrictEqual(error.cause, failed);
				return true;
			},
		);
		deepStrictEqual(calls, 1);
	});

	test(`awsSNSPublishMessageStream should flush before exceeding 256KB`, async (_t) => {
		const client = mockClient(SNSClient);
		awsSNSSetClient(client);

		client.on(PublishBatchCommand).resolves({ Successful: [], Failed: [] });

		const body = "x".repeat(100 * 1024);
		const input = Array.from({ length: 5 }, (_v, i) => ({
			Id: `${i}`,
			Message: body,
		}));
		const options = {
			TopicArn: "arn:aws:sns:us-east-1:000000000000:test",
		};

		const stream = [
			createReadableStream(input),
			awsSNSPublishMessageStream(options),
		];
		await pipeline(stream);

		const calls = client.commandCalls(PublishBatchCommand);
		deepStrictEqual(calls.length > 1, true);
		for (const call of calls) {
			deepStrictEqual(
				call.args[0].input.PublishBatchRequestEntries.length <= 2,
				true,
			);
		}
	});

	test(`awsSNSPublishMessageStream first retry backoff is floored at 50ms (no 1ms hammer)`, async (t) => {
		t.mock.timers.enable({ apis: ["setTimeout"] });

		const client = mockClient(SNSClient);
		awsSNSSetClient(client);

		client
			.on(PublishBatchCommand)
			.resolvesOnce({
				Successful: [],
				Failed: [{ Id: "0", Code: "ThrottlingException", SenderFault: false }],
			})
			.resolves({ Successful: [{ Id: "0" }], Failed: [] });

		const input = [{ Id: "0", Message: "a" }];
		const options = {
			TopicArn: "arn:aws:sns:us-east-1:000000000000:test",
		};
		const stream = [
			createReadableStream(input),
			awsSNSPublishMessageStream(options),
		];
		const consuming = pipeline(stream);

		await new Promise((resolve) => setImmediate(resolve));
		t.mock.timers.tick(1);
		await new Promise((resolve) => setImmediate(resolve));
		deepStrictEqual(client.commandCalls(PublishBatchCommand).length, 1);

		t.mock.timers.tick(49);
		await consuming;
		deepStrictEqual(client.commandCalls(PublishBatchCommand).length, 2);
	});

	test(`awsSNSPublishMessageStream should reject a single entry over 256KB`, async (_t) => {
		const client = mockClient(SNSClient);
		awsSNSSetClient(client);
		client.on(PublishBatchCommand).resolves({ Successful: [], Failed: [] });

		const tooBig = "x".repeat(256 * 1024 + 1);
		const input = [{ Id: "0", Message: tooBig }];
		const options = {
			TopicArn: "arn:aws:sns:us-east-1:000000000000:test",
		};

		const stream = [
			createReadableStream(input),
			awsSNSPublishMessageStream(options),
		];
		await rejects(() => pipeline(stream), {
			name: "RangeError",
			message: "awsSNSPublishBatch entry exceeds 256KiB limit",
		});
		deepStrictEqual(client.commandCalls(PublishBatchCommand).length, 0);
	});

	test(`awsSNSPublishMessageStream should not mutate caller options`, async (_t) => {
		const client = mockClient(SNSClient);
		awsSNSSetClient(client);
		client.on(PublishBatchCommand).resolves({ Successful: [], Failed: [] });

		const options = {
			TopicArn: "arn:aws:sns:us-east-1:000000000000:test",
		};
		const optionsCopy = { ...options };
		const input = [{ Id: "0", Message: "a" }];

		const stream = [
			createReadableStream(input),
			awsSNSPublishMessageStream(options),
		];
		await pipeline(stream);

		deepStrictEqual(options, optionsCopy);
	});

	test(`awsSNSPublishMessageStream error cause is the Failed entries array`, async (_t) => {
		const client = mockClient(SNSClient);
		awsSNSSetClient(client);

		const failed = [
			{ Id: "0", Code: "InternalError", SenderFault: false, Message: "boom" },
		];
		client.on(PublishBatchCommand).resolves({ Successful: [], Failed: failed });

		await rejects(
			() =>
				pipeline([
					createReadableStream([{ Id: "0", Message: "a" }]),
					awsSNSPublishMessageStream({ TopicArn: "t", retryMaxCount: 0 }),
				]),
			(error) => {
				deepStrictEqual(error.message, "awsSNSPublishBatch has failed entries");
				deepStrictEqual(error.cause, failed);
				return true;
			},
		);
	});

	test(`awsSNSPublishMessageStream oversize error cause carries Id, bytes and limit`, async (_t) => {
		const client = mockClient(SNSClient);
		awsSNSSetClient(client);
		client.on(PublishBatchCommand).resolves({ Successful: [], Failed: [] });

		const tooBig = "x".repeat(256 * 1024 + 1);
		const chunk = { Id: "the-id", Message: tooBig };
		const expectedBytes = 256 * 1024 + 1;

		await rejects(
			() =>
				pipeline([
					createReadableStream([chunk]),
					awsSNSPublishMessageStream({ TopicArn: "t" }),
				]),
			(error) => {
				deepStrictEqual(
					error.message,
					"awsSNSPublishBatch entry exceeds 256KiB limit",
				);
				deepStrictEqual(error.cause, {
					Id: "the-id",
					bytes: expectedBytes,
					limit: 256 * 1024,
				});
				return true;
			},
		);
	});

	test(`awsSNSPublishMessageStream allows an entry whose message size is exactly 256KiB`, async (_t) => {
		const client = mockClient(SNSClient);
		awsSNSSetClient(client);
		client.on(PublishBatchCommand).resolves({ Successful: [], Failed: [] });

		const chunk = { Id: "0", Message: "x".repeat(256 * 1024) };

		await pipeline([
			createReadableStream([chunk]),
			awsSNSPublishMessageStream({ TopicArn: "t" }),
		]);

		deepStrictEqual(client.commandCalls(PublishBatchCommand).length, 1);
	});

	test(`awsSNSPublishMessageStream flushes at exactly 10 entries`, async (_t) => {
		const client = mockClient(SNSClient);
		awsSNSSetClient(client);
		client.on(PublishBatchCommand).resolves({ Successful: [], Failed: [] });

		const input = Array.from({ length: 11 }, (_v, i) => ({
			Id: `${i}`,
			Message: "x",
		}));
		await pipeline([
			createReadableStream(input),
			awsSNSPublishMessageStream({ TopicArn: "t" }),
		]);

		const calls = client.commandCalls(PublishBatchCommand);
		deepStrictEqual(calls.length, 2);
		deepStrictEqual(
			calls[0].args[0].input.PublishBatchRequestEntries.length,
			10,
		);
		deepStrictEqual(
			calls[1].args[0].input.PublishBatchRequestEntries.length,
			1,
		);
	});

	test(`awsSNSPublishMessageStream retries exactly retryMaxCount times then throws`, async (_t) => {
		const client = mockClient(SNSClient);
		awsSNSSetClient(client);
		client.on(PublishBatchCommand).resolves({
			Successful: [],
			Failed: [{ Id: "0", Code: "InternalError", SenderFault: false }],
		});

		await rejects(
			() =>
				pipeline([
					createReadableStream([{ Id: "0", Message: "a" }]),
					awsSNSPublishMessageStream({ TopicArn: "t", retryMaxCount: 2 }),
				]),
			{ message: "awsSNSPublishBatch has failed entries" },
		);
		deepStrictEqual(client.commandCalls(PublishBatchCommand).length, 3);
	});

	test(`awsSNSPublishMessageStream with retryMaxCount=1 attempts exactly twice`, async (_t) => {
		const client = mockClient(SNSClient);
		awsSNSSetClient(client);
		client.on(PublishBatchCommand).resolves({
			Successful: [],
			Failed: [{ Id: "0", Code: "InternalError", SenderFault: false }],
		});

		await rejects(
			() =>
				pipeline([
					createReadableStream([{ Id: "0", Message: "a" }]),
					awsSNSPublishMessageStream({ TopicArn: "t", retryMaxCount: 1 }),
				]),
			{ message: "awsSNSPublishBatch has failed entries" },
		);
		deepStrictEqual(client.commandCalls(PublishBatchCommand).length, 2);
	});

	test(`awsSNSPublishMessageStream keeps two entries summing to exactly 256KiB in one batch`, async (_t) => {
		const client = mockClient(SNSClient);
		awsSNSSetClient(client);
		client.on(PublishBatchCommand).resolves({ Successful: [], Failed: [] });

		const limit = 256 * 1024;
		const first = { Id: "0", Message: "x".repeat(100 * 1024) };
		const second = { Id: "1", Message: "y".repeat(limit - 100 * 1024) };

		await pipeline([
			createReadableStream([first, second]),
			awsSNSPublishMessageStream({ TopicArn: "t" }),
		]);

		const calls = client.commandCalls(PublishBatchCommand);
		deepStrictEqual(calls.length, 1);
		deepStrictEqual(
			calls[0].args[0].input.PublishBatchRequestEntries.length,
			2,
		);
	});

	test(`awsSNSSetClient routes subsequent sends to the new client`, async (_t) => {
		const first = mockClient(SNSClient);
		awsSNSSetClient(first);
		first.on(PublishBatchCommand).resolves({ Successful: [], Failed: [] });

		const second = mockClient(SNSClient);
		second.on(PublishBatchCommand).resolves({ Successful: [], Failed: [] });
		awsSNSSetClient(second);

		await pipeline([
			createReadableStream([{ Id: "0", Message: "a" }]),
			awsSNSPublishMessageStream({ TopicArn: "t" }),
		]);

		deepStrictEqual(second.commandCalls(PublishBatchCommand).length, 1);
		deepStrictEqual(first.commandCalls(PublishBatchCommand).length, 0);
	});

	// setClient must STORE the passed client; a plain stub (prototype-mock-proof)
	// proves the stored reference is used. A `setClient(){}` mutant leaves the prior
	// client in place and the stub's send would never run.
	test(`awsSNSSetClient stores the passed client reference`, async (_t) => {
		let calls = 0;
		const stub = {
			send: async () => {
				calls++;
				return { Successful: [{ Id: "0" }], Failed: [] };
			},
		};
		awsSNSSetClient(stub);

		await pipeline([
			createReadableStream([{ Id: "0", Message: "a" }]),
			awsSNSPublishMessageStream({ TopicArn: "t" }),
		]);

		deepStrictEqual(calls, 1);
	});

	test(`awsSNSPublishMessageStream uses the per-call client option and does not forward it`, async (_t) => {
		awsSNSSetClient({
			send: async () => {
				throw new Error("module client used");
			},
		});
		const inputs = [];
		const client = {
			send: async (command) => {
				inputs.push(command.input);
				return { Failed: [] };
			},
		};

		await pipeline([
			createReadableStream([{ Id: "0", Message: "a" }]),
			awsSNSPublishMessageStream({ TopicArn: "t", client }),
		]);

		deepStrictEqual(inputs, [
			{
				TopicArn: "t",
				PublishBatchRequestEntries: [{ Id: "0", Message: "a" }],
			},
		]);
	});

	test(`awsSNSPublishMessageStream forwards abortSignal to client.send`, async (_t) => {
		const client = mockClient(SNSClient);
		awsSNSSetClient(client);
		client.on(PublishBatchCommand).resolves({ Successful: [], Failed: [] });

		const controller = new AbortController();
		await pipeline([
			createReadableStream([{ Id: "0", Message: "a" }]),
			awsSNSPublishMessageStream(
				{ TopicArn: "t" },
				{ signal: controller.signal },
			),
		]);

		const calls = client.commandCalls(PublishBatchCommand);
		deepStrictEqual(calls[0].args[1]?.abortSignal, controller.signal);
	});

	test(`awsSNSPublishMessageStream aborts a pending retry backoff via signal`, async (t) => {
		t.mock.timers.enable({ apis: ["setTimeout"] });
		const client = mockClient(SNSClient);
		awsSNSSetClient(client);

		let sends = 0;
		client.on(PublishBatchCommand).callsFake(() => {
			sends++;
			return Promise.resolve({
				Successful: [],
				Failed: [{ Id: "0", Code: "ThrottlingException", SenderFault: false }],
			});
		});

		const controller = new AbortController();
		const consuming = pipeline([
			createReadableStream([{ Id: "0", Message: "a" }]),
			awsSNSPublishMessageStream(
				{ TopicArn: "t" },
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

	test(`sns has no default export`, (_t) => {
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

	test(`awsSNSPublishMessageStream destroy(error) attaches the buffered entries`, async (_t) => {
		const client = mockClient(SNSClient);
		awsSNSSetClient(client);
		client.on(PublishBatchCommand).resolves({});
		const stream = awsSNSPublishMessageStream({ TopicArn: "t" });
		const chunks = Array.from({ length: 2 }, (_v, i) => ({
			Id: `${i}`,
			Message: "m",
		}));
		await writeEach(stream, chunks);
		const error = new Error("boom");
		strictEqual(await destroyedWith(stream, error), error);
		deepStrictEqual(error.message, "boom");
		deepStrictEqual(error.failedMessages, chunks);
		deepStrictEqual(client.commandCalls(PublishBatchCommand).length, 0);
	});

	test(`awsSNSPublishMessageStream destroy() raises an error carrying the buffered entries`, async (_t) => {
		const client = mockClient(SNSClient);
		awsSNSSetClient(client);
		client.on(PublishBatchCommand).resolves({});
		const stream = awsSNSPublishMessageStream({ TopicArn: "t" });
		const chunks = Array.from({ length: 1 }, (_v, i) => ({
			Id: `${i}`,
			Message: "m",
		}));
		await writeEach(stream, chunks);
		const error = await destroyedWith(stream);
		deepStrictEqual(
			error.message,
			"awsSNSPublishBatch destroyed with buffered entries",
		);
		deepStrictEqual(error.failedMessages, chunks);
		deepStrictEqual(client.commandCalls(PublishBatchCommand).length, 0);
	});

	test(`awsSNSPublishMessageStream destroy() with nothing buffered raises no error`, async (_t) => {
		const client = mockClient(SNSClient);
		awsSNSSetClient(client);
		client.on(PublishBatchCommand).resolves({});
		const stream = awsSNSPublishMessageStream({ TopicArn: "t" });
		const closed = new Promise((resolve) => stream.once("close", resolve));
		stream.destroy();
		await closed;
		strictEqual(stream.errored, null);
		deepStrictEqual(client.commandCalls(PublishBatchCommand).length, 0);
	});

	test(`awsSNSPublishMessageStream attaches only the entries buffered after the last flush`, async (_t) => {
		const client = mockClient(SNSClient);
		awsSNSSetClient(client);
		client.on(PublishBatchCommand).resolves({});
		const stream = awsSNSPublishMessageStream({ TopicArn: "t" });
		const chunks = Array.from({ length: 11 }, (_v, i) => ({
			Id: `${i}`,
			Message: "m",
		}));
		await writeEach(stream, chunks);
		const error = await destroyedWith(stream);
		deepStrictEqual(error.failedMessages, chunks.slice(10));
		deepStrictEqual(client.commandCalls(PublishBatchCommand).length, 1);
	});

	test(`awsSNSPublishMessageStream abort attaches the buffered entries without sending`, async (_t) => {
		const client = mockClient(SNSClient);
		awsSNSSetClient(client);
		client.on(PublishBatchCommand).resolves({});
		const controller = new AbortController();
		const stream = awsSNSPublishMessageStream(
			{ TopicArn: "t" },
			{
				signal: controller.signal,
			},
		);
		const chunks = Array.from({ length: 2 }, (_v, i) => ({
			Id: `${i}`,
			Message: "m",
		}));
		await writeEach(stream, chunks);
		const closed = new Promise((resolve) => stream.once("close", resolve));
		stream.once("error", () => {});
		controller.abort();
		await closed;
		const error = stream.errored;
		deepStrictEqual(error.name, "AbortError");
		deepStrictEqual(error.failedMessages, chunks);
		deepStrictEqual(client.commandCalls(PublishBatchCommand).length, 0);
	});

	test(`awsSNSPublishMessageStream attaches buffered entries to an oversize error`, async (_t) => {
		const client = mockClient(SNSClient);
		awsSNSSetClient(client);
		client.on(PublishBatchCommand).resolves({});
		const buffered = [{ Id: "0", Message: "a" }];
		await rejects(
			() =>
				pipeline([
					createReadableStream([
						...buffered,
						{ Id: "1", Message: "x".repeat(256 * 1024 + 1) },
					]),
					awsSNSPublishMessageStream({ TopicArn: "t" }),
				]),
			(error) => {
				strictEqual(error.name, "RangeError");
				deepStrictEqual(error.failedMessages, buffered);
				return true;
			},
		);
		deepStrictEqual(client.commandCalls(PublishBatchCommand).length, 0);
	});

	const sizeOf = async (chunks) => {
		const client = mockClient(SNSClient);
		awsSNSSetClient(client);
		client.on(PublishBatchCommand).resolves({});
		await pipeline([
			createReadableStream(chunks),
			awsSNSPublishMessageStream({ TopicArn: "t" }),
		]);
		return client
			.commandCalls(PublishBatchCommand)
			.map((call) =>
				call.args[0].input.PublishBatchRequestEntries.map((entry) => entry.Id),
			);
	};

	test(`awsSNSPublishMessageStream does not count JSON escaping toward the limit`, async (_t) => {
		deepStrictEqual(
			await sizeOf([{ Id: "0", Message: '"'.repeat(200 * 1024) }]),
			[["0"]],
		);
	});

	test(`awsSNSPublishMessageStream counts multi-byte UTF-8 in the Message`, async (_t) => {
		const chunk = { Id: "0", Message: "é".repeat(262_144 / 2 + 1) };
		await rejects(
			() =>
				pipeline([
					createReadableStream([chunk]),
					awsSNSPublishMessageStream({ TopicArn: "t" }),
				]),
			(error) => {
				deepStrictEqual(error.cause, {
					Id: "0",
					bytes: 262_144 + 2,
					limit: 262_144,
				});
				return true;
			},
		);
	});

	test(`awsSNSPublishMessageStream allows a Message of exactly the limit whatever the Id`, async (_t) => {
		const Id = "i".repeat(80);
		deepStrictEqual(await sizeOf([{ Id, Message: "x".repeat(262_144) }]), [
			[Id],
		]);
	});

	test(`awsSNSPublishMessageStream counts attribute names, types and string values`, async (_t) => {
		const MessageAttributes = { k: { DataType: "String", StringValue: "v" } };
		deepStrictEqual(
			await sizeOf([
				{ Id: "0", Message: "x".repeat(262_144 - 8), MessageAttributes },
			]),
			[["0"]],
		);
		await rejects(
			() =>
				pipeline([
					createReadableStream([
						{ Id: "1", Message: "x".repeat(262_144 - 7), MessageAttributes },
					]),
					awsSNSPublishMessageStream({ TopicArn: "t" }),
				]),
			(error) => {
				deepStrictEqual(error.cause, {
					Id: "1",
					bytes: 262_144 + 1,
					limit: 262_144,
				});
				return true;
			},
		);
	});

	test(`awsSNSPublishMessageStream counts binary attribute values`, async (_t) => {
		const MessageAttributes = {
			k: { DataType: "Binary", BinaryValue: new Uint8Array(3) },
		};
		await rejects(
			() =>
				pipeline([
					createReadableStream([
						{ Id: "0", Message: "x".repeat(262_144 - 9), MessageAttributes },
					]),
					awsSNSPublishMessageStream({ TopicArn: "t" }),
				]),
			(error) => {
				deepStrictEqual(error.cause, {
					Id: "0",
					bytes: 262_144 + 1,
					limit: 262_144,
				});
				return true;
			},
		);
	});

	test(`awsSNSPublishMessageStream counts an attribute without a value by name and type`, async (_t) => {
		const MessageAttributes = { kk: { DataType: "Number" } };
		await rejects(
			() =>
				pipeline([
					createReadableStream([
						{ Id: "0", Message: "x".repeat(262_144 - 7), MessageAttributes },
					]),
					awsSNSPublishMessageStream({ TopicArn: "t" }),
				]),
			(error) => {
				deepStrictEqual(error.cause, {
					Id: "0",
					bytes: 262_144 + 1,
					limit: 262_144,
				});
				return true;
			},
		);
	});

	test(`awsSNSPublishMessageStream splits the batch one byte past the limit`, async (_t) => {
		const first = { Id: "0", Message: "x".repeat(100 * 1024) };
		deepStrictEqual(
			await sizeOf([
				first,
				{ Id: "1", Message: "y".repeat(262_144 - 100 * 1024) },
			]),
			[["0", "1"]],
		);
		deepStrictEqual(
			await sizeOf([
				first,
				{ Id: "1", Message: "y".repeat(262_144 - 100 * 1024 + 1) },
			]),
			[["0"], ["1"]],
		);
	});
});
