import { deepStrictEqual, rejects, strictEqual } from "node:assert";
import test, { describe } from "node:test";
import {
	DeleteMessageBatchCommand,
	ReceiveMessageCommand,
	SendMessageBatchCommand,
	SQSClient,
} from "@aws-sdk/client-sqs";
import * as awsModule from "@datastream/aws/sqs";
import {
	awsSQSDeleteMessageStream,
	awsSQSReceiveMessageStream,
	awsSQSSendMessageStream,
	awsSQSSetClient,
} from "@datastream/aws/sqs";
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

describe(`@datastream/aws/sqs (${variant})`, () => {
	test(`awsSQSReceiveMessageStream should get chunk`, async (_t) => {
		const client = mockClient(SQSClient);
		awsSQSSetClient(client);

		client
			.on(ReceiveMessageCommand)
			.resolvesOnce({
				Messages: [{ id: "a" }],
			})
			.resolvesOnce({
				Messages: [],
			});

		const options = {};
		const stream = await awsSQSReceiveMessageStream(options);
		const output = await streamToArray(stream);

		deepStrictEqual(output, [{ id: "a" }]);
	});

	test(`awsSQSReceiveMessageStream should handle empty queue (Messages: undefined)`, async (_t) => {
		const client = mockClient(SQSClient);
		awsSQSSetClient(client);

		client.on(ReceiveMessageCommand).resolves({});

		const options = {};
		const stream = await awsSQSReceiveMessageStream(options);
		const output = await streamToArray(stream);

		deepStrictEqual(output, []);
	});

	test(`awsSQSReceiveMessageStream should keep polling with pollingActive option`, async (_t) => {
		const client = mockClient(SQSClient);
		awsSQSSetClient(client);

		client
			.on(ReceiveMessageCommand)
			.resolvesOnce({ Messages: [{ id: "a" }] })
			.resolvesOnce({ Messages: [] })
			.resolvesOnce({ Messages: [{ id: "b" }] })
			.callsFake(neverResolves);

		const options = { pollingActive: true, pollingDelay: 0 };
		const stream = await awsSQSReceiveMessageStream(options);

		// Collect first 2 items manually since pollingActive means infinite generator
		const output = [];
		for await (const item of stream) {
			output.push(item);
			if (output.length >= 2) break;
		}

		deepStrictEqual(output, [{ id: "a" }, { id: "b" }]);
	});

	test(`awsSQSReceiveMessageStream should delay polling when pollingActive and queue is empty`, async (t) => {
		t.mock.timers.enable({ apis: ["setTimeout"] });

		const client = mockClient(SQSClient);
		awsSQSSetClient(client);

		client
			.on(ReceiveMessageCommand)
			.resolvesOnce({ Messages: [] })
			.resolvesOnce({ Messages: [{ id: "a" }] })
			.resolves({ Messages: [] });

		const options = { pollingActive: true, pollingDelay: 1000 };
		const stream = await awsSQSReceiveMessageStream(options);

		const output = [];
		const consuming = (async () => {
			for await (const item of stream) {
				output.push(item);
				if (output.length >= 1) break;
			}
		})();

		// Allow microtasks to settle so generator reaches setTimeout
		await new Promise((resolve) => setImmediate(resolve));
		// Advance mock timer past pollingDelay
		t.mock.timers.tick(1000);

		await consuming;

		deepStrictEqual(output, [{ id: "a" }]);
	});

	test(`awsSQSReceiveMessageStream should abort the idle poll delay on signal`, async (_t) => {
		const client = mockClient(SQSClient);
		awsSQSSetClient(client);

		client.on(ReceiveMessageCommand).resolves({ Messages: [] });

		const controller = new AbortController();
		const options = { pollingActive: true, pollingDelay: 60_000 };
		const stream = await awsSQSReceiveMessageStream(options, {
			signal: controller.signal,
		});

		const consuming = (async () => {
			for await (const _item of stream) {
				// queue is always empty
			}
		})();

		await new Promise((resolve) => setImmediate(resolve));
		controller.abort();

		await rejects(consuming, (error) => {
			return error.cause?.code === "AbortError" || /Abort/i.test(error.message);
		});
	});

	test(`awsSQSReceiveMessageStream should not delay polling when messages are returned`, async (_t) => {
		const client = mockClient(SQSClient);
		awsSQSSetClient(client);

		client
			.on(ReceiveMessageCommand)
			.resolvesOnce({ Messages: [{ id: "a" }] })
			.resolvesOnce({ Messages: [{ id: "b" }] })
			.resolves({ Messages: [] });

		const options = { pollingActive: true, pollingDelay: 1000 };
		const stream = await awsSQSReceiveMessageStream(options);

		const output = [];
		for await (const item of stream) {
			output.push(item);
			if (output.length >= 2) break;
		}

		deepStrictEqual(output, [{ id: "a" }, { id: "b" }]);
	});

	test(`awsSQSDeleteMessageStream should delete chunk`, async (_t) => {
		const client = mockClient(SQSClient);
		awsSQSSetClient(client);

		const input = "abcdefghijk".split("").map((Id) => ({ Id }));
		const options = {
			QueueUrl: "https://sqs.us-east-1.amazonaws.com/000000000000/test",
		};

		client
			.on(DeleteMessageBatchCommand, {
				Entries: "abcdefghij".split("").map((Id) => ({ Id })),
			})
			.resolves({})
			.on(DeleteMessageBatchCommand, {
				Entries: "k".split("").map((Id) => ({ Id })),
			})
			.resolves({});

		const stream = [
			createReadableStream(input),
			awsSQSDeleteMessageStream(options),
		];
		const result = await pipeline(stream);

		deepStrictEqual(result, {});
	});

	test(`awsSQSSendMessageStream should put chunk`, async (_t) => {
		const client = mockClient(SQSClient);
		awsSQSSetClient(client);

		const input = "abcdefghijk".split("").map((id) => ({ id }));
		const options = {
			QueueUrl: "https://sqs.us-east-1.amazonaws.com/000000000000/test",
		};

		client
			.on(SendMessageBatchCommand, {
				Entries: "abcdefghij".split("").map((id) => ({ id })),
			})
			.resolves({})
			.on(SendMessageBatchCommand, {
				Entries: "k".split("").map((id) => ({ id })),
			})
			.resolves({});

		const stream = [
			createReadableStream(input),
			awsSQSSendMessageStream(options),
		];
		const result = await pipeline(stream);

		deepStrictEqual(result, {});
	});

	test(`awsSQSDeleteMessageStream should handle empty input`, async (_t) => {
		const client = mockClient(SQSClient);
		awsSQSSetClient(client);

		const input = [];
		const options = {
			QueueUrl: "https://sqs.us-east-1.amazonaws.com/000000000000/test",
		};

		const stream = [
			createReadableStream(input),
			awsSQSDeleteMessageStream(options),
		];
		const result = await pipeline(stream);

		deepStrictEqual(result, {});
	});

	test(`awsSQSSendMessageStream should handle empty input`, async (_t) => {
		const client = mockClient(SQSClient);
		awsSQSSetClient(client);

		const input = [];
		const options = {
			QueueUrl: "https://sqs.us-east-1.amazonaws.com/000000000000/test",
		};

		const stream = [
			createReadableStream(input),
			awsSQSSendMessageStream(options),
		];
		const result = await pipeline(stream);

		deepStrictEqual(result, {});
	});

	// *** Failed entries (data loss) *** //
	test(`awsSQSSendMessageStream should retry failed entries`, async (_t) => {
		const client = mockClient(SQSClient);
		awsSQSSetClient(client);

		const input = [
			{ Id: "0", MessageBody: "a" },
			{ Id: "1", MessageBody: "b" },
		];
		const options = {
			QueueUrl: "https://sqs.us-east-1.amazonaws.com/000000000000/test",
		};

		client
			.on(SendMessageBatchCommand)
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
			awsSQSSendMessageStream(options),
		];
		await pipeline(stream);

		const calls = client.commandCalls(SendMessageBatchCommand);
		deepStrictEqual(calls.length, 2);
		deepStrictEqual(calls[1].args[0].input.Entries, [
			{ Id: "1", MessageBody: "b" },
		]);
	});

	test(`awsSQSSendMessageStream should throw when entries keep failing`, async (_t) => {
		const client = mockClient(SQSClient);
		awsSQSSetClient(client);

		const input = [{ Id: "0", MessageBody: "a" }];
		const options = {
			QueueUrl: "https://sqs.us-east-1.amazonaws.com/000000000000/test",
			retryMaxCount: 0,
		};

		client.on(SendMessageBatchCommand).resolves({
			Successful: [],
			Failed: [{ Id: "0", Code: "InternalError", SenderFault: false }],
		});

		const stream = [
			createReadableStream(input),
			awsSQSSendMessageStream(options),
		];
		await rejects(() => pipeline(stream), {
			message: "awsSQSSendMessageBatch has failed entries",
		});
	});

	test(`awsSQSDeleteMessageStream should throw when entries keep failing`, async (_t) => {
		const client = mockClient(SQSClient);
		awsSQSSetClient(client);

		const input = [{ Id: "0", ReceiptHandle: "r" }];
		const options = {
			QueueUrl: "https://sqs.us-east-1.amazonaws.com/000000000000/test",
			retryMaxCount: 0,
		};

		client.on(DeleteMessageBatchCommand).resolves({
			Successful: [],
			Failed: [{ Id: "0", Code: "InternalError", SenderFault: false }],
		});

		const stream = [
			createReadableStream(input),
			awsSQSDeleteMessageStream(options),
		];
		await rejects(() => pipeline(stream), {
			message: "awsSQSDeleteMessageBatch has failed entries",
		});
	});

	// SenderFault: true marks a permanent (caller-side) failure: it is set aside
	// (never retried) while retryable failures in the same batch keep backing
	// off, then the stream throws with every entry that did not get through.
	test(`awsSQSSendMessageStream retries retryable failures alongside a SenderFault entry, then throws`, async (_t) => {
		const permanent = {
			Id: "1",
			Code: "InvalidParameterValue",
			SenderFault: true,
		};
		const sent = [];
		awsSQSSetClient({
			send: async (command) => {
				sent.push(command.input.Entries);
				return sent.length === 1
					? {
							Successful: [],
							Failed: [
								{ Id: "0", Code: "ThrottlingException", SenderFault: false },
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
						{ Id: "0", MessageBody: "a" },
						{ Id: "1", MessageBody: "b" },
					]),
					awsSQSSendMessageStream({ QueueUrl: "q", retryMaxCount: 1 }),
				]),
			(error) => {
				deepStrictEqual(
					error.message,
					"awsSQSSendMessageBatch has failed entries",
				);
				deepStrictEqual(error.cause, [permanent]);
				return true;
			},
		);
		// Only the retryable entry is re-sent; the SenderFault entry is not.
		deepStrictEqual(sent, [
			[
				{ Id: "0", MessageBody: "a" },
				{ Id: "1", MessageBody: "b" },
			],
			[{ Id: "0", MessageBody: "a" }],
		]);
	});

	test(`awsSQSSendMessageStream error cause holds SenderFault and still-failing entries once retries run out`, async (_t) => {
		const permanent = {
			Id: "1",
			Code: "InvalidParameterValue",
			SenderFault: true,
		};
		const stillFailing = {
			Id: "0",
			Code: "ThrottlingException",
			SenderFault: false,
		};
		let calls = 0;
		awsSQSSetClient({
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
						{ Id: "0", MessageBody: "a" },
						{ Id: "1", MessageBody: "b" },
					]),
					awsSQSSendMessageStream({ QueueUrl: "q", retryMaxCount: 1 }),
				]),
			(error) => {
				deepStrictEqual(
					error.message,
					"awsSQSSendMessageBatch has failed entries",
				);
				deepStrictEqual(error.cause, [permanent, stillFailing]);
				return true;
			},
		);
		deepStrictEqual(calls, 2);
	});

	test(`awsSQSSendMessageStream throws without retrying when every failed entry has SenderFault`, async (_t) => {
		const failed = [
			{ Id: "1", Code: "InvalidParameterValue", SenderFault: true },
		];
		let calls = 0;
		awsSQSSetClient({
			send: async () => {
				calls++;
				return { Successful: [{ Id: "0" }], Failed: failed };
			},
		});

		await rejects(
			() =>
				pipeline([
					createReadableStream([
						{ Id: "0", MessageBody: "a" },
						{ Id: "1", MessageBody: "b" },
					]),
					awsSQSSendMessageStream({ QueueUrl: "q", retryMaxCount: 1 }),
				]),
			(error) => {
				deepStrictEqual(
					error.message,
					"awsSQSSendMessageBatch has failed entries",
				);
				deepStrictEqual(error.cause, failed);
				return true;
			},
		);
		deepStrictEqual(calls, 1);
	});

	test(`awsSQSSendMessageStream should flush before exceeding 1MiB`, async (_t) => {
		const client = mockClient(SQSClient);
		awsSQSSetClient(client);

		client.on(SendMessageBatchCommand).resolves({ Successful: [], Failed: [] });

		// 5 messages of ~400KB each => 2MB total, well under count of 10
		// but over the 1MiB byte limit, forcing multiple flushes.
		const body = "x".repeat(400 * 1024);
		const input = Array.from({ length: 5 }, (_v, i) => ({
			Id: `${i}`,
			MessageBody: body,
		}));
		const options = {
			QueueUrl: "https://sqs.us-east-1.amazonaws.com/000000000000/test",
		};

		const stream = [
			createReadableStream(input),
			awsSQSSendMessageStream(options),
		];
		await pipeline(stream);

		const calls = client.commandCalls(SendMessageBatchCommand);
		deepStrictEqual(calls.length > 1, true);
		for (const call of calls) {
			deepStrictEqual(call.args[0].input.Entries.length <= 2, true);
		}
	});

	test(`awsSQSSendMessageStream first retry backoff is floored at 50ms (no 1ms hammer)`, async (t) => {
		t.mock.timers.enable({ apis: ["setTimeout"] });

		const client = mockClient(SQSClient);
		awsSQSSetClient(client);

		client
			.on(SendMessageBatchCommand)
			.resolvesOnce({
				Successful: [],
				Failed: [{ Id: "0", Code: "ThrottlingException", SenderFault: false }],
			})
			.resolves({ Successful: [{ Id: "0" }], Failed: [] });

		const input = [{ Id: "0", MessageBody: "a" }];
		const options = {
			QueueUrl: "https://sqs.us-east-1.amazonaws.com/000000000000/test",
		};
		const stream = [
			createReadableStream(input),
			awsSQSSendMessageStream(options),
		];
		const consuming = pipeline(stream);

		// Let the first send + partial-failure path schedule the backoff timer.
		await new Promise((resolve) => setImmediate(resolve));
		// At 1ms the retry must NOT have fired (old behavior was 3**0 == 1ms).
		t.mock.timers.tick(1);
		await new Promise((resolve) => setImmediate(resolve));
		deepStrictEqual(client.commandCalls(SendMessageBatchCommand).length, 1);

		// Advancing to the 50ms floor lets the retry proceed.
		t.mock.timers.tick(49);
		await consuming;
		deepStrictEqual(client.commandCalls(SendMessageBatchCommand).length, 2);
	});

	test(`awsSQSSendMessageStream should reject a single entry over 1MiB`, async (_t) => {
		const client = mockClient(SQSClient);
		awsSQSSetClient(client);
		client.on(SendMessageBatchCommand).resolves({ Successful: [], Failed: [] });

		// A single message whose serialized size already exceeds the 1MiB batch
		// limit must be rejected with a descriptive error instead of being sent and
		// having SQS reject the whole batch.
		const tooBig = "x".repeat(1024 * 1024 + 1);
		const input = [{ Id: "0", MessageBody: tooBig }];
		const options = {
			QueueUrl: "https://sqs.us-east-1.amazonaws.com/000000000000/test",
		};

		const stream = [
			createReadableStream(input),
			awsSQSSendMessageStream(options),
		];
		await rejects(() => pipeline(stream), {
			name: "RangeError",
			message: "awsSQSSendMessageBatch entry exceeds 1MiB limit",
		});
		// The oversize entry must NOT have been sent to SQS.
		deepStrictEqual(client.commandCalls(SendMessageBatchCommand).length, 0);
	});

	test(`awsSQSSendMessageStream should not mutate caller options`, async (_t) => {
		const client = mockClient(SQSClient);
		awsSQSSetClient(client);
		client.on(SendMessageBatchCommand).resolves({ Successful: [], Failed: [] });

		const options = {
			QueueUrl: "https://sqs.us-east-1.amazonaws.com/000000000000/test",
		};
		const optionsCopy = { ...options };
		const input = [{ Id: "0", MessageBody: "a" }];

		const stream = [
			createReadableStream(input),
			awsSQSSendMessageStream(options),
		];
		await pipeline(stream);

		deepStrictEqual(options, optionsCopy);
	});

	test(`awsSQSReceiveMessageStream is a Readable that pipejoin accepts`, async (_t) => {
		const client = mockClient(SQSClient);
		awsSQSSetClient(client);
		client
			.on(ReceiveMessageCommand)
			.resolvesOnce({ Messages: [{ id: "a" }] })
			.resolves({ Messages: [] });

		const stream = await awsSQSReceiveMessageStream({});
		strictEqual(isReadable(stream), true);
		deepStrictEqual(await streamToArray(pipejoin([stream])), [{ id: "a" }]);
	});

	// A generator's return() only lands at its next yield; destroying the stream
	// must also abort the idle poll wait so polling stops and 'close' fires.
	test(`awsSQSReceiveMessageStream destroy stops an idle poll`, {
		timeout: 1000,
	}, async (_t) => {
		const client = mockClient(SQSClient);
		awsSQSSetClient(client);
		client.on(ReceiveMessageCommand).resolves({ Messages: [] });

		const stream = await awsSQSReceiveMessageStream({
			pollingActive: true,
			pollingDelay: 60_000,
		});
		stream.resume();
		await new Promise((resolve) => setImmediate(resolve));
		const closed = new Promise((resolve) => stream.once("close", resolve));
		stream.destroy();
		await closed;
		deepStrictEqual(client.commandCalls(ReceiveMessageCommand).length, 1);
	});

	// Named exports are canonical; a default export must not come back.
	test(`sqs has no default export`, (_t) => {
		deepStrictEqual(Object.hasOwn(awsModule, "default"), false);
	});

	// *** setClient swaps the active client *** //
	test(`awsSQSSetClient routes subsequent sends to the new client`, async (_t) => {
		const first = mockClient(SQSClient);
		awsSQSSetClient(first);
		first.on(SendMessageBatchCommand).resolves({ Successful: [], Failed: [] });

		const second = mockClient(SQSClient);
		second.on(SendMessageBatchCommand).resolves({ Successful: [], Failed: [] });
		awsSQSSetClient(second);

		await pipeline([
			createReadableStream([{ Id: "0", MessageBody: "a" }]),
			awsSQSSendMessageStream({ QueueUrl: "q" }),
		]);

		deepStrictEqual(second.commandCalls(SendMessageBatchCommand).length, 1);
		deepStrictEqual(first.commandCalls(SendMessageBatchCommand).length, 0);
	});

	// setClient must STORE the passed client; a plain stub (prototype-mock-proof)
	// proves the stored reference is used. A `setClient(){}` mutant leaves the prior
	// client in place.
	test(`awsSQSSetClient stores the passed client reference`, async (_t) => {
		let calls = 0;
		const stub = {
			send: async () => {
				calls++;
				return { Messages: calls === 1 ? [{ id: "stub" }] : [] };
			},
		};
		awsSQSSetClient(stub);

		// Not polling: one page with messages, then the empty page ends the stream.
		const stream = await awsSQSReceiveMessageStream({});
		const output = await streamToArray(stream);

		deepStrictEqual(output, [{ id: "stub" }]);
		deepStrictEqual(calls, 2);
	});

	// A per-call `client` option must route that call's sends to it (not the
	// module client) and must not be forwarded into the command input.
	test(`awsSQS streams use the per-call client option and do not forward it`, async (_t) => {
		awsSQSSetClient({
			send: async () => {
				throw new Error("module client used");
			},
		});
		const inputs = [];
		const client = {
			send: async (command) => {
				inputs.push(command.input);
				return command.input.Entries ? { Failed: [] } : {};
			},
		};

		const received = await streamToArray(
			await awsSQSReceiveMessageStream({ QueueUrl: "q", client }),
		);
		await pipeline([
			createReadableStream([{ Id: "0", MessageBody: "a" }]),
			awsSQSSendMessageStream({ QueueUrl: "q", client }),
		]);
		await pipeline([
			createReadableStream([{ Id: "1", ReceiptHandle: "r" }]),
			awsSQSDeleteMessageStream({ QueueUrl: "q", client }),
		]);

		deepStrictEqual(received, []);
		deepStrictEqual(inputs, [
			{ QueueUrl: "q" },
			{ QueueUrl: "q", Entries: [{ Id: "0", MessageBody: "a" }] },
			{ QueueUrl: "q", Entries: [{ Id: "1", ReceiptHandle: "r" }] },
		]);
	});

	// *** idle-wait guard conjuncts (mock timers) *** //
	test(`awsSQSReceiveMessageStream does not schedule a timer when messages are present`, async (t) => {
		t.mock.timers.enable({ apis: ["setTimeout"] });
		const client = mockClient(SQSClient);
		awsSQSSetClient(client);
		// Messages always present so `messages.length === 0` is false -> no idle wait.
		// A `true` mutant on that conjunct would delay after every poll and the
		// consumer would stall on the un-ticked mock timer.
		client.on(ReceiveMessageCommand).resolves({ Messages: [{ id: "a" }] });

		const options = { pollingActive: true, pollingDelay: 60_000 };
		const stream = await awsSQSReceiveMessageStream(options);

		const output = [];
		for await (const item of stream) {
			output.push(item);
			if (output.length >= 3) break;
		}
		deepStrictEqual(output.length, 3);
	});

	test(`awsSQSReceiveMessageStream pollingDelay 0 schedules no timer`, async (t) => {
		t.mock.timers.enable({ apis: ["setTimeout"] });
		const client = mockClient(SQSClient);
		awsSQSSetClient(client);
		// pollingActive, empty queue, pollingDelay === 0: `pollingDelay > 0` is false
		// so no idle wait. A `> 0` -> `true` or `>= 0` mutant would await timeout(0)
		// which the un-ticked mock timer never resolves.
		client
			.on(ReceiveMessageCommand)
			.resolvesOnce({ Messages: [] })
			.resolves({ Messages: [{ id: "a" }] });

		const options = { pollingActive: true, pollingDelay: 0 };
		const stream = await awsSQSReceiveMessageStream(options);

		const output = [];
		for await (const item of stream) {
			output.push(item);
			if (output.length >= 1) break;
		}
		deepStrictEqual(output, [{ id: "a" }]);
	});

	test(`awsSQSReceiveMessageStream non-polling empty page ends without an idle wait`, async (t) => {
		t.mock.timers.enable({ apis: ["setTimeout"] });
		const client = mockClient(SQSClient);
		awsSQSSetClient(client);
		// Not polling: an empty page ends the stream WITHOUT entering the idle wait.
		// The `&&` -> `||` mutant makes `pollingActive || messages.length === 0` true
		// on the empty page, scheduling a (default 1000ms) timeout the un-ticked mock
		// timer never resolves -> the stream would hang instead of completing.
		client
			.on(ReceiveMessageCommand)
			.resolvesOnce({ Messages: [{ id: "a" }] })
			.resolves({ Messages: [] });

		const stream = await awsSQSReceiveMessageStream({});
		const output = await streamToArray(stream);

		deepStrictEqual(output, [{ id: "a" }]);
	});

	// *** backoff is abortable (signal forwarded to the timeout) *** //
	test(`awsSQSSendMessageStream aborts during retry backoff`, async (t) => {
		t.mock.timers.enable({ apis: ["setTimeout"] });
		const client = mockClient(SQSClient);
		awsSQSSetClient(client);
		// Persistent partial failure so the stream enters the backoff.
		let sends = 0;
		client.on(SendMessageBatchCommand).callsFake(() => {
			sends++;
			return Promise.resolve({
				Successful: [],
				Failed: [{ Id: "0", Code: "ThrottlingException", SenderFault: false }],
			});
		});

		const controller = new AbortController();
		const consuming = pipeline([
			createReadableStream([{ Id: "0", MessageBody: "a" }]),
			awsSQSSendMessageStream({ QueueUrl: "q" }, { signal: controller.signal }),
		]);

		// Let the first send fail and enter the backoff timer, then abort.
		await new Promise((resolve) => setImmediate(resolve));
		controller.abort();

		await rejects(consuming, (error) => {
			return error.cause?.code === "AbortError" || /Abort/i.test(error.message);
		});
		// The backoff timeout was given `{ signal }`; a `{}` mutant (dropping the
		// signal) leaves the timer pending, so advancing the mocked clock past it
		// wakes it and retries -> more than one send.
		t.mock.timers.tick(60_000);
		await new Promise((resolve) => setImmediate(resolve));
		deepStrictEqual(sends, 1);
	});

	// *** AbortSignal forwarding *** //
	test(`awsSQSReceiveMessageStream forwards abortSignal to client.send`, async (_t) => {
		const client = mockClient(SQSClient);
		awsSQSSetClient(client);
		client
			.on(ReceiveMessageCommand)
			.resolvesOnce({ Messages: [{ id: "a" }] })
			.resolves({ Messages: [] });

		const controller = new AbortController();
		const stream = await awsSQSReceiveMessageStream(
			{},
			{ signal: controller.signal },
		);
		await streamToArray(stream);

		const calls = client.commandCalls(ReceiveMessageCommand);
		// Forwarded as a signal that follows streamOptions.signal (and also
		// aborts when the stream is destroyed early).
		const sent = calls[0].args[1]?.abortSignal;
		deepStrictEqual(sent.aborted, false);
		controller.abort();
		deepStrictEqual(sent.aborted, true);
	});

	test(`awsSQSSendMessageStream forwards abortSignal to client.send`, async (_t) => {
		const client = mockClient(SQSClient);
		awsSQSSetClient(client);
		client.on(SendMessageBatchCommand).resolves({ Successful: [], Failed: [] });

		const controller = new AbortController();
		await pipeline([
			createReadableStream([{ Id: "0", MessageBody: "a" }]),
			awsSQSSendMessageStream({ QueueUrl: "q" }, { signal: controller.signal }),
		]);

		const calls = client.commandCalls(SendMessageBatchCommand);
		deepStrictEqual(calls[0].args[1]?.abortSignal, controller.signal);
	});

	// *** Error cause carries the Failed subset *** //
	test(`awsSQSSendMessageStream error cause is the Failed entries array`, async (_t) => {
		const client = mockClient(SQSClient);
		awsSQSSetClient(client);

		const failed = [
			{ Id: "0", Code: "InternalError", SenderFault: false, Message: "boom" },
		];
		client
			.on(SendMessageBatchCommand)
			.resolves({ Successful: [], Failed: failed });

		await rejects(
			() =>
				pipeline([
					createReadableStream([{ Id: "0", MessageBody: "a" }]),
					awsSQSSendMessageStream({ QueueUrl: "q", retryMaxCount: 0 }),
				]),
			(error) => {
				deepStrictEqual(
					error.message,
					"awsSQSSendMessageBatch has failed entries",
				);
				deepStrictEqual(error.cause, failed);
				return true;
			},
		);
	});

	// *** Oversize entry: error cause shape and chunk?.Id *** //
	test(`awsSQSSendMessageStream oversize error cause carries Id, bytes and limit`, async (_t) => {
		const client = mockClient(SQSClient);
		awsSQSSetClient(client);
		client.on(SendMessageBatchCommand).resolves({ Successful: [], Failed: [] });

		const tooBig = "x".repeat(1024 * 1024 + 1);
		const chunk = { Id: "the-id", MessageBody: tooBig };
		const expectedBytes = 1024 * 1024 + 1;

		await rejects(
			() =>
				pipeline([
					createReadableStream([chunk]),
					awsSQSSendMessageStream({ QueueUrl: "q" }),
				]),
			(error) => {
				deepStrictEqual(
					error.message,
					"awsSQSSendMessageBatch entry exceeds 1MiB limit",
				);
				deepStrictEqual(error.cause, {
					Id: "the-id",
					bytes: expectedBytes,
					limit: 1024 * 1024,
				});
				return true;
			},
		);
	});

	// *** Per-entry boundary: exactly 1MiB is allowed (strict `>`) *** //
	test(`awsSQSSendMessageStream allows an entry whose message size is exactly 1MiB`, async (_t) => {
		const client = mockClient(SQSClient);
		awsSQSSetClient(client);
		client.on(SendMessageBatchCommand).resolves({ Successful: [], Failed: [] });

		const chunk = { Id: "0", MessageBody: "x".repeat(1024 * 1024) };

		await pipeline([
			createReadableStream([chunk]),
			awsSQSSendMessageStream({ QueueUrl: "q" }),
		]);

		// Exactly at the limit is allowed (strict `>`), so it is sent.
		deepStrictEqual(client.commandCalls(SendMessageBatchCommand).length, 1);
	});

	// *** Delete entries carry no message, so only the 10-entry cap applies *** //
	test(`awsSQSDeleteMessageStream batches by count only`, async (_t) => {
		const client = mockClient(SQSClient);
		awsSQSSetClient(client);
		client
			.on(DeleteMessageBatchCommand)
			.resolves({ Successful: [], Failed: [] });

		// DeleteMessageBatch has no message-byte limit. Received messages piped
		// straight in still carry their attributes (here 2MiB in total), which
		// must not split or reject the batch.
		const input = Array.from({ length: 10 }, (_v, i) => ({
			Id: `${i}`,
			ReceiptHandle: "r",
			MessageAttributes: {
				k: { DataType: "String", StringValue: "v".repeat(200 * 1024) },
			},
		}));
		await pipeline([
			createReadableStream(input),
			awsSQSDeleteMessageStream({ QueueUrl: "q" }),
		]);
		const calls = client.commandCalls(DeleteMessageBatchCommand);
		deepStrictEqual(calls.length, 1);
		deepStrictEqual(calls[0].args[0].input.Entries.length, 10);
	});

	// *** Count cap split at exactly 10 entries *** //
	test(`awsSQSSendMessageStream flushes at exactly 10 entries`, async (_t) => {
		const client = mockClient(SQSClient);
		awsSQSSetClient(client);
		client.on(SendMessageBatchCommand).resolves({ Successful: [], Failed: [] });

		// 11 tiny entries: the count cap (10) forces a flush of 10, then 1.
		const input = Array.from({ length: 11 }, (_v, i) => ({
			Id: `${i}`,
			MessageBody: "x",
		}));
		await pipeline([
			createReadableStream(input),
			awsSQSSendMessageStream({ QueueUrl: "q" }),
		]);

		const calls = client.commandCalls(SendMessageBatchCommand);
		deepStrictEqual(calls.length, 2);
		deepStrictEqual(calls[0].args[0].input.Entries.length, 10);
		deepStrictEqual(calls[1].args[0].input.Entries.length, 1);
	});

	// *** Receive without pollingActive stops once the queue drains *** //
	test(`awsSQSReceiveMessageStream without pollingActive stops when queue empties`, async (_t) => {
		const client = mockClient(SQSClient);
		awsSQSSetClient(client);
		client
			.on(ReceiveMessageCommand)
			.resolvesOnce({ Messages: [{ id: "a" }] })
			.resolves({ Messages: [] });

		const stream = await awsSQSReceiveMessageStream({});
		const output = await streamToArray(stream);

		deepStrictEqual(output, [{ id: "a" }]);
		// One call returns a message (keep going), the next is empty (stop):
		// expectMore = pollingActive(false) || messages.length > 0.
		deepStrictEqual(client.commandCalls(ReceiveMessageCommand).length, 2);
	});

	// *** retryMaxCount is an inclusive ceiling on the number of retries *** //
	test(`awsSQSSendMessageStream retries exactly retryMaxCount times then throws`, async (_t) => {
		const client = mockClient(SQSClient);
		awsSQSSetClient(client);
		client.on(SendMessageBatchCommand).resolves({
			Successful: [],
			Failed: [{ Id: "0", Code: "InternalError", SenderFault: false }],
		});

		await rejects(
			() =>
				pipeline([
					createReadableStream([{ Id: "0", MessageBody: "a" }]),
					awsSQSSendMessageStream({ QueueUrl: "q", retryMaxCount: 2 }),
				]),
			{ message: "awsSQSSendMessageBatch has failed entries" },
		);
		// initial attempt + 2 retries == 3 calls (kills `>=`->`>` and `++`->`--`).
		deepStrictEqual(client.commandCalls(SendMessageBatchCommand).length, 3);
	});

	test(`awsSQSSendMessageStream with retryMaxCount=1 attempts exactly twice`, async (_t) => {
		const client = mockClient(SQSClient);
		awsSQSSetClient(client);
		client.on(SendMessageBatchCommand).resolves({
			Successful: [],
			Failed: [{ Id: "0", Code: "InternalError", SenderFault: false }],
		});

		await rejects(
			() =>
				pipeline([
					createReadableStream([{ Id: "0", MessageBody: "a" }]),
					awsSQSSendMessageStream({ QueueUrl: "q", retryMaxCount: 1 }),
				]),
			{ message: "awsSQSSendMessageBatch has failed entries" },
		);
		deepStrictEqual(client.commandCalls(SendMessageBatchCommand).length, 2);
	});

	// *** retryMaxCount: null = unlimited retries (undefined keeps the default) *** //
	test(`awsSQSSendMessageStream retries without limit when retryMaxCount is null`, async (t) => {
		const client = mockClient(SQSClient);
		awsSQSSetClient(client);
		const failed = {
			Successful: [],
			Failed: [{ Id: "0", Code: "InternalError", SenderFault: false }],
		};
		// 11 failures: one more than the default retryMaxCount of 10. Backoff
		// timers fire on the next macrotask so the retries take no real time.
		t.mock.method(globalThis, "setTimeout", (fn) => setImmediate(fn));
		const mock = client.on(SendMessageBatchCommand);
		for (let i = 0; i < 11; i++) mock.resolvesOnce(failed);
		mock.resolves({ Successful: [{ Id: "0" }], Failed: [] });

		await pipeline([
			createReadableStream([{ Id: "0", MessageBody: "a" }]),
			awsSQSSendMessageStream({ QueueUrl: "q", retryMaxCount: null }),
		]);
		deepStrictEqual(client.commandCalls(SendMessageBatchCommand).length, 12);
	});

	// *** batch byte-cap is exclusive: two entries summing to exactly 1MiB stay
	// together in a single batch (strict `>`) *** //
	test(`awsSQSSendMessageStream keeps two entries summing to exactly 1MiB in one batch`, async (_t) => {
		const client = mockClient(SQSClient);
		awsSQSSetClient(client);
		client.on(SendMessageBatchCommand).resolves({ Successful: [], Failed: [] });

		const limit = 1024 * 1024;
		const first = { Id: "0", MessageBody: "x".repeat(100 * 1024) };
		const second = { Id: "1", MessageBody: "y".repeat(limit - 100 * 1024) };

		await pipeline([
			createReadableStream([first, second]),
			awsSQSSendMessageStream({ QueueUrl: "q" }),
		]);

		// Sum is exactly at the cap: `> cap` is false, both go in ONE batch. A
		// `>= cap` mutant would split them.
		const calls = client.commandCalls(SendMessageBatchCommand);
		deepStrictEqual(calls.length, 1);
		deepStrictEqual(calls[0].args[0].input.Entries.length, 2);
	});

	// --- buffered entries on destroy / abort ---
	// final() never runs on destroy() or an abort, so entries still buffered
	// below the batch limit are handed to the teardown error as failedMessages
	// (the kafkaProduceStream contract) and nothing is sent.
	const writeEach = async (stream, chunks) => {
		for (const chunk of chunks) {
			await new Promise((resolve, reject) =>
				stream.write(chunk, (error) => (error ? reject(error) : resolve())),
			);
		}
	};
	// Resolves stream.errored once closed (null when no error was raised).
	const destroyedWith = async (stream, error) => {
		const closed = new Promise((resolve) => stream.once("close", resolve));
		stream.once("error", () => {});
		stream.destroy(error);
		await closed;
		return stream.errored;
	};

	test(`awsSQSSendMessageStream destroy(error) attaches the buffered entries`, async (_t) => {
		const client = mockClient(SQSClient);
		awsSQSSetClient(client);
		client.on(SendMessageBatchCommand).resolves({});
		const stream = awsSQSSendMessageStream({ QueueUrl: "q" });
		const chunks = Array.from({ length: 2 }, (_v, i) => ({
			Id: `${i}`,
			MessageBody: "m",
		}));
		await writeEach(stream, chunks);
		const error = new Error("boom");
		strictEqual(await destroyedWith(stream, error), error);
		deepStrictEqual(error.message, "boom");
		deepStrictEqual(error.failedMessages, chunks);
		deepStrictEqual(client.commandCalls(SendMessageBatchCommand).length, 0);
	});

	test(`awsSQSSendMessageStream destroy() raises an error carrying the buffered entries`, async (_t) => {
		const client = mockClient(SQSClient);
		awsSQSSetClient(client);
		client.on(SendMessageBatchCommand).resolves({});
		const stream = awsSQSSendMessageStream({ QueueUrl: "q" });
		const chunks = Array.from({ length: 1 }, (_v, i) => ({
			Id: `${i}`,
			MessageBody: "m",
		}));
		await writeEach(stream, chunks);
		const error = await destroyedWith(stream);
		deepStrictEqual(
			error.message,
			"awsSQSSendMessageBatch destroyed with buffered entries",
		);
		deepStrictEqual(error.failedMessages, chunks);
		deepStrictEqual(client.commandCalls(SendMessageBatchCommand).length, 0);
	});

	test(`awsSQSSendMessageStream destroy() with nothing buffered raises no error`, async (_t) => {
		const client = mockClient(SQSClient);
		awsSQSSetClient(client);
		client.on(SendMessageBatchCommand).resolves({});
		const stream = awsSQSSendMessageStream({ QueueUrl: "q" });
		const closed = new Promise((resolve) => stream.once("close", resolve));
		stream.destroy();
		await closed;
		strictEqual(stream.errored, null);
		deepStrictEqual(client.commandCalls(SendMessageBatchCommand).length, 0);
	});

	test(`awsSQSSendMessageStream attaches only the entries buffered after the last flush`, async (_t) => {
		const client = mockClient(SQSClient);
		awsSQSSetClient(client);
		client.on(SendMessageBatchCommand).resolves({});
		const stream = awsSQSSendMessageStream({ QueueUrl: "q" });
		const chunks = Array.from({ length: 11 }, (_v, i) => ({
			Id: `${i}`,
			MessageBody: "m",
		}));
		await writeEach(stream, chunks);
		const error = await destroyedWith(stream);
		deepStrictEqual(error.failedMessages, chunks.slice(10));
		deepStrictEqual(client.commandCalls(SendMessageBatchCommand).length, 1);
	});

	test(`awsSQSSendMessageStream abort attaches the buffered entries without sending`, async (_t) => {
		const client = mockClient(SQSClient);
		awsSQSSetClient(client);
		client.on(SendMessageBatchCommand).resolves({});
		const controller = new AbortController();
		const stream = awsSQSSendMessageStream(
			{ QueueUrl: "q" },
			{
				signal: controller.signal,
			},
		);
		const chunks = Array.from({ length: 2 }, (_v, i) => ({
			Id: `${i}`,
			MessageBody: "m",
		}));
		await writeEach(stream, chunks);
		const closed = new Promise((resolve) => stream.once("close", resolve));
		stream.once("error", () => {});
		controller.abort();
		await closed;
		const error = stream.errored;
		deepStrictEqual(error.name, "AbortError");
		deepStrictEqual(error.failedMessages, chunks);
		deepStrictEqual(client.commandCalls(SendMessageBatchCommand).length, 0);
	});

	test(`awsSQSDeleteMessageStream destroy(error) attaches the buffered entries`, async (_t) => {
		const client = mockClient(SQSClient);
		awsSQSSetClient(client);
		client.on(DeleteMessageBatchCommand).resolves({});
		const stream = awsSQSDeleteMessageStream({ QueueUrl: "q" });
		const chunks = Array.from({ length: 2 }, (_v, i) => ({
			Id: `${i}`,
			ReceiptHandle: "r",
		}));
		await writeEach(stream, chunks);
		const error = new Error("boom");
		strictEqual(await destroyedWith(stream, error), error);
		deepStrictEqual(error.message, "boom");
		deepStrictEqual(error.failedMessages, chunks);
		deepStrictEqual(client.commandCalls(DeleteMessageBatchCommand).length, 0);
	});

	test(`awsSQSDeleteMessageStream destroy() raises an error carrying the buffered entries`, async (_t) => {
		const client = mockClient(SQSClient);
		awsSQSSetClient(client);
		client.on(DeleteMessageBatchCommand).resolves({});
		const stream = awsSQSDeleteMessageStream({ QueueUrl: "q" });
		const chunks = Array.from({ length: 1 }, (_v, i) => ({
			Id: `${i}`,
			ReceiptHandle: "r",
		}));
		await writeEach(stream, chunks);
		const error = await destroyedWith(stream);
		deepStrictEqual(
			error.message,
			"awsSQSDeleteMessageBatch destroyed with buffered entries",
		);
		deepStrictEqual(error.failedMessages, chunks);
		deepStrictEqual(client.commandCalls(DeleteMessageBatchCommand).length, 0);
	});

	test(`awsSQSDeleteMessageStream destroy() with nothing buffered raises no error`, async (_t) => {
		const client = mockClient(SQSClient);
		awsSQSSetClient(client);
		client.on(DeleteMessageBatchCommand).resolves({});
		const stream = awsSQSDeleteMessageStream({ QueueUrl: "q" });
		const closed = new Promise((resolve) => stream.once("close", resolve));
		stream.destroy();
		await closed;
		strictEqual(stream.errored, null);
		deepStrictEqual(client.commandCalls(DeleteMessageBatchCommand).length, 0);
	});

	test(`awsSQSDeleteMessageStream attaches only the entries buffered after the last flush`, async (_t) => {
		const client = mockClient(SQSClient);
		awsSQSSetClient(client);
		client.on(DeleteMessageBatchCommand).resolves({});
		const stream = awsSQSDeleteMessageStream({ QueueUrl: "q" });
		const chunks = Array.from({ length: 11 }, (_v, i) => ({
			Id: `${i}`,
			ReceiptHandle: "r",
		}));
		await writeEach(stream, chunks);
		const error = await destroyedWith(stream);
		deepStrictEqual(error.failedMessages, chunks.slice(10));
		deepStrictEqual(client.commandCalls(DeleteMessageBatchCommand).length, 1);
	});

	test(`awsSQSDeleteMessageStream abort attaches the buffered entries without sending`, async (_t) => {
		const client = mockClient(SQSClient);
		awsSQSSetClient(client);
		client.on(DeleteMessageBatchCommand).resolves({});
		const controller = new AbortController();
		const stream = awsSQSDeleteMessageStream(
			{ QueueUrl: "q" },
			{
				signal: controller.signal,
			},
		);
		const chunks = Array.from({ length: 2 }, (_v, i) => ({
			Id: `${i}`,
			ReceiptHandle: "r",
		}));
		await writeEach(stream, chunks);
		const closed = new Promise((resolve) => stream.once("close", resolve));
		stream.once("error", () => {});
		controller.abort();
		await closed;
		const error = stream.errored;
		deepStrictEqual(error.name, "AbortError");
		deepStrictEqual(error.failedMessages, chunks);
		deepStrictEqual(client.commandCalls(DeleteMessageBatchCommand).length, 0);
	});

	// --- message size: what SQS counts ---
	// SQS counts the UTF-8 bytes of the body plus each message attribute's
	// name, data type and value; Id and JSON escaping do not count.
	const sizeOf = async (chunks) => {
		const client = mockClient(SQSClient);
		awsSQSSetClient(client);
		client.on(SendMessageBatchCommand).resolves({});
		await pipeline([
			createReadableStream(chunks),
			awsSQSSendMessageStream({ QueueUrl: "q" }),
		]);
		return client
			.commandCalls(SendMessageBatchCommand)
			.map((call) => call.args[0].input.Entries.map((entry) => entry.Id));
	};

	test(`awsSQSSendMessageStream does not count JSON escaping toward the limit`, async (_t) => {
		// 600KiB of quotes is 600KiB of UTF-8, though JSON.stringify doubles it.
		deepStrictEqual(
			await sizeOf([{ Id: "0", MessageBody: '"'.repeat(600 * 1024) }]),
			[["0"]],
		);
	});

	test(`awsSQSSendMessageStream counts multi-byte UTF-8 in the MessageBody`, async (_t) => {
		const chunk = { Id: "0", MessageBody: "é".repeat(1_048_576 / 2 + 1) };
		await rejects(
			() =>
				pipeline([
					createReadableStream([chunk]),
					awsSQSSendMessageStream({ QueueUrl: "q" }),
				]),
			(error) => {
				deepStrictEqual(error.cause, {
					Id: "0",
					bytes: 1_048_576 + 2,
					limit: 1_048_576,
				});
				return true;
			},
		);
	});

	test(`awsSQSSendMessageStream allows a MessageBody of exactly the limit whatever the Id`, async (_t) => {
		const Id = "i".repeat(80);
		deepStrictEqual(
			await sizeOf([{ Id, MessageBody: "x".repeat(1_048_576) }]),
			[[Id]],
		);
	});

	test(`awsSQSSendMessageStream counts attribute names, types and string values`, async (_t) => {
		// "k" + "String" + "v" = 8 bytes.
		const MessageAttributes = { k: { DataType: "String", StringValue: "v" } };
		deepStrictEqual(
			await sizeOf([
				{ Id: "0", MessageBody: "x".repeat(1_048_576 - 8), MessageAttributes },
			]),
			[["0"]],
		);
		await rejects(
			() =>
				pipeline([
					createReadableStream([
						{
							Id: "1",
							MessageBody: "x".repeat(1_048_576 - 7),
							MessageAttributes,
						},
					]),
					awsSQSSendMessageStream({ QueueUrl: "q" }),
				]),
			(error) => {
				deepStrictEqual(error.cause, {
					Id: "1",
					bytes: 1_048_576 + 1,
					limit: 1_048_576,
				});
				return true;
			},
		);
	});

	test(`awsSQSSendMessageStream counts a missing MessageBody as 0 bytes`, async (_t) => {
		// "k" + "String" + value = exactly the limit, with no body at all.
		const MessageAttributes = {
			k: { DataType: "String", StringValue: "v".repeat(1_048_576 - 7) },
		};
		deepStrictEqual(await sizeOf([{ Id: "0", MessageAttributes }]), [["0"]]);
	});

	test(`awsSQSSendMessageStream counts binary attribute values`, async (_t) => {
		// "k" + "Binary" + 3 bytes = 10 bytes.
		const MessageAttributes = {
			k: { DataType: "Binary", BinaryValue: new Uint8Array(3) },
		};
		await rejects(
			() =>
				pipeline([
					createReadableStream([
						{
							Id: "0",
							MessageBody: "x".repeat(1_048_576 - 9),
							MessageAttributes,
						},
					]),
					awsSQSSendMessageStream({ QueueUrl: "q" }),
				]),
			(error) => {
				deepStrictEqual(error.cause, {
					Id: "0",
					bytes: 1_048_576 + 1,
					limit: 1_048_576,
				});
				return true;
			},
		);
	});

	test(`awsSQSSendMessageStream counts an attribute without a value by name and type`, async (_t) => {
		// "kk" + "Number" = 8 bytes; no StringValue / BinaryValue.
		const MessageAttributes = { kk: { DataType: "Number" } };
		await rejects(
			() =>
				pipeline([
					createReadableStream([
						{
							Id: "0",
							MessageBody: "x".repeat(1_048_576 - 7),
							MessageAttributes,
						},
					]),
					awsSQSSendMessageStream({ QueueUrl: "q" }),
				]),
			(error) => {
				deepStrictEqual(error.cause, {
					Id: "0",
					bytes: 1_048_576 + 1,
					limit: 1_048_576,
				});
				return true;
			},
		);
	});

	test(`awsSQSSendMessageStream splits the batch one byte past the limit`, async (_t) => {
		const first = { Id: "0", MessageBody: "x".repeat(100 * 1024) };
		deepStrictEqual(
			await sizeOf([
				first,
				{ Id: "1", MessageBody: "y".repeat(1_048_576 - 100 * 1024) },
			]),
			[["0", "1"]],
		);
		deepStrictEqual(
			await sizeOf([
				first,
				{ Id: "1", MessageBody: "y".repeat(1_048_576 - 100 * 1024 + 1) },
			]),
			[["0"], ["1"]],
		);
	});
});
