import { deepStrictEqual, rejects } from "node:assert";
import test from "node:test";
import {
	DeleteMessageBatchCommand,
	ReceiveMessageCommand,
	SendMessageBatchCommand,
	SQSClient,
} from "@aws-sdk/client-sqs";
import sqsDefault, {
	awsSQSDeleteMessageStream,
	awsSQSReceiveMessageStream,
	awsSQSSendMessageStream,
	awsSQSSetClient,
} from "@datastream/aws/sqs";

import {
	createReadableStream,
	pipeline,
	streamToArray,
} from "@datastream/core";
import { mockClient } from "aws-sdk-client-mock";

let variant = "unknown";
for (const execArgv of process.execArgv) {
	const flag = "--conditions=";
	if (execArgv.includes(flag)) {
		variant = execArgv.replace(flag, "");
	}
}

test(`${variant}: awsSQSReceiveMessageStream should get chunk`, async (_t) => {
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

test(`${variant}: awsSQSReceiveMessageStream should handle empty queue (Messages: undefined)`, async (_t) => {
	const client = mockClient(SQSClient);
	awsSQSSetClient(client);

	client.on(ReceiveMessageCommand).resolves({});

	const options = {};
	const stream = await awsSQSReceiveMessageStream(options);
	const output = await streamToArray(stream);

	deepStrictEqual(output, []);
});

test(`${variant}: awsSQSReceiveMessageStream should keep polling with pollingActive option`, async (_t) => {
	const client = mockClient(SQSClient);
	awsSQSSetClient(client);

	client
		.on(ReceiveMessageCommand)
		.resolvesOnce({ Messages: [{ id: "a" }] })
		.resolvesOnce({ Messages: [] })
		.resolvesOnce({ Messages: [{ id: "b" }] })
		.resolves({ Messages: [] });

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

test(`${variant}: awsSQSReceiveMessageStream should delay polling when pollingActive and queue is empty`, async (t) => {
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

test(`${variant}: awsSQSReceiveMessageStream should abort the idle poll delay on signal`, async (_t) => {
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

test(`${variant}: awsSQSReceiveMessageStream should not delay polling when messages are returned`, async (_t) => {
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

test(`${variant}: awsSQSDeleteMessageStream should delete chunk`, async (_t) => {
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

test(`${variant}: awsSQSSendMessageStream should put chunk`, async (_t) => {
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

test(`${variant}: awsSQSDeleteMessageStream should handle empty input`, async (_t) => {
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

test(`${variant}: awsSQSSendMessageStream should handle empty input`, async (_t) => {
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
test(`${variant}: awsSQSSendMessageStream should retry failed entries`, async (_t) => {
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

test(`${variant}: awsSQSSendMessageStream should throw when entries keep failing`, async (_t) => {
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

test(`${variant}: awsSQSDeleteMessageStream should throw when entries keep failing`, async (_t) => {
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

test(`${variant}: awsSQSSendMessageStream should flush before exceeding 256KB`, async (_t) => {
	const client = mockClient(SQSClient);
	awsSQSSetClient(client);

	client.on(SendMessageBatchCommand).resolves({ Successful: [], Failed: [] });

	// 5 messages of ~100KB each => 500KB total, well under count of 10
	// but over the 256KB byte limit, forcing multiple flushes.
	const body = "x".repeat(100 * 1024);
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

test(`${variant}: awsSQSSendMessageStream first retry backoff is floored at 50ms (no 1ms hammer)`, async (t) => {
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

test(`${variant}: awsSQSSendMessageStream should reject a single entry over 256KB`, async (_t) => {
	const client = mockClient(SQSClient);
	awsSQSSetClient(client);
	client.on(SendMessageBatchCommand).resolves({ Successful: [], Failed: [] });

	// A single message whose serialized size already exceeds the 256KB batch
	// limit must be rejected with a descriptive error instead of being sent and
	// having SQS reject the whole batch.
	const tooBig = "x".repeat(256 * 1024 + 1);
	const input = [{ Id: "0", MessageBody: tooBig }];
	const options = {
		QueueUrl: "https://sqs.us-east-1.amazonaws.com/000000000000/test",
	};

	const stream = [
		createReadableStream(input),
		awsSQSSendMessageStream(options),
	];
	await rejects(() => pipeline(stream), {
		message: "awsSQSSendMessageBatch entry exceeds 256KiB limit",
	});
	// The oversize entry must NOT have been sent to SQS.
	deepStrictEqual(client.commandCalls(SendMessageBatchCommand).length, 0);
});

test(`${variant}: awsSQSSendMessageStream should not mutate caller options`, async (_t) => {
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

test(`${variant}: default export should include all stream functions`, (_t) => {
	deepStrictEqual(Object.keys(sqsDefault).sort(), [
		"deleteMessageStream",
		"receiveMessageStream",
		"sendMessageStream",
		"setClient",
	]);
});

// *** setClient swaps the active client *** //
test(`${variant}: awsSQSSetClient routes subsequent sends to the new client`, async (_t) => {
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
test(`${variant}: awsSQSSetClient stores the passed client reference`, async (_t) => {
	let calls = 0;
	const stub = {
		send: async () => {
			calls++;
			return { Messages: [{ id: "stub" }] };
		},
	};
	awsSQSSetClient(stub);

	// Not polling: one page with messages then the generator stops when the next
	// (default) empty page arrives — but with a single resolved value the stub
	// returns messages each call, so cap the consumption.
	const stream = await awsSQSReceiveMessageStream({});
	const output = [];
	for await (const item of stream) {
		output.push(item);
		if (output.length >= 1) break;
	}

	deepStrictEqual(output, [{ id: "stub" }]);
	deepStrictEqual(calls, 1);
});

// *** idle-wait guard conjuncts (mock timers) *** //
test(`${variant}: awsSQSReceiveMessageStream does not schedule a timer when messages are present`, async (t) => {
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

test(`${variant}: awsSQSReceiveMessageStream pollingDelay 0 schedules no timer`, async (t) => {
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

test(`${variant}: awsSQSReceiveMessageStream non-polling empty page ends without an idle wait`, async (t) => {
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
test(`${variant}: awsSQSSendMessageStream aborts during retry backoff`, async (_t) => {
	const client = mockClient(SQSClient);
	awsSQSSetClient(client);
	// Persistent partial failure so the stream enters the (real-timer) backoff.
	client.on(SendMessageBatchCommand).resolves({
		Successful: [],
		Failed: [{ Id: "0", Code: "ThrottlingException", SenderFault: false }],
	});

	const controller = new AbortController();
	const consuming = pipeline([
		createReadableStream([{ Id: "0", MessageBody: "a" }]),
		awsSQSSendMessageStream({ QueueUrl: "q" }, { signal: controller.signal }),
	]);

	// Let the first send fail and enter the backoff timer, then abort. The backoff
	// timeout was given `{ signal }`; a `{}` mutant (dropping the signal) would let
	// the backoff run and keep retrying, so this would never reject.
	await new Promise((resolve) => setImmediate(resolve));
	controller.abort();

	await rejects(consuming, (error) => {
		return error.cause?.code === "AbortError" || /Abort/i.test(error.message);
	});
});

// *** AbortSignal forwarding *** //
test(`${variant}: awsSQSReceiveMessageStream forwards abortSignal to client.send`, async (_t) => {
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
	deepStrictEqual(calls[0].args[1]?.abortSignal, controller.signal);
});

test(`${variant}: awsSQSSendMessageStream forwards abortSignal to client.send`, async (_t) => {
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
test(`${variant}: awsSQSSendMessageStream error cause is the Failed entries array`, async (_t) => {
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
test(`${variant}: awsSQSSendMessageStream oversize error cause carries Id, bytes and limit`, async (_t) => {
	const client = mockClient(SQSClient);
	awsSQSSetClient(client);
	client.on(SendMessageBatchCommand).resolves({ Successful: [], Failed: [] });

	const tooBig = "x".repeat(256 * 1024 + 1);
	const chunk = { Id: "the-id", MessageBody: tooBig };
	const expectedBytes = new TextEncoder().encode(
		JSON.stringify(chunk),
	).byteLength;

	await rejects(
		() =>
			pipeline([
				createReadableStream([chunk]),
				awsSQSSendMessageStream({ QueueUrl: "q" }),
			]),
		(error) => {
			deepStrictEqual(
				error.message,
				"awsSQSSendMessageBatch entry exceeds 256KiB limit",
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

// *** Per-entry boundary: exactly 256KiB is allowed (strict `>`) *** //
test(`${variant}: awsSQSSendMessageStream allows an entry whose serialized size is exactly 256KiB`, async (_t) => {
	const client = mockClient(SQSClient);
	awsSQSSetClient(client);
	client.on(SendMessageBatchCommand).resolves({ Successful: [], Failed: [] });

	// Build a chunk whose JSON.stringify byteLength is exactly 256*1024.
	const limit = 256 * 1024;
	const envelope = JSON.stringify({ Id: "0", MessageBody: "" }).length; // 24
	const body = "x".repeat(limit - envelope);
	const chunk = { Id: "0", MessageBody: body };
	const bytes = new TextEncoder().encode(JSON.stringify(chunk)).byteLength;
	deepStrictEqual(bytes, limit); // sanity: exactly at the limit

	await pipeline([
		createReadableStream([chunk]),
		awsSQSSendMessageStream({ QueueUrl: "q" }),
	]);

	// Exactly at the limit is allowed (strict `>`), so it is sent.
	deepStrictEqual(client.commandCalls(SendMessageBatchCommand).length, 1);
});

// *** Delete oversize path uses the Delete-specific message *** //
test(`${variant}: awsSQSDeleteMessageStream rejects an oversize entry with the delete message`, async (_t) => {
	const client = mockClient(SQSClient);
	awsSQSSetClient(client);
	client.on(DeleteMessageBatchCommand).resolves({ Successful: [], Failed: [] });

	const tooBig = "x".repeat(256 * 1024 + 1);
	await rejects(
		() =>
			pipeline([
				createReadableStream([{ Id: "0", ReceiptHandle: tooBig }]),
				awsSQSDeleteMessageStream({ QueueUrl: "q" }),
			]),
		{ message: "awsSQSDeleteMessageBatch entry exceeds 256KiB limit" },
	);
	deepStrictEqual(client.commandCalls(DeleteMessageBatchCommand).length, 0);
});

// *** Count cap split at exactly 10 entries *** //
test(`${variant}: awsSQSSendMessageStream flushes at exactly 10 entries`, async (_t) => {
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
test(`${variant}: awsSQSReceiveMessageStream without pollingActive stops when queue empties`, async (_t) => {
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
test(`${variant}: awsSQSSendMessageStream retries exactly retryMaxCount times then throws`, async (_t) => {
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

test(`${variant}: awsSQSSendMessageStream with retryMaxCount=1 attempts exactly twice`, async (_t) => {
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

// *** batch byte-cap is exclusive: two entries summing to exactly 256KiB stay
// together in a single batch (strict `>`) *** //
test(`${variant}: awsSQSSendMessageStream keeps two entries summing to exactly 256KiB in one batch`, async (_t) => {
	const client = mockClient(SQSClient);
	awsSQSSetClient(client);
	client.on(SendMessageBatchCommand).resolves({ Successful: [], Failed: [] });

	const limit = 256 * 1024;
	const enc = (c) => new TextEncoder().encode(JSON.stringify(c)).byteLength;
	const first = { Id: "0", MessageBody: "x".repeat(100 * 1024) };
	const firstBytes = enc(first);
	const envelope = enc({ Id: "1", MessageBody: "" });
	const second = {
		Id: "1",
		MessageBody: "y".repeat(limit - firstBytes - envelope),
	};
	deepStrictEqual(firstBytes + enc(second), limit); // sanity: exactly the cap

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
