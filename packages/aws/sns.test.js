import { deepStrictEqual, rejects } from "node:assert";
import test from "node:test";
import { PublishBatchCommand, SNSClient } from "@aws-sdk/client-sns";
import snsDefault, {
	awsSNSPublishMessageStream,
	awsSNSSetClient,
} from "@datastream/aws/sns";

import { createReadableStream, pipeline } from "@datastream/core";
import { mockClient } from "aws-sdk-client-mock";

let variant = "unknown";
for (const execArgv of process.execArgv) {
	const flag = "--conditions=";
	if (execArgv.includes(flag)) {
		variant = execArgv.replace(flag, "");
	}
}

test(`${variant}: awsSNSPublishMessageStream should put chunk`, async (_t) => {
	const client = mockClient(SNSClient);
	awsSNSSetClient(client);

	const input = "abcdefghijk".split("").map((id) => ({ id }));
	const options = {
		TopicArn: "arn:aws:sns:us-east-1:000000000000:test",
	};

	client
		.on(PublishBatchCommand, {
			PublishBatchRequestEntries: "abcdefghij".split("").map((id) => ({ id })),
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

test(`${variant}: awsSNSPublishMessageStream should handle empty input`, async (_t) => {
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

// *** Failed entries (data loss) *** //
test(`${variant}: awsSNSPublishMessageStream should retry failed entries`, async (_t) => {
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

test(`${variant}: awsSNSPublishMessageStream should throw when entries keep failing`, async (_t) => {
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

test(`${variant}: awsSNSPublishMessageStream should flush before exceeding 256KB`, async (_t) => {
	const client = mockClient(SNSClient);
	awsSNSSetClient(client);

	client.on(PublishBatchCommand).resolves({ Successful: [], Failed: [] });

	// 5 messages of ~100KB each => 500KB total, well under count of 10
	// but over the 256KB byte limit, forcing multiple flushes.
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

test(`${variant}: awsSNSPublishMessageStream first retry backoff is floored at 50ms (no 1ms hammer)`, async (t) => {
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

	// Let the first send + partial-failure path schedule the backoff timer.
	await new Promise((resolve) => setImmediate(resolve));
	// At 1ms the retry must NOT have fired (old behavior was 3**0 == 1ms).
	t.mock.timers.tick(1);
	await new Promise((resolve) => setImmediate(resolve));
	deepStrictEqual(client.commandCalls(PublishBatchCommand).length, 1);

	// Advancing to the 50ms floor lets the retry proceed.
	t.mock.timers.tick(49);
	await consuming;
	deepStrictEqual(client.commandCalls(PublishBatchCommand).length, 2);
});

test(`${variant}: awsSNSPublishMessageStream should reject a single entry over 256KB`, async (_t) => {
	const client = mockClient(SNSClient);
	awsSNSSetClient(client);
	client.on(PublishBatchCommand).resolves({ Successful: [], Failed: [] });

	// A single message whose serialized size already exceeds the 256KB batch
	// limit must be rejected with a descriptive error instead of being sent and
	// having SNS reject the whole batch.
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
		message: "awsSNSPublishBatch entry exceeds 256KiB limit",
	});
	// The oversize entry must NOT have been sent to SNS.
	deepStrictEqual(client.commandCalls(PublishBatchCommand).length, 0);
});

test(`${variant}: awsSNSPublishMessageStream should not mutate caller options`, async (_t) => {
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

// *** Error cause carries the Failed subset *** //
test(`${variant}: awsSNSPublishMessageStream error cause is the Failed entries array`, async (_t) => {
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

// *** Oversize entry: error cause shape and chunk?.Id *** //
test(`${variant}: awsSNSPublishMessageStream oversize error cause carries Id, bytes and limit`, async (_t) => {
	const client = mockClient(SNSClient);
	awsSNSSetClient(client);
	client.on(PublishBatchCommand).resolves({ Successful: [], Failed: [] });

	const tooBig = "x".repeat(256 * 1024 + 1);
	const chunk = { Id: "the-id", Message: tooBig };
	const expectedBytes = new TextEncoder().encode(
		JSON.stringify(chunk),
	).byteLength;

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

// *** Per-entry boundary: exactly 256KiB is allowed (strict `>`) *** //
test(`${variant}: awsSNSPublishMessageStream allows an entry whose serialized size is exactly 256KiB`, async (_t) => {
	const client = mockClient(SNSClient);
	awsSNSSetClient(client);
	client.on(PublishBatchCommand).resolves({ Successful: [], Failed: [] });

	// Build a chunk whose JSON.stringify byteLength is exactly 256*1024.
	const limit = 256 * 1024;
	const envelope = JSON.stringify({ Id: "0", Message: "" }).length;
	const body = "x".repeat(limit - envelope);
	const chunk = { Id: "0", Message: body };
	const bytes = new TextEncoder().encode(JSON.stringify(chunk)).byteLength;
	deepStrictEqual(bytes, limit); // sanity: exactly at the limit

	await pipeline([
		createReadableStream([chunk]),
		awsSNSPublishMessageStream({ TopicArn: "t" }),
	]);

	// Exactly at the limit is allowed (strict `>`), so it is sent.
	deepStrictEqual(client.commandCalls(PublishBatchCommand).length, 1);
});

// *** Count cap split at exactly 10 entries *** //
test(`${variant}: awsSNSPublishMessageStream flushes at exactly 10 entries`, async (_t) => {
	const client = mockClient(SNSClient);
	awsSNSSetClient(client);
	client.on(PublishBatchCommand).resolves({ Successful: [], Failed: [] });

	// 11 tiny entries: the count cap (10) forces a flush of 10, then 1.
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
	deepStrictEqual(calls[0].args[0].input.PublishBatchRequestEntries.length, 10);
	deepStrictEqual(calls[1].args[0].input.PublishBatchRequestEntries.length, 1);
});

// *** retryMaxCount is an inclusive ceiling on the number of retries *** //
test(`${variant}: awsSNSPublishMessageStream retries exactly retryMaxCount times then throws`, async (_t) => {
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
	// initial attempt + 2 retries == 3 calls (kills `>=`->`>` and `++`->`--`).
	deepStrictEqual(client.commandCalls(PublishBatchCommand).length, 3);
});

test(`${variant}: awsSNSPublishMessageStream with retryMaxCount=1 attempts exactly twice`, async (_t) => {
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

// *** batch byte-cap is exclusive: two entries summing to exactly 256KiB stay
// together in a single batch (strict `>`) *** //
test(`${variant}: awsSNSPublishMessageStream keeps two entries summing to exactly 256KiB in one batch`, async (_t) => {
	const client = mockClient(SNSClient);
	awsSNSSetClient(client);
	client.on(PublishBatchCommand).resolves({ Successful: [], Failed: [] });

	const limit = 256 * 1024;
	const enc = (c) => new TextEncoder().encode(JSON.stringify(c)).byteLength;
	const first = { Id: "0", Message: "x".repeat(100 * 1024) };
	const firstBytes = enc(first);
	const envelope = enc({ Id: "1", Message: "" });
	const second = {
		Id: "1",
		Message: "y".repeat(limit - firstBytes - envelope),
	};
	deepStrictEqual(firstBytes + enc(second), limit); // sanity: exactly the cap

	await pipeline([
		createReadableStream([first, second]),
		awsSNSPublishMessageStream({ TopicArn: "t" }),
	]);

	// Sum is exactly at the cap: `> cap` is false, both go in ONE batch. A
	// `>= cap` mutant would split them.
	const calls = client.commandCalls(PublishBatchCommand);
	deepStrictEqual(calls.length, 1);
	deepStrictEqual(calls[0].args[0].input.PublishBatchRequestEntries.length, 2);
});

// *** setClient swaps the active client *** //
test(`${variant}: awsSNSSetClient routes subsequent sends to the new client`, async (_t) => {
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
test(`${variant}: awsSNSSetClient stores the passed client reference`, async (_t) => {
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

// *** AbortSignal forwarding *** //
test(`${variant}: awsSNSPublishMessageStream forwards abortSignal to client.send`, async (_t) => {
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

// *** Backoff honours the abort signal: aborting mid-backoff rejects instead
// of completing the delay and retrying (kills the backoff timeout-options
// ObjectLiteral mutant that would drop `signal`). *** //
test(`${variant}: awsSNSPublishMessageStream aborts a pending retry backoff via signal`, async (_t) => {
	const client = mockClient(SNSClient);
	awsSNSSetClient(client);

	// Persistent partial failure so the stream stays in the retry/backoff loop.
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

	// Let the first send + partial-failure path schedule the backoff timer, then
	// abort while it is pending.
	await new Promise((resolve) => setImmediate(resolve));
	controller.abort();

	await rejects(consuming, (error) => {
		return error.cause?.code === "AbortError" || /Abort/i.test(error.message);
	});

	// The backoff timeout was given `{ signal }`, so the abort rejects the pending
	// backoff immediately and NO retry PublishBatch is issued. A `{}` mutant
	// (dropping the signal) leaves the backoff running so it wakes and retries
	// repeatedly during this window -> far more than one send.
	await new Promise((resolve) => setTimeout(resolve, 300));
	deepStrictEqual(sends, 1);
});

test(`${variant}: default export should include all stream functions`, (_t) => {
	deepStrictEqual(Object.keys(snsDefault).sort(), [
		"publishMessageStream",
		"setClient",
	]);
});
