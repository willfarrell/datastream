import { deepStrictEqual, rejects } from "node:assert";
import test from "node:test";
import {
	GetRecordsCommand,
	KinesisClient,
	PutRecordsCommand,
} from "@aws-sdk/client-kinesis";
import kinesisDefault, {
	awsKinesisGetRecordsStream,
	awsKinesisPutRecordsStream,
	awsKinesisSetClient,
} from "@datastream/aws/kinesis";

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

test(`${variant}: awsKinesisGetRecordsStream should get chunk`, async (_t) => {
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

test(`${variant}: awsKinesisGetRecordsStream should handle empty Records (undefined)`, async (_t) => {
	const client = mockClient(KinesisClient);
	awsKinesisSetClient(client);

	client.on(GetRecordsCommand).resolves({ NextShardIterator: "iter2" });

	const options = { ShardIterator: "iter1" };
	const stream = await awsKinesisGetRecordsStream(options);
	const output = await streamToArray(stream);

	deepStrictEqual(output, []);
});

test(`${variant}: awsKinesisGetRecordsStream should track NextShardIterator across calls`, async (_t) => {
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

test(`${variant}: awsKinesisGetRecordsStream should keep polling with pollingActive option`, async (_t) => {
	const client = mockClient(KinesisClient);
	awsKinesisSetClient(client);

	client
		.on(GetRecordsCommand)
		.resolvesOnce({ Records: [{ Data: "a" }], NextShardIterator: "iter2" })
		.resolvesOnce({ Records: [], NextShardIterator: "iter3" })
		.resolvesOnce({ Records: [{ Data: "b" }], NextShardIterator: "iter4" })
		.resolves({ Records: [], NextShardIterator: "iter5" });

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

test(`${variant}: awsKinesisGetRecordsStream should delay polling when pollingActive and no records`, async (t) => {
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

test(`${variant}: awsKinesisPutRecordsStream should put chunk`, async (_t) => {
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

test(`${variant}: awsKinesisPutRecordsStream should handle empty input`, async (_t) => {
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

// *** PutRecords partial failure (data loss) *** //
test(`${variant}: awsKinesisPutRecordsStream should retry failed records on partial failure`, async (_t) => {
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
	// The retry must resubmit only the failed record (b)
	deepStrictEqual(calls[1].args[0].input.Records, [
		{ Data: "b", PartitionKey: "b" },
	]);
});

test(`${variant}: awsKinesisPutRecordsStream should throw when records keep failing`, async (_t) => {
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

test(`${variant}: awsKinesisPutRecordsStream should flush when aggregate bytes exceed 5MiB`, async (_t) => {
	const client = mockClient(KinesisClient);
	awsKinesisSetClient(client);

	client.on(PutRecordsCommand).resolves({ FailedRecordCount: 0 });

	// 8 records just under 1 MiB each => ~8 MiB total, well below 500 count
	// but over the 5 MiB aggregate limit, forcing multiple flushes.
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
	// ~8 MiB cannot fit in a single 5 MiB request -> multiple flushes
	deepStrictEqual(calls.length > 1, true);
	// And no single flush may carry more than the 5 MiB aggregate
	for (const call of calls) {
		deepStrictEqual(call.args[0].input.Records.length <= 5, true);
	}
});

test(`${variant}: awsKinesisPutRecordsStream should count ExplicitHashKey toward record size`, async (_t) => {
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

test(`${variant}: awsKinesisPutRecordsStream should not retry when FailedRecordCount has no ErrorCode entries`, async (_t) => {
	const client = mockClient(KinesisClient);
	awsKinesisSetClient(client);
	// Defensive: count says failures but no entry carries an ErrorCode.
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

	// No ErrorCode => no retry
	deepStrictEqual(client.commandCalls(PutRecordsCommand).length, 1);
});

test(`${variant}: awsKinesisPutRecordsStream should reject a single record over 1MiB`, async (_t) => {
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
		message: "awsKinesisPutRecords record exceeds 1MiB limit",
	});
});

test(`${variant}: awsKinesisPutRecordsStream should not mutate caller options`, async (_t) => {
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

test(`${variant}: awsKinesisPutRecordsStream should forward abort signal to client.send`, async (_t) => {
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

test(`${variant}: awsKinesisGetRecordsStream should abort the idle poll delay on signal`, async (_t) => {
	const client = mockClient(KinesisClient);
	awsKinesisSetClient(client);

	// Always empty so the poller enters the idle pollingDelay wait.
	client
		.on(GetRecordsCommand)
		.resolves({ Records: [], NextShardIterator: "i" });

	const controller = new AbortController();
	const options = {
		ShardIterator: "iter1",
		pollingActive: true,
		// Large delay: if the timer is not abort-aware the consumer would hang
		// far longer than this test's window.
		pollingDelay: 60_000,
	};
	const stream = await awsKinesisGetRecordsStream(options, {
		signal: controller.signal,
	});

	const consuming = (async () => {
		for await (const _item of stream) {
			// no records ever arrive
		}
	})();

	// Let the generator reach the idle delay, then abort. The abortable timeout
	// must reject promptly instead of waiting out the 60s delay.
	await new Promise((resolve) => setImmediate(resolve));
	controller.abort();

	await rejects(consuming, (error) => {
		return error.cause?.code === "AbortError" || /Abort/i.test(error.message);
	});
});

// *** AbortSignal *** //
test(`${variant}: awsKinesisGetRecordsStream should pass signal to client`, async (_t) => {
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
	deepStrictEqual(calls[0].args[1]?.abortSignal, controller.signal);
});

// *** Batch byte-cap boundary (KINESIS_MAX_BATCH_BYTES = 5MiB - 64KiB) *** //
test(`${variant}: awsKinesisPutRecordsStream splits at the 5MiB-64KiB aggregate cap, not 5MiB`, async (_t) => {
	const client = mockClient(KinesisClient);
	awsKinesisSetClient(client);
	client.on(PutRecordsCommand).resolves({ FailedRecordCount: 0 });

	// Each record is 128KiB Data + 1-byte PartitionKey = 131,073 bytes, chosen so
	// the real cap (5MiB - 64KiB = 5,177,344) and a mutated `+64KiB` cap
	// (5,308,416) disagree on where to split:
	//   real cap:  39 records = 5,111,847 (fits); 40th = 5,242,920 (> cap)
	//              -> flush after 39
	//   +64KiB:    40 records = 5,242,920 (fits); 41st = 5,373,993 (> cap)
	//              -> flush after 40
	// Multiplicative mutants (`*1024/1024`, `5/1024*1024`) drive the cap negative
	// so every record after the first flushes -> a completely different shape.
	const each = 128 * 1024; // 131,072 bytes of Data
	const data = "x".repeat(each);
	const input = Array.from({ length: 41 }, (_v, i) => ({
		Data: data,
		PartitionKey: `${i % 10}`, // single-char key => 1 byte
	}));
	const options = { StreamName: "test-stream" };
	await pipeline([
		createReadableStream(input),
		awsKinesisPutRecordsStream(options),
	]);

	const calls = client.commandCalls(PutRecordsCommand);
	deepStrictEqual(calls.length, 2);
	// The true cap splits after exactly 39 records.
	deepStrictEqual(calls[0].args[0].input.Records.length, 39);
	deepStrictEqual(calls[1].args[0].input.Records.length, 2);
});

test(`${variant}: awsKinesisPutRecordsStream keeps two small records under the cap in one batch`, async (_t) => {
	const client = mockClient(KinesisClient);
	awsKinesisSetClient(client);
	client.on(PutRecordsCommand).resolves({ FailedRecordCount: 0 });

	// Two tiny records: well under both the count and byte caps -> single flush.
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

// *** Per-record 1MiB boundary (strict `>` not `>=`) *** //
test(`${variant}: awsKinesisPutRecordsStream accepts a record exactly at the 1MiB limit`, async (_t) => {
	const client = mockClient(KinesisClient);
	awsKinesisSetClient(client);
	client.on(PutRecordsCommand).resolves({ FailedRecordCount: 0 });

	// Exactly 1 MiB of single-byte chars (1 byte each in UTF-8), no PartitionKey,
	// so byteLength === KINESIS_MAX_RECORD_BYTES. With strict `>` this is allowed;
	// a `>=` mutant would wrongly reject it.
	const exact = "x".repeat(1024 * 1024);
	const input = [{ Data: exact }];
	await pipeline([
		createReadableStream(input),
		awsKinesisPutRecordsStream({ StreamName: "test-stream" }),
	]);

	deepStrictEqual(client.commandCalls(PutRecordsCommand).length, 1);
});

// *** recordByteLength counts PartitionKey & ExplicitHashKey *** //
test(`${variant}: awsKinesisPutRecordsStream counts PartitionKey bytes toward the 1MiB limit`, async (_t) => {
	const client = mockClient(KinesisClient);
	awsKinesisSetClient(client);
	client.on(PutRecordsCommand).resolves({ FailedRecordCount: 0 });

	// Data alone is exactly 1MiB (allowed), but adding a non-empty PartitionKey
	// pushes the record over 1MiB -> must reject. This proves PartitionKey is
	// summed in (the `if (record.PartitionKey != null)` block and `+=`).
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

test(`${variant}: awsKinesisPutRecordsStream counts ExplicitHashKey bytes toward the 1MiB limit`, async (_t) => {
	const client = mockClient(KinesisClient);
	awsKinesisSetClient(client);
	client.on(PutRecordsCommand).resolves({ FailedRecordCount: 0 });

	// Data exactly 1MiB; a non-empty ExplicitHashKey tips it over -> reject.
	// Proves the ExplicitHashKey branch and its `+=` (a `-=` mutant would keep
	// it under the limit and the record would be accepted).
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

// *** Failure cause only carries ErrorCode entries *** //
test(`${variant}: awsKinesisPutRecordsStream error cause lists only failed (ErrorCode) records`, async (_t) => {
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
			deepStrictEqual(error.message, "awsKinesisPutRecords has failed records");
			// cause must be filtered to only the ErrorCode-bearing entry.
			deepStrictEqual(error.cause, [
				{ ErrorCode: "InternalFailure", ErrorMessage: "boom" },
			]);
			return true;
		},
	);
});

// *** Count cap split at exactly 500 records *** //
test(`${variant}: awsKinesisPutRecordsStream flushes at exactly 500 records`, async (_t) => {
	const client = mockClient(KinesisClient);
	awsKinesisSetClient(client);
	client.on(PutRecordsCommand).resolves({ FailedRecordCount: 0 });

	// 501 tiny records: count cap (500) forces exactly one mid-stream flush of
	// 500, then a final flush of 1. Proves `=== 500` (not 499/501) and that the
	// count check, not bytes, triggers here.
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

// *** Backoff floor: first retry waits >= 50ms, not 3^0 == 1ms *** //
test(`${variant}: awsKinesisPutRecordsStream first retry backoff is floored at 50ms`, async (t) => {
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

	// Let the first send + failure happen, entering the backoff timer.
	await new Promise((resolve) => setImmediate(resolve));
	// 3^0 == 1ms would already have elapsed; the floor means the retry must
	// still be pending after 1ms but fire by 50ms.
	t.mock.timers.tick(1);
	await new Promise((resolve) => setImmediate(resolve));
	deepStrictEqual(client.commandCalls(PutRecordsCommand).length, 1);
	t.mock.timers.tick(49);
	await consuming;
	deepStrictEqual(client.commandCalls(PutRecordsCommand).length, 2);
});

// *** setClient swaps the active client *** //
test(`${variant}: awsKinesisSetClient routes subsequent sends to the new client`, async (_t) => {
	const first = mockClient(KinesisClient);
	awsKinesisSetClient(first);
	first.on(PutRecordsCommand).resolves({ FailedRecordCount: 0 });

	const second = mockClient(KinesisClient);
	second.on(PutRecordsCommand).resolves({ FailedRecordCount: 0 });
	// Swap before any send: only `second` should receive the command. If
	// setClient's body were removed, the stale `first` would be used.
	awsKinesisSetClient(second);

	await pipeline([
		createReadableStream([{ Data: "a", PartitionKey: "a" }]),
		awsKinesisPutRecordsStream({ StreamName: "test-stream" }),
	]);

	deepStrictEqual(second.commandCalls(PutRecordsCommand).length, 1);
	deepStrictEqual(first.commandCalls(PutRecordsCommand).length, 0);
});

// *** GetRecords stops when NextShardIterator becomes null (shard closed) *** //
test(`${variant}: awsKinesisGetRecordsStream stops polling when NextShardIterator is null even with pollingActive`, async (_t) => {
	const client = mockClient(KinesisClient);
	awsKinesisSetClient(client);

	// A closed shard returns a null NextShardIterator: the generator must stop
	// regardless of pollingActive. If `ShardIterator !== null` were dropped from
	// the expectMore condition, pollingActive would loop forever.
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
	// Exactly one GetRecords call: the null iterator ends the loop.
	deepStrictEqual(client.commandCalls(GetRecordsCommand).length, 1);
});

test(`${variant}: awsKinesisGetRecordsStream without pollingActive stops once records run out`, async (_t) => {
	const client = mockClient(KinesisClient);
	awsKinesisSetClient(client);

	// Non-null iterator but empty Records and no pollingActive: expectMore must
	// become false (records.length > 0 is false). A surviving mutant that forces
	// the loop true would call GetRecords forever.
	client
		.on(GetRecordsCommand)
		.resolvesOnce({ Records: [{ Data: "a" }], NextShardIterator: "iter2" })
		.resolves({ Records: [], NextShardIterator: "iter3" });

	const options = { ShardIterator: "iter1" };
	const stream = await awsKinesisGetRecordsStream(options);
	const output = await streamToArray(stream);

	deepStrictEqual(output, [{ Data: "a" }]);
	// First call returns a record (keep going), second returns empty (stop).
	deepStrictEqual(client.commandCalls(GetRecordsCommand).length, 2);
});

// *** setClient stores the reference (not just prototype-mocked behavior) *** //
test(`${variant}: awsKinesisSetClient stores the passed client reference`, async (_t) => {
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

// *** getRecords forwards the caller options (spread, not {}) to the command *** //
test(`${variant}: awsKinesisGetRecordsStream forwards ShardIterator to GetRecords`, async (_t) => {
	// Capture the ShardIterator at send time: the generator mutates opts.ShardIterator
	// to the (null) NextShardIterator AFTER the await, so we must read it synchronously
	// inside send. A `command({})` object-literal mutant starts with an empty opts and
	// the first command would carry ShardIterator === undefined.
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

// *** polling delay guard: each conjunct of the idle-wait condition *** //
test(`${variant}: awsKinesisGetRecordsStream does not delay when records are present (pollingActive)`, async (t) => {
	t.mock.timers.enable({ apis: ["setTimeout"] });
	const client = mockClient(KinesisClient);
	awsKinesisSetClient(client);
	// Always returns records: the idle-wait `records.length === 0` conjunct is
	// false, so no timeout should ever be scheduled. A `true` mutant on that
	// conjunct would delay between every (non-empty) poll and the consumer would
	// stall on the (un-ticked) mock timer.
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

test(`${variant}: awsKinesisGetRecordsStream pollingDelay 0 schedules no timer`, async (t) => {
	t.mock.timers.enable({ apis: ["setTimeout"] });
	const client = mockClient(KinesisClient);
	awsKinesisSetClient(client);
	// pollingActive with empty records but pollingDelay === 0: the `pollingDelay > 0`
	// guard is false so no idle wait runs. A `> 0` -> `true` or `>= 0` mutant would
	// schedule a timeout(0) that the un-ticked mock timer never resolves, stalling
	// the consumer (which would otherwise reach the 2nd record immediately).
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
	for await (const item of stream) {
		output.push(item);
		if (output.length >= 1) break;
	}
	deepStrictEqual(output, [{ Data: "a" }]);
});

test(`${variant}: awsKinesisGetRecordsStream non-polling empty page ends without an idle wait`, async (t) => {
	t.mock.timers.enable({ apis: ["setTimeout"] });
	const client = mockClient(KinesisClient);
	awsKinesisSetClient(client);
	// Not polling: an empty page must end the stream WITHOUT entering the idle
	// wait. The `&&` -> `||` mutant makes `pollingActive || records.length === 0`
	// true on the empty page, scheduling a (default 1000ms) timeout the un-ticked
	// mock timer never resolves -> the stream would hang instead of completing.
	client
		.on(GetRecordsCommand)
		.resolvesOnce({ Records: [{ Data: "a" }], NextShardIterator: "iter2" })
		.resolves({ Records: [], NextShardIterator: "iter3" });

	const stream = await awsKinesisGetRecordsStream({ ShardIterator: "iter1" });
	const output = await streamToArray(stream);

	deepStrictEqual(output, [{ Data: "a" }]);
});

// *** record byte accounting *** //
test(`${variant}: awsKinesisPutRecordsStream accepts a record with no Data field`, async (_t) => {
	const client = mockClient(KinesisClient);
	awsKinesisSetClient(client);
	client.on(PutRecordsCommand).resolves({ FailedRecordCount: 0 });

	// No Data property: the `record.Data != null` guard must skip the Data byte
	// count. An `if (true)` mutant would call _byteLength(undefined) and throw.
	const input = [{ PartitionKey: "p" }];
	await pipeline([
		createReadableStream(input),
		awsKinesisPutRecordsStream({ StreamName: "test-stream" }),
	]);
	deepStrictEqual(client.commandCalls(PutRecordsCommand).length, 1);
});

test(`${variant}: awsKinesisPutRecordsStream counts ExplicitHashKey as the deciding byte`, async (_t) => {
	const client = mockClient(KinesisClient);
	awsKinesisSetClient(client);
	client.on(PutRecordsCommand).resolves({ FailedRecordCount: 0 });

	// Data is EXACTLY 1MiB (allowed on its own, no PartitionKey). The 1-byte
	// ExplicitHashKey is the ONLY thing tipping the record over 1MiB, so the
	// `if (record.ExplicitHashKey != null)` branch (and its `+=`) must run. A
	// `false`/`{}` mutant skips it and the record would be wrongly accepted.
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

test(`${variant}: awsKinesisPutRecordsStream measures non-string Data by byteLength`, async (_t) => {
	const client = mockClient(KinesisClient);
	awsKinesisSetClient(client);
	client.on(PutRecordsCommand).resolves({ FailedRecordCount: 0 });

	// A Uint8Array exactly at the 1MiB limit must be accepted via .byteLength.
	// The `typeof value === "string"` branch must be FALSE here; a `true` mutant
	// would TextEncoder.encode() the typed array (encoding its iterable/UTF-8 view
	// rather than measuring byteLength) and the boundary check would shift.
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

test(`${variant}: awsKinesisPutRecordsStream oversize error cause carries bytes and limit`, async (_t) => {
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
			// `{}` mutant on the cause object would drop these fields. bytes counts
			// Data (1MiB+1) plus the 1-byte PartitionKey.
			deepStrictEqual(error.cause.limit, 1024 * 1024);
			deepStrictEqual(error.cause.bytes, 1024 * 1024 + 2);
			return true;
		},
	);
});

// *** success short-circuit: a stray ErrorCode with FailedRecordCount 0 must not retry *** //
test(`${variant}: awsKinesisPutRecordsStream does not retry when FailedRecordCount is 0`, async (_t) => {
	const client = mockClient(KinesisClient);
	awsKinesisSetClient(client);
	// FailedRecordCount 0 but a stray ErrorCode entry: the `if (!FailedRecordCount)`
	// early return must fire so we do NOT loop into the filter-and-retry path. A
	// `false` mutant (or removed return block) would see the ErrorCode and retry.
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

// *** results indexing tolerates a short Records array (optional chaining) *** //
test(`${variant}: awsKinesisPutRecordsStream tolerates a results array shorter than records`, async (_t) => {
	const client = mockClient(KinesisClient);
	awsKinesisSetClient(client);
	// Two records sent, but the response Records array has a single entry. The
	// failed-records filter indexes results[1] which is undefined; the
	// `results[index]?.ErrorCode` optional chain must guard it. A non-optional
	// `results[index].ErrorCode` mutant throws a TypeError on index 1.
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
	// Only the first record (the one with an ErrorCode) is retried.
	deepStrictEqual(calls[1].args[0].input.Records, [
		{ Data: "a", PartitionKey: "a" },
	]);
});

// *** retryCount must increment so retryMaxCount is eventually reached *** //
test(`${variant}: awsKinesisPutRecordsStream stops retrying after retryMaxCount`, async (_t) => {
	const client = mockClient(KinesisClient);
	awsKinesisSetClient(client);
	// Persistent failure with retryMaxCount 2: retryCount must climb 0->1->2 and
	// throw after 3 attempts. A `retryCount--` mutant would drive it negative,
	// never reach the cap, and retry forever (test timeout = killed).
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
	// 1 initial + 2 retries = 3 attempts.
	deepStrictEqual(client.commandCalls(PutRecordsCommand).length, 3);
});

// *** aggregate byte cap is 5MiB - 64KiB (subtract 64*1024, not 64/1024) *** //
test(`${variant}: awsKinesisPutRecordsStream cap subtracts 64KiB (not 64/1024)`, async (_t) => {
	const client = mockClient(KinesisClient);
	awsKinesisSetClient(client);
	client.on(PutRecordsCommand).resolves({ FailedRecordCount: 0 });

	// Real cap = 5*1024*1024 - 64*1024 = 5,177,344. A `64 / 1024` mutant makes the
	// cap ~5,242,879.94 (essentially 5MiB). Five 1,048,001-byte records sum to
	// 5,240,005 which is OVER the real cap (so the 5th forces a flush after 4) but
	// UNDER the mutated cap (so all five would batch together).
	const data = "x".repeat(1_048_000); // 1,048,000 Data + 1-byte PartitionKey
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

// *** byte-cap boundary uses strict `>` (equal-to-cap stays in the batch) *** //
test(`${variant}: awsKinesisPutRecordsStream keeps a batch summing exactly to the cap`, async (_t) => {
	const client = mockClient(KinesisClient);
	awsKinesisSetClient(client);
	client.on(PutRecordsCommand).resolves({ FailedRecordCount: 0 });

	// 65,536-byte records (65,535 Data + 1-byte PartitionKey). 79 of them sum to
	// exactly the cap (79 * 65,536 = 5,177,344). With strict `>` the 79th stays in
	// the batch (flush happens only when the 80th would exceed the cap) -> first
	// flush carries 79. A `>=` mutant flushes one record early -> 78.
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

// *** PutRecords response.Records undefined fallback (`?? []`) *** //
test(`${variant}: awsKinesisPutRecordsStream falls back to empty array when response.Records is undefined`, async (_t) => {
	const client = mockClient(KinesisClient);
	awsKinesisSetClient(client);
	// FailedRecordCount is truthy but Records is absent (undefined) -> `?? []` must
	// produce an empty array so the failed.length check returns [] and returns early.
	// A missing `?? []` would call .filter() on undefined and throw a TypeError.
	client
		.on(PutRecordsCommand)
		.resolvesOnce({
			FailedRecordCount: 1,
			// Records intentionally absent
		})
		.resolves({ FailedRecordCount: 0 });

	const input = [{ Data: "a", PartitionKey: "a" }];
	const stream = [
		createReadableStream(input),
		awsKinesisPutRecordsStream({ StreamName: "test-stream" }),
	];
	await pipeline(stream);

	// The first response had no ErrorCode entries (empty array after fallback),
	// so no retry. Only one PutRecords call.
	deepStrictEqual(client.commandCalls(PutRecordsCommand).length, 1);
});

// *** putRecords backoff is abortable (signal forwarded to the timeout) *** //
test(`${variant}: awsKinesisPutRecordsStream aborts during retry backoff`, async (_t) => {
	const client = mockClient(KinesisClient);
	awsKinesisSetClient(client);
	// Persistent partial failure so the stream enters the (real-timer) backoff.
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

	// Let the first send fail and enter the backoff timer, then abort.
	await new Promise((resolve) => setImmediate(resolve));
	controller.abort();

	await rejects(consuming, (error) => {
		return error.cause?.code === "AbortError" || /Abort/i.test(error.message);
	});

	// The backoff timeout was given `{ signal }`, so the abort rejects the pending
	// backoff immediately and NO further PutRecords is issued. A `{}` mutant
	// (dropping the signal) leaves the backoff running so it wakes and retries
	// repeatedly during this window -> far more than one send.
	await new Promise((resolve) => setTimeout(resolve, 300));
	deepStrictEqual(sends, 1);
});

// The default export must expose exactly the public surface (setClient plus the
// two stream factories). An `ObjectLiteral` mutant collapsing it to `{}` would
// drop every key.
test(`${variant}: kinesis default export exposes all stream functions`, (_t) => {
	deepStrictEqual(Object.keys(kinesisDefault).sort(), [
		"getRecordsStream",
		"putRecordsStream",
		"setClient",
	]);
	deepStrictEqual(kinesisDefault.setClient, awsKinesisSetClient);
	deepStrictEqual(kinesisDefault.getRecordsStream, awsKinesisGetRecordsStream);
	deepStrictEqual(kinesisDefault.putRecordsStream, awsKinesisPutRecordsStream);
});
