import { deepStrictEqual } from "node:assert";
import test from "node:test";
import {
	GetRecordsCommand,
	KinesisClient,
	PutRecordsCommand,
} from "@aws-sdk/client-kinesis";
import {
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
