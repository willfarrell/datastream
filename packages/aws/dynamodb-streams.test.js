import { deepStrictEqual } from "node:assert";
import test from "node:test";
import {
	DynamoDBStreamsClient,
	GetRecordsCommand,
} from "@aws-sdk/client-dynamodb-streams";
import {
	awsDynamoDBStreamsGetRecordsStream,
	awsDynamoDBStreamsSetClient,
} from "@datastream/aws/dynamodb-streams";

import { streamToArray } from "@datastream/core";
import { mockClient } from "aws-sdk-client-mock";

let variant = "unknown";
for (const execArgv of process.execArgv) {
	const flag = "--conditions=";
	if (execArgv.includes(flag)) {
		variant = execArgv.replace(flag, "");
	}
}

test(`${variant}: awsDynamoDBStreamsGetRecordsStream should get chunk`, async (_t) => {
	const client = mockClient(DynamoDBStreamsClient);
	awsDynamoDBStreamsSetClient(client);

	client
		.on(GetRecordsCommand)
		.resolvesOnce({
			Records: [{ eventID: "1" }],
			NextShardIterator: "iter2",
		})
		.resolvesOnce({
			Records: [],
			NextShardIterator: "iter3",
		});

	const options = { ShardIterator: "iter1" };
	const stream = await awsDynamoDBStreamsGetRecordsStream(options);
	const output = await streamToArray(stream);

	deepStrictEqual(output, [{ eventID: "1" }]);
});

test(`${variant}: awsDynamoDBStreamsGetRecordsStream should handle empty Records (undefined)`, async (_t) => {
	const client = mockClient(DynamoDBStreamsClient);
	awsDynamoDBStreamsSetClient(client);

	client.on(GetRecordsCommand).resolves({ NextShardIterator: "iter2" });

	const options = { ShardIterator: "iter1" };
	const stream = await awsDynamoDBStreamsGetRecordsStream(options);
	const output = await streamToArray(stream);

	deepStrictEqual(output, []);
});

test(`${variant}: awsDynamoDBStreamsGetRecordsStream should track NextShardIterator across calls`, async (_t) => {
	const client = mockClient(DynamoDBStreamsClient);
	awsDynamoDBStreamsSetClient(client);

	client
		.on(GetRecordsCommand)
		.resolvesOnce({
			Records: [{ eventID: "1" }],
			NextShardIterator: "iter2",
		})
		.resolvesOnce({
			Records: [{ eventID: "2" }],
			NextShardIterator: "iter3",
		})
		.resolvesOnce({
			Records: [],
			NextShardIterator: "iter4",
		});

	const options = { ShardIterator: "iter1" };
	const stream = await awsDynamoDBStreamsGetRecordsStream(options);
	const output = await streamToArray(stream);

	deepStrictEqual(output, [{ eventID: "1" }, { eventID: "2" }]);
});

test(`${variant}: awsDynamoDBStreamsGetRecordsStream should stop when NextShardIterator is null (closed shard)`, async (_t) => {
	const client = mockClient(DynamoDBStreamsClient);
	awsDynamoDBStreamsSetClient(client);

	client.on(GetRecordsCommand).resolvesOnce({
		Records: [{ eventID: "1" }],
		NextShardIterator: null,
	});

	const options = { ShardIterator: "iter1" };
	const stream = await awsDynamoDBStreamsGetRecordsStream(options);
	const output = await streamToArray(stream);

	deepStrictEqual(output, [{ eventID: "1" }]);
});

test(`${variant}: awsDynamoDBStreamsGetRecordsStream should keep polling with pollingActive option`, async (_t) => {
	const client = mockClient(DynamoDBStreamsClient);
	awsDynamoDBStreamsSetClient(client);

	client
		.on(GetRecordsCommand)
		.resolvesOnce({ Records: [{ eventID: "1" }], NextShardIterator: "iter2" })
		.resolvesOnce({ Records: [], NextShardIterator: "iter3" })
		.resolvesOnce({ Records: [{ eventID: "2" }], NextShardIterator: "iter4" })
		.resolves({ Records: [], NextShardIterator: "iter5" });

	const options = {
		ShardIterator: "iter1",
		pollingActive: true,
		pollingDelay: 0,
	};
	const stream = await awsDynamoDBStreamsGetRecordsStream(options);

	const output = [];
	for await (const item of stream) {
		output.push(item);
		if (output.length >= 2) break;
	}

	deepStrictEqual(output, [{ eventID: "1" }, { eventID: "2" }]);
});

test(`${variant}: awsDynamoDBStreamsGetRecordsStream should delay polling when pollingActive and no records`, async (t) => {
	t.mock.timers.enable({ apis: ["setTimeout"] });

	const client = mockClient(DynamoDBStreamsClient);
	awsDynamoDBStreamsSetClient(client);

	client
		.on(GetRecordsCommand)
		.resolvesOnce({ Records: [], NextShardIterator: "iter2" })
		.resolvesOnce({ Records: [{ eventID: "1" }], NextShardIterator: "iter3" })
		.resolves({ Records: [], NextShardIterator: "iter4" });

	const options = {
		ShardIterator: "iter1",
		pollingActive: true,
		pollingDelay: 1000,
	};
	const stream = await awsDynamoDBStreamsGetRecordsStream(options);

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

	deepStrictEqual(output, [{ eventID: "1" }]);
});

// *** AbortSignal *** //
test(`${variant}: awsDynamoDBStreamsGetRecordsStream should pass signal to client`, async (_t) => {
	const client = mockClient(DynamoDBStreamsClient);
	awsDynamoDBStreamsSetClient(client);
	client.on(GetRecordsCommand).resolves({
		Records: [{ eventID: "1" }],
		NextShardIterator: null,
	});

	const controller = new AbortController();
	const options = { ShardIterator: "iter-1" };
	const stream = await awsDynamoDBStreamsGetRecordsStream(options, {
		signal: controller.signal,
	});
	await streamToArray(stream);

	const calls = client.commandCalls(GetRecordsCommand);
	deepStrictEqual(calls[0].args[1]?.abortSignal, controller.signal);
});
