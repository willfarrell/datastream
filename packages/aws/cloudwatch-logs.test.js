import { deepStrictEqual } from "node:assert";
import test from "node:test";
import {
	CloudWatchLogsClient,
	FilterLogEventsCommand,
	GetLogEventsCommand,
} from "@aws-sdk/client-cloudwatch-logs";
import {
	awsCloudWatchLogsFilterLogEventsStream,
	awsCloudWatchLogsGetLogEventsStream,
	awsCloudWatchLogsSetClient,
} from "@datastream/aws/cloudwatch-logs";

import { streamToArray } from "@datastream/core";
import { mockClient } from "aws-sdk-client-mock";

let variant = "unknown";
for (const execArgv of process.execArgv) {
	const flag = "--conditions=";
	if (execArgv.includes(flag)) {
		variant = execArgv.replace(flag, "");
	}
}

// --- GetLogEventsStream ---

test(`${variant}: awsCloudWatchLogsGetLogEventsStream should get events`, async (_t) => {
	const client = mockClient(CloudWatchLogsClient);
	awsCloudWatchLogsSetClient(client);

	client
		.on(GetLogEventsCommand)
		.resolvesOnce({
			events: [{ message: "log1" }],
			nextForwardToken: "token2",
		})
		.resolvesOnce({
			events: [],
			nextForwardToken: "token2",
		});

	const options = { logGroupName: "/test/group", logStreamName: "stream1" };
	const stream = await awsCloudWatchLogsGetLogEventsStream(options);
	const output = await streamToArray(stream);

	deepStrictEqual(output, [{ message: "log1" }]);
});

test(`${variant}: awsCloudWatchLogsGetLogEventsStream should handle empty events (undefined)`, async (_t) => {
	const client = mockClient(CloudWatchLogsClient);
	awsCloudWatchLogsSetClient(client);

	client.on(GetLogEventsCommand).resolves({
		nextForwardToken: "token1",
	});

	const options = { logGroupName: "/test/group", logStreamName: "stream1" };
	const stream = await awsCloudWatchLogsGetLogEventsStream(options);
	const output = await streamToArray(stream);

	deepStrictEqual(output, []);
});

test(`${variant}: awsCloudWatchLogsGetLogEventsStream should stop when nextForwardToken repeats`, async (_t) => {
	const client = mockClient(CloudWatchLogsClient);
	awsCloudWatchLogsSetClient(client);

	client
		.on(GetLogEventsCommand)
		.resolvesOnce({
			events: [{ message: "log1" }],
			nextForwardToken: "token2",
		})
		.resolvesOnce({
			events: [{ message: "log2" }],
			nextForwardToken: "token3",
		})
		.resolvesOnce({
			events: [],
			nextForwardToken: "token3",
		});

	const options = { logGroupName: "/test/group", logStreamName: "stream1" };
	const stream = await awsCloudWatchLogsGetLogEventsStream(options);
	const output = await streamToArray(stream);

	deepStrictEqual(output, [{ message: "log1" }, { message: "log2" }]);
});

test(`${variant}: awsCloudWatchLogsGetLogEventsStream should keep polling with pollingActive option`, async (_t) => {
	const client = mockClient(CloudWatchLogsClient);
	awsCloudWatchLogsSetClient(client);

	client
		.on(GetLogEventsCommand)
		.resolvesOnce({ events: [{ message: "log1" }], nextForwardToken: "token2" })
		.resolvesOnce({ events: [], nextForwardToken: "token2" })
		.resolvesOnce({ events: [{ message: "log2" }], nextForwardToken: "token3" })
		.resolves({ events: [], nextForwardToken: "token3" });

	const options = {
		logGroupName: "/test/group",
		logStreamName: "stream1",
		pollingActive: true,
		pollingDelay: 0,
	};
	const stream = await awsCloudWatchLogsGetLogEventsStream(options);

	const output = [];
	for await (const item of stream) {
		output.push(item);
		if (output.length >= 2) break;
	}

	deepStrictEqual(output, [{ message: "log1" }, { message: "log2" }]);
});

test(`${variant}: awsCloudWatchLogsGetLogEventsStream should delay polling when pollingActive and no events`, async (t) => {
	t.mock.timers.enable({ apis: ["setTimeout"] });

	const client = mockClient(CloudWatchLogsClient);
	awsCloudWatchLogsSetClient(client);

	client
		.on(GetLogEventsCommand)
		.resolvesOnce({ events: [], nextForwardToken: "token1" })
		.resolvesOnce({ events: [{ message: "log1" }], nextForwardToken: "token2" })
		.resolves({ events: [], nextForwardToken: "token2" });

	const options = {
		logGroupName: "/test/group",
		logStreamName: "stream1",
		pollingActive: true,
		pollingDelay: 1000,
	};
	const stream = await awsCloudWatchLogsGetLogEventsStream(options);

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

	deepStrictEqual(output, [{ message: "log1" }]);
});

// --- FilterLogEventsStream ---

test(`${variant}: awsCloudWatchLogsFilterLogEventsStream should get events`, async (_t) => {
	const client = mockClient(CloudWatchLogsClient);
	awsCloudWatchLogsSetClient(client);

	client
		.on(FilterLogEventsCommand)
		.resolvesOnce({
			events: [{ message: "log1" }],
			nextToken: "token2",
		})
		.resolvesOnce({
			events: [{ message: "log2" }],
		});

	const options = { logGroupName: "/test/group" };
	const stream = await awsCloudWatchLogsFilterLogEventsStream(options);
	const output = await streamToArray(stream);

	deepStrictEqual(output, [{ message: "log1" }, { message: "log2" }]);
});

test(`${variant}: awsCloudWatchLogsFilterLogEventsStream should stop when no nextToken`, async (_t) => {
	const client = mockClient(CloudWatchLogsClient);
	awsCloudWatchLogsSetClient(client);

	client.on(FilterLogEventsCommand).resolves({
		events: [{ message: "log1" }],
	});

	const options = { logGroupName: "/test/group" };
	const stream = await awsCloudWatchLogsFilterLogEventsStream(options);
	const output = await streamToArray(stream);

	deepStrictEqual(output, [{ message: "log1" }]);
});

test(`${variant}: awsCloudWatchLogsFilterLogEventsStream should handle empty events (undefined)`, async (_t) => {
	const client = mockClient(CloudWatchLogsClient);
	awsCloudWatchLogsSetClient(client);

	client.on(FilterLogEventsCommand).resolves({});

	const options = { logGroupName: "/test/group" };
	const stream = await awsCloudWatchLogsFilterLogEventsStream(options);
	const output = await streamToArray(stream);

	deepStrictEqual(output, []);
});
