import { deepStrictEqual, rejects } from "node:assert";
import test from "node:test";
import {
	CloudWatchLogsClient,
	FilterLogEventsCommand,
	GetLogEventsCommand,
} from "@aws-sdk/client-cloudwatch-logs";
import cwlDefault, {
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

test(`${variant}: awsCloudWatchLogsGetLogEventsStream should abort the idle poll delay on signal`, async (_t) => {
	const client = mockClient(CloudWatchLogsClient);
	awsCloudWatchLogsSetClient(client);

	// Same token every call => tokenUnchanged => enters the idle poll delay.
	client.on(GetLogEventsCommand).resolves({
		events: [],
		nextForwardToken: "same-token",
	});

	const controller = new AbortController();
	const options = {
		logGroupName: "/test/group",
		logStreamName: "stream1",
		pollingActive: true,
		pollingDelay: 60_000,
	};
	const stream = await awsCloudWatchLogsGetLogEventsStream(options, {
		signal: controller.signal,
	});

	const consuming = (async () => {
		for await (const _item of stream) {
			// no events ever arrive
		}
	})();

	await new Promise((resolve) => setImmediate(resolve));
	controller.abort();

	await rejects(consuming, (error) => {
		return error.cause?.code === "AbortError" || /Abort/i.test(error.message);
	});
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

// *** AbortSignal *** //
test(`${variant}: awsCloudWatchLogsGetLogEventsStream should pass signal to client`, async (_t) => {
	const client = mockClient(CloudWatchLogsClient);
	awsCloudWatchLogsSetClient(client);
	client.on(GetLogEventsCommand).resolves({
		events: [{ message: "log line" }],
		nextForwardToken: "same-token",
	});

	const controller = new AbortController();
	const options = { logGroupName: "g", logStreamName: "s" };
	const stream = await awsCloudWatchLogsGetLogEventsStream(options, {
		signal: controller.signal,
	});
	await streamToArray(stream);

	const calls = client.commandCalls(GetLogEventsCommand);
	deepStrictEqual(calls[0].args[1]?.abortSignal, controller.signal);
});

// *** startFromHead default & options passthrough *** //
test(`${variant}: awsCloudWatchLogsGetLogEventsStream defaults startFromHead to true`, async (_t) => {
	const client = mockClient(CloudWatchLogsClient);
	awsCloudWatchLogsSetClient(client);
	client
		.on(GetLogEventsCommand)
		.resolves({ events: [], nextForwardToken: "t" });

	const options = { logGroupName: "g", logStreamName: "s" };
	const stream = await awsCloudWatchLogsGetLogEventsStream(options);
	await streamToArray(stream);

	const input = client.commandCalls(GetLogEventsCommand)[0].args[0].input;
	// Defaulted via `??=`: a `&&=` mutant would leave it undefined (because
	// undefined &&= true stays undefined).
	deepStrictEqual(input.startFromHead, true);
	// Caller options are passed through to the command.
	deepStrictEqual(input.logGroupName, "g");
	deepStrictEqual(input.logStreamName, "s");
});

test(`${variant}: awsCloudWatchLogsGetLogEventsStream keeps an explicit startFromHead=false`, async (_t) => {
	const client = mockClient(CloudWatchLogsClient);
	awsCloudWatchLogsSetClient(client);
	client
		.on(GetLogEventsCommand)
		.resolves({ events: [], nextForwardToken: "t" });

	const options = {
		logGroupName: "g",
		logStreamName: "s",
		startFromHead: false,
	};
	const stream = await awsCloudWatchLogsGetLogEventsStream(options);
	await streamToArray(stream);

	// `??=` must NOT overwrite an explicit false. A plain `=` mutant would force
	// it back to true.
	const input = client.commandCalls(GetLogEventsCommand)[0].args[0].input;
	deepStrictEqual(input.startFromHead, false);
});

test(`${variant}: awsCloudWatchLogsFilterLogEventsStream passes options through and paginates on nextToken`, async (_t) => {
	const client = mockClient(CloudWatchLogsClient);
	awsCloudWatchLogsSetClient(client);
	client
		.on(FilterLogEventsCommand)
		.resolvesOnce({ events: [{ message: "a" }], nextToken: "n2" })
		.resolvesOnce({ events: [{ message: "b" }] });

	const options = { logGroupName: "g", filterPattern: "ERROR" };
	const stream = await awsCloudWatchLogsFilterLogEventsStream(options);
	const output = await streamToArray(stream);

	deepStrictEqual(output, [{ message: "a" }, { message: "b" }]);
	const calls = client.commandCalls(FilterLogEventsCommand);
	// Two calls: pagination continued because the first response had a nextToken
	// (kills the `expectMore = !!nextToken` related behavior) and stopped when the
	// second had none.
	deepStrictEqual(calls.length, 2);
	// Options spread through to the command (kills the `command({})` mutant).
	deepStrictEqual(calls[0].args[0].input.logGroupName, "g");
	deepStrictEqual(calls[0].args[0].input.filterPattern, "ERROR");
});

test(`${variant}: awsCloudWatchLogsFilterLogEventsStream stops after a single page when no nextToken`, async (_t) => {
	const client = mockClient(CloudWatchLogsClient);
	awsCloudWatchLogsSetClient(client);
	// No nextToken on the only response => exactly one call.
	client.on(FilterLogEventsCommand).resolves({ events: [{ message: "a" }] });

	const stream = await awsCloudWatchLogsFilterLogEventsStream({
		logGroupName: "g",
	});
	const output = await streamToArray(stream);

	deepStrictEqual(output, [{ message: "a" }]);
	deepStrictEqual(client.commandCalls(FilterLogEventsCommand).length, 1);
});

test(`${variant}: awsCloudWatchLogsFilterLogEventsStream forwards abortSignal to client.send`, async (_t) => {
	const client = mockClient(CloudWatchLogsClient);
	awsCloudWatchLogsSetClient(client);
	client.on(FilterLogEventsCommand).resolves({ events: [{ message: "a" }] });

	const controller = new AbortController();
	const stream = await awsCloudWatchLogsFilterLogEventsStream(
		{ logGroupName: "g" },
		{ signal: controller.signal },
	);
	await streamToArray(stream);

	const calls = client.commandCalls(FilterLogEventsCommand);
	deepStrictEqual(calls[0].args[1]?.abortSignal, controller.signal);
});

// *** setClient swap *** //
test(`${variant}: awsCloudWatchLogsSetClient routes to the new client`, async (_t) => {
	const first = mockClient(CloudWatchLogsClient);
	awsCloudWatchLogsSetClient(first);
	first.on(FilterLogEventsCommand).resolves({ events: [] });

	const second = mockClient(CloudWatchLogsClient);
	second.on(FilterLogEventsCommand).resolves({ events: [] });
	awsCloudWatchLogsSetClient(second);

	const stream = await awsCloudWatchLogsFilterLogEventsStream({
		logGroupName: "g",
	});
	await streamToArray(stream);

	deepStrictEqual(second.commandCalls(FilterLogEventsCommand).length, 1);
	deepStrictEqual(first.commandCalls(FilterLogEventsCommand).length, 0);
});

// setClient must STORE the passed client (mockClient hides instance identity via
// prototype patching). A plain stub proves the stored reference is used; a
// `setClient(){}` mutant leaves the prior client in place.
test(`${variant}: awsCloudWatchLogsSetClient stores the passed client reference`, async (_t) => {
	let calls = 0;
	const stub = {
		send: async () => {
			calls++;
			return { events: [{ message: "stub" }] };
		},
	};
	awsCloudWatchLogsSetClient(stub);

	const stream = await awsCloudWatchLogsFilterLogEventsStream({
		logGroupName: "g",
	});
	const output = await streamToArray(stream);

	deepStrictEqual(output, [{ message: "stub" }]);
	deepStrictEqual(calls, 1);
});

// GetLogEvents stops as soon as nextForwardToken equals the token that was sent
// (opts.nextToken), even on the very first call when the caller resumes from a
// known token. Pins `nextForwardToken === opts.nextToken` (a `false`/always-loop
// mutant would never stop; this also kills the unchanged-token guard).
test(`${variant}: awsCloudWatchLogsGetLogEventsStream stops when the echoed token equals the sent token`, async (_t) => {
	const client = mockClient(CloudWatchLogsClient);
	awsCloudWatchLogsSetClient(client);
	// Caller resumes from "tokenX"; CloudWatch immediately echoes it back -> done.
	client.on(GetLogEventsCommand).resolves({
		events: [{ message: "log1" }],
		nextForwardToken: "tokenX",
	});

	const options = {
		logGroupName: "g",
		logStreamName: "s",
		nextToken: "tokenX",
	};
	const stream = await awsCloudWatchLogsGetLogEventsStream(options);
	const output = await streamToArray(stream);

	deepStrictEqual(output, [{ message: "log1" }]);
	// Exactly one call: the echoed token matched the sent token on the first page.
	deepStrictEqual(client.commandCalls(GetLogEventsCommand).length, 1);
});

// pollingActive with pollingDelay === 0 must NOT schedule a timer: the
// `pollingDelay > 0` guard is false. A `true` or `>= 0` mutant would await
// timeout(0); under un-ticked mock timers the consumer would never reach the
// later (changed-token) event.
test(`${variant}: awsCloudWatchLogsGetLogEventsStream pollingDelay 0 schedules no timer`, async (t) => {
	t.mock.timers.enable({ apis: ["setTimeout"] });
	const client = mockClient(CloudWatchLogsClient);
	awsCloudWatchLogsSetClient(client);
	client
		.on(GetLogEventsCommand)
		// caught-up page: token unchanged from the (undefined) sent token? No -
		// send a repeated token so tokenUnchanged is true and we enter the polling
		// branch, then a fresh token + event without any timer tick.
		.resolvesOnce({ events: [], nextForwardToken: "t1" })
		.resolvesOnce({ events: [], nextForwardToken: "t1" })
		.resolvesOnce({ events: [{ message: "log1" }], nextForwardToken: "t2" })
		.resolves({ events: [], nextForwardToken: "t2" });

	const options = {
		logGroupName: "g",
		logStreamName: "s",
		pollingActive: true,
		pollingDelay: 0,
	};
	const stream = await awsCloudWatchLogsGetLogEventsStream(options);

	const output = [];
	for await (const item of stream) {
		output.push(item);
		if (output.length >= 1) break;
	}
	deepStrictEqual(output, [{ message: "log1" }]);
});

test(`${variant}: cloudwatch-logs default export exposes all stream functions`, (_t) => {
	deepStrictEqual(Object.keys(cwlDefault).sort(), [
		"filterLogEventsStream",
		"getLogEventsStream",
		"setClient",
	]);
});
