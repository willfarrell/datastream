import { deepStrictEqual, rejects, strictEqual } from "node:assert";
import test, { describe } from "node:test";
import {
	CloudWatchLogsClient,
	FilterLogEventsCommand,
	GetLogEventsCommand,
	PutLogEventsCommand,
} from "@aws-sdk/client-cloudwatch-logs";
import * as awsModule from "@datastream/aws/cloudwatch-logs";
import {
	awsCloudWatchLogsFilterLogEventsStream,
	awsCloudWatchLogsGetLogEventsStream,
	awsCloudWatchLogsPutLogEventsStream,
	awsCloudWatchLogsSetClient,
} from "@datastream/aws/cloudwatch-logs";
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

const DAY_MS = 24 * 60 * 60 * 1000;

const putLogEventsClient = (response = {}) => {
	const calls = [];
	return {
		calls,
		send: async (command, sendOptions) => {
			strictEqual(command instanceof PutLogEventsCommand, true);
			calls.push({ input: command.input, sendOptions });
			return response;
		},
	};
};
const putLogEvents = (events, options = {}, streamOptions = undefined) =>
	pipeline([
		createReadableStream(events),
		awsCloudWatchLogsPutLogEventsStream(
			{ logGroupName: "g", logStreamName: "s", ...options },
			streamOptions,
		),
	]);
const batchTimestamps = (client) =>
	client.calls.map(({ input }) =>
		input.logEvents.map((event) => event.timestamp),
	);

describe(`@datastream/aws/cloudwatch-logs (${variant})`, () => {
	test(`awsCloudWatchLogsGetLogEventsStream should get events`, async (_t) => {
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

	test(`awsCloudWatchLogsGetLogEventsStream should handle empty events (undefined)`, async (_t) => {
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

	test(`awsCloudWatchLogsGetLogEventsStream should stop when nextForwardToken repeats`, async (_t) => {
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

	test(`awsCloudWatchLogsGetLogEventsStream should keep polling with pollingActive option`, async (_t) => {
		const client = mockClient(CloudWatchLogsClient);
		awsCloudWatchLogsSetClient(client);

		client
			.on(GetLogEventsCommand)
			.resolvesOnce({
				events: [{ message: "log1" }],
				nextForwardToken: "token2",
			})
			.resolvesOnce({ events: [], nextForwardToken: "token2" })
			.resolvesOnce({
				events: [{ message: "log2" }],
				nextForwardToken: "token3",
			})
			.callsFake(neverResolves);

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

	test(`awsCloudWatchLogsGetLogEventsStream should delay polling when pollingActive and no events`, async (t) => {
		t.mock.timers.enable({ apis: ["setTimeout"] });

		const client = mockClient(CloudWatchLogsClient);
		awsCloudWatchLogsSetClient(client);

		client
			.on(GetLogEventsCommand)
			.resolvesOnce({ events: [], nextForwardToken: "token1" })
			.resolvesOnce({ events: [], nextForwardToken: "token1" })
			.resolvesOnce({
				events: [{ message: "log1" }],
				nextForwardToken: "token2",
			})
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

	test(`awsCloudWatchLogsGetLogEventsStream should abort the idle poll delay on signal`, async (_t) => {
		const client = mockClient(CloudWatchLogsClient);
		awsCloudWatchLogsSetClient(client);

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
			}
		})();

		await new Promise((resolve) => setImmediate(resolve));
		controller.abort();

		await rejects(consuming, (error) => {
			return error.cause?.code === "AbortError" || /Abort/i.test(error.message);
		});
	});

	test(`awsCloudWatchLogsFilterLogEventsStream should get events`, async (_t) => {
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

	test(`awsCloudWatchLogsFilterLogEventsStream should stop when no nextToken`, async (_t) => {
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

	test(`awsCloudWatchLogsFilterLogEventsStream should handle empty events (undefined)`, async (_t) => {
		const client = mockClient(CloudWatchLogsClient);
		awsCloudWatchLogsSetClient(client);

		client.on(FilterLogEventsCommand).resolves({});

		const options = { logGroupName: "/test/group" };
		const stream = await awsCloudWatchLogsFilterLogEventsStream(options);
		const output = await streamToArray(stream);

		deepStrictEqual(output, []);
	});

	test(`awsCloudWatchLogsGetLogEventsStream should pass signal to client`, async (_t) => {
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
		const sent = calls[0].args[1]?.abortSignal;
		deepStrictEqual(sent.aborted, false);
		controller.abort();
		deepStrictEqual(sent.aborted, true);
	});

	test(`awsCloudWatchLogsGetLogEventsStream defaults startFromHead to true`, async (_t) => {
		const client = mockClient(CloudWatchLogsClient);
		awsCloudWatchLogsSetClient(client);
		client
			.on(GetLogEventsCommand)
			.resolves({ events: [], nextForwardToken: "t" });

		const options = { logGroupName: "g", logStreamName: "s" };
		const stream = await awsCloudWatchLogsGetLogEventsStream(options);
		await streamToArray(stream);

		const input = client.commandCalls(GetLogEventsCommand)[0].args[0].input;
		deepStrictEqual(input.startFromHead, true);
		deepStrictEqual(input.logGroupName, "g");
		deepStrictEqual(input.logStreamName, "s");
	});

	test(`awsCloudWatchLogsGetLogEventsStream keeps an explicit startFromHead=false`, async (_t) => {
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

		const input = client.commandCalls(GetLogEventsCommand)[0].args[0].input;
		deepStrictEqual(input.startFromHead, false);
	});

	test(`awsCloudWatchLogsFilterLogEventsStream passes options through and paginates on nextToken`, async (_t) => {
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
		deepStrictEqual(calls.length, 2);
		deepStrictEqual(calls[0].args[0].input.logGroupName, "g");
		deepStrictEqual(calls[0].args[0].input.filterPattern, "ERROR");
	});

	test(`awsCloudWatchLogsFilterLogEventsStream stops after a single page when no nextToken`, async (_t) => {
		const client = mockClient(CloudWatchLogsClient);
		awsCloudWatchLogsSetClient(client);
		client.on(FilterLogEventsCommand).resolves({ events: [{ message: "a" }] });

		const stream = await awsCloudWatchLogsFilterLogEventsStream({
			logGroupName: "g",
		});
		const output = await streamToArray(stream);

		deepStrictEqual(output, [{ message: "a" }]);
		deepStrictEqual(client.commandCalls(FilterLogEventsCommand).length, 1);
	});

	test(`awsCloudWatchLogsFilterLogEventsStream forwards abortSignal to client.send`, async (_t) => {
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
		const sent = calls[0].args[1]?.abortSignal;
		deepStrictEqual(sent.aborted, false);
		controller.abort();
		deepStrictEqual(sent.aborted, true);
	});

	test(`awsCloudWatchLogs streams use the per-call client option and do not forward it`, async (_t) => {
		awsCloudWatchLogsSetClient({
			send: async () => {
				throw new Error("module client used");
			},
		});
		const inputs = [];
		const client = {
			send: async (command) => {
				inputs.push({ ...command.input });
				return {};
			},
		};

		const got = await streamToArray(
			await awsCloudWatchLogsGetLogEventsStream({ logGroupName: "g", client }),
		);
		const filtered = await streamToArray(
			await awsCloudWatchLogsFilterLogEventsStream({
				logGroupName: "g",
				client,
			}),
		);

		deepStrictEqual(got, []);
		deepStrictEqual(filtered, []);
		deepStrictEqual(inputs, [
			{ logGroupName: "g", startFromHead: true },
			{ logGroupName: "g" },
		]);
	});

	test(`awsCloudWatchLogsSetClient routes to the new client`, async (_t) => {
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
	test(`awsCloudWatchLogsSetClient stores the passed client reference`, async (_t) => {
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

	test(`awsCloudWatchLogsGetLogEventsStream stops when the echoed token equals the sent token`, async (_t) => {
		const client = mockClient(CloudWatchLogsClient);
		awsCloudWatchLogsSetClient(client);
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
		deepStrictEqual(client.commandCalls(GetLogEventsCommand).length, 1);
	});

	test(`awsCloudWatchLogsGetLogEventsStream pollingDelay 0 schedules no timer`, async (t) => {
		t.mock.timers.enable({ apis: ["setTimeout"] });
		const client = mockClient(CloudWatchLogsClient);
		awsCloudWatchLogsSetClient(client);
		client
			.on(GetLogEventsCommand)
			.resolvesOnce({ events: [], nextForwardToken: "t1" })
			.resolvesOnce({ events: [], nextForwardToken: "t1" })
			.resolvesOnce({ events: [{ message: "log1" }], nextForwardToken: "t2" })
			.callsFake(neverResolves);

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

	test(`awsCloudWatchLogs read streams are Readables that pipejoin accepts`, async (_t) => {
		const client = mockClient(CloudWatchLogsClient);
		awsCloudWatchLogsSetClient(client);
		client
			.on(GetLogEventsCommand)
			.resolves({ events: [{ message: "a" }], nextForwardToken: undefined });
		client.on(FilterLogEventsCommand).resolves({ events: [{ message: "b" }] });

		const get = await awsCloudWatchLogsGetLogEventsStream({
			logGroupName: "g",
			logStreamName: "s",
		});
		const filter = await awsCloudWatchLogsFilterLogEventsStream({
			logGroupName: "g",
		});
		strictEqual(isReadable(get), true);
		strictEqual(isReadable(filter), true);
		deepStrictEqual(await streamToArray(pipejoin([get])), [{ message: "a" }]);
		deepStrictEqual(await streamToArray(pipejoin([filter])), [
			{ message: "b" },
		]);
	});

	test(`awsCloudWatchLogsGetLogEventsStream destroy stops an idle poll`, {
		timeout: 1000,
	}, async (_t) => {
		const client = mockClient(CloudWatchLogsClient);
		awsCloudWatchLogsSetClient(client);
		client
			.on(GetLogEventsCommand)
			.resolves({ events: [], nextForwardToken: undefined });

		const stream = await awsCloudWatchLogsGetLogEventsStream({
			logGroupName: "g",
			logStreamName: "s",
			pollingActive: true,
			pollingDelay: 60_000,
		});
		stream.resume();
		await new Promise((resolve) => setImmediate(resolve));
		const closed = new Promise((resolve) => stream.once("close", resolve));
		stream.destroy();
		await closed;
		deepStrictEqual(client.commandCalls(GetLogEventsCommand).length, 1);
	});

	test(`awsCloudWatchLogsPutLogEventsStream sends one batch sorted by timestamp`, async (_t) => {
		const client = putLogEventsClient();
		const events = [
			{ timestamp: 3, message: "c" },
			{ timestamp: 1, message: "a" },
			{ timestamp: 2, message: "b" },
		];
		const result = await putLogEvents(events, { client });

		deepStrictEqual(result, {});
		deepStrictEqual(client.calls, [
			{
				input: {
					logGroupName: "g",
					logStreamName: "s",
					logEvents: [
						{ timestamp: 1, message: "a" },
						{ timestamp: 2, message: "b" },
						{ timestamp: 3, message: "c" },
					],
				},
				sendOptions: { abortSignal: undefined },
			},
		]);
	});

	test(`awsCloudWatchLogsPutLogEventsStream sends nothing for empty input`, async (_t) => {
		const client = putLogEventsClient();
		await putLogEvents([], { client });
		deepStrictEqual(client.calls, []);
	});

	test(`awsCloudWatchLogsPutLogEventsStream flushes at 10,000 events`, async (_t) => {
		const client = putLogEventsClient();
		const events = Array.from({ length: 10_001 }, (_v, i) => ({
			timestamp: i,
			message: "m",
		}));
		await putLogEvents(events, { client });

		deepStrictEqual(
			client.calls.map(({ input }) => input.logEvents.length),
			[10_000, 1],
		);
	});

	test(`awsCloudWatchLogsPutLogEventsStream keeps a batch of exactly 1,048,576 bytes`, async (_t) => {
		const client = putLogEventsClient();
		const message = "é".repeat(262_131);
		await putLogEvents(
			[
				{ timestamp: 1, message },
				{ timestamp: 2, message },
				{ timestamp: 3, message: "a" },
			],
			{ client },
		);

		deepStrictEqual(batchTimestamps(client), [[1, 2], [3]]);
	});

	test(`awsCloudWatchLogsPutLogEventsStream flushes before exceeding 1,048,576 bytes`, async (_t) => {
		const client = putLogEventsClient();
		await putLogEvents(
			[
				{ timestamp: 1, message: "é".repeat(262_131) },
				{ timestamp: 2, message: `${"é".repeat(262_131)}a` },
			],
			{ client },
		);

		deepStrictEqual(batchTimestamps(client), [[1], [2]]);
	});

	test(`awsCloudWatchLogsPutLogEventsStream keeps events spanning exactly 24 hours`, async (_t) => {
		const client = putLogEventsClient();
		await putLogEvents(
			[
				{ timestamp: 1000 + DAY_MS, message: "b" },
				{ timestamp: 1000, message: "a" },
			],
			{ client },
		);

		deepStrictEqual(batchTimestamps(client), [[1000, 1000 + DAY_MS]]);
	});

	test(`awsCloudWatchLogsPutLogEventsStream flushes before an event below the 24 hour span`, async (_t) => {
		const client = putLogEventsClient();
		await putLogEvents(
			[
				{ timestamp: 1000 + DAY_MS, message: "c" },
				{ timestamp: 1000, message: "b" },
				{ timestamp: 999, message: "a" },
			],
			{ client },
		);

		deepStrictEqual(batchTimestamps(client), [[1000, 1000 + DAY_MS], [999]]);
	});

	test(`awsCloudWatchLogsPutLogEventsStream flushes before an event above the 24 hour span`, async (_t) => {
		const client = putLogEventsClient();
		await putLogEvents(
			[
				{ timestamp: 1000, message: "b" },
				{ timestamp: 995, message: "a" },
				{ timestamp: 996 + DAY_MS, message: "c" },
				{ timestamp: 997 + DAY_MS, message: "d" },
			],
			{ client },
		);

		deepStrictEqual(batchTimestamps(client), [
			[995, 1000],
			[996 + DAY_MS, 997 + DAY_MS],
		]);
	});

	test(`awsCloudWatchLogsPutLogEventsStream accepts a single event of exactly 1,048,576 bytes`, async (_t) => {
		const client = putLogEventsClient();
		await putLogEvents([{ timestamp: 1, message: "x".repeat(1_048_550) }], {
			client,
		});
		deepStrictEqual(client.calls.length, 1);
	});

	test(`awsCloudWatchLogsPutLogEventsStream rejects an oversize event with RangeError`, async (_t) => {
		const client = putLogEventsClient();
		await rejects(
			() =>
				putLogEvents(
					[
						{ timestamp: 1, message: "a" },
						{ timestamp: 2, message: "x".repeat(1_048_551) },
					],
					{ client },
				),
			(error) => {
				strictEqual(error.name, "RangeError");
				strictEqual(
					error.message,
					"awsCloudWatchLogsPutLogEvents event exceeds 1MiB limit",
				);
				deepStrictEqual(error.cause, { bytes: 1_048_577, limit: 1_048_576 });
				return true;
			},
		);
		deepStrictEqual(client.calls, []);
	});

	test(`awsCloudWatchLogsPutLogEventsStream attaches the rejected events to the error`, async (_t) => {
		const rejectedLogEventsInfo = {
			tooOldLogEventEndIndex: 1,
			expiredLogEventEndIndex: 2,
			tooNewLogEventStartIndex: 3,
		};
		const client = putLogEventsClient({ rejectedLogEventsInfo });
		await rejects(
			() =>
				putLogEvents(
					[
						{ timestamp: 5, message: "e" },
						{ timestamp: 1, message: "a" },
						{ timestamp: 4, message: "d" },
						{ timestamp: 2, message: "b" },
						{ timestamp: 3, message: "c" },
					],
					{ client },
				),
			(error) => {
				strictEqual(
					error.message,
					"awsCloudWatchLogsPutLogEvents has rejected events",
				);
				deepStrictEqual(error.cause, {
					rejectedLogEventsInfo,
					rejected: {
						tooNew: [
							{ timestamp: 4, message: "d" },
							{ timestamp: 5, message: "e" },
						],
						tooOld: [{ timestamp: 1, message: "a" }],
						expired: [
							{ timestamp: 1, message: "a" },
							{ timestamp: 2, message: "b" },
						],
					},
				});
				strictEqual(error.cause.rejectedLogEventsInfo, rejectedLogEventsInfo);
				strictEqual(error.failedMessages, undefined);
				return true;
			},
		);
	});

	test(`awsCloudWatchLogsPutLogEventsStream attaches only the too-new events`, async (_t) => {
		const rejectedLogEventsInfo = { tooNewLogEventStartIndex: 1 };
		const client = putLogEventsClient({ rejectedLogEventsInfo });
		await rejects(
			() =>
				putLogEvents(
					[
						{ timestamp: 2, message: "b" },
						{ timestamp: 1, message: "a" },
					],
					{ client },
				),
			(error) => {
				deepStrictEqual(error.cause, {
					rejectedLogEventsInfo,
					rejected: {
						tooNew: [{ timestamp: 2, message: "b" }],
						tooOld: [],
						expired: [],
					},
				});
				return true;
			},
		);
	});

	test(`awsCloudWatchLogsPutLogEventsStream attaches only the too-old events`, async (_t) => {
		const rejectedLogEventsInfo = { tooOldLogEventEndIndex: 1 };
		const client = putLogEventsClient({ rejectedLogEventsInfo });
		await rejects(
			() =>
				putLogEvents(
					[
						{ timestamp: 2, message: "b" },
						{ timestamp: 1, message: "a" },
					],
					{ client },
				),
			(error) => {
				deepStrictEqual(error.cause, {
					rejectedLogEventsInfo,
					rejected: {
						tooNew: [],
						tooOld: [{ timestamp: 1, message: "a" }],
						expired: [],
					},
				});
				return true;
			},
		);
	});

	for (const [label, event, message] of [
		[
			"NaN timestamp",
			{ timestamp: Number.NaN, message: "a" },
			"awsCloudWatchLogsPutLogEvents event timestamp must be a finite number",
		],
		[
			"Infinity timestamp",
			{ timestamp: Number.POSITIVE_INFINITY, message: "a" },
			"awsCloudWatchLogsPutLogEvents event timestamp must be a finite number",
		],
		[
			"string timestamp",
			{ timestamp: "1", message: "a" },
			"awsCloudWatchLogsPutLogEvents event timestamp must be a finite number",
		],
		[
			"missing timestamp",
			{ message: "a" },
			"awsCloudWatchLogsPutLogEvents event timestamp must be a finite number",
		],
		[
			"number message",
			{ timestamp: 1, message: 1 },
			"awsCloudWatchLogsPutLogEvents event message must be a string",
		],
		[
			"missing message",
			{ timestamp: 1 },
			"awsCloudWatchLogsPutLogEvents event message must be a string",
		],
	]) {
		test(`awsCloudWatchLogsPutLogEventsStream rejects an event with a ${label}`, async (_t) => {
			const client = putLogEventsClient();
			await rejects(
				() => putLogEvents([event], { client }),
				(error) => {
					strictEqual(error.name, "TypeError");
					strictEqual(error.message, message);
					return true;
				},
			);
			deepStrictEqual(client.calls, []);
		});
	}

	test(`awsCloudWatchLogsPutLogEventsStream rejects a NaN timestamp after valid events`, async (_t) => {
		const client = putLogEventsClient();
		await rejects(
			() =>
				putLogEvents(
					[
						{ timestamp: 1, message: "a" },
						{ timestamp: Number.NaN, message: "b" },
					],
					{ client },
				),
			{
				name: "TypeError",
				message:
					"awsCloudWatchLogsPutLogEvents event timestamp must be a finite number",
			},
		);
		deepStrictEqual(client.calls, []);
	});

	test(`awsCloudWatchLogsPutLogEventsStream forwards the abort signal`, async (_t) => {
		const client = putLogEventsClient();
		const controller = new AbortController();
		await putLogEvents(
			[{ timestamp: 1, message: "a" }],
			{ client },
			{
				signal: controller.signal,
			},
		);
		strictEqual(client.calls[0].sendOptions.abortSignal, controller.signal);
	});

	test(`awsCloudWatchLogsPutLogEventsStream uses the module client by default`, async (_t) => {
		const client = mockClient(CloudWatchLogsClient);
		awsCloudWatchLogsSetClient(client);
		client.on(PutLogEventsCommand).resolves({});

		const options = { logGroupName: "g", logStreamName: "s" };
		const optionsCopy = { ...options };
		await pipeline([
			createReadableStream([{ timestamp: 1, message: "a" }]),
			awsCloudWatchLogsPutLogEventsStream(options),
		]);

		deepStrictEqual(client.commandCalls(PutLogEventsCommand).length, 1);
		deepStrictEqual(options, optionsCopy);
	});

	test(`cloudwatch-logs has no default export`, (_t) => {
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

	test(`awsCloudWatchLogsPutLogEventsStream destroy(error) attaches the buffered entries`, async (_t) => {
		const client = mockClient(CloudWatchLogsClient);
		awsCloudWatchLogsSetClient(client);
		client.on(PutLogEventsCommand).resolves({});
		const stream = awsCloudWatchLogsPutLogEventsStream({
			logGroupName: "g",
			logStreamName: "s",
		});
		const chunks = Array.from({ length: 2 }, (_v, i) => ({
			timestamp: i,
			message: `${i}`,
		}));
		await writeEach(stream, chunks);
		const error = new Error("boom");
		strictEqual(await destroyedWith(stream, error), error);
		deepStrictEqual(error.message, "boom");
		deepStrictEqual(error.failedMessages, chunks);
		deepStrictEqual(client.commandCalls(PutLogEventsCommand).length, 0);
	});

	test(`awsCloudWatchLogsPutLogEventsStream destroy() raises an error carrying the buffered entries`, async (_t) => {
		const client = mockClient(CloudWatchLogsClient);
		awsCloudWatchLogsSetClient(client);
		client.on(PutLogEventsCommand).resolves({});
		const stream = awsCloudWatchLogsPutLogEventsStream({
			logGroupName: "g",
			logStreamName: "s",
		});
		const chunks = Array.from({ length: 1 }, (_v, i) => ({
			timestamp: i,
			message: `${i}`,
		}));
		await writeEach(stream, chunks);
		const error = await destroyedWith(stream);
		deepStrictEqual(
			error.message,
			"awsCloudWatchLogsPutLogEvents destroyed with buffered events",
		);
		deepStrictEqual(error.failedMessages, chunks);
		deepStrictEqual(client.commandCalls(PutLogEventsCommand).length, 0);
	});

	test(`awsCloudWatchLogsPutLogEventsStream destroy() with nothing buffered raises no error`, async (_t) => {
		const client = mockClient(CloudWatchLogsClient);
		awsCloudWatchLogsSetClient(client);
		client.on(PutLogEventsCommand).resolves({});
		const stream = awsCloudWatchLogsPutLogEventsStream({
			logGroupName: "g",
			logStreamName: "s",
		});
		const closed = new Promise((resolve) => stream.once("close", resolve));
		stream.destroy();
		await closed;
		strictEqual(stream.errored, null);
		deepStrictEqual(client.commandCalls(PutLogEventsCommand).length, 0);
	});

	test(`awsCloudWatchLogsPutLogEventsStream attaches only the entries buffered after the last flush`, async (_t) => {
		const client = mockClient(CloudWatchLogsClient);
		awsCloudWatchLogsSetClient(client);
		client.on(PutLogEventsCommand).resolves({});
		const stream = awsCloudWatchLogsPutLogEventsStream({
			logGroupName: "g",
			logStreamName: "s",
		});
		const chunks = Array.from({ length: 10001 }, (_v, i) => ({
			timestamp: i,
			message: `${i}`,
		}));
		await writeEach(stream, chunks);
		const error = await destroyedWith(stream);
		deepStrictEqual(error.failedMessages, chunks.slice(10000));
		deepStrictEqual(client.commandCalls(PutLogEventsCommand).length, 1);
	});

	test(`awsCloudWatchLogsPutLogEventsStream abort attaches the buffered entries without sending`, async (_t) => {
		const client = mockClient(CloudWatchLogsClient);
		awsCloudWatchLogsSetClient(client);
		client.on(PutLogEventsCommand).resolves({});
		const controller = new AbortController();
		const stream = awsCloudWatchLogsPutLogEventsStream(
			{ logGroupName: "g", logStreamName: "s" },
			{
				signal: controller.signal,
			},
		);
		const chunks = Array.from({ length: 2 }, (_v, i) => ({
			timestamp: i,
			message: `${i}`,
		}));
		await writeEach(stream, chunks);
		const closed = new Promise((resolve) => stream.once("close", resolve));
		stream.once("error", () => {});
		controller.abort();
		await closed;
		const error = stream.errored;
		deepStrictEqual(error.name, "AbortError");
		deepStrictEqual(error.failedMessages, chunks);
		deepStrictEqual(client.commandCalls(PutLogEventsCommand).length, 0);
	});

	test(`awsCloudWatchLogsPutLogEventsStream attaches buffered events to a validation error`, async (_t) => {
		const client = putLogEventsClient();
		const buffered = [{ timestamp: 1, message: "a" }];
		await rejects(
			() =>
				putLogEvents([...buffered, { timestamp: 2, message: 2 }], { client }),
			(error) => {
				strictEqual(error.name, "TypeError");
				deepStrictEqual(error.failedMessages, buffered);
				return true;
			},
		);
		deepStrictEqual(client.calls, []);
	});
});
