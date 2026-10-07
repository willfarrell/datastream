import { deepStrictEqual, rejects } from "node:assert";
import test, { describe } from "node:test";
import {
	DynamoDBStreamsClient,
	GetRecordsCommand,
} from "@aws-sdk/client-dynamodb-streams";
import * as awsModule from "@datastream/aws/dynamodb-streams";
import {
	awsDynamoDBStreamsGetRecordsStream,
	awsDynamoDBStreamsSetClient,
} from "@datastream/aws/dynamodb-streams";
import { streamToArray } from "@datastream/core";
import { mockClient } from "aws-sdk-client-mock";
import { variant } from "../variant.js";

describe(`@datastream/aws/dynamodb-streams (${variant})`, () => {
	test(`awsDynamoDBStreamsGetRecordsStream should get chunk`, async (_t) => {
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

	test(`awsDynamoDBStreamsGetRecordsStream should handle empty Records (undefined)`, async (_t) => {
		const client = mockClient(DynamoDBStreamsClient);
		awsDynamoDBStreamsSetClient(client);

		client.on(GetRecordsCommand).resolves({ NextShardIterator: "iter2" });

		const options = { ShardIterator: "iter1" };
		const stream = await awsDynamoDBStreamsGetRecordsStream(options);
		const output = await streamToArray(stream);

		deepStrictEqual(output, []);
	});

	test(`awsDynamoDBStreamsGetRecordsStream should track NextShardIterator across calls`, async (_t) => {
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

	test(`awsDynamoDBStreamsGetRecordsStream should stop when NextShardIterator is null (closed shard)`, async (_t) => {
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

	// SDK v3 omits NextShardIterator (undefined, not null) for a closed shard; the
	// loop must treat that as closed too, or pollingActive calls GetRecords forever
	// with an undefined ShardIterator.
	test(`awsDynamoDBStreamsGetRecordsStream stops polling when NextShardIterator is undefined`, async (_t) => {
		let calls = 0;
		awsDynamoDBStreamsSetClient({
			send: async () => {
				calls++;
				if (calls > 1) {
					throw new Error("GetRecords called after shard closed");
				}
				return { Records: [{ eventID: "a" }] };
			},
		});

		const stream = await awsDynamoDBStreamsGetRecordsStream({
			ShardIterator: "iter1",
			pollingActive: true,
			pollingDelay: 0,
		});
		const output = await streamToArray(stream);

		deepStrictEqual(output, [{ eventID: "a" }]);
		deepStrictEqual(calls, 1);
	});

	test(`awsDynamoDBStreamsGetRecordsStream should keep polling with pollingActive option`, async (_t) => {
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

	test(`awsDynamoDBStreamsGetRecordsStream should delay polling when pollingActive and no records`, async (t) => {
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

	test(`awsDynamoDBStreamsGetRecordsStream should abort the idle poll delay on signal`, async (_t) => {
		const client = mockClient(DynamoDBStreamsClient);
		awsDynamoDBStreamsSetClient(client);

		client
			.on(GetRecordsCommand)
			.resolves({ Records: [], NextShardIterator: "i" });

		const controller = new AbortController();
		const options = {
			ShardIterator: "iter1",
			pollingActive: true,
			pollingDelay: 60_000,
		};
		const stream = await awsDynamoDBStreamsGetRecordsStream(options, {
			signal: controller.signal,
		});

		const consuming = (async () => {
			for await (const _item of stream) {
				// no records ever arrive
			}
		})();

		await new Promise((resolve) => setImmediate(resolve));
		controller.abort();

		await rejects(consuming, (error) => {
			return error.cause?.code === "AbortError" || /Abort/i.test(error.message);
		});
	});

	// *** AbortSignal *** //
	test(`awsDynamoDBStreamsGetRecordsStream should pass signal to client`, async (_t) => {
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

	// *** options spread through to the command *** //
	test(`awsDynamoDBStreamsGetRecordsStream passes Limit through to GetRecords`, async (_t) => {
		const client = mockClient(DynamoDBStreamsClient);
		awsDynamoDBStreamsSetClient(client);
		client
			.on(GetRecordsCommand)
			.resolvesOnce({ Records: [{ eventID: "1" }], NextShardIterator: null });

		const stream = await awsDynamoDBStreamsGetRecordsStream({
			ShardIterator: "shard-abc",
			Limit: 25,
		});
		await streamToArray(stream);

		// `opts.ShardIterator` is reassigned inside the loop and captured by
		// reference, so assert on `Limit` (not mutated) -- it proves the caller
		// options were spread through (a `command({})` mutant would drop it).
		const input = client.commandCalls(GetRecordsCommand)[0].args[0].input;
		deepStrictEqual(input.Limit, 25);
	});

	// A per-call `client` option must route that call's sends to it (not the
	// module client) and must not be forwarded into the command input.
	test(`awsDynamoDBStreamsGetRecordsStream uses the per-call client option and does not forward it`, async (_t) => {
		awsDynamoDBStreamsSetClient({
			send: async () => {
				throw new Error("module client used");
			},
		});
		const inputs = [];
		const client = {
			send: async (command) => {
				inputs.push({ ...command.input });
				return { NextShardIterator: null };
			},
		};

		const output = await streamToArray(
			await awsDynamoDBStreamsGetRecordsStream({ ShardIterator: "s", client }),
		);

		deepStrictEqual(output, []);
		deepStrictEqual(inputs, [{ ShardIterator: "s" }]);
	});

	// *** setClient swap *** //
	test(`awsDynamoDBStreamsSetClient routes to the new client`, async (_t) => {
		const first = mockClient(DynamoDBStreamsClient);
		awsDynamoDBStreamsSetClient(first);
		first
			.on(GetRecordsCommand)
			.resolves({ Records: [], NextShardIterator: null });

		const second = mockClient(DynamoDBStreamsClient);
		second
			.on(GetRecordsCommand)
			.resolves({ Records: [], NextShardIterator: null });
		awsDynamoDBStreamsSetClient(second);

		const stream = await awsDynamoDBStreamsGetRecordsStream({
			ShardIterator: "i",
		});
		await streamToArray(stream);

		deepStrictEqual(second.commandCalls(GetRecordsCommand).length, 1);
		deepStrictEqual(first.commandCalls(GetRecordsCommand).length, 0);
	});

	// setClient must STORE the passed client; a plain stub (prototype-mock-proof)
	// proves the stored reference is used. A `setClient(){}` mutant leaves the prior
	// client in place.
	test(`awsDynamoDBStreamsSetClient stores the passed client reference`, async (_t) => {
		let calls = 0;
		const stub = {
			send: async () => {
				calls++;
				return { Records: [{ eventID: "stub" }], NextShardIterator: null };
			},
		};
		awsDynamoDBStreamsSetClient(stub);

		const stream = await awsDynamoDBStreamsGetRecordsStream({
			ShardIterator: "iter1",
		});
		const output = await streamToArray(stream);

		deepStrictEqual(output, [{ eventID: "stub" }]);
		deepStrictEqual(calls, 1);
	});

	// *** idle-wait guard conjuncts *** //
	test(`awsDynamoDBStreamsGetRecordsStream does not delay when records are present`, async (t) => {
		t.mock.timers.enable({ apis: ["setTimeout"] });
		const client = mockClient(DynamoDBStreamsClient);
		awsDynamoDBStreamsSetClient(client);
		// Records always present so the `records.length === 0` conjunct is false and
		// no timer is scheduled. A `true` mutant on that conjunct would delay after
		// every (non-empty) poll and the consumer would stall on the mock timer.
		client.on(GetRecordsCommand).resolves({
			Records: [{ eventID: "1" }],
			NextShardIterator: "iter-next",
		});

		const options = {
			ShardIterator: "iter1",
			pollingActive: true,
			pollingDelay: 60_000,
		};
		const stream = await awsDynamoDBStreamsGetRecordsStream(options);

		const output = [];
		for await (const item of stream) {
			output.push(item);
			if (output.length >= 3) break;
		}
		deepStrictEqual(output.length, 3);
	});

	test(`awsDynamoDBStreamsGetRecordsStream pollingDelay 0 schedules no timer`, async (t) => {
		t.mock.timers.enable({ apis: ["setTimeout"] });
		const client = mockClient(DynamoDBStreamsClient);
		awsDynamoDBStreamsSetClient(client);
		// pollingActive, empty records, pollingDelay === 0: `pollingDelay > 0` is false
		// so no idle wait. A `> 0` -> `true` or `>= 0` mutant would await timeout(0)
		// which the un-ticked mock timer never resolves.
		client
			.on(GetRecordsCommand)
			.resolvesOnce({ Records: [], NextShardIterator: "iter2" })
			.resolves({ Records: [{ eventID: "1" }], NextShardIterator: "iter3" });

		const options = {
			ShardIterator: "iter1",
			pollingActive: true,
			pollingDelay: 0,
		};
		const stream = await awsDynamoDBStreamsGetRecordsStream(options);

		const output = [];
		for await (const item of stream) {
			output.push(item);
			if (output.length >= 1) break;
		}
		deepStrictEqual(output, [{ eventID: "1" }]);
	});

	test(`awsDynamoDBStreamsGetRecordsStream non-polling empty page ends without an idle wait`, async (t) => {
		t.mock.timers.enable({ apis: ["setTimeout"] });
		const client = mockClient(DynamoDBStreamsClient);
		awsDynamoDBStreamsSetClient(client);
		// Not polling: an empty page ends the stream WITHOUT entering the idle wait.
		// The `&&` -> `||` mutant makes `pollingActive || records.length === 0` true on
		// the empty page, scheduling a (default 1000ms) timeout the un-ticked mock
		// timer never resolves -> the stream would hang instead of completing.
		client
			.on(GetRecordsCommand)
			.resolvesOnce({ Records: [{ eventID: "1" }], NextShardIterator: "iter2" })
			.resolves({ Records: [], NextShardIterator: "iter3" });

		const stream = await awsDynamoDBStreamsGetRecordsStream({
			ShardIterator: "iter1",
		});
		const output = await streamToArray(stream);

		deepStrictEqual(output, [{ eventID: "1" }]);
	});

	// Named exports are canonical; a default export must not come back.
	test(`dynamodb-streams has no default export`, (_t) => {
		deepStrictEqual(Object.hasOwn(awsModule, "default"), false);
	});
});
