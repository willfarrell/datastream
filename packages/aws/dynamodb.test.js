import { deepStrictEqual, rejects } from "node:assert";
import test from "node:test";
import {
	BatchGetItemCommand,
	BatchWriteItemCommand,
	DynamoDBClient,
	ExecuteStatementCommand,
	QueryCommand,
	ScanCommand,
} from "@aws-sdk/client-dynamodb";
import dynamodbDefault, {
	awsDynamoDBDeleteItemStream,
	awsDynamoDBExecuteStatementStream,
	awsDynamoDBGetItemStream,
	awsDynamoDBPutItemStream,
	awsDynamoDBQueryStream,
	awsDynamoDBScanStream,
	awsDynamoDBSetClient,
} from "@datastream/aws/dynamodb";

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

test(`${variant}: awsDynamoDBGetStream should return items`, async (_t) => {
	const client = mockClient(DynamoDBClient);
	awsDynamoDBSetClient(client);
	client
		.on(BatchGetItemCommand, {
			RequestItems: {
				TableName: {
					Keys: [{ key: "a" }, { key: "b" }, { key: "c" }],
				},
			},
		})
		.resolves({
			Responses: {
				TableName: [
					{ key: "a", value: 1 },
					{ key: "b", value: 2 },
				],
			},
			UnprocessedKeys: {
				TableName: {
					Keys: [{ key: "c" }],
				},
			},
		})
		.on(BatchGetItemCommand, {
			RequestItems: {
				TableName: {
					Keys: [{ key: "c" }],
				},
			},
		})
		.resolves({
			Responses: {
				TableName: [{ key: "c", value: 3 }],
			},
			UnprocessedKeys: {},
		});

	const options = {
		TableName: "TableName",
		Keys: [{ key: "a" }, { key: "b" }, { key: "c" }],
	};
	const stream = await awsDynamoDBGetItemStream(options);
	const output = await streamToArray(stream);

	deepStrictEqual(output, [
		{ key: "a", value: 1 },
		{ key: "b", value: 2 },
		{ key: "c", value: 3 },
	]);
});

test(`${variant}: awsDynamoDBGetStream should throw error`, async (_t) => {
	const client = mockClient(DynamoDBClient);
	awsDynamoDBSetClient(client);
	client
		.on(BatchGetItemCommand, {
			RequestItems: {
				TableName: {
					Keys: [{ key: "a" }, { key: "b" }, { key: "c" }],
				},
			},
		})
		.resolves({
			Responses: {
				TableName: [
					{ key: "a", value: 1 },
					{ key: "b", value: 2 },
				],
			},
			UnprocessedKeys: {
				TableName: {
					Keys: [{ key: "c" }],
				},
			},
		})
		.on(BatchGetItemCommand, {
			RequestItems: {
				TableName: {
					Keys: [{ key: "c" }],
				},
			},
		})
		.resolves({
			Responses: {
				TableName: [],
			},
			UnprocessedKeys: {
				TableName: {
					Keys: [{ key: "c" }],
				},
			},
		});

	const options = {
		TableName: "TableName",
		Keys: [{ key: "a" }, { key: "b" }, { key: "c" }],
		retryMaxCount: 0, // force
	};
	const stream = await awsDynamoDBGetItemStream(options);
	await rejects(() => streamToArray(stream), {
		message: "awsDynamoDBBatchGetItem has UnprocessedKeys",
	});
});

test(`${variant}: awsDynamoDBQueryStream should return items`, async (_t) => {
	const client = mockClient(DynamoDBClient);
	awsDynamoDBSetClient(client);
	client
		.on(QueryCommand, {
			TableName: "TableName",
		})
		.resolves({
			Items: [
				{ key: "a", value: 1 },
				{ key: "b", value: 2 },
			],
			LastEvaluatedKey: {
				key: "b",
			},
		})
		.on(QueryCommand, {
			TableName: "TableName",
			ExclusiveStartKey: { key: "b" },
		})
		.resolves({
			Items: [{ key: "c", value: 3 }],
		});

	const options = {
		TableName: "TableName",
	};
	const stream = await awsDynamoDBQueryStream(options);
	const output = await streamToArray(stream);

	deepStrictEqual(output, [
		{ key: "a", value: 1 },
		{ key: "b", value: 2 },
		{ key: "c", value: 3 },
	]);
});

test(`${variant}: awsDynamoDBScanStream should return items`, async (_t) => {
	const client = mockClient(DynamoDBClient);
	awsDynamoDBSetClient(client);
	client
		.on(ScanCommand, {
			TableName: "TableName",
		})
		.resolves({
			Items: [
				{ key: "a", value: 1 },
				{ key: "b", value: 2 },
			],
			LastEvaluatedKey: {
				key: "b",
			},
		})
		.on(ScanCommand, {
			TableName: "TableName",
			ExclusiveStartKey: { key: "b" },
		})
		.resolves({
			Items: [{ key: "c", value: 3 }],
		});

	const options = {
		TableName: "TableName",
	};
	const stream = await awsDynamoDBScanStream(options);
	const output = await streamToArray(stream);

	deepStrictEqual(output, [
		{ key: "a", value: 1 },
		{ key: "b", value: 2 },
		{ key: "c", value: 3 },
	]);
});

test(`${variant}: awsDynamoDBExecuteStatementStream should return items`, async (_t) => {
	const client = mockClient(DynamoDBClient);
	awsDynamoDBSetClient(client);
	client
		.on(ExecuteStatementCommand, {
			Statement: 'SELECT * FROM "TableName"',
		})
		.resolves({
			Items: [
				{ key: "a", value: 1 },
				{ key: "b", value: 2 },
			],
			NextToken: "token1",
		})
		.on(ExecuteStatementCommand, {
			Statement: 'SELECT * FROM "TableName"',
			NextToken: "token1",
		})
		.resolves({
			Items: [{ key: "c", value: 3 }],
		});

	const options = {
		Statement: 'SELECT * FROM "TableName"',
	};
	const stream = await awsDynamoDBExecuteStatementStream(options);
	const output = await streamToArray(stream);

	deepStrictEqual(output, [
		{ key: "a", value: 1 },
		{ key: "b", value: 2 },
		{ key: "c", value: 3 },
	]);
});

test(`${variant}: awsDynamoDBExecuteStatementStream should handle empty result`, async (_t) => {
	const client = mockClient(DynamoDBClient);
	awsDynamoDBSetClient(client);
	client.on(ExecuteStatementCommand).resolves({
		Items: [],
	});

	const options = {
		Statement: "SELECT * FROM \"TableName\" WHERE key = 'missing'",
	};
	const stream = await awsDynamoDBExecuteStatementStream(options);
	const output = await streamToArray(stream);

	deepStrictEqual(output, []);
});

test(`${variant}: awsDynamoDBPutItemStream should store items`, async (_t) => {
	const client = mockClient(DynamoDBClient);
	awsDynamoDBSetClient(client);
	client
		.on(BatchWriteItemCommand, {
			RequestItems: {
				TableName: "abcdefghijklmnopqrstuvwxy".split("").map((key, value) => ({
					PutRequest: {
						Item: {
							key,
							value,
						},
					},
				})),
			},
		})
		.resolves({
			UnprocessedItems: {
				TableName: [
					{
						PutRequest: {
							Item: {
								key: "y",
								value: 24,
							},
						},
					},
				],
			},
		})
		// y failed, retry
		.on(BatchWriteItemCommand, {
			RequestItems: {
				TableName: [
					{
						PutRequest: {
							Item: {
								key: "y",
								value: 24,
							},
						},
					},
				],
			},
		})
		.resolves({ UnprocessedItems: {} })
		// final
		.on(BatchWriteItemCommand, {
			RequestItems: {
				TableName: [
					{
						PutRequest: {
							Item: {
								key: "z",
								value: 25,
							},
						},
					},
				],
			},
		})
		.resolves({ UnprocessedItems: {} });

	const input = "abcdefghijklmnopqrstuvwxyz"
		.split("")
		.map((key, value) => ({ key, value }));
	const options = {
		TableName: "TableName",
	};
	const stream = [
		createReadableStream(input),
		awsDynamoDBPutItemStream(options),
	];
	const output = await pipeline(stream);

	deepStrictEqual(output, {});
});

test(`${variant}: awsDynamoDBPutItemStream should throw error`, async (_t) => {
	const client = mockClient(DynamoDBClient);
	awsDynamoDBSetClient(client);
	client
		.on(BatchWriteItemCommand, {
			RequestItems: {
				TableName: [
					{
						PutRequest: {
							Item: {
								key: "a",
								value: 0,
							},
						},
					},
				],
			},
		})
		.resolves({
			UnprocessedItems: {
				TableName: [
					{
						PutRequest: {
							Item: {
								key: "a",
								value: 0,
							},
						},
					},
				],
			},
		})
		.on(BatchWriteItemCommand, {
			RequestItems: {
				TableName: [
					{
						PutRequest: {
							Item: {
								key: "a",
								value: 0,
							},
						},
					},
				],
			},
		})
		.resolves({
			UnprocessedItems: {
				TableName: [
					{
						PutRequest: {
							Item: {
								key: "a",
								value: 0,
							},
						},
					},
				],
			},
		});

	const input = [{ key: "a", value: 0 }];
	const options = {
		TableName: "TableName",
		retryMaxCount: 0, // force
	};
	const stream = [
		createReadableStream(input),
		awsDynamoDBPutItemStream(options),
	];
	await rejects(() => pipeline(stream), {
		message: "awsDynamoDBBatchWriteItem has UnprocessedItems",
	});
});

test(`${variant}: awsDynamoDBDeleteItemStream should delete items`, async (_t) => {
	const client = mockClient(DynamoDBClient);
	awsDynamoDBSetClient(client);
	client
		.on(BatchWriteItemCommand, {
			RequestItems: {
				TableName: "abcdefghijklmnopqrstuvwxy".split("").map((key) => ({
					DeleteRequest: {
						Key: { key },
					},
				})),
			},
		})
		.resolves({
			UnprocessedItems: {
				TableName: [
					{
						DeleteRequest: {
							Key: {
								key: "y",
							},
						},
					},
				],
			},
		})
		.on(BatchWriteItemCommand, {
			RequestItems: {
				TableName: [
					{
						DeleteRequest: {
							Key: {
								key: "y",
							},
						},
					},
				],
			},
		})
		.resolves({ UnprocessedItems: {} })
		// final
		.on(BatchWriteItemCommand, {
			RequestItems: {
				TableName: [
					{
						DeleteRequest: {
							Key: { key: "z" },
						},
					},
				],
			},
		})
		.resolves({ UnprocessedItems: {} });

	const input = "abcdefghijklmnopqrstuvwxyz".split("").map((key) => ({ key }));
	const options = {
		TableName: "TableName",
	};
	const stream = [
		createReadableStream(input),
		awsDynamoDBDeleteItemStream(options),
	];
	const output = await pipeline(stream);

	deepStrictEqual(output, {});
});

test(`${variant}: awsDynamoDBPutItemStream should handle empty input`, async (_t) => {
	const client = mockClient(DynamoDBClient);
	awsDynamoDBSetClient(client);

	const input = [];
	const options = { TableName: "TableName" };
	const stream = [
		createReadableStream(input),
		awsDynamoDBPutItemStream(options),
	];
	const output = await pipeline(stream);

	deepStrictEqual(output, {});
});

test(`${variant}: awsDynamoDBDeleteItemStream should handle empty input`, async (_t) => {
	const client = mockClient(DynamoDBClient);
	awsDynamoDBSetClient(client);

	const input = [];
	const options = { TableName: "TableName" };
	const stream = [
		createReadableStream(input),
		awsDynamoDBDeleteItemStream(options),
	];
	const output = await pipeline(stream);

	deepStrictEqual(output, {});
});

test(`${variant}: awsDynamoDBDeleteItemStream should throw error`, async (_t) => {
	const client = mockClient(DynamoDBClient);
	awsDynamoDBSetClient(client);
	client
		.on(BatchWriteItemCommand, {
			RequestItems: {
				TableName: [
					{
						DeleteRequest: {
							Key: { key: "a" },
						},
					},
				],
			},
		})
		.resolves({
			UnprocessedItems: {
				TableName: [
					{
						DeleteRequest: {
							Key: { key: "a" },
						},
					},
				],
			},
		})
		.on(BatchWriteItemCommand, {
			RequestItems: {
				TableName: [
					{
						DeleteRequest: {
							Key: { key: "a" },
						},
					},
				],
			},
		})
		.resolves({
			UnprocessedItems: {
				TableName: [
					{
						DeleteRequest: {
							Key: { key: "a" },
						},
					},
				],
			},
		});

	const input = [{ key: "a" }];
	const options = {
		TableName: "TableName",
		retryMaxCount: 0, // force
	};
	const stream = [
		createReadableStream(input),
		awsDynamoDBDeleteItemStream(options),
	];
	await rejects(() => pipeline(stream), {
		message: "awsDynamoDBBatchWriteItem has UnprocessedItems",
	});
});

test(`${variant}: awsDynamoDBPutItemStream should pass abort signal to batch write`, async (_t) => {
	const client = mockClient(DynamoDBClient);
	awsDynamoDBSetClient(client);
	client.on(BatchWriteItemCommand).resolves({ UnprocessedItems: {} });

	const controller = new AbortController();
	const input = [{ key: "a", value: 1 }];
	const stream = [
		createReadableStream(input),
		awsDynamoDBPutItemStream({ TableName: "T" }, { signal: controller.signal }),
	];
	await pipeline(stream);

	const calls = client.commandCalls(BatchWriteItemCommand);
	deepStrictEqual(calls[0].args[1]?.abortSignal, controller.signal);
});

// *** options mutation *** //
test(`${variant}: awsDynamoDBQueryStream should not mutate caller options`, async (_t) => {
	const client = mockClient(DynamoDBClient);
	awsDynamoDBSetClient(client);
	client.on(QueryCommand).resolves({
		Items: [{ key: "a" }],
		LastEvaluatedKey: { key: "a" },
	});
	client.on(QueryCommand).resolves({
		Items: [{ key: "b" }],
	});

	const options = { TableName: "T" };
	const optionsCopy = { ...options };
	const stream = await awsDynamoDBQueryStream(options);
	await streamToArray(stream);

	deepStrictEqual(options, optionsCopy);
});

test(`${variant}: awsDynamoDBGetItemStream should reject when Keys.length exceeds 100`, async (_t) => {
	const client = mockClient(DynamoDBClient);
	awsDynamoDBSetClient(client);
	const options = {
		TableName: "TableName",
		Keys: new Array(101).fill({ key: "x" }),
	};
	await rejects(
		() => awsDynamoDBGetItemStream(options),
		/exceeds BatchGetItem limit of 100/,
	);
});

// *** empty-page / fully-throttled guards *** //
test(`${variant}: awsDynamoDBQueryStream should handle a page with no Items`, async (_t) => {
	const client = mockClient(DynamoDBClient);
	awsDynamoDBSetClient(client);
	client
		.on(QueryCommand, { TableName: "TableName" })
		.resolvesOnce({
			// filtered page: no Items but still paginating
			Count: 0,
			LastEvaluatedKey: { key: "a" },
		})
		.resolvesOnce({
			Items: [{ key: "b", value: 2 }],
		});

	const options = { TableName: "TableName" };
	const stream = await awsDynamoDBQueryStream(options);
	const output = await streamToArray(stream);

	deepStrictEqual(output, [{ key: "b", value: 2 }]);
});

test(`${variant}: awsDynamoDBScanStream should handle a page with no Items`, async (_t) => {
	const client = mockClient(DynamoDBClient);
	awsDynamoDBSetClient(client);
	client
		.on(ScanCommand, { TableName: "TableName" })
		.resolvesOnce({
			Count: 0,
			LastEvaluatedKey: { key: "a" },
		})
		.resolvesOnce({
			Items: [{ key: "b", value: 2 }],
		});

	const options = { TableName: "TableName" };
	const stream = await awsDynamoDBScanStream(options);
	const output = await streamToArray(stream);

	deepStrictEqual(output, [{ key: "b", value: 2 }]);
});

test(`${variant}: awsDynamoDBGetItemStream should handle fully-throttled batch (empty Responses)`, async (_t) => {
	const client = mockClient(DynamoDBClient);
	awsDynamoDBSetClient(client);
	client
		.on(BatchGetItemCommand, {
			RequestItems: { TableName: { Keys: [{ key: "a" }] } },
		})
		.resolvesOnce({
			// entire batch throttled: Responses has no entry for the table
			Responses: {},
			UnprocessedKeys: { TableName: { Keys: [{ key: "a" }] } },
		})
		.resolvesOnce({
			Responses: { TableName: [{ key: "a", value: 1 }] },
			UnprocessedKeys: {},
		});

	const options = { TableName: "TableName", Keys: [{ key: "a" }] };
	const stream = await awsDynamoDBGetItemStream(options);
	const output = await streamToArray(stream);

	deepStrictEqual(output, [{ key: "a", value: 1 }]);
});

// *** backoff is abortable: aborting during the backoff wait rejects *** //
test(`${variant}: awsDynamoDBGetItemStream aborts during retry backoff`, async (_t) => {
	const client = mockClient(DynamoDBClient);
	awsDynamoDBSetClient(client);
	// Always throttled so the stream enters the (real-timer) backoff wait.
	client.on(BatchGetItemCommand).resolves({
		Responses: {},
		UnprocessedKeys: { TableName: { Keys: [{ key: "a" }] } },
	});

	const controller = new AbortController();
	const options = { TableName: "TableName", Keys: [{ key: "a" }] };
	const stream = await awsDynamoDBGetItemStream(options, {
		signal: controller.signal,
	});

	const consuming = streamToArray(stream);
	// Let the first throttled batch settle and enter the backoff timer, then
	// abort. The backoff timeout was given `{ signal }`, so it must reject
	// promptly. A `{}` mutant (dropping the signal) would let the backoff run to
	// completion and keep retrying, so this would hang instead of rejecting.
	await new Promise((resolve) => setImmediate(resolve));
	controller.abort();

	await rejects(consuming, (error) => {
		return error.cause?.code === "AbortError" || /Abort/i.test(error.message);
	});
});

// *** backoff floor: the first retry waits at least 50ms (not 3^0 == 1ms) *** //
test(`${variant}: awsDynamoDBGetItemStream first retry backoff is floored at 50ms`, async (t) => {
	t.mock.timers.enable({ apis: ["setTimeout"] });

	const client = mockClient(DynamoDBClient);
	awsDynamoDBSetClient(client);
	client
		.on(BatchGetItemCommand)
		.resolvesOnce({
			Responses: {},
			UnprocessedKeys: { TableName: { Keys: [{ key: "a" }] } },
		})
		.resolves({
			Responses: { TableName: [{ key: "a", value: 1 }] },
			UnprocessedKeys: {},
		});

	const options = { TableName: "TableName", Keys: [{ key: "a" }] };
	const stream = await awsDynamoDBGetItemStream(options);
	const consuming = streamToArray(stream);

	// Let the first (throttled) batch settle and enter the backoff timer.
	await new Promise((resolve) => setImmediate(resolve));
	// 3^0 == 1ms would already fire; the 50ms floor means the retry must still be
	// pending at 1ms (kills the Math.max -> Math.min floor mutant and the
	// arrow-body removal).
	t.mock.timers.tick(1);
	await new Promise((resolve) => setImmediate(resolve));
	deepStrictEqual(client.commandCalls(BatchGetItemCommand).length, 1);

	t.mock.timers.tick(49);
	const output = await consuming;
	deepStrictEqual(output, [{ key: "a", value: 1 }]);
	deepStrictEqual(client.commandCalls(BatchGetItemCommand).length, 2);
});

// *** setClient swap *** //
test(`${variant}: awsDynamoDBSetClient routes subsequent sends to the new client`, async (_t) => {
	const first = mockClient(DynamoDBClient);
	awsDynamoDBSetClient(first);
	first.on(QueryCommand).resolves({ Items: [{ key: "stale" }] });

	const second = mockClient(DynamoDBClient);
	second.on(QueryCommand).resolves({ Items: [{ key: "fresh" }] });
	awsDynamoDBSetClient(second);

	const stream = await awsDynamoDBQueryStream({ TableName: "T" });
	const output = await streamToArray(stream);

	deepStrictEqual(output, [{ key: "fresh" }]);
	deepStrictEqual(second.commandCalls(QueryCommand).length, 1);
	deepStrictEqual(first.commandCalls(QueryCommand).length, 0);
});

// setClient must STORE the passed client; a plain stub (prototype-mock-proof)
// proves the stored reference is used. A `setClient(){}` mutant leaves the prior
// client in place.
test(`${variant}: awsDynamoDBSetClient stores the passed client reference`, async (_t) => {
	let calls = 0;
	const stub = {
		send: async () => {
			calls++;
			return { Items: [{ key: "stub" }] };
		},
	};
	awsDynamoDBSetClient(stub);

	const stream = await awsDynamoDBQueryStream({ TableName: "T" });
	const output = await streamToArray(stream);

	deepStrictEqual(output, [{ key: "stub" }]);
	deepStrictEqual(calls, 1);
});

// *** abortSignal forwarding on each read command (kills the `{}` options mutant) *** //
test(`${variant}: awsDynamoDBQueryStream forwards abortSignal to client.send`, async (_t) => {
	const client = mockClient(DynamoDBClient);
	awsDynamoDBSetClient(client);
	client.on(QueryCommand).resolves({ Items: [{ key: "a" }] });

	const controller = new AbortController();
	const stream = await awsDynamoDBQueryStream(
		{ TableName: "T" },
		{ signal: controller.signal },
	);
	await streamToArray(stream);

	const calls = client.commandCalls(QueryCommand);
	deepStrictEqual(calls[0].args[1]?.abortSignal, controller.signal);
});

test(`${variant}: awsDynamoDBScanStream forwards abortSignal to client.send`, async (_t) => {
	const client = mockClient(DynamoDBClient);
	awsDynamoDBSetClient(client);
	client.on(ScanCommand).resolves({ Items: [{ key: "a" }] });

	const controller = new AbortController();
	const stream = await awsDynamoDBScanStream(
		{ TableName: "T" },
		{ signal: controller.signal },
	);
	await streamToArray(stream);

	const calls = client.commandCalls(ScanCommand);
	deepStrictEqual(calls[0].args[1]?.abortSignal, controller.signal);
});

test(`${variant}: awsDynamoDBExecuteStatementStream forwards abortSignal to client.send`, async (_t) => {
	const client = mockClient(DynamoDBClient);
	awsDynamoDBSetClient(client);
	client.on(ExecuteStatementCommand).resolves({ Items: [{ key: "a" }] });

	const controller = new AbortController();
	const stream = await awsDynamoDBExecuteStatementStream(
		{ Statement: "SELECT" },
		{ signal: controller.signal },
	);
	await streamToArray(stream);

	const calls = client.commandCalls(ExecuteStatementCommand);
	deepStrictEqual(calls[0].args[1]?.abortSignal, controller.signal);
});

test(`${variant}: awsDynamoDBGetItemStream forwards abortSignal to client.send`, async (_t) => {
	const client = mockClient(DynamoDBClient);
	awsDynamoDBSetClient(client);
	client
		.on(BatchGetItemCommand)
		.resolves({ Responses: { T: [{ key: "a" }] }, UnprocessedKeys: {} });

	const controller = new AbortController();
	const stream = await awsDynamoDBGetItemStream(
		{ TableName: "T", Keys: [{ key: "a" }] },
		{ signal: controller.signal },
	);
	await streamToArray(stream);

	const calls = client.commandCalls(BatchGetItemCommand);
	deepStrictEqual(calls[0].args[1]?.abortSignal, controller.signal);
});

// *** executeStatement yields nothing when the response has no Items field *** //
test(`${variant}: awsDynamoDBExecuteStatementStream yields nothing for a response without Items`, async (_t) => {
	const client = mockClient(DynamoDBClient);
	awsDynamoDBSetClient(client);
	// No Items field at all: the `response.Items ?? []` default must be an EMPTY
	// array. A `["Stryker was here"]` mutant would yield that sentinel.
	client.on(ExecuteStatementCommand).resolves({});

	const stream = await awsDynamoDBExecuteStatementStream({
		Statement: "SELECT",
	});
	const output = await streamToArray(stream);

	deepStrictEqual(output, []);
});

// *** Keys boundary: exactly 100 is allowed (strict `>`) *** //
test(`${variant}: awsDynamoDBGetItemStream allows exactly 100 Keys`, async (_t) => {
	const client = mockClient(DynamoDBClient);
	awsDynamoDBSetClient(client);
	client
		.on(BatchGetItemCommand)
		.resolves({ Responses: { T: [] }, UnprocessedKeys: {} });

	// Exactly 100 must NOT throw (strict `>`); a `>=` mutant would reject it.
	const options = { TableName: "T", Keys: new Array(100).fill({ key: "x" }) };
	const stream = await awsDynamoDBGetItemStream(options);
	const output = await streamToArray(stream);
	deepStrictEqual(output, []);
});

// *** getItem with no Keys (optional chaining on options.Keys) *** //
test(`${variant}: awsDynamoDBGetItemStream tolerates options without Keys`, async (_t) => {
	const client = mockClient(DynamoDBClient);
	awsDynamoDBSetClient(client);
	client
		.on(BatchGetItemCommand)
		.resolves({ Responses: { T: [{ key: "a" }] }, UnprocessedKeys: {} });

	// No Keys property: the `options.Keys?.length > 100` guard must short-circuit
	// via the optional chain. A non-optional `options.Keys.length` mutant throws.
	const stream = await awsDynamoDBGetItemStream({ TableName: "T" });
	const output = await streamToArray(stream);
	deepStrictEqual(output, [{ key: "a" }]);
});

// *** getItem tolerates a response missing Responses / UnprocessedKeys *** //
test(`${variant}: awsDynamoDBGetItemStream tolerates a response with neither Responses nor UnprocessedKeys`, async (_t) => {
	const client = mockClient(DynamoDBClient);
	awsDynamoDBSetClient(client);
	// Bare response: `response.Responses?.[T]` and `response?.UnprocessedKeys?.[T]?.Keys`
	// optional chains must yield [] (no items, no retry). Non-optional mutants
	// (`response.Responses[T]`, `response.UnprocessedKeys`, `response?.UnprocessedKeys[T]`)
	// would throw a TypeError reading a property of undefined.
	client.on(BatchGetItemCommand).resolves({});

	const stream = await awsDynamoDBGetItemStream({
		TableName: "T",
		Keys: [{ key: "a" }],
	});
	const output = await streamToArray(stream);
	deepStrictEqual(output, []);
});

// *** getItem retry accounting: retryMaxCount honoured, error cause populated *** //
test(`${variant}: awsDynamoDBGetItemStream throws after exactly retryMaxCount retries with cause`, async (_t) => {
	const client = mockClient(DynamoDBClient);
	awsDynamoDBSetClient(client);
	// Always throttled. retryMaxCount 1: attempt #1 (retryCount 0, 0 >= 1 false ->
	// backoff, retryCount -> 1), attempt #2 (1 >= 1 true -> throw). Exactly 2 calls.
	// A `>` mutant would need retryCount > 1 (3 calls); a `--` mutant on the
	// counter would never reach the cap (retry forever -> timeout).
	client.on(BatchGetItemCommand).resolves({
		Responses: {},
		UnprocessedKeys: { T: { Keys: [{ key: "a" }] } },
	});

	await rejects(
		async () =>
			streamToArray(
				await awsDynamoDBGetItemStream({
					TableName: "T",
					Keys: [{ key: "a" }],
					retryMaxCount: 1,
				}),
			),
		(error) => {
			deepStrictEqual(
				error.message,
				"awsDynamoDBBatchGetItem has UnprocessedKeys",
			);
			// `{}` mutants on the error options / cause would drop these fields.
			deepStrictEqual(error.cause.UnprocessedKeysCount, 1);
			deepStrictEqual(error.cause.TableName, "T");
			return true;
		},
	);
	deepStrictEqual(client.commandCalls(BatchGetItemCommand).length, 2);
});

// *** getItem: retryMaxCount ?? 10 must use nullish coalescing (not &&) *** //
test(`${variant}: awsDynamoDBGetItemStream honours an explicit retryMaxCount of 2`, async (_t) => {
	const client = mockClient(DynamoDBClient);
	awsDynamoDBSetClient(client);
	// retryMaxCount 2: `2 ?? 10` === 2 (throw after 3 calls). A `2 && 10` mutant
	// yields 10 -> 11 calls (and a huge cumulative backoff -> timeout). Assert the
	// exact call count pins the coalescing operator.
	client.on(BatchGetItemCommand).resolves({
		Responses: {},
		UnprocessedKeys: { T: { Keys: [{ key: "a" }] } },
	});

	await rejects(
		async () =>
			streamToArray(
				await awsDynamoDBGetItemStream({
					TableName: "T",
					Keys: [{ key: "a" }],
					retryMaxCount: 2,
				}),
			),
		{ message: "awsDynamoDBBatchGetItem has UnprocessedKeys" },
	);
	deepStrictEqual(client.commandCalls(BatchGetItemCommand).length, 3);
});

// *** batchWrite: response with no UnprocessedItems is a success *** //
test(`${variant}: awsDynamoDBPutItemStream treats a response without UnprocessedItems as success`, async (_t) => {
	const client = mockClient(DynamoDBClient);
	awsDynamoDBSetClient(client);
	// Bare response (no UnprocessedItems): the `UnprocessedItems?.[T]?.length`
	// optional chain must be falsy (no retry). A non-optional mutant throws.
	client.on(BatchWriteItemCommand).resolves({});

	const output = await pipeline([
		createReadableStream([{ key: "a", value: 1 }]),
		awsDynamoDBPutItemStream({ TableName: "T" }),
	]);
	deepStrictEqual(output, {});
	deepStrictEqual(client.commandCalls(BatchWriteItemCommand).length, 1);
});

// *** batchWrite retry accounting: retryMaxCount honoured, counter increments,
// error cause populated, recursion uses retryCount + 1 *** //
test(`${variant}: awsDynamoDBPutItemStream throws after exactly retryMaxCount retries with cause`, async (_t) => {
	const client = mockClient(DynamoDBClient);
	awsDynamoDBSetClient(client);
	// Always returns UnprocessedItems. retryMaxCount 1 -> throw after 2 calls.
	// `>` mutant -> 3 calls; `retryCount + 1` -> `- 1` mutant never reaches the cap
	// (recurses forever -> timeout).
	client.on(BatchWriteItemCommand).resolves({
		UnprocessedItems: { T: [{ PutRequest: { Item: { key: "a" } } }] },
	});

	await rejects(
		() =>
			pipeline([
				createReadableStream([{ key: "a" }]),
				awsDynamoDBPutItemStream({ TableName: "T", retryMaxCount: 1 }),
			]),
		(error) => {
			deepStrictEqual(
				error.message,
				"awsDynamoDBBatchWriteItem has UnprocessedItems",
			);
			deepStrictEqual(error.cause.UnprocessedItemsCount, 1);
			deepStrictEqual(error.cause.TableName, "T");
			return true;
		},
	);
	deepStrictEqual(client.commandCalls(BatchWriteItemCommand).length, 2);
});

test(`${variant}: awsDynamoDBPutItemStream honours an explicit retryMaxCount of 2`, async (_t) => {
	const client = mockClient(DynamoDBClient);
	awsDynamoDBSetClient(client);
	// retryMaxCount 2: `2 ?? 10` === 2 -> throw after 3 calls. A `2 && 10` mutant
	// yields 10 -> many more calls (huge backoff -> timeout).
	client.on(BatchWriteItemCommand).resolves({
		UnprocessedItems: { T: [{ PutRequest: { Item: { key: "a" } } }] },
	});

	await rejects(
		() =>
			pipeline([
				createReadableStream([{ key: "a" }]),
				awsDynamoDBPutItemStream({ TableName: "T", retryMaxCount: 2 }),
			]),
		{ message: "awsDynamoDBBatchWriteItem has UnprocessedItems" },
	);
	deepStrictEqual(client.commandCalls(BatchWriteItemCommand).length, 3);
});

test(`${variant}: default export should include all stream functions`, (_t) => {
	deepStrictEqual(Object.keys(dynamodbDefault).sort(), [
		"deleteItemStream",
		"executeStatementStream",
		"getItemStream",
		"putItemStream",
		"queryStream",
		"scanStream",
		"setClient",
	]);
});
