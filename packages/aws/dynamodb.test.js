import { deepStrictEqual, rejects } from "node:assert";
import test from "node:test";
import {
	BatchGetItemCommand,
	BatchWriteItemCommand,
	DynamoDBClient,
	QueryCommand,
	ScanCommand,
} from "@aws-sdk/client-dynamodb";
import {
	awsDynamoDBDeleteItemStream,
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
