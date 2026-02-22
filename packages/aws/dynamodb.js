// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
import {
	BatchGetItemCommand,
	BatchWriteItemCommand,
	DynamoDBClient,
	QueryCommand,
	ScanCommand,
} from "@aws-sdk/client-dynamodb";
import { createWritableStream, timeout } from "@datastream/core";
import { awsClientDefaults } from "./client.js";

let client = new DynamoDBClient(awsClientDefaults);
export const awsDynamoDBSetClient = (ddbClient, _translateConfig) => {
	client = ddbClient;
};
awsDynamoDBSetClient(client);

// options = {TableName, ...}

export const awsDynamoDBQueryStream = async (options, _streamOptions = {}) => {
	async function* command(options) {
		let expectMore = true;
		while (expectMore) {
			const response = await client.send(new QueryCommand(options));
			for (const item of response.Items) {
				yield item;
			}
			options.ExclusiveStartKey = response.LastEvaluatedKey;
			expectMore = Object.keys(options.ExclusiveStartKey).length;
		}
	}
	return command(options);
};

export const awsDynamoDBScanStream = async (options, _streamOptions = {}) => {
	async function* command(options) {
		let expectMore = true;
		while (expectMore) {
			const response = await client.send(new ScanCommand(options));
			for (const item of response.Items) {
				yield item;
			}
			options.ExclusiveStartKey = response.LastEvaluatedKey;
			expectMore = Object.keys(options.ExclusiveStartKey).length;
		}
	}
	return command(options);
};

// TODO awsDynamoDBExecuteStatementStream

// TODO max Keys.length = 100
export const awsDynamoDBGetItemStream = async (
	options,
	_streamOptions = {},
) => {
	options.retryCount ??= 0;
	options.retryMaxCount ??= 10;
	async function* command(options) {
		while (true) {
			const response = await client.send(
				new BatchGetItemCommand({
					RequestItems: {
						[options.TableName]: { Keys: options.Keys },
					},
				}),
			);
			for (const item of response.Responses[options.TableName]) {
				yield item;
			}
			const UnprocessedKeys =
				response?.UnprocessedKeys?.[options.TableName]?.Keys ?? [];
			if (!UnprocessedKeys.length) {
				break;
			}

			if (options.retryCount >= options.retryMaxCount) {
				throw new Error("awsDynamoDBBatchGetItem has UnprocessedKeys", {
					cause: {
						...options,
						UnprocessedKeysCount: UnprocessedKeys.length,
					},
				});
			}

			await timeout(3 ** options.retryCount++); // 3^10 == 59sec

			options.Keys = UnprocessedKeys;
		}
	}
	return command(options);
};

export const awsDynamoDBPutItemStream = (options, streamOptions) => {
	let batch = [];
	const write = async (chunk) => {
		if (batch.length === 25) {
			await dynamodbBatchWrite(options, batch, streamOptions);
			batch = [];
		}
		batch.push({
			PutRequest: {
				Item: chunk,
			},
		});
	};
	const final = () => dynamodbBatchWrite(options, batch, streamOptions);
	return createWritableStream(write, final, streamOptions);
};

export const awsDynamoDBDeleteItemStream = (options, streamOptions) => {
	let batch = [];
	const write = async (chunk) => {
		if (batch.length === 25) {
			await dynamodbBatchWrite(options, batch, streamOptions);
			batch = [];
		}
		batch.push({
			DeleteRequest: {
				Key: chunk,
			},
		});
	};
	const final = () => dynamodbBatchWrite(options, batch, streamOptions);
	return createWritableStream(write, final, streamOptions);
};

const dynamodbBatchWrite = async (options, batch, streamOptions) => {
	options.retryCount ??= 0;
	options.retryMaxCount ??= 10;
	const { UnprocessedItems } = await client.send(
		new BatchWriteItemCommand({
			RequestItems: {
				[options.TableName]: batch,
			},
		}),
	);
	if (UnprocessedItems?.[options.TableName]?.length) {
		if (options.retryCount >= options.retryMaxCount) {
			throw new Error("awsDynamoDBBatchWriteItem has UnprocessedItems", {
				cause: {
					...options,
					UnprocessedItemsCount: UnprocessedItems[options.TableName].length,
				},
			});
		}

		await timeout(3 ** options.retryCount++); // 3^10 == 59sec
		return dynamodbBatchWrite(
			options,
			UnprocessedItems[options.TableName],
			streamOptions,
		);
	}
	options.retryCount = 0; // reset for next batch
};

export default {
	setClient: awsDynamoDBSetClient,
	queryStream: awsDynamoDBQueryStream,
	scanStream: awsDynamoDBScanStream,
	getItemStream: awsDynamoDBGetItemStream,
	putItemStream: awsDynamoDBPutItemStream,
	deleteItemStream: awsDynamoDBDeleteItemStream,
};
