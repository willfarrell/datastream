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
			expectMore = !!response.LastEvaluatedKey;
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
			expectMore = !!response.LastEvaluatedKey;
		}
	}
	return command(options);
};

// TODO awsDynamoDBExecuteStatementStream

export const awsDynamoDBGetItemStream = async (
	options,
	_streamOptions = {},
) => {
	if (options.Keys?.length > 100) {
		throw new Error(
			`awsDynamoDBGetItemStream Keys.length (${options.Keys.length}) exceeds BatchGetItem limit of 100`,
		);
	}
	async function* command(options) {
		let keys = options.Keys;
		let retryCount = options.retryCount ?? 0;
		const retryMaxCount = options.retryMaxCount ?? 10;
		while (true) {
			const response = await client.send(
				new BatchGetItemCommand({
					RequestItems: {
						[options.TableName]: { Keys: keys },
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

			if (retryCount >= retryMaxCount) {
				throw new Error("awsDynamoDBBatchGetItem has UnprocessedKeys", {
					cause: {
						...options,
						UnprocessedKeysCount: UnprocessedKeys.length,
					},
				});
			}

			await timeout(3 ** retryCount); // 3^10 == 59sec
			retryCount++;
			keys = UnprocessedKeys;
		}
	}
	return command(options);
};

export const awsDynamoDBPutItemStream = (options, streamOptions = {}) => {
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
	const final = () =>
		batch.length
			? dynamodbBatchWrite(options, batch, streamOptions)
			: undefined;
	return createWritableStream(write, final, streamOptions);
};

export const awsDynamoDBDeleteItemStream = (options, streamOptions = {}) => {
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
	const final = () =>
		batch.length
			? dynamodbBatchWrite(options, batch, streamOptions)
			: undefined;
	return createWritableStream(write, final, streamOptions);
};

const dynamodbBatchWrite = async (
	options,
	batch,
	streamOptions,
	retryCount = 0,
) => {
	const retryMaxCount = options.retryMaxCount ?? 10;
	const { UnprocessedItems } = await client.send(
		new BatchWriteItemCommand({
			RequestItems: {
				[options.TableName]: batch,
			},
		}),
	);
	if (UnprocessedItems?.[options.TableName]?.length) {
		if (retryCount >= retryMaxCount) {
			throw new Error("awsDynamoDBBatchWriteItem has UnprocessedItems", {
				cause: {
					...options,
					UnprocessedItemsCount: UnprocessedItems[options.TableName].length,
				},
			});
		}

		await timeout(3 ** retryCount); // 3^10 == 59sec
		return dynamodbBatchWrite(
			options,
			UnprocessedItems[options.TableName],
			streamOptions,
			retryCount + 1,
		);
	}
};

export default {
	setClient: awsDynamoDBSetClient,
	queryStream: awsDynamoDBQueryStream,
	scanStream: awsDynamoDBScanStream,
	getItemStream: awsDynamoDBGetItemStream,
	putItemStream: awsDynamoDBPutItemStream,
	deleteItemStream: awsDynamoDBDeleteItemStream,
};
