// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
import {
	BatchGetItemCommand,
	BatchWriteItemCommand,
	DynamoDBClient,
	ExecuteStatementCommand,
	QueryCommand,
	ScanCommand,
} from "@aws-sdk/client-dynamodb";
import { createWritableStream } from "@datastream/core";
import {
	awsBackoff,
	awsClientDefaults,
	awsReadableStream,
	awsUnsentOnDestroy,
} from "./client.js";

const DYNAMODB_MAX_BATCH_WRITE_ITEMS = 25;

let defaultClient;
const getDefaultClient = () =>
	(defaultClient ??= new DynamoDBClient(awsClientDefaults));
export const awsDynamoDBSetClient = (ddbClient) => {
	defaultClient = ddbClient;
};

// options = {TableName, ...}

export const awsDynamoDBQueryStream = async (options, streamOptions = {}) => {
	const { client, ...queryOptions } = options;
	async function* command(opts, signal) {
		let expectMore = true;
		while (expectMore) {
			const response = await (client ?? getDefaultClient()).send(
				new QueryCommand(opts),
				{ abortSignal: signal },
			);
			for (const item of response.Items ?? []) {
				yield item;
			}
			opts.ExclusiveStartKey = response.LastEvaluatedKey;
			expectMore = !!response.LastEvaluatedKey;
		}
	}
	return awsReadableStream(
		(signal) => command(queryOptions, signal),
		streamOptions,
	);
};

export const awsDynamoDBScanStream = async (options, streamOptions = {}) => {
	const { client, ...scanOptions } = options;
	async function* command(opts, signal) {
		let expectMore = true;
		while (expectMore) {
			const response = await (client ?? getDefaultClient()).send(
				new ScanCommand(opts),
				{ abortSignal: signal },
			);
			for (const item of response.Items ?? []) {
				yield item;
			}
			opts.ExclusiveStartKey = response.LastEvaluatedKey;
			expectMore = !!response.LastEvaluatedKey;
		}
	}
	return awsReadableStream(
		(signal) => command(scanOptions, signal),
		streamOptions,
	);
};

export const awsDynamoDBExecuteStatementStream = async (
	options,
	streamOptions = {},
) => {
	const { client, ...statementOptions } = options;
	async function* command(opts, signal) {
		let expectMore = true;
		while (expectMore) {
			const response = await (client ?? getDefaultClient()).send(
				new ExecuteStatementCommand(opts),
				{ abortSignal: signal },
			);
			for (const item of response.Items ?? []) {
				yield item;
			}
			opts.NextToken = response.NextToken;
			expectMore = !!response.NextToken;
		}
	}
	return awsReadableStream(
		(signal) => command(statementOptions, signal),
		streamOptions,
	);
};

export const awsDynamoDBGetItemStream = async (options, streamOptions = {}) => {
	if (options.Keys?.length > 100) {
		throw new RangeError(
			`awsDynamoDBGetItemStream Keys.length (${options.Keys.length}) exceeds BatchGetItem limit of 100`,
		);
	}
	const {
		client,
		TableName,
		Keys,
		retryMaxCount = 10,
		ConsistentRead,
		ProjectionExpression,
		ExpressionAttributeNames,
		AttributesToGet,
		...requestOptions
	} = options;
	const keysAndAttributes = {
		ConsistentRead,
		ProjectionExpression,
		ExpressionAttributeNames,
		AttributesToGet,
	};
	async function* command(signal) {
		let keys = Keys;
		let retryCount = 0;
		const maxCount = retryMaxCount ?? Number.POSITIVE_INFINITY;
		while (true) {
			const response = await (client ?? getDefaultClient()).send(
				new BatchGetItemCommand({
					...requestOptions,
					RequestItems: {
						[TableName]: { ...keysAndAttributes, Keys: keys },
					},
				}),
				{ abortSignal: signal },
			);
			for (const item of response.Responses?.[TableName] ?? []) {
				yield item;
			}
			const UnprocessedKeys = response.UnprocessedKeys?.[TableName]?.Keys ?? [];
			if (!UnprocessedKeys.length) {
				break;
			}

			if (retryCount >= maxCount) {
				throw new Error("awsDynamoDBBatchGetItem has UnprocessedKeys", {
					cause: {
						TableName,
						UnprocessedKeysCount: UnprocessedKeys.length,
					},
				});
			}

			await awsBackoff(retryCount, { signal });
			retryCount++;
			keys = UnprocessedKeys;
		}
	}
	return awsReadableStream(command, streamOptions);
};

const dynamodbBatchWriteStream = (
	toRequest,
	destroyMessage,
	options,
	streamOptions,
) => {
	let batch = [];
	const send = () => {
		const requests = batch.map(toRequest);
		batch = [];
		return dynamodbBatchWrite(options, requests, streamOptions);
	};
	const write = async (chunk) => {
		if (batch.length === DYNAMODB_MAX_BATCH_WRITE_ITEMS) {
			await send();
		}
		batch.push(chunk);
	};
	const final = () => (batch.length ? send() : undefined);
	return awsUnsentOnDestroy(
		createWritableStream(write, final, streamOptions),
		() => batch,
		destroyMessage,
	);
};

export const awsDynamoDBPutItemStream = (options, streamOptions = {}) =>
	dynamodbBatchWriteStream(
		(Item) => ({ PutRequest: { Item } }),
		"awsDynamoDBPutItem destroyed with buffered items",
		options,
		streamOptions,
	);

export const awsDynamoDBDeleteItemStream = (options, streamOptions = {}) =>
	dynamodbBatchWriteStream(
		(Key) => ({ DeleteRequest: { Key } }),
		"awsDynamoDBDeleteItem destroyed with buffered keys",
		options,
		streamOptions,
	);

const dynamodbBatchWrite = async (
	options,
	batch,
	streamOptions,
	retryCount = 0,
) => {
	const { client, ...writeOptions } = options;
	const { TableName, retryMaxCount = 10, ...requestOptions } = writeOptions;
	const maxCount = retryMaxCount ?? Number.POSITIVE_INFINITY;
	const { UnprocessedItems } = await (client ?? getDefaultClient()).send(
		new BatchWriteItemCommand({
			...requestOptions,
			RequestItems: {
				[TableName]: batch,
			},
		}),
		// streamOptions is always supplied by put/delete (defaulting to {}).
		{ abortSignal: streamOptions.signal },
	);
	if (UnprocessedItems?.[TableName]?.length) {
		if (retryCount >= maxCount) {
			throw new Error("awsDynamoDBBatchWriteItem has UnprocessedItems", {
				cause: {
					...writeOptions,
					UnprocessedItemsCount: UnprocessedItems[TableName].length,
				},
			});
		}

		await awsBackoff(retryCount, streamOptions);
		return dynamodbBatchWrite(
			options,
			UnprocessedItems[TableName],
			streamOptions,
			retryCount + 1,
		);
	}
};
