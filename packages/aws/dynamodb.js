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
import { awsBackoff, awsClientDefaults } from "./client.js";

// Created on first use, so importing the module (or always passing a per-call
// client) never constructs an unused SDK client.
let defaultClient;
const getDefaultClient = () =>
	(defaultClient ??= new DynamoDBClient(awsClientDefaults));
export const awsDynamoDBSetClient = (ddbClient, _translateConfig) => {
	defaultClient = ddbClient;
};

// options = {TableName, ...}

export const awsDynamoDBQueryStream = async (options, streamOptions = {}) => {
	const { client, ...queryOptions } = options;
	async function* command(opts) {
		let expectMore = true;
		while (expectMore) {
			const response = await (client ?? getDefaultClient()).send(
				new QueryCommand(opts),
				{ abortSignal: streamOptions.signal },
			);
			for (const item of response.Items ?? []) {
				yield item;
			}
			opts.ExclusiveStartKey = response.LastEvaluatedKey;
			expectMore = !!response.LastEvaluatedKey;
		}
	}
	return command(queryOptions);
};

export const awsDynamoDBScanStream = async (options, streamOptions = {}) => {
	const { client, ...scanOptions } = options;
	async function* command(opts) {
		let expectMore = true;
		while (expectMore) {
			const response = await (client ?? getDefaultClient()).send(
				new ScanCommand(opts),
				{ abortSignal: streamOptions.signal },
			);
			for (const item of response.Items ?? []) {
				yield item;
			}
			opts.ExclusiveStartKey = response.LastEvaluatedKey;
			expectMore = !!response.LastEvaluatedKey;
		}
	}
	return command(scanOptions);
};

export const awsDynamoDBExecuteStatementStream = async (
	options,
	streamOptions = {},
) => {
	const { client, ...statementOptions } = options;
	async function* command(opts) {
		let expectMore = true;
		while (expectMore) {
			const response = await (client ?? getDefaultClient()).send(
				new ExecuteStatementCommand(opts),
				{ abortSignal: streamOptions.signal },
			);
			for (const item of response.Items ?? []) {
				yield item;
			}
			opts.NextToken = response.NextToken;
			expectMore = !!response.NextToken;
		}
	}
	return command(statementOptions);
};

export const awsDynamoDBGetItemStream = async (options, streamOptions = {}) => {
	if (options.Keys?.length > 100) {
		throw new RangeError(
			`awsDynamoDBGetItemStream Keys.length (${options.Keys.length}) exceeds BatchGetItem limit of 100`,
		);
	}
	// Only KeysAndAttributes fields go in the per-table entry (on the first
	// request and on every retry); anything else that is not a stream option
	// (ReturnConsumedCapacity) is a request-level BatchGetItem field.
	const {
		client,
		TableName,
		Keys,
		retryCount: initialRetryCount,
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
	async function* command() {
		let keys = Keys;
		let retryCount = initialRetryCount ?? 0;
		// null = retry without limit
		const maxCount = retryMaxCount ?? Number.POSITIVE_INFINITY;
		while (true) {
			const response = await (client ?? getDefaultClient()).send(
				new BatchGetItemCommand({
					...requestOptions,
					RequestItems: {
						[TableName]: { ...keysAndAttributes, Keys: keys },
					},
				}),
				{ abortSignal: streamOptions.signal },
			);
			for (const item of response.Responses?.[TableName] ?? []) {
				yield item;
			}
			const UnprocessedKeys = response.UnprocessedKeys?.[TableName]?.Keys ?? [];
			if (!UnprocessedKeys.length) {
				break;
			}

			if (retryCount >= maxCount) {
				// Non-data fields only: Keys values may be PII and must not leak
				// into logged error causes.
				throw new Error("awsDynamoDBBatchGetItem has UnprocessedKeys", {
					cause: {
						TableName,
						UnprocessedKeysCount: UnprocessedKeys.length,
					},
				});
			}

			await awsBackoff(retryCount, streamOptions);
			retryCount++;
			keys = UnprocessedKeys;
		}
	}
	return command();
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
	const { client, ...writeOptions } = options;
	// undefined = default (10); null = retry without limit
	const { retryMaxCount = 10 } = writeOptions;
	const maxCount = retryMaxCount ?? Number.POSITIVE_INFINITY;
	const { UnprocessedItems } = await (client ?? getDefaultClient()).send(
		new BatchWriteItemCommand({
			RequestItems: {
				[writeOptions.TableName]: batch,
			},
		}),
		// streamOptions is always supplied by put/delete (defaulting to {}).
		{ abortSignal: streamOptions.signal },
	);
	if (UnprocessedItems?.[writeOptions.TableName]?.length) {
		if (retryCount >= maxCount) {
			throw new Error("awsDynamoDBBatchWriteItem has UnprocessedItems", {
				cause: {
					...writeOptions,
					UnprocessedItemsCount:
						UnprocessedItems[writeOptions.TableName].length,
				},
			});
		}

		await awsBackoff(retryCount, streamOptions);
		return dynamodbBatchWrite(
			options,
			UnprocessedItems[writeOptions.TableName],
			streamOptions,
			retryCount + 1,
		);
	}
};
