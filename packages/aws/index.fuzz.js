import test from "node:test";
import {
	BatchWriteItemCommand,
	DynamoDBClient,
	QueryCommand,
} from "@aws-sdk/client-dynamodb";
import { PublishBatchCommand, SNSClient } from "@aws-sdk/client-sns";
import { SendMessageBatchCommand, SQSClient } from "@aws-sdk/client-sqs";
import {
	awsDynamoDBDeleteItemStream,
	awsDynamoDBPutItemStream,
	awsDynamoDBQueryStream,
	awsDynamoDBSetClient,
} from "@datastream/aws/dynamodb";
import {
	awsSNSPublishMessageStream,
	awsSNSSetClient,
} from "@datastream/aws/sns";
import { awsSQSSendMessageStream, awsSQSSetClient } from "@datastream/aws/sqs";
import {
	createReadableStream,
	pipeline,
	streamToArray,
} from "@datastream/core";
import { mockClient } from "aws-sdk-client-mock";
import fc from "fast-check";

const catchError = (input, e) => {
	const expectedErrors = [];
	if (expectedErrors.includes(e.message)) {
		return;
	}
	console.error(input, e);
	throw e;
};

// *** awsDynamoDBPutItemStream w/ varying batch sizes *** //
test("fuzz awsDynamoDBPutItemStream w/ random item count", async () => {
	await fc.assert(
		fc.asyncProperty(
			fc.array(
				fc.record({
					key: fc.string({ minLength: 1, maxLength: 10 }),
					value: fc.anything().filter((v) => v !== null),
				}),
				{ minLength: 0, maxLength: 100 },
			),
			async (input) => {
				const ddbMock = mockClient(DynamoDBClient);
				awsDynamoDBSetClient(ddbMock);
				ddbMock.on(BatchWriteItemCommand).resolves({ UnprocessedItems: {} });

				try {
					const streams = [
						createReadableStream(input),
						awsDynamoDBPutItemStream({ TableName: "FuzzTable" }),
					];
					await pipeline(streams);
				} catch (e) {
					catchError(input, e);
				} finally {
					ddbMock.restore();
				}
			},
		),
		{
			numRuns: 1_000,
			verbose: 2,
			examples: [],
		},
	);
});

// *** awsDynamoDBDeleteItemStream w/ varying batch sizes *** //
test("fuzz awsDynamoDBDeleteItemStream w/ random item count", async () => {
	await fc.assert(
		fc.asyncProperty(
			fc.array(
				fc.record({
					key: fc.string({ minLength: 1, maxLength: 10 }),
				}),
				{ minLength: 0, maxLength: 100 },
			),
			async (input) => {
				const ddbMock = mockClient(DynamoDBClient);
				awsDynamoDBSetClient(ddbMock);
				ddbMock.on(BatchWriteItemCommand).resolves({ UnprocessedItems: {} });

				try {
					const streams = [
						createReadableStream(input),
						awsDynamoDBDeleteItemStream({ TableName: "FuzzTable" }),
					];
					await pipeline(streams);
				} catch (e) {
					catchError(input, e);
				} finally {
					ddbMock.restore();
				}
			},
		),
		{
			numRuns: 1_000,
			verbose: 2,
			examples: [],
		},
	);
});

// *** awsDynamoDBQueryStream w/ varying page sizes *** //
test("fuzz awsDynamoDBQueryStream w/ random page sizes", async () => {
	await fc.assert(
		fc.asyncProperty(
			fc.array(
				fc.array(
					fc.record({
						key: fc.string({ minLength: 1, maxLength: 10 }),
					}),
					{ minLength: 1, maxLength: 20 },
				),
				{ minLength: 1, maxLength: 5 },
			),
			async (pages) => {
				const ddbMock = mockClient(DynamoDBClient);
				awsDynamoDBSetClient(ddbMock);

				let callIdx = 0;
				ddbMock.on(QueryCommand).callsFake(() => {
					const items = pages[callIdx] ?? [];
					callIdx++;
					return {
						Items: items,
						LastEvaluatedKey:
							callIdx < pages.length ? { key: "next" } : undefined,
					};
				});

				try {
					const stream = await awsDynamoDBQueryStream({
						TableName: "FuzzTable",
					});
					await streamToArray(stream);
				} catch (e) {
					catchError(pages, e);
				} finally {
					ddbMock.restore();
				}
			},
		),
		{
			numRuns: 500,
			verbose: 2,
			examples: [],
		},
	);
});

// *** awsSNSPublishMessageStream w/ varying batch sizes *** //
test("fuzz awsSNSPublishMessageStream w/ random message count", async () => {
	await fc.assert(
		fc.asyncProperty(
			fc.array(
				fc.record({
					Id: fc.string({ minLength: 1, maxLength: 10 }),
					Message: fc.string(),
				}),
				{ minLength: 0, maxLength: 50 },
			),
			async (input) => {
				const snsMock = mockClient(SNSClient);
				awsSNSSetClient(snsMock);
				snsMock.on(PublishBatchCommand).resolves({});

				try {
					const streams = [
						createReadableStream(input),
						awsSNSPublishMessageStream({
							TopicArn: "arn:aws:sns:us-east-1:000000000000:fuzz",
						}),
					];
					await pipeline(streams);
				} catch (e) {
					catchError(input, e);
				} finally {
					snsMock.restore();
				}
			},
		),
		{
			numRuns: 1_000,
			verbose: 2,
			examples: [],
		},
	);
});

// *** awsSQSSendMessageStream w/ varying batch sizes *** //
test("fuzz awsSQSSendMessageStream w/ random message count", async () => {
	await fc.assert(
		fc.asyncProperty(
			fc.array(
				fc.record({
					Id: fc.string({ minLength: 1, maxLength: 10 }),
					MessageBody: fc.string(),
				}),
				{ minLength: 0, maxLength: 50 },
			),
			async (input) => {
				const sqsMock = mockClient(SQSClient);
				awsSQSSetClient(sqsMock);
				sqsMock.on(SendMessageBatchCommand).resolves({});

				try {
					const streams = [
						createReadableStream(input),
						awsSQSSendMessageStream({
							QueueUrl: "https://sqs.us-east-1.amazonaws.com/000000000000/fuzz",
						}),
					];
					await pipeline(streams);
				} catch (e) {
					catchError(input, e);
				} finally {
					sqsMock.restore();
				}
			},
		),
		{
			numRuns: 1_000,
			verbose: 2,
			examples: [],
		},
	);
});
