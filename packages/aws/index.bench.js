import { before, bench, suite } from "node:bench";
import {
	BatchWriteItemCommand,
	DynamoDBClient,
	QueryCommand,
	ScanCommand,
} from "@aws-sdk/client-dynamodb";
import { GetObjectCommand, S3Client } from "@aws-sdk/client-s3";
import { PublishBatchCommand, SNSClient } from "@aws-sdk/client-sns";
import {
	DeleteMessageBatchCommand,
	ReceiveMessageCommand,
	SendMessageBatchCommand,
	SQSClient,
} from "@aws-sdk/client-sqs";
import {
	awsDynamoDBDeleteItemStream,
	awsDynamoDBPutItemStream,
	awsDynamoDBQueryStream,
	awsDynamoDBScanStream,
	awsDynamoDBSetClient,
} from "@datastream/aws/dynamodb";
import {
	awsS3ChecksumStream,
	awsS3GetObjectStream,
	awsS3SetClient,
} from "@datastream/aws/s3";
import {
	awsSNSPublishMessageStream,
	awsSNSSetClient,
} from "@datastream/aws/sns";
import {
	awsSQSDeleteMessageStream,
	awsSQSReceiveMessageStream,
	awsSQSSendMessageStream,
	awsSQSSetClient,
} from "@datastream/aws/sqs";
import {
	createReadableStream,
	pipeline,
	streamToArray,
	streamToString,
} from "@datastream/core";
import { mockClient } from "aws-sdk-client-mock";

// -- Config --

const ITEMS = 1_000;
const OPS = 10;
const benchOptions = { warmup: 2, samples: 30 };

const generateItems = (count) =>
	Array.from({ length: count }, (_, i) => ({
		id: `item_${i}`,
		name: `name_${i}`,
		value: i,
	}));

const items = generateItems(ITEMS);

// -- SNS Tests --

suite("awsSNSPublishMessageStream", () => {
	before(() => {
		const client = mockClient(SNSClient);
		awsSNSSetClient(client);
		client.on(PublishBatchCommand).resolves({});
	});

	bench(`${ITEMS} messages`, benchOptions, async (b) => {
		b.start();
		for (let op = 0; op < OPS; op++) {
			const options = {
				TopicArn: "arn:aws:sns:us-east-1:000000000000:test",
			};
			const stream = [
				createReadableStream(items),
				awsSNSPublishMessageStream(options),
			];
			await pipeline(stream);
		}
		b.end(OPS);
	});
});

// -- SQS Tests --

suite("awsSQSSendMessageStream", () => {
	before(() => {
		const client = mockClient(SQSClient);
		awsSQSSetClient(client);
		client.on(SendMessageBatchCommand).resolves({});
	});

	bench(`${ITEMS} messages`, benchOptions, async (b) => {
		b.start();
		for (let op = 0; op < OPS; op++) {
			const options = {
				QueueUrl: "https://sqs.us-east-1.amazonaws.com/000000000000/test",
			};
			const stream = [
				createReadableStream(items),
				awsSQSSendMessageStream(options),
			];
			await pipeline(stream);
		}
		b.end(OPS);
	});
});

suite("awsSQSDeleteMessageStream", () => {
	before(() => {
		const client = mockClient(SQSClient);
		awsSQSSetClient(client);
		client.on(DeleteMessageBatchCommand).resolves({});
	});

	bench(`${ITEMS} messages`, benchOptions, async (b) => {
		b.start();
		for (let op = 0; op < OPS; op++) {
			const options = {
				QueueUrl: "https://sqs.us-east-1.amazonaws.com/000000000000/test",
			};
			const stream = [
				createReadableStream(items),
				awsSQSDeleteMessageStream(options),
			];
			await pipeline(stream);
		}
		b.end(OPS);
	});
});

suite("awsSQSReceiveMessageStream", () => {
	bench(`${ITEMS} messages, 10/batch`, benchOptions, async (b) => {
		b.start();
		for (let op = 0; op < OPS; op++) {
			const client = mockClient(SQSClient);
			awsSQSSetClient(client);

			// Generate sequential responses: ITEMS/10 batches of 10, then empty
			const batchCount = Math.ceil(ITEMS / 10);
			let stub = client.on(ReceiveMessageCommand);
			for (let i = 0; i < batchCount; i++) {
				const batchSize = Math.min(10, ITEMS - i * 10);
				stub = stub.resolvesOnce({
					Messages: Array.from({ length: batchSize }, (_, j) => ({
						id: `msg_${i * 10 + j}`,
					})),
				});
			}
			stub.resolvesOnce({ Messages: [] });

			const stream = await awsSQSReceiveMessageStream({});
			await streamToArray(stream);
		}
		b.end(OPS);
	});
});

// -- DynamoDB Tests --

suite("awsDynamoDBPutItemStream", () => {
	before(() => {
		const client = mockClient(DynamoDBClient);
		awsDynamoDBSetClient(client);
		client.on(BatchWriteItemCommand).resolves({ UnprocessedItems: {} });
	});

	bench(`${ITEMS} items`, benchOptions, async (b) => {
		b.start();
		for (let op = 0; op < OPS; op++) {
			const options = { TableName: "TestTable" };
			const stream = [
				createReadableStream(items),
				awsDynamoDBPutItemStream(options),
			];
			await pipeline(stream);
		}
		b.end(OPS);
	});
});

suite("awsDynamoDBDeleteItemStream", () => {
	before(() => {
		const client = mockClient(DynamoDBClient);
		awsDynamoDBSetClient(client);
		client.on(BatchWriteItemCommand).resolves({ UnprocessedItems: {} });
	});

	bench(`${ITEMS} items`, benchOptions, async (b) => {
		b.start();
		for (let op = 0; op < OPS; op++) {
			const options = { TableName: "TestTable" };
			const stream = [
				createReadableStream(items),
				awsDynamoDBDeleteItemStream(options),
			];
			await pipeline(stream);
		}
		b.end(OPS);
	});
});

suite("awsDynamoDBQueryStream", () => {
	bench(`${ITEMS} items, 100/page`, benchOptions, async (b) => {
		b.start();
		for (let op = 0; op < OPS; op++) {
			const client = mockClient(DynamoDBClient);
			awsDynamoDBSetClient(client);

			const pageCount = Math.ceil(ITEMS / 100);
			let stub = client.on(QueryCommand);
			for (let i = 0; i < pageCount; i++) {
				const pageSize = Math.min(100, ITEMS - i * 100);
				const pageItems = Array.from({ length: pageSize }, (_, j) => ({
					id: `item_${i * 100 + j}`,
				}));
				const response = { Items: pageItems };
				if (i < pageCount - 1) {
					response.LastEvaluatedKey = { id: pageItems[pageSize - 1].id };
				}
				stub = stub.resolvesOnce(response);
			}

			const stream = await awsDynamoDBQueryStream({ TableName: "TestTable" });
			await streamToArray(stream);
		}
		b.end(OPS);
	});
});

suite("awsDynamoDBScanStream", () => {
	bench(`${ITEMS} items, 100/page`, benchOptions, async (b) => {
		b.start();
		for (let op = 0; op < OPS; op++) {
			const client = mockClient(DynamoDBClient);
			awsDynamoDBSetClient(client);

			const pageCount = Math.ceil(ITEMS / 100);
			let stub = client.on(ScanCommand);
			for (let i = 0; i < pageCount; i++) {
				const pageSize = Math.min(100, ITEMS - i * 100);
				const pageItems = Array.from({ length: pageSize }, (_, j) => ({
					id: `item_${i * 100 + j}`,
				}));
				const response = { Items: pageItems };
				if (i < pageCount - 1) {
					response.LastEvaluatedKey = { id: pageItems[pageSize - 1].id };
				}
				stub = stub.resolvesOnce(response);
			}

			const stream = await awsDynamoDBScanStream({ TableName: "TestTable" });
			await streamToArray(stream);
		}
		b.end(OPS);
	});
});

// -- S3 Tests --

suite("awsS3GetObjectStream", () => {
	const bigString = "x".repeat(1_024 * 1_024);

	bench("1MB object", benchOptions, async (b) => {
		b.start();
		for (let op = 0; op < OPS; op++) {
			const client = mockClient(S3Client);
			awsS3SetClient(client);
			client.on(GetObjectCommand).resolves({
				Body: createReadableStream(bigString),
			});

			const stream = await awsS3GetObjectStream({
				Bucket: "bucket",
				Key: "file.ext",
			});
			await streamToString(stream);
		}
		b.end(OPS);
	});
});

suite("awsS3ChecksumStream", () => {
	const bigString = "x".repeat(1_024 * 1_024);

	bench("1MB SHA256 checksum", benchOptions, async (b) => {
		b.start();
		for (let op = 0; op < OPS; op++) {
			const stream = [
				createReadableStream(bigString),
				awsS3ChecksumStream({ ChecksumAlgorithm: "SHA256" }),
			];
			await pipeline(stream);
		}
		b.end(OPS);
	});

	bench("1MB SHA1 checksum", benchOptions, async (b) => {
		b.start();
		for (let op = 0; op < OPS; op++) {
			const stream = [
				createReadableStream(bigString),
				awsS3ChecksumStream({ ChecksumAlgorithm: "SHA1" }),
			];
			await pipeline(stream);
		}
		b.end(OPS);
	});
});
