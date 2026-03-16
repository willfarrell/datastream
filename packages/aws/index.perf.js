import test from "node:test";
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
import { Bench } from "tinybench";

// -- Config --

const ITEMS = 1_000;
const time = Number(process.env.BENCH_TIME ?? 5_000);

const generateItems = (count) =>
	Array.from({ length: count }, (_, i) => ({
		id: `item_${i}`,
		name: `name_${i}`,
		value: i,
	}));

const items = generateItems(ITEMS);

// -- SNS Tests --

test("perf: awsSNSPublishMessageStream", async () => {
	const bench = new Bench({ name: "awsSNSPublishMessageStream", time });
	const client = mockClient(SNSClient);
	awsSNSSetClient(client);
	client.on(PublishBatchCommand).resolves({});

	bench.add(`${ITEMS} messages`, async () => {
		const options = {
			TopicArn: "arn:aws:sns:us-east-1:000000000000:test",
		};
		const stream = [
			createReadableStream(items),
			awsSNSPublishMessageStream(options),
		];
		await pipeline(stream);
	});

	await bench.run();
	console.log(`\n${bench.name}`);
	console.table(bench.table());
});

// -- SQS Tests --

test("perf: awsSQSSendMessageStream", async () => {
	const bench = new Bench({ name: "awsSQSSendMessageStream", time });
	const client = mockClient(SQSClient);
	awsSQSSetClient(client);
	client.on(SendMessageBatchCommand).resolves({});

	bench.add(`${ITEMS} messages`, async () => {
		const options = {
			QueueUrl: "https://sqs.us-east-1.amazonaws.com/000000000000/test",
		};
		const stream = [
			createReadableStream(items),
			awsSQSSendMessageStream(options),
		];
		await pipeline(stream);
	});

	await bench.run();
	console.log(`\n${bench.name}`);
	console.table(bench.table());
});

test("perf: awsSQSDeleteMessageStream", async () => {
	const bench = new Bench({ name: "awsSQSDeleteMessageStream", time });
	const client = mockClient(SQSClient);
	awsSQSSetClient(client);
	client.on(DeleteMessageBatchCommand).resolves({});

	bench.add(`${ITEMS} messages`, async () => {
		const options = {
			QueueUrl: "https://sqs.us-east-1.amazonaws.com/000000000000/test",
		};
		const stream = [
			createReadableStream(items),
			awsSQSDeleteMessageStream(options),
		];
		await pipeline(stream);
	});

	await bench.run();
	console.log(`\n${bench.name}`);
	console.table(bench.table());
});

test("perf: awsSQSReceiveMessageStream", async () => {
	const bench = new Bench({ name: "awsSQSReceiveMessageStream", time });

	bench.add(`${ITEMS} messages, 10/batch`, async () => {
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
	});

	await bench.run();
	console.log(`\n${bench.name}`);
	console.table(bench.table());
});

// -- DynamoDB Tests --

test("perf: awsDynamoDBPutItemStream", async () => {
	const bench = new Bench({ name: "awsDynamoDBPutItemStream", time });
	const client = mockClient(DynamoDBClient);
	awsDynamoDBSetClient(client);
	client.on(BatchWriteItemCommand).resolves({ UnprocessedItems: {} });

	bench.add(`${ITEMS} items`, async () => {
		const options = { TableName: "TestTable" };
		const stream = [
			createReadableStream(items),
			awsDynamoDBPutItemStream(options),
		];
		await pipeline(stream);
	});

	await bench.run();
	console.log(`\n${bench.name}`);
	console.table(bench.table());
});

test("perf: awsDynamoDBDeleteItemStream", async () => {
	const bench = new Bench({ name: "awsDynamoDBDeleteItemStream", time });
	const client = mockClient(DynamoDBClient);
	awsDynamoDBSetClient(client);
	client.on(BatchWriteItemCommand).resolves({ UnprocessedItems: {} });

	bench.add(`${ITEMS} items`, async () => {
		const options = { TableName: "TestTable" };
		const stream = [
			createReadableStream(items),
			awsDynamoDBDeleteItemStream(options),
		];
		await pipeline(stream);
	});

	await bench.run();
	console.log(`\n${bench.name}`);
	console.table(bench.table());
});

test("perf: awsDynamoDBQueryStream", async () => {
	const bench = new Bench({ name: "awsDynamoDBQueryStream", time });

	bench.add(`${ITEMS} items, 100/page`, async () => {
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
	});

	await bench.run();
	console.log(`\n${bench.name}`);
	console.table(bench.table());
});

test("perf: awsDynamoDBScanStream", async () => {
	const bench = new Bench({ name: "awsDynamoDBScanStream", time });

	bench.add(`${ITEMS} items, 100/page`, async () => {
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
	});

	await bench.run();
	console.log(`\n${bench.name}`);
	console.table(bench.table());
});

// -- S3 Tests --

test("perf: awsS3GetObjectStream", async () => {
	const bench = new Bench({ name: "awsS3GetObjectStream", time });
	const bigString = "x".repeat(1_024 * 1_024);

	bench.add("1MB object", async () => {
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
	});

	await bench.run();
	console.log(`\n${bench.name}`);
	console.table(bench.table());
});

test("perf: awsS3ChecksumStream", async () => {
	const bench = new Bench({ name: "awsS3ChecksumStream", time });
	const bigString = "x".repeat(1_024 * 1_024);

	bench.add("1MB SHA256 checksum", async () => {
		const stream = [
			createReadableStream(bigString),
			awsS3ChecksumStream({ ChecksumAlgorithm: "SHA256" }),
		];
		await pipeline(stream);
	});

	bench.add("1MB SHA1 checksum", async () => {
		const stream = [
			createReadableStream(bigString),
			awsS3ChecksumStream({ ChecksumAlgorithm: "SHA1" }),
		];
		await pipeline(stream);
	});

	await bench.run();
	console.log(`\n${bench.name}`);
	console.table(bench.table());
});
