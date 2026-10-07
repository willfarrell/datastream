/// <reference lib="dom" />
/// <reference types="node" />
import {
	awsCloudWatchLogsFilterLogEventsStream,
	awsCloudWatchLogsGetLogEventsStream,
	awsCloudWatchLogsSetClient,
	awsDynamoDBDeleteItemStream,
	awsDynamoDBExecuteStatementStream,
	awsDynamoDBGetItemStream,
	awsDynamoDBPutItemStream,
	awsDynamoDBQueryStream,
	awsDynamoDBScanStream,
	awsDynamoDBSetClient,
	awsDynamoDBStreamsGetRecordsStream,
	awsDynamoDBStreamsSetClient,
	awsKinesisGetRecordsStream,
	awsKinesisPutRecordsStream,
	awsKinesisSetClient,
	awsLambdaReadableStream,
	awsLambdaResponseStream,
	awsLambdaSetClient,
	awsS3ChecksumStream,
	awsS3GetObjectStream,
	awsS3PutObjectStream,
	awsS3SetClient,
	awsSNSPublishMessageStream,
	awsSNSSetClient,
	awsSQSDeleteMessageStream,
	awsSQSReceiveMessageStream,
	awsSQSSendMessageStream,
	awsSQSSetClient,
} from "@datastream/aws";
import { awsGlueSchemaRegistryResolver } from "@datastream/aws/glue-schema-registry";
import { describe, expect, test } from "tstyche";

describe("CloudWatch Logs", () => {
	test("awsCloudWatchLogsSetClient accepts client", () => {
		expect(awsCloudWatchLogsSetClient({})).type.toBe<void>();
	});

	test("awsCloudWatchLogsGetLogEventsStream returns promise", () => {
		expect(
			awsCloudWatchLogsGetLogEventsStream({
				logGroupName: "/test/group",
				logStreamName: "stream1",
			}),
		).type.toBeAssignableTo<Promise<unknown>>();
	});

	test("awsCloudWatchLogsFilterLogEventsStream returns promise", () => {
		expect(
			awsCloudWatchLogsFilterLogEventsStream({
				logGroupName: "/test/group",
			}),
		).type.toBeAssignableTo<Promise<unknown>>();
	});
});

describe("Kinesis", () => {
	test("awsKinesisSetClient accepts client", () => {
		expect(awsKinesisSetClient({})).type.toBe<void>();
	});

	test("awsKinesisGetRecordsStream returns promise", () => {
		expect(
			awsKinesisGetRecordsStream({ ShardIterator: "iter1" }),
		).type.toBeAssignableTo<Promise<unknown>>();
	});

	test("awsKinesisPutRecordsStream returns stream", () => {
		expect(
			awsKinesisPutRecordsStream({ StreamName: "test-stream" }),
		).type.not.toBeAssignableTo<never>();
	});
});

describe("S3", () => {
	test("awsS3SetClient accepts client", () => {
		expect(awsS3SetClient({})).type.toBe<void>();
	});

	test("awsS3GetObjectStream returns promise", () => {
		expect(
			awsS3GetObjectStream({ Bucket: "b", Key: "k" }),
		).type.toBeAssignableTo<Promise<unknown>>();
	});

	test("awsS3PutObjectStream returns stream", () => {
		expect(
			awsS3PutObjectStream({ Bucket: "b", Key: "k" }),
		).type.not.toBeAssignableTo<never>();
	});

	test("awsS3PutObjectStream accepts numeric partSize and queueSize", () => {
		expect(awsS3PutObjectStream).type.toBeCallableWith({
			Bucket: "b",
			Key: "k",
			partSize: 8 * 1024 * 1024,
			queueSize: 8,
		});
		expect(awsS3PutObjectStream).type.not.toBeCallableWith({
			queueSize: "8",
		});
		expect(awsS3PutObjectStream).type.not.toBeCallableWith({
			partSize: "8",
		});
	});

	test("awsS3ChecksumStream accepts options", () => {
		expect(
			awsS3ChecksumStream({ ChecksumAlgorithm: "SHA256" }),
		).type.not.toBeAssignableTo<never>();
	});

	test("awsS3ChecksumStream accepts no options", () => {
		expect(awsS3ChecksumStream()).type.not.toBeAssignableTo<never>();
	});
});

describe("DynamoDB", () => {
	test("awsDynamoDBSetClient accepts client", () => {
		expect(awsDynamoDBSetClient({})).type.toBe<void>();
	});

	test("awsDynamoDBQueryStream returns promise", () => {
		expect(awsDynamoDBQueryStream({ TableName: "t" })).type.toBeAssignableTo<
			Promise<unknown>
		>();
	});

	test("awsDynamoDBScanStream returns promise", () => {
		expect(awsDynamoDBScanStream({ TableName: "t" })).type.toBeAssignableTo<
			Promise<unknown>
		>();
	});

	test("awsDynamoDBExecuteStatementStream returns promise", () => {
		expect(
			awsDynamoDBExecuteStatementStream({
				Statement: 'SELECT * FROM "TableName"',
			}),
		).type.toBeAssignableTo<Promise<unknown>>();
	});

	test("awsDynamoDBGetItemStream returns promise", () => {
		expect(
			awsDynamoDBGetItemStream({ TableName: "t", Keys: [{}] }),
		).type.toBeAssignableTo<Promise<unknown>>();
	});

	test("awsDynamoDBGetItemStream accepts KeysAndAttributes options", () => {
		expect(awsDynamoDBGetItemStream).type.toBeCallableWith({
			TableName: "t",
			Keys: [{}],
			ConsistentRead: true,
			ProjectionExpression: "#n",
			ExpressionAttributeNames: { "#n": "name" },
		});
		expect(awsDynamoDBGetItemStream).type.toBeCallableWith({
			TableName: "t",
			Keys: [{}],
			AttributesToGet: ["id"],
			ReturnConsumedCapacity: "TOTAL",
		});
		expect(awsDynamoDBGetItemStream).type.not.toBeCallableWith({
			ConsistentRead: "yes",
		});
		expect(awsDynamoDBGetItemStream).type.not.toBeCallableWith({
			TableName: "t",
			Keys: [{}],
			Unknown: true,
		});
	});

	test("awsDynamoDBPutItemStream returns stream", () => {
		expect(
			awsDynamoDBPutItemStream({ TableName: "t" }),
		).type.not.toBeAssignableTo<never>();
	});

	test("awsDynamoDBDeleteItemStream returns stream", () => {
		expect(
			awsDynamoDBDeleteItemStream({ TableName: "t" }),
		).type.not.toBeAssignableTo<never>();
	});
});

describe("DynamoDB Streams", () => {
	test("awsDynamoDBStreamsSetClient accepts client", () => {
		expect(awsDynamoDBStreamsSetClient({})).type.toBe<void>();
	});

	test("awsDynamoDBStreamsGetRecordsStream returns promise", () => {
		expect(
			awsDynamoDBStreamsGetRecordsStream({ ShardIterator: "iter1" }),
		).type.toBeAssignableTo<Promise<unknown>>();
	});
});

describe("Lambda", () => {
	test("awsLambdaSetClient accepts client", () => {
		expect(awsLambdaSetClient({})).type.toBe<void>();
	});

	test("awsLambdaReadableStream returns stream", () => {
		expect(
			awsLambdaReadableStream({ FunctionName: "f" }),
		).type.not.toBeAssignableTo<never>();
	});

	test("awsLambdaResponseStream is alias", () => {
		expect(awsLambdaResponseStream).type.toBe<typeof awsLambdaReadableStream>();
	});
});

describe("SNS", () => {
	test("awsSNSSetClient accepts client", () => {
		expect(awsSNSSetClient({})).type.toBe<void>();
	});

	test("awsSNSPublishMessageStream returns stream", () => {
		expect(
			awsSNSPublishMessageStream({
				TopicArn: "arn:aws:sns:us-east-1:123:topic",
			}),
		).type.not.toBeAssignableTo<never>();
	});
});

describe("SQS", () => {
	test("awsSQSSetClient accepts client", () => {
		expect(awsSQSSetClient({})).type.toBe<void>();
	});

	test("awsSQSReceiveMessageStream returns promise", () => {
		expect(
			awsSQSReceiveMessageStream({ QueueUrl: "https://sqs.example.com" }),
		).type.toBeAssignableTo<Promise<unknown>>();
	});

	test("awsSQSSendMessageStream returns stream", () => {
		expect(
			awsSQSSendMessageStream({ QueueUrl: "https://sqs.example.com" }),
		).type.not.toBeAssignableTo<never>();
	});

	test("awsSQSDeleteMessageStream returns stream", () => {
		expect(
			awsSQSDeleteMessageStream({ QueueUrl: "https://sqs.example.com" }),
		).type.not.toBeAssignableTo<never>();
	});
});

describe("Limits accept null (unlimited)", () => {
	test("retryMaxCount accepts number | null", () => {
		expect(awsDynamoDBGetItemStream).type.toBeCallableWith({
			TableName: "t",
			Keys: [{}],
			retryMaxCount: null,
		});
		expect(awsDynamoDBPutItemStream).type.toBeCallableWith({
			TableName: "t",
			retryMaxCount: null,
		});
		expect(awsDynamoDBDeleteItemStream).type.toBeCallableWith({
			TableName: "t",
			retryMaxCount: 3,
		});
		expect(awsKinesisPutRecordsStream).type.toBeCallableWith({
			StreamName: "s",
			retryMaxCount: null,
		});
		expect(awsSNSPublishMessageStream).type.toBeCallableWith({
			TopicArn: "arn",
			retryMaxCount: null,
		});
		expect(awsSQSSendMessageStream).type.toBeCallableWith({
			QueueUrl: "q",
			retryMaxCount: null,
		});
		expect(awsSQSDeleteMessageStream).type.toBeCallableWith({
			QueueUrl: "q",
			retryMaxCount: null,
		});
		expect(awsKinesisPutRecordsStream).type.not.toBeCallableWith({
			StreamName: "s",
			retryMaxCount: "10",
		});
	});

	test("glue maxCacheSize accepts number | null", () => {
		expect(awsGlueSchemaRegistryResolver).type.toBeCallableWith({
			maxCacheSize: null,
		});
		expect(awsGlueSchemaRegistryResolver).type.toBeCallableWith({
			maxCacheSize: 10,
		});
	});
});
