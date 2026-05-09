// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
export {
	awsCloudWatchLogsFilterLogEventsStream,
	awsCloudWatchLogsGetLogEventsStream,
	awsCloudWatchLogsSetClient,
} from "@datastream/aws/cloudwatch-logs";
export {
	awsDynamoDBDeleteItemStream,
	awsDynamoDBExecuteStatementStream,
	awsDynamoDBGetItemStream,
	awsDynamoDBPutItemStream,
	awsDynamoDBQueryStream,
	awsDynamoDBScanStream,
	awsDynamoDBSetClient,
} from "@datastream/aws/dynamodb";
export {
	awsKinesisGetRecordsStream,
	awsKinesisPutRecordsStream,
	awsKinesisSetClient,
} from "@datastream/aws/kinesis";
export {
	awsLambdaReadableStream,
	awsLambdaResponseStream,
	awsLambdaSetClient,
} from "@datastream/aws/lambda";
export {
	awsS3ChecksumStream,
	awsS3GetObjectStream,
	awsS3PutObjectStream,
	awsS3SetClient,
} from "@datastream/aws/s3";
export {
	awsSNSPublishMessageStream,
	awsSNSSetClient,
} from "@datastream/aws/sns";
export {
	awsSQSDeleteMessageStream,
	awsSQSReceiveMessageStream,
	awsSQSSendMessageStream,
	awsSQSSetClient,
} from "@datastream/aws/sqs";
