// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
export {
	awsS3GetObjectStream,
	awsS3PutObjectStream,
	awsS3ChecksumStream,
	awsS3SetClient,
} from "@datastream/aws/s3";
export {
	awsDynamoDBQueryStream,
	awsDynamoDBScanStream,
	awsDynamoDBGetItemStream,
	awsDynamoDBPutItemStream,
	awsDynamoDBDeleteItemStream,
	awsDynamoDBSetClient,
} from "@datastream/aws/dynamodb";
export {
	awsLambdaReadableStream,
	awsLambdaResponseStream,
	awsLambdaSetClient,
} from "@datastream/aws/lambda";
export {
	awsSNSPublishMessageStream,
	awsSNSSetClient,
} from "@datastream/aws/sns";
export {
	awsSQSReceiveMessageStream,
	awsSQSSendMessageStream,
	awsSQSDeleteMessageStream,
	awsSQSSetClient,
} from "@datastream/aws/sqs";
