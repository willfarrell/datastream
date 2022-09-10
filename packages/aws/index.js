import dynamodb from '@datastream/aws/dynamodb'
import s3 from '@datastream/aws/s3'
import sns from '@datastream/aws/sns'
import sqs from '@datastream/aws/sqs'

export const awsDynamoDBSetClient = dynamodb.setClient
export const awsDynamoDBQueryStream = dynamodb.queryStream
export const awsDynamoDBScanStream = dynamodb.scanStream
export const awsDynamoDBGetItemStream = dynamodb.getItemStream
export const awsDynamoDBPutItemStream = dynamodb.putItemStream
export const awsDynamoDBDeleteItemStream = dynamodb.deleteItemStream

export const awsS3SetClient = s3.setClient
export const awsS3GetObjectStream = s3.getObjectStream
export const awsS3PutObjectStream = s3.putObjectStream

export const awsSNSSetClient = sns.setClient
export const awsSNSPublishMessageStream = sns.publishMessageStream

export const awsSQSSetClient = sqs.setClient
export const awsSQSReceiveMessageStream = sqs.receiveMessageStream
export const awsSQSDeleteMessageStream = sqs.deleteMessageStream
export const awsSQSSendMessageStream = sqs.sendMessageStream

export default {
  dynamodbSetClient: awsDynamoDBSetClient,
  dynamodbQueryStream: awsDynamoDBQueryStream,
  dynamodbScanStream: awsDynamoDBScanStream,
  dynamodbGetItemStream: awsDynamoDBGetItemStream,
  dynamodbPutItemStream: awsDynamoDBPutItemStream,
  dynamodbDeleteItemStream: awsDynamoDBDeleteItemStream,

  s3SetClient: awsS3SetClient,
  s3GetObjectStream: awsS3GetObjectStream,
  s3PutObjectStream: awsS3PutObjectStream,

  snsSetClient: awsSNSSetClient,
  snsPublishMessageStream: awsSNSPublishMessageStream,

  sqsSetClient: awsSQSSetClient,
  sqsReceiveMessageStream: awsSQSReceiveMessageStream,
  sqsDeleteMessageStream: awsSQSDeleteMessageStream,
  sqsSendMessageStream: awsSQSSendMessageStream
}
