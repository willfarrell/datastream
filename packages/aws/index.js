import s3 from '@datastream/aws/s3'
import dynamodb from '@datastream/aws/dynamodb'

export const awsS3SetClient = s3.setClient
export const awsS3GetStream = s3.getStream
export const awsS3PutStream = s3.putStream
export const awsDynamoDBSetClient = dynamodb.setClient
export const awsDynamoDBGetStream = dynamodb.getStream
export const awsDynamoDBQueryStream = dynamodb.queryStream
export const awsDynamoDBScanStream = dynamodb.scanStream
export const awsDynamoDBPutStream = dynamodb.putStream
export const awsDynamoDBDeleteStream = dynamodb.Stream

export default {
  s3SetClient: awsS3SetClient,
  s3GetStream: awsS3GetStream,
  s3PutStream: awsS3PutStream,
  dynamodbSetClient: awsDynamoDBSetClient,
  dynamodbGetStream: awsDynamoDBGetStream,
  dynamodbQueryStream: awsDynamoDBQueryStream,
  dynamodbScanStream: awsDynamoDBScanStream,
  dynamodbPutStream: awsDynamoDBPutStream,
  dynamodbDeleteStream: awsDynamoDBDeleteStream
}
