import { createWritableStream, timeout } from '@datastream/core'
import { DynamoDBClient } from '@aws-sdk/client-dynamodb'

import AWSXRay from 'aws-xray-sdk-core'
import {
  BatchGetCommand,
  BatchWriteCommand,
  QueryCommand,
  ScanCommand,
  DynamoDBDocumentClient
} from '@aws-sdk/lib-dynamodb'

const awsClientDefaults = {
  // https://aws.amazon.com/compliance/fips/
  useFipsEndpoint: [
    'us-east-1',
    'us-east-2',
    'us-west-1',
    'us-west-2',
    'ca-central-1'
  ].includes(process.env.AWS_REGION)
}

let client = AWSXRay.captureAWSv3Client(new DynamoDBClient(awsClientDefaults))
let dynamodbDocument
export const awsDynamoDBSetClient = (ddbClient, translateConfig) => {
  client = ddbClient
  awsDynamoDBDocumentSetClient(
    DynamoDBDocumentClient.from(client, translateConfig)
  )
}
export const awsDynamoDBDocumentSetClient = (ddbdocClient) => {
  dynamodbDocument = ddbdocClient
}
awsDynamoDBSetClient(client)

// Docs: https://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/DynamoDB/DocumentClient.html

// options = {TableName, ...}

export const awsDynamoDBQueryStream = async (options, streamOptions) => {
  async function * command (options) {
    let expectMore = true
    while (expectMore) {
      const response = await dynamodbDocument.send(new QueryCommand(options))
      for (const item of response.Items) {
        yield item
      }
      options.ExclusiveStartKey = response.LastEvaluatedKey
      expectMore = Object.keys(options.ExclusiveStartKey).length
    }
  }
  return command(options)
}

export const awsDynamoDBScanStream = async (options, streamOptions) => {
  async function * command (options) {
    let expectMore = true
    while (expectMore) {
      const response = await dynamodbDocument.send(new ScanCommand(options))
      for (const item of response.Items) {
        yield item
      }
      options.ExclusiveStartKey = response.LastEvaluatedKey
      expectMore = Object.keys(options.ExclusiveStartKey).length
    }
  }
  return command(options)
}

// max Keys.length = 100
export const awsDynamoDBGetItemStream = async (options, streamOptions) => {
  options.retryCount ??= 0
  options.retryMaxCount ??= 10
  async function * command (options) {
    while (true) {
      const response = await dynamodbDocument.send(
        new BatchGetCommand({
          RequestItems: {
            [options.TableName]: { Keys: options.Keys }
          }
        })
      )
      for (const item of response.Responses[options.TableName]) {
        yield item
      }
      const UnprocessedKeys =
        response?.UnprocessedKeys?.[options.TableName]?.Keys ?? []
      if (!UnprocessedKeys.length) {
        break
      }
      if (options.retryCount >= options.retryMaxCount) {
        throw new Error('awsDynamoDBBatchGetItem has UnprocessedKeys', {
          cause: {
            ...options,
            UnprocessedKeysCount: UnprocessedKeys.length
          }
        })
      }

      await timeout(3 ** options.retryCount++) // 3^10 == 59sec

      options.Keys = UnprocessedKeys
    }
  }
  return command(options)
}

export const awsDynamoDBPutItemStream = (options, streamOptions = {}) => {
  options.retryCount ??= 0
  options.retryMaxCount ??= 10
  let batch = []
  const write = async (chunk) => {
    if (batch.length === 25) {
      await dynamodbBatchWrite(options, batch, streamOptions)
      batch = []
    }
    batch.push({
      PutRequest: {
        Item: chunk
      }
    })
  }
  streamOptions.final = () => dynamodbBatchWrite(options, batch, streamOptions)
  return createWritableStream(write, streamOptions)
}

export const awsDynamoDBDeleteItemStream = (options, streamOptions = {}) => {
  options.retryCount ??= 0
  options.retryMaxCount ??= 10
  let batch = []
  const write = async (chunk) => {
    if (batch.length === 25) {
      await dynamodbBatchWrite(options, batch, streamOptions)
      batch = []
    }
    batch.push({
      DeleteRequest: {
        Key: chunk
      }
    })
  }
  streamOptions.final = () => dynamodbBatchWrite(options, batch, streamOptions)
  return createWritableStream(write, streamOptions)
}

const dynamodbBatchWrite = async (options, batch, streamOptions) => {
  const { UnprocessedItems } = await dynamodbDocument.send(
    new BatchWriteCommand({
      RequestItems: {
        [options.TableName]: batch
      }
    })
  )
  if (UnprocessedItems?.[options.TableName]?.length) {
    if (options.retryCount >= options.retryMaxCount) {
      throw new Error('awsDynamoDBBatchWriteItem has UnprocessedItems', {
        cause: {
          ...options,
          UnprocessedItemsCount: UnprocessedItems[options.TableName].length
        }
      })
    }

    await timeout(3 ** options.retryCount++) // 3^10 == 59sec
    return dynamodbBatchWrite(
      options,
      UnprocessedItems[options.TableName],
      streamOptions
    )
  }
  options.retryCount = 0 // reset for next batch
}

export default {
  setClient: awsDynamoDBSetClient,
  queryStream: awsDynamoDBQueryStream,
  scanStream: awsDynamoDBScanStream,
  getItemStream: awsDynamoDBGetItemStream,
  putItemStream: awsDynamoDBPutItemStream,
  deleteItemStream: awsDynamoDBDeleteItemStream
}
