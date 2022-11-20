import test from 'node:test'
import { deepEqual } from 'node:assert'
// import sinon from 'sinon'
import { mockClient } from 'aws-sdk-client-mock'
import {
  BatchGetCommand,
  QueryCommand,
  ScanCommand,
  BatchWriteCommand,
  DynamoDBDocumentClient
} from '@aws-sdk/lib-dynamodb'

import {
  pipeline,
  streamToArray,
  createReadableStream
} from '@datastream/core'

import {
  awsDynamoDBDocumentSetClient,
  awsDynamoDBQueryStream,
  awsDynamoDBScanStream,
  awsDynamoDBGetItemStream,
  awsDynamoDBPutItemStream,
  awsDynamoDBDeleteItemStream
} from '@datastream/aws'

let variant = 'unknown'
for (const execArgv of process.execArgv) {
  const flag = '--conditions='
  if (execArgv.includes(flag)) {
    variant = execArgv.replace(flag, '')
  }
}

test(`${variant}: awsDynamoDBGetStream should return items`, async (t) => {
  const client = mockClient(DynamoDBDocumentClient)
  awsDynamoDBDocumentSetClient(client)
  client
    .on(BatchGetCommand, {
      RequestItems: {
        TableName: {
          Keys: [{ key: 'a' }, { key: 'b' }, { key: 'c' }]
        }
      }
    })
    .resolves({
      Responses: {
        TableName: [
          { key: 'a', value: 1 },
          { key: 'b', value: 2 }
        ]
      },
      UnprocessedKeys: {
        TableName: {
          Keys: [{ key: 'c' }]
        }
      }
    })
    .on(BatchGetCommand, {
      RequestItems: {
        TableName: {
          Keys: [{ key: 'c' }]
        }
      }
    })
    .resolves({
      Responses: {
        TableName: [{ key: 'c', value: 3 }]
      },
      UnprocessedKeys: {
        TableName: {
          Keys: []
        }
      }
    })

  const options = {
    TableName: 'TableName',
    Keys: [{ key: 'a' }, { key: 'b' }, { key: 'c' }]
  }
  const stream = await awsDynamoDBGetItemStream(options)
  const output = await streamToArray(stream)

  deepEqual(output, [
    { key: 'a', value: 1 },
    { key: 'b', value: 2 },
    { key: 'c', value: 3 }
  ])
})

test(`${variant}: awsDynamoDBQueryStream should return items`, async (t) => {
  const client = mockClient(DynamoDBDocumentClient)
  awsDynamoDBDocumentSetClient(client)
  client
    .on(QueryCommand, {
      TableName: 'TableName'
    })
    .resolves({
      Items: [
        { key: 'a', value: 1 },
        { key: 'b', value: 2 }
      ],
      LastEvaluatedKey: {
        key: 'b'
      }
    })
    .on(QueryCommand, {
      TableName: 'TableName',
      ExclusiveStartKey: { key: 'b' }
    })
    .resolves({
      Items: [{ key: 'c', value: 3 }],
      LastEvaluatedKey: {}
    })

  const options = {
    TableName: 'TableName'
  }
  const stream = await awsDynamoDBQueryStream(options)
  const output = await streamToArray(stream)

  deepEqual(output, [
    { key: 'a', value: 1 },
    { key: 'b', value: 2 },
    { key: 'c', value: 3 }
  ])
})

test(`${variant}: awsDynamoDBScanStream should return items`, async (t) => {
  const client = mockClient(DynamoDBDocumentClient)
  awsDynamoDBDocumentSetClient(client)
  client
    .on(ScanCommand, {
      TableName: 'TableName'
    })
    .resolves({
      Items: [
        { key: 'a', value: 1 },
        { key: 'b', value: 2 }
      ],
      LastEvaluatedKey: {
        key: 'b'
      }
    })
    .on(ScanCommand, {
      TableName: 'TableName',
      ExclusiveStartKey: { key: 'b' }
    })
    .resolves({
      Items: [{ key: 'c', value: 3 }],
      LastEvaluatedKey: {}
    })

  const options = {
    TableName: 'TableName'
  }
  const stream = await awsDynamoDBScanStream(options)
  const output = await streamToArray(stream)

  deepEqual(output, [
    { key: 'a', value: 1 },
    { key: 'b', value: 2 },
    { key: 'c', value: 3 }
  ])
})

test(`${variant}: awsDynamoDBPutItemStream should store items`, async (t) => {
  const client = mockClient(DynamoDBDocumentClient)
  awsDynamoDBDocumentSetClient(client)
  client
    .on(BatchWriteCommand, {
      RequestItems: {
        TableName: [
          {
            PutRequest: {
              Item: {
                key: 'a',
                value: 1
              }
            }
          },
          {
            PutRequest: {
              Item: {
                key: 'b',
                value: 2
              }
            }
          },
          {
            PutRequest: {
              Item: {
                key: 'c',
                value: 3
              }
            }
          }
        ]
      }
    })
    .resolves({
      UnprocessedItems: {
        TableName: [
          {
            PutRequest: {
              Item: {
                key: 'c',
                value: 3
              }
            }
          }
        ]
      }
    })
    .on(BatchWriteCommand, {
      RequestItems: {
        TableName: [
          {
            PutRequest: {
              Item: {
                key: 'c',
                value: 3
              }
            }
          }
        ]
      }
    })
    .resolves({ UnprocessedItems: {} })

  const input = [
    { key: 'a', value: 1 },
    { key: 'b', value: 2 },
    { key: 'c', value: 3 }
  ]
  const options = {
    TableName: 'TableName'
  }
  const stream = [
    createReadableStream(input),
    awsDynamoDBPutItemStream(options)
  ]
  const output = await pipeline(stream)

  deepEqual(output, {})
})

test(`${variant}: awsDynamoDBDeleteItemStream should store items`, async (t) => {
  const client = mockClient(DynamoDBDocumentClient)
  awsDynamoDBDocumentSetClient(client)
  client
    .on(BatchWriteCommand, {
      RequestItems: {
        TableName: [
          {
            DeleteRequest: {
              Key: {
                key: 'a'
              }
            }
          },
          {
            DeleteRequest: {
              Key: {
                key: 'b'
              }
            }
          },
          {
            DeleteRequest: {
              Key: {
                key: 'c'
              }
            }
          }
        ]
      }
    })
    .resolves({
      UnprocessedItems: {
        TableName: [
          {
            DeleteRequest: {
              Key: {
                key: 'c'
              }
            }
          }
        ]
      }
    })
    .on(BatchWriteCommand, {
      RequestItems: {
        TableName: [
          {
            DeleteRequest: {
              Key: {
                key: 'c'
              }
            }
          }
        ]
      }
    })
    .resolves({ UnprocessedItems: {} })

  const input = [{ key: 'a' }, { key: 'b' }, { key: 'c' }]
  const options = {
    TableName: 'TableName'
  }
  const stream = [
    createReadableStream(input),
    awsDynamoDBDeleteItemStream(options)
  ]
  const output = await pipeline(stream)

  deepEqual(output, {})
})
