import test from 'node:test'
import { deepEqual, equal } from 'node:assert'
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
      UnprocessedKeys: {}
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

test(`${variant}: awsDynamoDBGetStream should throw error`, async (t) => {
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
        TableName: []
      },
      UnprocessedKeys: {
        TableName: {
          Keys: [{ key: 'c' }]
        }
      }
    })

  const options = {
    TableName: 'TableName',
    Keys: [{ key: 'a' }, { key: 'b' }, { key: 'c' }],
    retryMaxCount: 0 // force
  }
  const stream = await awsDynamoDBGetItemStream(options)
  try {
    await streamToArray(stream)
  } catch (e) {
    equal(e.message, 'awsDynamoDBBatchGetItem has UnprocessedKeys')
  }
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
        TableName: 'abcdefghijklmnopqrstuvwxy'.split('').map((key, value) => ({
          PutRequest: {
            Item: {
              key,
              value
            }
          }
        }))
      }
    })
    .resolves({
      UnprocessedItems: {
        TableName: [
          {
            PutRequest: {
              Item: {
                key: 'y',
                value: 24
              }
            }
          }
        ]
      }
    })
    // y failed, retry
    .on(BatchWriteCommand, {
      RequestItems: {
        TableName: [
          {
            PutRequest: {
              Item: {
                key: 'y',
                value: 24
              }
            }
          }
        ]
      }
    })
    .resolves({ UnprocessedItems: {} })
    // final
    .on(BatchWriteCommand, {
      RequestItems: {
        TableName: [
          {
            PutRequest: {
              Item: {
                key: 'z',
                value: 25
              }
            }
          }
        ]
      }
    })
    .resolves({ UnprocessedItems: {} })

  const input = 'abcdefghijklmnopqrstuvwxyz'
    .split('')
    .map((key, value) => ({ key, value }))
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

test(`${variant}: awsDynamoDBPutItemStream should throw error`, async (t) => {
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
                value: 0
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
                key: 'a',
                value: 0
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
                key: 'a',
                value: 0
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
                key: 'a',
                value: 0
              }
            }
          }
        ]
      }
    })

  const input = [{ key: 'a', value: 0 }]
  const options = {
    TableName: 'TableName',
    retryMaxCount: 0 // force
  }
  const stream = [
    createReadableStream(input),
    awsDynamoDBPutItemStream(options)
  ]
  try {
    await pipeline(stream)
  } catch (e) {
    equal(e.message, 'awsDynamoDBBatchWriteItem has UnprocessedItems')
  }
})

test(`${variant}: awsDynamoDBDeleteItemStream should store items`, async (t) => {
  const client = mockClient(DynamoDBDocumentClient)
  awsDynamoDBDocumentSetClient(client)
  client
    .on(BatchWriteCommand, {
      RequestItems: {
        TableName: 'abcdefghijklmnopqrstuvwxy'.split('').map((key) => ({
          DeleteRequest: {
            Key: { key }
          }
        }))
      }
    })
    .resolves({
      UnprocessedItems: {
        TableName: [
          {
            DeleteRequest: {
              Key: {
                key: 'y'
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
                key: 'y'
              }
            }
          }
        ]
      }
    })
    .resolves({ UnprocessedItems: {} })
    // final
    .on(BatchWriteCommand, {
      RequestItems: {
        TableName: [
          {
            DeleteRequest: {
              Key: { key: 'z' }
            }
          }
        ]
      }
    })
    .resolves({ UnprocessedItems: {} })

  const input = 'abcdefghijklmnopqrstuvwxyz'.split('').map((key) => ({ key }))
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

/* test(`${variant}: awsDynamoDBDeleteItemStream should throw error`, async (t) => {
  const client = mockClient(DynamoDBDocumentClient)
  awsDynamoDBDocumentSetClient(client)
  client
    .on(BatchWriteCommand, {
      RequestItems: {
        TableName: [
          {
            DeleteRequest: {
              Key: { key: 'a' }
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
              Key: { key: 'a' }
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
              Key: { key: 'a' }
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
              Key: { key: 'a' }
            }
          }
        ]
      }
    })

  const input = [{ key: 'a' }]
  const options = {
    TableName: 'TableName',
    retryMaxCount: 0 // force
  }
  const stream = [
    createReadableStream(input),
    awsDynamoDBDeleteItemStream(options)
  ]
  try {
    await pipeline(stream)
  } catch (e) {
    equal(e.message, 'awsDynamoDBBatchWriteItem has UnprocessedItems')
  }
}) */
