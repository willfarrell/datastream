import test from 'node:test'
import { deepEqual } from 'node:assert'
// import sinon from 'sinon'
import { mockClient } from 'aws-sdk-client-mock'
import { DynamoDBClient } from '@aws-sdk/client-dynamodb'
import {
  BatchGetCommand,
  QueryCommand,
  ScanCommand,
  BatchWriteCommand
} from '@aws-sdk/lib-dynamodb'

import {
  pipeline,
  streamToArray,
  createReadableStream
} from '@datastream/core'

import {
  awsDynamoDBSetClient,
  awsDynamoDBGetStream,
  awsDynamoDBQueryStream,
  awsDynamoDBScanStream,
  awsDynamoDBPutStream,
  awsDynamoDBDeleteStream
} from '@datastream/aws'

let variant = 'unknown'
for (const execArgv of process.execArgv) {
  const flag = '--conditions='
  if (execArgv.includes(flag)) {
    variant = execArgv.replace(flag, '')
  }
}

test(`${variant}: awsDynamoDBGetStream should return items`, async (t) => {
  const client = mockClient(DynamoDBClient)
  awsDynamoDBSetClient(client)
  client
    .on(BatchGetCommand, {
      RequestItems: {
        TableName: {
          Keys: [{ key: { S: 'a' } }, { key: { S: 'b' } }, { key: { S: 'c' } }]
        }
      }
    })
    .resolves({
      Responses: {
        TableName: [
          { key: { S: 'a' }, value: { N: 1 } },
          { key: { S: 'b' }, value: { S: 2 } }
        ],
        UnprocessedKeys: {
          TableName: {
            Keys: [{ key: { S: 'c' } }]
          }
        }
      }
    })
    .on(BatchGetCommand, {
      RequestItems: {
        TableName: {
          Keys: [{ key: { S: 'c' } }]
        }
      }
    })
    .resolves({
      Responses: {
        TableName: [{ key: { S: 'c' }, value: { N: 3 } }],
        UnprocessedKeys: {
          TableName: {
            Keys: []
          }
        }
      }
    })

  const options = {
    TableName: 'TableName',
    Keys: [{ key: 'a' }, { key: 'b' }, { key: 'c' }]
  }
  const stream = awsDynamoDBGetStream(options)
  const output = await streamToArray(stream)

  deepEqual(output, [
    { key: 'a', value: 1 },
    { key: 'b', value: 2 },
    { key: 'c', value: 3 }
  ])
})

test(`${variant}: awsDynamoDBQueryStream should return items`, async (t) => {
  const client = mockClient(DynamoDBClient)
  awsDynamoDBSetClient(client)
  client
    .on(QueryCommand, {
      TableName: 'TableName'
    })
    .resolves({
      Items: [
        { key: { S: 'a' }, value: { N: 1 } },
        { key: { S: 'b' }, value: { N: 2 } }
      ],
      LastEvaluatedKey: {
        key: { S: 'b' }
      }
    })
    .on(QueryCommand, {
      TableName: 'TableName',
      ExclusiveStartKey: { key: { S: 'b' } }
    })
    .resolves({
      Items: [{ key: { S: 'c' }, value: { N: 3 } }]
    })

  const options = {
    TableName: 'TableName'
  }
  const stream = awsDynamoDBQueryStream(options)
  const output = await streamToArray(stream)

  deepEqual(output, [
    { key: 'a', value: 1 },
    { key: 'b', value: 2 },
    { key: 'c', value: 3 }
  ])
})

test(`${variant}: awsDynamoDBScanStream should return items`, async (t) => {
  const client = mockClient(DynamoDBClient)
  awsDynamoDBSetClient(client)
  client
    .on(ScanCommand, {
      TableName: 'TableName'
    })
    .resolves({
      Items: [
        { key: { S: 'a' }, value: { N: 1 } },
        { key: { S: 'b' }, value: { N: 2 } }
      ],
      LastEvaluatedKey: {
        key: { S: 'b' }
      }
    })
    .on(ScanCommand, {
      TableName: 'TableName',
      ExclusiveStartKey: { key: 'b' }
    })
    .resolves({
      Items: [{ key: { S: 'c' }, value: { N: 3 } }]
    })

  const options = {
    TableName: 'TableName'
  }
  const stream = awsDynamoDBScanStream(options)
  const output = await streamToArray(stream)

  deepEqual(output, [
    { key: 'a', value: 1 },
    { key: 'b', value: 2 },
    { key: 'c', value: 3 }
  ])
})

test(`${variant}: awsDynamoDBPutItemStream should store items`, async (t) => {
  const client = mockClient(DynamoDBClient)
  awsDynamoDBSetClient(client)
  client
    .on(BatchWriteCommand, {
      RequestItems: {
        TableName: [
          {
            PutRequest: {
              Item: {
                key: { S: 'a' },
                value: { N: '1' }
              }
            }
          },
          {
            PutRequest: {
              Item: {
                key: { S: 'b' },
                value: { N: '2' }
              }
            }
          },
          {
            PutRequest: {
              Item: {
                key: { S: 'c' },
                value: { N: '3' }
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
                key: { S: 'c' },
                value: { N: '3' }
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
                key: { S: 'c' },
                value: { N: '3' }
              }
            }
          }
        ]
      }
    })
    .resolves({})

  const input = [
    { key: 'a', value: 1 },
    { key: 'b', value: 2 },
    { key: 'c', value: 3 }
  ]
  const options = {
    TableName: 'TableName'
  }
  const stream = [createReadableStream(input), awsDynamoDBPutStream(options)]
  const output = await pipeline(stream)

  deepEqual(output, {})
})

test(`${variant}: awsDynamoDBDeleteItemStream should store items`, async (t) => {
  const client = mockClient(DynamoDBClient)
  awsDynamoDBSetClient(client)
  client
    .on(BatchWriteCommand, {
      RequestItems: {
        TableName: [
          {
            DeleteRequest: {
              Key: {
                key: { S: 'a' }
              }
            }
          },
          {
            DeleteRequest: {
              Key: {
                key: { S: 'b' }
              }
            }
          },
          {
            DeleteRequest: {
              Key: {
                key: { S: 'c' }
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
                key: { S: 'c' }
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
                key: { S: 'c' }
              }
            }
          }
        ]
      }
    })
    .resolves({})

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
    awsDynamoDBDeleteStream(options)
  ]
  const output = await pipeline(stream)

  deepEqual(output, {})
})
