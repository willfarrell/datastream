import test from 'node:test'
import { equal, deepEqual } from 'node:assert'
// import sinon from 'sinon'
import { mockClient } from 'aws-sdk-client-mock'
import {
  LambdaClient,
  InvokeWithResponseStreamCommand
} from '@aws-sdk/client-lambda'

import { pipeline, createReadableStream } from '@datastream/core'

import { awsLambdaSetClient, awsLambdaReadableStream } from '@datastream/aws'

let variant = 'unknown'
for (const execArgv of process.execArgv) {
  const flag = '--conditions='
  if (execArgv.includes(flag)) {
    variant = execArgv.replace(flag, '')
  }
}

test(`${variant}: awsLambdaReadableStream should return chunk`, async (t) => {
  const client = mockClient(LambdaClient)
  awsLambdaSetClient(client)

  const encoder = new TextEncoder()
  const decoder = new TextDecoder()

  client.on(InvokeWithResponseStreamCommand, {}).resolves({
    EventStream: createReadableStream([
      {
        PayloadChunk: {
          Payload: encoder.encode('1')
        }
      },
      {
        PayloadChunk: {
          Payload: encoder.encode('2')
        }
      }
    ])
  })

  let result = ''
  for await (const chunk of await awsLambdaReadableStream({})) {
    result += decoder.decode(chunk)
  }

  deepEqual(result, '12')
})

test(`${variant}: awsLambdaReadableStream should throw error`, async (t) => {
  const client = mockClient(LambdaClient)
  awsLambdaSetClient(client)

  client.on(InvokeWithResponseStreamCommand, {}).resolves({
    EventStream: createReadableStream([
      {
        InvokeComplete: {
          ErrorCode: 'ErrorCode',
          ErrorDetails: 'ErrorDetails'
        }
      }
    ])
  })

  try {
    await pipeline(await awsLambdaReadableStream({}))

    equal(true, false)
  } catch (e) {
    console.error(e)
    equal(e.message, 'ErrorCode')
    equal(e.cause, 'ErrorDetails')
  }
})
