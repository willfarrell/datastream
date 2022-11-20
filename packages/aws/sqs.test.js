import test from 'node:test'
import { deepEqual } from 'node:assert'
// import sinon from 'sinon'
import { mockClient } from 'aws-sdk-client-mock'
import { SQSClient, SendMessageCommand } from '@aws-sdk/client-sqs'

import { pipeline, createReadableStream } from '@datastream/core'

import { awsSQSSetClient, awsSQSSendMessageStream } from '@datastream/aws'

let variant = 'unknown'
for (const execArgv of process.execArgv) {
  const flag = '--conditions='
  if (execArgv.includes(flag)) {
    variant = execArgv.replace(flag, '')
  }
}

test(`${variant}: awsSQSSendMessageStream should put chunk`, async (t) => {
  const client = mockClient(SQSClient)
  awsSQSSetClient(client)

  const input = [{ id: 'x' }]
  const options = {
    // TODO
  }

  client.on(SendMessageCommand).resolves({}) // TODO

  const stream = [
    createReadableStream(input),
    awsSQSSendMessageStream(options)
  ]
  const result = await pipeline(stream)

  deepEqual(result, {})
})
