import {
  SQSClient,
  // ReceiveMessageCommand,
  DeleteMessageBatchCommand,
  SendMessageBatchCommand
} from '@aws-sdk/client-sqs'
import { createWritableStream } from '@datastream/core'

import AWSXRay from 'aws-xray-sdk-core'

const awsClientDefaults = {
  // https://aws.amazon.com/compliance/fips/
  useFipsEndpoint: [
    'us-east-1',
    'us-east-2',
    'us-west-1',
    'us-west-2'
  ].includes(process.env.AWS_REGION)
}

let client = AWSXRay.captureAWSv3Client(new SQSClient(awsClientDefaults))
export const awsSQSSetClient = (sqsClient) => {
  client = sqsClient
}

export const awsSQSReceiveMessageStream = (options, streamOptions = {}) => {
  // TODO receiveMessage(params = {}, callback) ⇒ AWS.Request
  // TODO use `pull`
  // await client.send(new ReceiveMessageCommand)
}

export const awsSQSDeleteMessageStream = (options, streamOptions = {}) => {
  let batch = []
  const send = () => {
    options.Entries = batch
    batch = []
    return client.send(new DeleteMessageBatchCommand(options))
  }
  const write = async (chunk) => {
    if (batch.length === 10) {
      await send()
    }
    batch.push(chunk)
  }
  streamOptions.final = send
  return createWritableStream(write, streamOptions)
}

export const awsSQSSendMessageStream = (options, streamOptions = {}) => {
  let batch = []
  const send = () => {
    options.Entries = batch
    batch = []
    return client.send(new SendMessageBatchCommand(options))
  }
  const write = async (chunk) => {
    if (batch.length === 10) {
      await send()
    }
    batch.push(chunk)
  }
  streamOptions.final = send
  return createWritableStream(write, streamOptions)
}

export default {
  setClient: awsSQSSetClient,
  sendMessageStream: awsSQSSendMessageStream,
  receiveMessageStream: awsSQSReceiveMessageStream,
  deleteMessageStream: awsSQSDeleteMessageStream
}
