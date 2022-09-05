import { SNSClient, PublishBatchCommand } from '@aws-sdk/client-sns'
import { createWritableStream } from '@datastream/core'

import { Agent } from 'node:https'
import { NodeHttpHandler } from '@aws-sdk/node-http-handler'
import AWSXRay from 'aws-xray-sdk-core'

const awsClientDefaults = {
  requestHandler: new NodeHttpHandler({
    httpsAgent: new Agent({
      keepAlive: true,
      secureProtocol: 'TLSv1_2_method'
    })
  }),
  // https://aws.amazon.com/compliance/fips/
  useFipsEndpoint: [
    'us-east-1',
    'us-east-2',
    'us-west-1',
    'us-west-2'
  ].includes(process.env.AWS_REGION)
}

let client = AWSXRay.captureAWSv3Client(new SNSClient(awsClientDefaults))
export const awsSNSSetClient = (snsClient) => {
  client = snsClient
}

export const awsSNSPublishMessageStream = (options, streamOptions) => {
  let batch = []
  const send = () => {
    options.PublishBatchRequestEntries = batch
    batch = []
    return client.send(new PublishBatchCommand(options))
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
  setClient: awsSNSSetClient,
  publishMessageStream: awsSNSPublishMessageStream
}
