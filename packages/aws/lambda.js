import { createReadableStream } from '@datastream/core'
import {
  LambdaClient,
  InvokeWithResponseStreamCommand
} from '@aws-sdk/client-lambda'

const awsClientDefaults = {
  // https://aws.amazon.com/compliance/fips/
  useFipsEndpoint: [
    'us-east-1',
    'us-east-2',
    'us-west-1',
    'us-west-2'
    // 'ca-central-1'
  ].includes(process.env.AWS_REGION)
}

let defaultClient = new LambdaClient(awsClientDefaults)
export const awsLambdaSetClient = (lambdaClient) => {
  defaultClient = lambdaClient
}

export const awsLambdaReadableStream = async (lambdaOptions, streamOptions) => {
  return createReadableStream(awsLambdaGenerator(lambdaOptions), streamOptions)
}
export const awsLambdaResponseStream = awsLambdaReadableStream

async function * awsLambdaGenerator (lambdaOptions, streamOptions) {
  if (!Array.isArray(lambdaOptions)) lambdaOptions = [lambdaOptions]
  for (const options of lambdaOptions) {
    const response = await defaultClient.send(
      new InvokeWithResponseStreamCommand(options)
    )
    for await (const chunk of response.EventStream) {
      if (chunk?.PayloadChunk?.Payload) {
        yield chunk.PayloadChunk.Payload
      } else if (chunk?.InvokeComplete?.ErrorCode) {
        // TODO not handled by readable properly
        throw new Error(chunk.InvokeComplete.ErrorCode, {
          cause: chunk.InvokeComplete.ErrorDetails
        })
      }
    }
  }
}

export default {
  setClient: awsLambdaSetClient,
  readableStream: awsLambdaReadableStream,
  responseStream: awsLambdaReadableStream
}
