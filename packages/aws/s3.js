import {
  createReadableStream,
  createPassThroughStream
} from '@datastream/core'
import { S3Client, GetObjectCommand } from '@aws-sdk/client-s3'
import { Upload } from '@aws-sdk/lib-storage'

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

let client = new S3Client(awsClientDefaults)
export const awsS3SetClient = (s3Client) => {
  client = s3Client
}

export const awsS3GetObjectStream = async (options, streamOptions) => {
  const { Body } = await client.send(new GetObjectCommand(options))
  if (!Body) {
    throw new Error('S3.GetObject not Found', { cause: options })
  }
  return createReadableStream(Body, streamOptions)
}

export const awsS3PutObjectStream = (options, streamOptions) => {
  const stream = createPassThroughStream(() => {}, streamOptions)
  const upload = new Upload({
    client,
    params: {
      ServerSideEncryption: 'AES256',
      ...options,
      Body: stream
    },
    tags: options.tags
  })
  if (options.onProgress) {
    stream.on('httpUploadProgress', options.onProgress)
  }
  const result = upload.done()

  stream.result = async () => {
    await result
    return {}
  }
  return stream
}

export default {
  setClient: awsS3SetClient,
  getObjectStream: awsS3GetObjectStream,
  putObjectStream: awsS3PutObjectStream
}
