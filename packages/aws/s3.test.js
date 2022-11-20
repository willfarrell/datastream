import test from 'node:test'
import { deepEqual } from 'node:assert'
// import sinon from 'sinon'
import { mockClient } from 'aws-sdk-client-mock'
import {
  S3Client,
  GetObjectCommand,
  PutObjectCommand,
  CreateMultipartUploadCommand,
  UploadPartCommand
} from '@aws-sdk/client-s3'

import {
  pipeline,
  streamToString,
  createReadableStream
} from '@datastream/core'

import {
  awsS3SetClient,
  awsS3GetObjectStream,
  awsS3PutObjectStream
} from '@datastream/aws'

let variant = 'unknown'
for (const execArgv of process.execArgv) {
  const flag = '--conditions='
  if (execArgv.includes(flag)) {
    variant = execArgv.replace(flag, '')
  }
}

test(`${variant}: awsS3GetObjectStream should return chunks`, async (t) => {
  const client = mockClient(S3Client)
  awsS3SetClient(client)
  client
    .on(GetObjectCommand, {
      Bucket: 'bucket',
      Key: 'file.ext'
    })
    .resolves({
      Body: createReadableStream('contents')
    })

  const options = {
    Bucket: 'bucket',
    Key: 'file.ext'
  }
  const stream = await awsS3GetObjectStream(options)
  const output = await streamToString(stream)

  deepEqual(output, 'contents')
})

if (variant === 'node') {
  test(`${variant}: awsS3PutObjectStream should put chunk`, async (t) => {
    process.env.AWS_REGION = 'ca-central-1' // not mocked when using PutObjectCommand for some reason
    const client = mockClient(S3Client)
    awsS3SetClient(client)

    const input = 'x'.repeat(1024)
    const options = {
      Bucket: 'bucket',
      Key: 'file.ext'
    }

    client
      .on(PutObjectCommand)
      .resolves({ ETag: '1' })
      .on(CreateMultipartUploadCommand)
      .rejects()
      .on(UploadPartCommand)
      .rejects()

    const stream = [createReadableStream(input), awsS3PutObjectStream(options)]
    const result = await pipeline(stream)

    deepEqual(result, {})
  })

  test(`${variant}: awsS3PutObjectStream should put chunks`, async (t) => {
    const client = mockClient(S3Client)
    awsS3SetClient(client)

    const input = 'x'.repeat(6 * 1024 * 1024)
    const options = {
      Bucket: 'bucket',
      Key: 'file.ext'
    }

    client
      .on(PutObjectCommand)
      .rejects()
      .on(CreateMultipartUploadCommand)
      .resolves({ UploadId: '1' })
      .on(UploadPartCommand)
      .resolves({ ETag: '1' })

    const stream = [createReadableStream(input), awsS3PutObjectStream(options)]
    const result = await pipeline(stream)

    deepEqual(result, {})
  })
} else {
  console.log("awsS3PutObjectStream doesn't work with webstreams at this time")
}
