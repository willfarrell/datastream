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
  awsS3PutObjectStream,
  awsS3ChecksumStream
} from './s3.js'

let variant = 'unknown'
for (const execArgv of process.execArgv) {
  const flag = '--conditions='
  if (execArgv.includes(flag)) {
    variant = execArgv.replace(flag, '')
  }
}

if (variant === 'node') {
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

  // TODO update
  /* test(`${variant}: awsS3PutObjectStream should put chunk`, async (t) => {
    //process.env.AWS_REGION = 'ca-central-1' // not mocked when using PutObjectCommand for some reason
    const client = mockClient(S3Client)
    awsS3SetClient(client)

    const input = 'x'.repeat(512)
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
  }) */

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
  console.info(
    "awsS3PutObjectStream doesn't work with webstreams at this time"
  )

  test(`${variant}: awsS3ChecksumStream should make checksum of 8MB string (0.5 block)`, async (t) => {
    const input = 'x'.repeat(8 * 1024 * 1024)
    const options = {
      ChecksumAlgorithm: 'SHA256'
    }

    const stream = [createReadableStream(input), awsS3ChecksumStream(options)]
    const result = await pipeline(stream)

    deepEqual(result, {
      checksum: 'DHe8CgeVqTYS1FJWiXRW0PyyTxUcRMFQ0H7NA/TvUWg='
    })
  })
  //   test(`${variant}: awsS3ChecksumStream should make checksum of 16 MB string (1 block)`, async (t) => {
  //     const input = 'x'.repeat(17179870)
  //     const options = {
  //       ChecksumAlgorithm: 'SHA256'
  //     }
  //
  //     const stream = [createReadableStream(input), awsS3ChecksumStream(options)]
  //     const result = await pipeline(stream)
  //
  //     deepEqual(result, {
  //       checksum: 'WN4WZJbH8owC673D8TAJBGXF4n7cIY7lDhbZmvIOX5o='
  //     })
  //   })
  //   test(`${variant}: awsS3ChecksumStream should make checksum of 24MB string (1.5 blocks)`, async (t) => {
  //     const input = 'x'.repeat(24 * 1024 * 1024)
  //     const options = {
  //       ChecksumAlgorithm: 'SHA256'
  //     }
  //
  //     const stream = [createReadableStream(input), awsS3ChecksumStream(options)]
  //     const result = await pipeline(stream)
  //
  //     deepEqual(result, {
  //       checksum: 'eWQzGj3USSV0NvWbhxtpmbkHgNReYxUzwVBXAU86X/4=-2'
  //     })
  //   })
  //   test(`${variant}: awsS3ChecksumStream should make checksum of file 32 MB string (2 blocks)`, async (t) => {
  //     const input = 'x'.repeat(17179870 * 2)
  //     const options = {
  //       ChecksumAlgorithm: 'SHA256'
  //     }
  //
  //     const stream = [createReadableStream(input), awsS3ChecksumStream(options)]
  //     const result = await pipeline(stream)
  //
  //     deepEqual(result, {
  //       checksum: '65/QvEoh9MiBIPeSgTqKTptI3Vnf+vaJ1om/MYYMpBU=-2'
  //     })
  //   })

  test(`${variant}: awsS3ChecksumStream should make checksum of 8MB ArrayBuffer (0.5 block)`, async (t) => {
    const input = new TextEncoder('utf-8').encode('x'.repeat(8 * 1024 * 1024))
    const options = {
      ChecksumAlgorithm: 'SHA256'
    }

    const stream = [createReadableStream(input), awsS3ChecksumStream(options)]
    const result = await pipeline(stream)

    deepEqual(result, {
      checksum: 'DHe8CgeVqTYS1FJWiXRW0PyyTxUcRMFQ0H7NA/TvUWg='
    })
  })
  //   test(`${variant}: awsS3ChecksumStream should make checksum of 16 MB ArrayBuffer (1 block)`, async (t) => {
  //     const input = new TextEncoder('utf-8').encode('x'.repeat(17179870))
  //     const options = {
  //       ChecksumAlgorithm: 'SHA256'
  //     }
  //
  //     const stream = [createReadableStream(input), awsS3ChecksumStream(options)]
  //     const result = await pipeline(stream)
  //
  //     deepEqual(result, {
  //       checksum: 'WN4WZJbH8owC673D8TAJBGXF4n7cIY7lDhbZmvIOX5o='
  //     })
  //   })
  //   test(`${variant}: awsS3ChecksumStream should make checksum of 24MB ArrayBuffer (1.5 blocks)`, async (t) => {
  //     const input = new TextEncoder('utf-8').encode('x'.repeat(24 * 1024 * 1024))
  //     const options = {
  //       ChecksumAlgorithm: 'SHA256'
  //     }
  //
  //     const stream = [createReadableStream(input), awsS3ChecksumStream(options)]
  //     const result = await pipeline(stream)
  //
  //     deepEqual(result, {
  //       checksum: 'eWQzGj3USSV0NvWbhxtpmbkHgNReYxUzwVBXAU86X/4=-2'
  //     })
  //   })
  //   test(`${variant}: awsS3ChecksumStream should make checksum of file 32 MB ArrayBuffer (2 blocks)`, async (t) => {
  //     const input = new TextEncoder('utf-8').encode('x'.repeat(17179870 * 2))
  //     const options = {
  //       ChecksumAlgorithm: 'SHA256'
  //     }
  //
  //     const stream = [createReadableStream(input), awsS3ChecksumStream(options)]
  //     const result = await pipeline(stream)
  //
  //     deepEqual(result, {
  //       checksum: '65/QvEoh9MiBIPeSgTqKTptI3Vnf+vaJ1om/MYYMpBU=-2'
  //     })
  //   })
}
