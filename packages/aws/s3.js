/* global crypto */
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

let defaultClient = new S3Client(awsClientDefaults)
export const awsS3SetClient = (s3Client) => {
  defaultClient = s3Client
}

export const awsS3GetObjectStream = async (options, streamOptions) => {
  const { client, ...params } = options
  const { Body } = await (client ?? defaultClient).send(
    new GetObjectCommand(params)
  )
  if (!Body) {
    throw new Error('S3.GetObject not Found', { cause: params })
  }
  return createReadableStream(Body, streamOptions)
}

export const awsS3PutObjectStream = (options, streamOptions) => {
  const { onProgress, client, tags, ...params } = options
  const stream = createPassThroughStream(() => {}, streamOptions)
  const upload = new Upload({
    client: client ?? defaultClient,
    params: {
      ...params,
      Body: stream
    },
    tags
  })
  if (onProgress) {
    stream.on('httpUploadProgress', onProgress)
  }
  const result = upload.done()

  stream.result = async () => {
    await result
    return {}
  }
  return stream
}

// This is designed to be used in the browser on a file that you want to upload via a presigned URL
// partSize; magic number, no 16MB as mentioned in the docs
const awsS3ChecksumStream = (
  { ChecksumAlgorithm, partSize, resultKey } = {},
  streamOptions
) => {
  ChecksumAlgorithm ??= 'SHA256'
  partSize ??= 17179870
  const algorithm = _algorithms[ChecksumAlgorithm]
  let checksums = []
  let bytes = new Uint8Array(0)
  const passThrough = async (chunk) => {
    if (typeof chunk === 'string') {
      chunk = new TextEncoder('utf-8').encode(chunk)
    }
    while (bytes.byteLength + chunk.byteLength > partSize) {
      chunk = _concatBuffers([bytes, chunk])
      const prefixChunk = chunk.slice(0, partSize)

      const checksum = await crypto.subtle.digest(algorithm, prefixChunk)
      checksums.push(checksum)
      chunk = chunk.slice(prefixChunk.byteLength)

      bytes = new Uint8Array(0)
    }
    bytes = _concatBuffers([bytes, chunk])
  }
  const flush = async () => {
    if (bytes.byteLength) {
      const checksum = await crypto.subtle.digest(algorithm, bytes)
      checksums.push(checksum)
    }
  }
  const stream = createPassThroughStream(passThrough, flush, streamOptions)
  let checksum
  stream.result = async () => {
    if (!checksum) {
      if (checksums.length > 1) {
        checksum = await crypto.subtle.digest(
          algorithm,
          _concatBuffers(checksums)
        )
        checksum = _arrayBufferToBase64(checksum) + '-' + checksums.length
      } else {
        checksum = _arrayBufferToBase64(checksums[0])
      }
      checksums = checksums.map(_arrayBufferToBase64)
    }
    return {
      key: resultKey ?? 's3',
      value: { checksum, checksums, partSize }
    }
  }
  return stream
}

// TODO support CRC32, CRC32C
const _algorithms = {
  SHA1: 'SHA-1',
  SHA256: 'SHA-256'
}
const _concatBuffers = function (buffers) {
  const tmp = new Uint8Array(
    buffers.reduce((byteLength, buffer) => byteLength + buffer.byteLength, 0)
  )
  let byteLength = 0
  for (let i = 0, l = buffers.length; i < l; i++) {
    tmp.set(new Uint8Array(buffers[i]), byteLength)
    byteLength += buffers[i].byteLength
  }
  return tmp.buffer
}
const _arrayBufferToBase64 = (buffer) => {
  let binary = ''
  const bytes = new Uint8Array(buffer)
  const len = bytes.byteLength
  for (let i = 0; i < len; i++) {
    binary += String.fromCharCode(bytes[i])
  }
  return btoa(binary)
}

export default {
  setClient: awsS3SetClient,
  getObjectStream: awsS3GetObjectStream,
  putObjectStream: awsS3PutObjectStream,
  checksumStream: awsS3ChecksumStream
}
