import test from 'node:test'
import { deepEqual } from 'node:assert'
// import sinon from 'sinon'
import {
  pipejoin,
  createReadableStream,
  streamToString
} from '@datastream/core'

import { base64EncodeStream, base64DecodeStream } from '@datastream/base64'

let variant = 'unknown'
for (const execArgv of process.execArgv) {
  const flag = '--conditions='
  if (execArgv.includes('--conditions=')) {
    variant = execArgv.replace(flag, '')
  }
}

// *** base64EncodeStream *** //
test(`${variant}: base64EncodeStream should encode`, async (t) => {
  const input = 'encode'
  const streams = [createReadableStream(input), base64EncodeStream()]
  const output = await streamToString(pipejoin(streams))

  deepEqual(output, Buffer.from(input).toString('base64'))
})

// *** base64DecodeStream *** //
test(`${variant}: base64DecodeStream should decode`, async (t) => {
  const input = 'decode'
  const streams = [
    createReadableStream(Buffer.from(input).toString('base64')),
    base64DecodeStream()
  ]
  const output = await streamToString(pipejoin(streams))

  deepEqual(output, input)
})
