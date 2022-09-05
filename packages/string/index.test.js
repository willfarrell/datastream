import test from 'node:test'
import { equal, deepEqual } from 'node:assert'
// import sinon from 'sinon'
import {
  pipeline,
  pipejoin,
  createReadableStream,
  streamToArray
} from '@datastream/core'

import { stringReadableStream, stringLengthStream } from '@datastream/string'

let variant = 'unknown'
for (const execArgv of process.execArgv) {
  const flag = '--conditions='
  if (execArgv.includes('--conditions=')) {
    variant = execArgv.replace(flag, '')
  }
}

// *** stringReadableStream *** //
test(`${variant}: stringReadableStream should read in initial chunks`, async (t) => {
  const input = 'abc'
  const streams = [stringReadableStream(input)]
  const stream = pipejoin(streams)
  const output = await streamToArray(stream)

  deepEqual(output, [input])
})

// *** stringSizeStream *** //
test(`${variant}: stringSizeStream should count length of chunks`, async (t) => {
  const input = ['1', '2', '3']
  const streams = [createReadableStream(input), stringLengthStream()]

  const result = await pipeline(streams)
  const { key, value } = streams[1].result()

  equal(key, 'length')
  equal(result.length, 3)
  equal(value, 3)
})

test(`${variant}: stringSizeStream should count length of chunks with custom key`, async (t) => {
  const input = ['1', '2', '3']
  const streams = [
    createReadableStream(input),
    stringLengthStream({ resultKey: 'string' })
  ]

  const result = await pipeline(streams)
  const { key, value } = streams[1].result()

  equal(key, 'string')
  equal(result.string, 3)
  equal(value, 3)
})
