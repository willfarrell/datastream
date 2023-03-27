import test from 'node:test'
import { equal, deepEqual } from 'node:assert'

import {
  pipeline,
  pipejoin,
  streamToArray,
  streamToString,
  createReadableStream
} from '@datastream/core'

import { csvParseStream, csvFormatStream } from '@datastream/csv'

let variant = 'unknown'
for (const execArgv of process.execArgv) {
  const flag = '--conditions='
  if (execArgv.includes('--conditions=')) {
    variant = execArgv.replace(flag, '')
  }
}

// *** csvParseStream *** //
test(`${variant}: csvParseStream should parse csv to object[]`, async (t) => {
  const streams = [
    createReadableStream('a,b,c,d\r\n1,2,3,4\r\n1,2,3,4\r\n'),
    csvParseStream()
  ]
  const stream = pipejoin(streams)
  const output = await streamToArray(stream)

  deepEqual(output, [
    { a: '1', b: '2', c: '3', d: '4' },
    { a: '1', b: '2', c: '3', d: '4' }
  ])
})

test(`${variant}: csvParseStream should parse csv to string[]`, async (t) => {
  const streams = [
    createReadableStream('1,2,3,4\r\n1,2,3,4\r\n'),
    csvParseStream({ header: false })
  ]
  const stream = pipejoin(streams)
  const output = await streamToArray(stream)

  deepEqual(output, [
    ['1', '2', '3', '4'],
    ['1', '2', '3', '4']
  ])
})

test(`${variant}: csvParseStream should return csv parsing errors`, async (t) => {
  const streams = [
    createReadableStream('a,b,c,d\r\n1,2,3\r\n1,2,3,4,5\r\n'),
    csvParseStream()
  ]
  const result = await pipeline(streams)

  const { key, value } = streams[1].result()

  const csvErrors = {
    FieldsMismatchTooFew: {
      id: 'FieldsMismatchTooFew',
      idx: [2],
      message: 'Too few fields were parsed, expected 4.'
    },
    FieldsMismatchTooMany: {
      id: 'FieldsMismatchTooMany',
      idx: [3],
      message: 'Too many fields were parsed, expected 4.'
    }
  }
  equal(key, 'csvErrors')
  deepEqual(result.csvErrors, csvErrors)
  deepEqual(value, csvErrors)
})

// *** csvFormatStream *** //
test(`${variant}: csvFormatStream should format csv from object[]`, async (t) => {
  const streams = [
    createReadableStream([
      { a: '1', b: '2', c: '3', d: '4' },
      { a: '1', b: '2', c: '3', d: '4' }
    ]),
    csvFormatStream()
  ]
  const stream = pipejoin(streams)
  const output = await streamToString(stream)

  deepEqual(output, 'a,b,c,d\r\n1,2,3,4\r\n1,2,3,4\r\n')
})

test(`${variant}: csvFormatStream should format csv from object[] with columns`, async (t) => {
  const streams = [
    createReadableStream([
      { a: '1', b: '2', c: '3', d: '4' },
      { a: '1', b: '2', c: '3', d: '4' }
    ]),
    csvFormatStream({ header: false, columns: ['d', 'c', 'b', 'a'] })
  ]
  const stream = pipejoin(streams)
  const output = await streamToString(stream)

  deepEqual(output, '4,3,2,1\r\n4,3,2,1\r\n')
})

test(`${variant}: csvFormatStream should format csv from string[]`, async (t) => {
  const streams = [
    createReadableStream([
      ['1', '2', '3', '4'],
      ['1', '2', '3', '4']
    ]),
    csvFormatStream({ columns: ['a', 'b', 'c', 'd'] })
  ]
  const stream = pipejoin(streams)
  const output = await streamToString(stream)

  deepEqual(output, 'a,b,c,d\r\n1,2,3,4\r\n1,2,3,4\r\n')
})
