import test from 'node:test'
import { deepEqual } from 'node:assert'
// import sinon from 'sinon'
import {
  pipeline,
  pipejoin,
  createReadableStream,
  streamToArray
} from '@datastream/core'

import { validateStream } from '@datastream/validate'

import Ajv from 'ajv'
const ajv = new Ajv()

let variant = 'unknown'
for (const execArgv of process.execArgv) {
  const flag = '--conditions='
  if (execArgv.includes('--conditions=')) {
    variant = execArgv.replace(flag, '')
  }
}

// *** validateStream *** //
test(`${variant}: validateStream should validate using json schema`, async (t) => {
  const input = [{ a: '1' }, { a: '2' }, { a: '3' }]
  const schema = {
    type: 'object',
    properties: {
      a: {
        type: 'number'
      }
    },
    required: ['a']
  }

  const streams = [createReadableStream(input), validateStream({ schema })]
  const stream = pipejoin(streams)
  const output = await streamToArray(stream)

  deepEqual(output, [{ a: 1 }, { a: 2 }, { a: 3 }])
})

test(`${variant}: validateStream should validate using compiled json schema`, async (t) => {
  const input = [{ a: '1' }, { a: '2' }, { a: '3' }]
  const schema = ajv.compile({
    type: 'object',
    properties: {
      a: {
        type: 'string'
      }
    },
    required: ['a']
  })

  const streams = [createReadableStream(input), validateStream({ schema })]
  const stream = pipejoin(streams)
  const output = await streamToArray(stream)

  deepEqual(output, [{ a: '1' }, { a: '2' }, { a: '3' }])
})

test(`${variant}: validateStream should have errors in result`, async (t) => {
  const input = [{ a: '1' }, { a: 'a' }, { a: '3' }]
  const schema = {
    type: 'object',
    properties: {
      a: {
        type: 'number'
      }
    },
    required: ['a']
  }

  const streams = [createReadableStream(input), validateStream({ schema })]
  const result = await pipeline(streams)

  deepEqual(result, {
    validate: {
      '#/properties/a/type': {
        id: '#/properties/a/type',
        idx: [1],
        keys: ['a'],
        message: 'must be number'
      }
    }
  })
})
