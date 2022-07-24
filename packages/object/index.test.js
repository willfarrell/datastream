import test from 'node:test'
import { equal, deepEqual } from 'node:assert'
//import sinon from 'sinon'
import {pipeline, pipejoin, createReadableStream, streamToArray} from '@datastream/core'

import { objectReadableStream, objectCountStream, objectBatchStream, objectOutputStream } from '@datastream/object'

let variant = 'unknown'
for(const execArgv of process.execArgv) {
  const flag = '--conditions='
  if (execArgv.includes('--conditions=')) {
    variant = execArgv.replace(flag, '')
  }
}

  // *** objectReadableStream *** //
  test(`${variant}: objectReadableStream should read in initial chunks`, async (t) => {
    const input = [{a:'1'},{b:'2'},{c:'3'}]
    const streams = [
      objectReadableStream(input)
    ]
    const stream = streams[0]
    const output = await streamToArray(stream)

    deepEqual(output, input)
  })

  // *** objectCountStream *** //
  test(`${variant}: objectCountStream should count length of chunks`, async (t) => {
    const input = ['1','2','3']
    const streams = [
      createReadableStream(input),
      objectCountStream()
    ]

    const result = await pipeline(streams)
    const {key, value} = streams[1].result()

    equal(key, 'count')
    equal(result.count, 3)
    equal(value, 3)
  })

  test(`${variant}: objectCountStream should count length of chunks with custom key`, async (t) => {
    const input = ['1','2','3']
    const streams = [
      createReadableStream(input),
      objectCountStream({key:'object'})
    ]

    const result = await pipeline(streams)
    const {key, value} = streams[1].result()

    equal(key, 'object')
    equal(result.object, 3)
    equal(value, 3)
  })

  // *** objectBatchStream *** //
  test(`${variant}: objectBatchStream should batch chunks by key`, async (t) => {
    const input = [{a:'1',b:'2'},{a:'1',b:'2'},{a:'2',b:'3'},{a:'3',b:'4'},{a:'3',b:'5'}]
    const streams = [
      createReadableStream(input),
      objectBatchStream(['a'])
    ]

    const stream = pipejoin(streams)
    const output = await streamToArray(stream)

    deepEqual(output, [
      [{a:'1',b:'2'},{a:'1',b:'2'}],
      [{a:'2',b:'3'}],
      [{a:'3',b:'4'},{a:'3',b:'5'}]
    ])
  })

  test(`${variant}: objectBatchStream should batch chunks by index`, async (t) => {
    const input = [['1','1'],['1','2'],['2','3'],['3','4'],['3','5']]
    const streams = [
      createReadableStream(input),
      objectBatchStream([0])
    ]

    const stream = pipejoin(streams)
    const output = await streamToArray(stream)

    deepEqual(output, [
      [['1','1'],['1','2']],
      [['2','3']],
      [['3','4'],['3','5']]
    ])
  })

  // *** objectOutputStream *** //
  test(`${variant}: objectOutputStream should output chunks`, async (t) => {
    const input = [{a:'1'},{b:'2'},{c:'3'}]
    const streams = [
      createReadableStream(input),
      objectOutputStream()
    ]

    const result = await pipeline(streams)
    const {key, value} = streams[1].result()

    equal(key, 'output')
    deepEqual(result.output, [{a:'1'},{b:'2'},{c:'3'}])
    deepEqual(value, [{a:'1'},{b:'2'},{c:'3'}])
  })

  test(`${variant}: objectOutputStream should output chunks with custom key`, async (t) => {
    const input = [{a:'1'},{b:'2'},{c:'3'}]
    const streams = [
      createReadableStream(input),
      objectOutputStream({key:'object'})
    ]

    const result = await pipeline(streams)
    const {key, value} = streams[1].result()

    equal(key, 'object')
    deepEqual(result.object, [{a:'1'},{b:'2'},{c:'3'}])
    deepEqual(value, [{a:'1'},{b:'2'},{c:'3'}])
  })

