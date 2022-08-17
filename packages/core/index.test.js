import test from 'node:test'
import { equal, deepEqual } from 'node:assert'
import sinon from 'sinon'
import {
  pipeline,
  pipejoin,
  streamToArray,
  streamToString,
  isReadable,
  isWritable,
  makeOptions,
  createReadableStream,
  createPassThroughStream,
  createTransformStream,
  createWritableStream
} from '@datastream/core'

let variant = 'unknown'
for (const execArgv of process.execArgv) {
  const flag = '--conditions='
  if (execArgv.includes('--conditions=')) {
    variant = execArgv.replace(flag, '')
  }
}

const countStream = (options = { key: 'size' }) => {
  const { key } = options
  let value = 0
  const transform = () => {
    value += 1
  }
  const stream = createTransformStream(transform, options)
  stream.result = () => ({ key, value })
  return stream
}

// *** streamTo{Array,String,Buffer} *** //
const types = {
  boolean: [true, false],
  integer: [-1, 0, 1],
  decimal: [-1.1, 0.0, 1.1],
  strings: ['a', 'b', 'c'],
  buffer: ['a', 'b', 'c'].map((i) => Buffer.from(i)),
  date: [new Date(), new Date()],
  array: [
    ['a', 'b'],
    ['1', '2']
  ],
  object: [{ a: '1' }, { a: '2' }, { a: '3' }]
}
for (const type of Object.keys(types)) {
  test(`${variant}: streamToArray should work with readable ${type} stream`, async (t) => {
    const input = types[type]
    const streams = [createReadableStream(input)]
    const stream = pipejoin(streams)
    const output = await streamToArray(stream)

    deepEqual(output, input)
  })

  test(`${variant}: streamToArray should work with transform ${type} stream`, async (t) => {
    const input = types[type]
    const streams = [createReadableStream(input), createTransformStream()]
    const stream = pipejoin(streams)
    const output = await streamToArray(stream)

    deepEqual(output, input)
  })

  test(`${variant}: streamToString should work with readable ${type} stream`, async (t) => {
    const input = types[type]
    const streams = [createReadableStream(input)]
    const stream = pipejoin(streams)
    const output = await streamToString(stream)

    deepEqual(output, input.join(''))
  })

  test(`${variant}: streamToString should work with transform ${type} stream`, async (t) => {
    const input = types[type]
    const streams = [createReadableStream(input), createTransformStream()]
    const stream = pipejoin(streams)
    const output = await streamToString(stream)

    deepEqual(output, input.join(''))
  })
}

// *** createReadableStream *** //
test(`${variant}: createReadableStream should create a readable stream from string`, async (t) => {
  const input = 'abc'
  const streams = [createReadableStream(input)]
  const stream = pipejoin(streams)
  const output = await streamToString(stream)

  equal(isReadable(streams[0]), true)
  equal(isWritable(streams[0]), false)
  deepEqual(output, input)
})

test(`${variant}: createReadableStream should create a readable stream from array`, async (t) => {
  const input = ['a', 'b', 'c']
  const streams = [createReadableStream(input)]
  const stream = pipejoin(streams)
  const output = await streamToArray(stream)

  equal(isReadable(streams[0]), true)
  equal(isWritable(streams[0]), false)
  deepEqual(output, input)
})

test(`${variant}: createReadableStream should create a readable stream from iterable`, async (t) => {
  function * input () {
    yield 'a'
    yield 'b'
    yield 'c'
  }
  const streams = [createReadableStream(input())]
  const stream = pipejoin(streams)
  const output = await streamToArray(stream)

  equal(isReadable(streams[0]), true)
  equal(isWritable(streams[0]), false)
  deepEqual(output, ['a', 'b', 'c'])
})

test(`${variant}: createReadableStream should chunk long strings`, async (t) => {
  const input = 'x'.repeat(17 * 1024) // where 16*1024 is the default chunkSize/highWaterMark
  const streams = [createReadableStream(input)]
  const stream = pipejoin(streams)
  const output = await streamToArray(stream)

  equal(output.length, 2)
})

// *** createPassThroughStream *** //
test(`${variant}: createPassThroughStream should create a passs through stream`, async (t) => {
  const input = ['a', 'b', 'c']
  const transform = sinon.spy()
  const streams = [
    createReadableStream(input),
    createPassThroughStream(transform)
  ]
  const stream = pipejoin(streams)
  const output = await streamToArray(stream)

  equal(isReadable(streams[1]), true)
  equal(isWritable(streams[1]), true)
  equal(transform.callCount, 3)
  deepEqual(output, input)
})

// *** createTransformStream *** //
test(`${variant}: createTransformStream should create a transform stream`, async (t) => {
  const input = ['a', 'b', 'c']
  const transform = sinon.spy()
  const streams = [
    createReadableStream(input),
    createTransformStream(transform)
  ]
  const stream = pipejoin(streams)
  const output = await streamToArray(stream)

  equal(isReadable(streams[1]), true)
  equal(isWritable(streams[1]), true)
  equal(transform.callCount, 3)
  deepEqual(output, [undefined, undefined, undefined])
})

// *** createWritableStream *** //
test(`${variant}: createWritableStream should create a writable stream`, async (t) => {
  const input = ['a', 'b', 'c']
  const transform = sinon.spy()
  const streams = [
    createReadableStream(input),
    createWritableStream(transform)
  ]

  equal(isReadable(streams[1]), false)
  equal(isWritable(streams[1]), true)

  const result = await pipeline(streams)

  equal(transform.callCount, 3)
  deepEqual(result, {})
})

// *** pipeline *** //
test(`${variant}: pipeline should should add writable to end of streams array`, async (t) => {
  const input = ['a', 'b', 'c']
  const transform = sinon.spy()
  const streams = [
    createReadableStream(input),
    countStream(),
    createTransformStream(transform)
  ]
  const result = await pipeline(streams)

  equal(isReadable(streams[1]), true)
  equal(isWritable(streams[1]), true)
  equal(transform.callCount, 3)
  deepEqual(result, { size: 3 })
})

// *** makeOptions *** //
if (variant === 'node') {
  test(`${variant}: makeOptions should return interoperable structure`, async (t) => {
    const options = makeOptions({
      objectMode: true,
      highWaterMark: 1,
      chunkSize: 2
    })
    deepEqual(options, {
      chunkSize: 2,
      highWaterMark: 1,
      writableHighWaterMark: 1,
      writableObjectMode: true,
      objectMode: true,
      readableObjectMode: true,
      readableHighWaterMark: 1
    })
  })
}

if (variant === 'webstream') {
  test(`${variant}: makeOptions should return interoperable structure`, async (t) => {
    // Web Stream always is in object mode
    const options = makeOptions({ highWaterMark: 1, chunkSize: 2 })
    deepEqual(options, {
      writableStrategy: {
        highWaterMark: 1,
        size: { chunk: 2 }
      },
      readableStrategy: {
        highWaterMark: 1,
        size: { chunk: 2 }
      }
    })
  })
}
