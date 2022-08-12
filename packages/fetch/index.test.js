/* global Headers, Response */
import test from 'node:test'
import { deepEqual } from 'node:assert'
// import sinon from 'sinon'
import { streamToArray } from '@datastream/core'

import { fetchParallel } from '@datastream/fetch'

let variant = 'unknown'
for (const execArgv of process.execArgv) {
  const flag = '--conditions='
  if (execArgv.includes(flag)) {
    variant = execArgv.replace(flag, '')
  }
}

const mockResponses = {
  'https://example.org/csv': () =>
    new Response('a,b,c\n1,2,3', {
      status: 200,
      statusText: 'OK',
      headers: new Headers({
        'Content-Type': 'text/csv; charset=UTF-8'
      })
    }),
  'https://example.org/json-obj/1': () =>
    new Response(JSON.stringify({ key: 'item', value: 1 }), {
      status: 200,
      statusText: 'OK',
      headers: new Headers({
        'Content-Type': 'application/json; charset=UTF-8'
      })
    }),
  'https://example.org/json-obj/2': () =>
    new Response(JSON.stringify({ key: 'item', value: 2 }), {
      status: 200,
      statusText: 'OK',
      headers: new Headers({
        'Content-Type': 'application/json; charset=UTF-8'
      })
    }),
  'https://example.org/json-arr/1': () =>
    new Response(
      JSON.stringify({
        data: [
          { key: 'item', value: 1 },
          { key: 'item', value: 2 },
          { key: 'item', value: 3 }
        ],
        next: 'https://example.org/json-arr/2'
      }),
      {
        status: 200,
        statusText: 'OK',
        headers: new Headers({
          'Content-Type': 'application/json; charset=UTF-8'
        })
      }
    ),
  'https://example.org/json-arr/2': () =>
    new Response(
      JSON.stringify({
        data: [
          { key: 'item', value: 4 },
          { key: 'item', value: 5 },
          { key: 'item', value: 6 }
        ],
        next: ''
      }),
      {
        status: 200,
        statusText: 'OK',
        headers: new Headers({
          'Content-Type': 'application/json; charset=UTF-8'
        })
      }
    ),

  'https://example.org/404': () =>
    new Response('', { status: 404, statusText: 'Not Found' })
}
// global override
global.fetch = (request) => {
  const mockResponse = mockResponses[request.url]()
  if (mockResponse) {
    return Promise.resolve(mockResponse)
  }
  throw new Error('mock missing')
}

// *** fetchParallel *** //
test(`${variant}: fetchParallel should fetch csv`, async (t) => {
  const config = [
    { url: 'https://example.org/csv', headers: { Accept: 'text/csv' } }
  ]
  const stream = fetchParallel(config)
  const output = await streamToArray(stream)

  deepEqual(output, [
    Uint8Array.from('a,b,c\n1,2,3'.split('').map((x) => x.charCodeAt()))
  ])
})

test(`${variant}: fetchParallel should fetch json objects in parallel`, async (t) => {
  const config = [
    { url: 'https://example.org/json-obj/1', dataPath: '' },
    { url: 'https://example.org/json-obj/2', dataPath: '' }
  ]
  const stream = fetchParallel(config)
  const output = await streamToArray(stream)

  deepEqual(output, [
    { key: 'item', value: 1 },
    { key: 'item', value: 2 }
  ])
})

test(`${variant}: fetchParallel should fetch paginated json in series`, async (t) => {
  const config = [
    {
      url: 'https://example.org/json-arr/1',
      dataPath: 'data',
      nextPath: 'next'
    }
  ]
  const stream = fetchParallel(config)
  const output = await streamToArray(stream)

  deepEqual(output, [
    { key: 'item', value: 1 },
    { key: 'item', value: 2 },
    { key: 'item', value: 3 },
    { key: 'item', value: 4 },
    { key: 'item', value: 5 },
    { key: 'item', value: 6 }
  ])
})
