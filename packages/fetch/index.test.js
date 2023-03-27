/* global Headers, Response */
import test from 'node:test'
import { deepEqual } from 'node:assert'
// import sinon from 'sinon'
import {
  pipejoin,
  pipeline,
  streamToArray,
  createPassThroughStream
} from '@datastream/core'

import { fetchResponseStream, fetchSetDefaults } from '@datastream/fetch'

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
  'https://example.org/csv?delimiter=_': () =>
    new Response('a_b_c\n1_2_3', {
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
        'Content-Type': 'application/json; charset=UTF-8',
        Link: '<https://example.org/json-obj/3>; rel="next"'
      })
    }),
  'https://example.org/json-obj/3': () =>
    new Response(JSON.stringify({ key: 'item', value: 3 }), {
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
  [`https://example.org/json-arr?${new URLSearchParams({ '$limit': 3, '$offset': 0 })}`]: () =>
    new Response(
      JSON.stringify({
        data: [
          { key: 'item', value: 1 },
          { key: 'item', value: 2 },
          { key: 'item', value: 3 }
        ]
      }),
      {
        status: 200,
        statusText: 'OK',
        headers: new Headers({
          'Content-Type': 'application/json; charset=UTF-8'
        })
      }
    ),
  [`https://example.org/json-arr?${new URLSearchParams({ '$limit': 3, '$offset': 3 })}`]: () =>
    new Response(
      JSON.stringify({
        data: [
          { key: 'item', value: 4 },
          { key: 'item', value: 5 }
        ]
      }),
      {
        status: 200,
        statusText: 'OK',
        headers: new Headers({
          'Content-Type': 'application/json; charset=UTF-8'
        })
      }
    ),
  [`https://example.org/json-arr?${new URLSearchParams({ '$limit': 3, '$offset': 6 })}`]: () =>
    new Response(
      JSON.stringify({ data: [] }),
      {
        status: 200,
        statusText: 'OK',
        headers: new Headers({
          'Content-Type': 'application/json; charset=UTF-8'
        })
      }
    ),
  'https://example.org/404': () =>
    new Response('', { status: 404, statusText: 'Not Found' }),
  'https://example.org/429': () =>
    new Response('', { status: 429, statusText: 'Too Many Requests' })
}
// global override
global.fetch = (url, request) => {
  const mockResponse = mockResponses[url]()
  if (mockResponse) {
    return Promise.resolve(mockResponse)
  }
  throw new Error('mock missing')
}

// *** fetchResponseStream *** //
test(`${variant}: fetchResponseStream should fetch csv`, async (t) => {
  fetchSetDefaults({ headers: { Accept: 'text/csv' } })
  const config = [{ url: 'https://example.org/csv' }]
  const stream = fetchResponseStream(config)
  const output = await streamToArray(stream)

  deepEqual(output, [
    Uint8Array.from('a,b,c\n1,2,3'.split('').map((x) => x.charCodeAt()))
  ])
})

test(`${variant}: fetchResponseStream should fetch with qs`, async (t) => {
  fetchSetDefaults({ headers: { Accept: 'text/csv' } })
  const config = [{ url: 'https://example.org/csv', qs: { delimiter: '_' } }]
  const stream = fetchResponseStream(config)
  const output = await streamToArray(stream)

  deepEqual(output, [
    Uint8Array.from('a_b_c\n1_2_3'.split('').map((x) => x.charCodeAt()))
  ])
})

test(`${variant}: fetchResponseStream should fetch json objects in parallel`, async (t) => {
  fetchSetDefaults({ dataPath: '', headers: { Accept: 'application/json' } })
  const config = [
    { url: 'https://example.org/json-obj/1' },
    { url: 'https://example.org/json-obj/2' }
  ]
  const stream = fetchResponseStream(config)
  const output = await streamToArray(stream)

  deepEqual(output, [
    { key: 'item', value: 1 },
    { key: 'item', value: 2 },
    { key: 'item', value: 3 }
  ])
})

test(`${variant}: fetchResponseStream should fetch paginated json in series`, async (t) => {
  fetchSetDefaults({ headers: { Accept: 'application/json' } })
  const config = {
    url: 'https://example.org/json-arr/1',
    dataPath: 'data',
    nextPath: 'next'
  }

  const stream = fetchResponseStream(config)
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

test(`${variant}: fetchResponseStream should work with pipejoin`, async (t) => {
  fetchSetDefaults({ headers: { Accept: 'application/json' } })
  const config = {
    url: 'https://example.org/json-arr/1',
    dataPath: 'data',
    nextPath: 'next'
  }

  const stream = pipejoin([fetchResponseStream(config)])
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

test(`${variant}: fetchResponseStream should work with pipeline`, async (t) => {
  fetchSetDefaults({ headers: { Accept: 'application/json' } })
  const config = {
    url: 'https://example.org/json-arr/1',
    dataPath: 'data',
    nextPath: 'next'
  }

  const result = await pipeline([
    fetchResponseStream(config),
    createPassThroughStream()
  ])

  deepEqual(result, {})
})

test(`${variant}: fetchResponseStream should paginate using query parameters`, async () => {
  fetchSetDefaults({ headers: { Accept: 'application/json' } })
  const config = {
    url: 'https://example.org/json-arr',
    qs: {
      $limit: 3
    },
    offsetParam: '$offset',
    offsetAmount: 3,
    dataPath: 'data'
  }

  const stream = pipejoin([fetchResponseStream(config)])
  const output = await streamToArray(stream)

  deepEqual(output, [
    { key: 'item', value: 1 },
    { key: 'item', value: 2 },
    { key: 'item', value: 3 },
    { key: 'item', value: 4 },
    { key: 'item', value: 5 },
  ])
})