/* global fetch */
import {
  createReadableStream,
  createWritableStream,
  timeout
} from '@datastream/core'

const defaults = {
  // custom
  rateLimit: 0.01, // 100 per sec
  rateLimitTimestamp: 0,
  dataPath: undefined, // for json response, where the data is to return form body root
  nextPath: undefined, // for json pagination, body root
  qs: undefined, // object to convert to query string

  // fetch
  method: 'GET',
  headers: {
    Accept: 'application/json',
    'Accept-Encoding': 'br, gzip, deflate'
  }
}

const mergeOptions = (options = {}) => {
  return {
    ...defaults,
    ...options,
    headers: { ...defaults.headers, ...options.headers }
  }
}

export const fetchSetDefaults = (options) => {
  Object.assign(defaults, mergeOptions(options))
}

// Note: requires EncodeStream to ensure it's Uint8Array
// Poor browser support - https://github.com/Fyrd/caniuse/issues/6375
// TODO needs testing
export const fetchWritableStream = async (options, streamOptions = {}) => {
  const body = createReadableStream()
  // Duplex: half - For browser compatibility - https://developer.chrome.com/articles/fetch-streaming-requests/#half-duplex
  const value = await fetchRateLimit({
    ...options,
    body,
    duplex: 'half',
    signal: streamOptions.signal
  })
  const write = (chunk) => {
    body.push(chunk)
  }
  const stream = createWritableStream(write, streamOptions)
  stream.result = () => ({ key: 'output', value })
  return stream
}
export const fetchRequestStream = fetchWritableStream

export const fetchReadableStream = (fetchOptions, streamOptions = {}) => {
  if (!Array.isArray(fetchOptions)) fetchOptions = [fetchOptions]
  return createReadableStream(
    fetchGenerator(fetchOptions, streamOptions),
    streamOptions
  )
}
export const fetchResponseStream = fetchReadableStream

async function * fetchGenerator (fetchOptionsArray, streamOptions) {
  const requests = []
  for (const options of fetchOptionsArray) {
    if (options.qs) {
      options.url += ('?' + new URLSearchParams(options.qs)).replaceAll(
        '+',
        '%20'
      )
    }
    requests.push(fetchUnknown(options, streamOptions))
  }
  for (const request of requests) {
    const response = await request
    for await (const chunk of response) {
      yield chunk
    }
  }
}

const jsonContentTypeRegExp = /^application\/(.+\+)?json($|;.+)/
const fetchUnknown = async (options, streamOptions) => {
  const response = await fetchRateLimit(options, streamOptions)
  if (jsonContentTypeRegExp.test(response.headers.get('Content-Type'))) {
    options.prefetchResponse = response // hack
    return fetchJson(options, streamOptions)
  }
  return response.body
}

const nextLinkRegExp = /<(.*?)>; rel="next"/
async function * fetchJson (options, streamOptions) {
  const { dataPath, nextPath } = options

  while (options.url) {
    const response =
      options.prefetchResponse ??
      (await fetchRateLimit(options, streamOptions))
    delete options.prefetchResponse
    const body = await response.json() // Blocking - stream parse?
    options.url = nextPath && pickPath(body, nextPath)
    options.url ??= parseLink(response.headers)
    const data = pickPath(body, dataPath)
    if (Array.isArray(data)) {
      for (const item of data) {
        yield item
      }
    } else {
      yield data
    }
  }
}

const parseLink = (headers) => {
  const link = headers.get('Link')
  if (link) {
    return link.match(nextLinkRegExp)?.[1]
  }
}

export const fetchRateLimit = async (options, { signal }) => {
  options.rateLimitTimestamp ??= 0
  const now = Date.now()
  if (now < options.rateLimitTimestamp) {
    await timeout(options.rateLimitTimestamp - now, { signal })
  }
  options.rateLimitTimestamp = now + 1000 * options.rateLimit

  options = mergeOptions(options)

  const response = await fetch(options.url, { ...options, signal })
  if (!response.ok) {
    // 429 Too Many Requests
    if (response.statusCode === 429) {
      return fetchRateLimit(options, { signal })
    }
    throw new Error('fetch', {
      cause: { request: options, response }
    })
  }
  return response
}

const pickPath = (obj, path = '') => {
  if (path === '') return obj
  if (!Array.isArray(path)) path = path.split('.')
  return path
    .slice(0) // clone
    .reduce((a, b) => {
      return a[b]
    }, obj)
}

export default {
  setDefaults: fetchSetDefaults,
  readableStream: fetchReadableStream,
  responseStream: fetchReadableStream
  // writableStream: fetchRequestStream,
}
