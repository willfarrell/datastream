/* global fetch */
import {
  createReadableStream,
  createWritableStream,
  timeout
} from '@datastream/core'

const defaults = {
  // custom
  rateLimit: 0.01, // 100 per sec
  dataPath: undefined, // for json response, where the data is to return form body root
  nextPath: undefined, // for json pagination, body root
  qs: {}, // object to convert to query string

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
    headers: { ...defaults.headers, ...options.headers },
    qs: { ...defaults.qs, ...options.qs }
  }
}

export const fetchSetDefaults = (options) => {
  Object.assign(defaults, mergeOptions(options))
}

// Note: requires EncodeStream to ensure it's Uint8Array
// Poor browser support - https://github.com/Fyrd/caniuse/issues/6375
// TODO needs testing
// TODO mulit-part upload
export const fetchWritableStream = async (options, streamOptions = {}) => {
  const body = createReadableStream()
  // Duplex: half - For browser compatibility - https://developer.chrome.com/articles/fetch-streaming-requests/#half-duplex
  options = mergeOptions(options)
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

export const fetchReadableStream = (fetchOptions, streamOptions) => {
  if (!Array.isArray(fetchOptions)) fetchOptions = [fetchOptions]
  return createReadableStream(
    fetchGenerator(fetchOptions, streamOptions),
    streamOptions
  )
}
export const fetchResponseStream = fetchReadableStream

async function * fetchGenerator (fetchOptionsArray, streamOptions) {
  let rateLimitTimestamp = 0
  for (let options of fetchOptionsArray) {
    options = mergeOptions(options)
    options.rateLimitTimestamp ??= rateLimitTimestamp
    if (Object.keys(options.qs).length) {
      options.url += ('?' + new URLSearchParams(options.qs)).replaceAll(
        '+',
        '%20'
      )
    }
    const response = await fetchUnknown(options, streamOptions)
    for await (const chunk of response) {
      yield chunk
    }
    // ensure there is rate limiting between req with different options
    rateLimitTimestamp = options.rateLimitTimestamp
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
    const body = await response.json()
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

export const fetchRateLimit = async (options, streamOptions = {}) => {
  const now = Date.now()
  if (now < options.rateLimitTimestamp ?? 0) {
    await timeout(options.rateLimitTimestamp - now, streamOptions)
  }
  options.rateLimitTimestamp = Date.now() + 1000 * options.rateLimit
  options = mergeOptions(options) // for when called directly

  const response = await fetch(options.url, {
    ...options,
    signal: streamOptions.signal
  })
  if (!response.ok) {
    // 429 Too Many Requests
    if (response.statusCode === 429) {
      return fetchRateLimit(options, streamOptions)
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
