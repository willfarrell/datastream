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

export const fetchSetDefaults = (options) => {
  const headers = { ...defaults.headers, ...options.headers }
  Object.assign(defaults, options, { headers })
}

// Note: requires EncodeStream to ensure it's Uint8Array
// Poor browser support - https://github.com/Fyrd/caniuse/issues/6375
export const fetchRequestStream = async (options, streamOptions = {}) => {
  const body = createReadableStream()
  // Duplex: half - For browser compatibility - https://developer.chrome.com/articles/fetch-streaming-requests/#half-duplex
  const value = await fetch(options.url, {
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
}

export const fetchResponseStream = (fetchOptions, streamOptions = {}) => {
  if (!Array.isArray(fetchOptions)) fetchOptions = [fetchOptions]
  return createReadableStream(
    fetchGenerator(fetchOptions, streamOptions),
    streamOptions
  )
}
async function * fetchGenerator (fetchOptionsArray, streamOptions) {
  const requests = []
  for (let options of fetchOptionsArray) {
    options.headers = { ...defaults.headers, ...options.headers }
    options = { ...defaults, ...options }
    if (options.qs) {
      options.url += ('?' + new URLSearchParams(options.qs)).replaceAll(
        '+',
        '%20'
      )
    }
    if (
      typeof options.dataPath !== 'undefined' &&
      /application\/([a-z.]+\+|)json/.test(options.headers.Accept)
    ) {
      requests.push(fetchJson(options, streamOptions))
    } else {
      requests.push(fetchBody(options, streamOptions))
    }
  }
  for (const request of requests) {
    const response = await request
    for await (const chunk of response) {
      yield chunk
    }
  }
}

const fetchBody = async (options, streamOptions) => {
  const response = await fetchRateLimit(options, streamOptions)
  return response.body
}

const nextLinkRegExp = /<(.*?)>; rel="next"/
async function * fetchJson (options, streamOptions) {
  const { dataPath, nextPath } = options

  while (options.url) {
    const response = await fetchRateLimit(options, streamOptions)
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

const fetchRateLimit = async (options, { signal }) => {
  options.rateLimitTimestamp ??= 0
  const now = Date.now()
  if (now < options.rateLimitTimestamp) {
    await timeout(options.rateLimitTimestamp - now, { signal })
  }
  options.rateLimitTimestamp = now + 1000 * options.rateLimit
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

const pickPath = (obj, path) => {
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
  responseStream: fetchResponseStream
  // writableStream: fetchWritableStream,
}
