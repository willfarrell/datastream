/* global fetch */

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

export const setDefaults = (options) => {
  const headers = { ...defaults.headers, ...options.headers }
  Object.assign(defaults, options, { headers })
}

export async function * fetchStream (optionsArr, { signal } = {}) {
  if (!Array.isArray(optionsArr)) optionsArr = [optionsArr]
  const requests = []
  for (let options of optionsArr) {
    options.headers = { ...defaults.headers, ...options.headers }
    options = { ...defaults, ...options }
    if (options.qs) options.url += '?' + new URLSearchParams(options.qs)
    if (options.headers.Accept.includes('application/json')) {
      requests.push(fetchJson(options, { signal }))
    } else {
      requests.push(fetchBody(options, { signal }))
    }
  }
  for (const request of requests) {
    const response = await request
    for await (const chunk of response) {
      yield chunk
    }
  }
}

const fetchBody = async (options, { signal }) => {
  const response = await fetchRateLimit(options, { signal })
  return response.body
}

const nextLinkRegExp = /<(.*?)>; rel="next"/
async function * fetchJson (options, { signal }) {
  const { dataPath, nextPath } = options

  while (options.url) {
    const response = await fetchRateLimit(options, { signal })
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

const fetchRateLimit = async (options, { signal }) => {
  options.rateLimitTimestamp ??= 0
  const now = Date.now()
  if (now < options.rateLimitTimestamp) {
    await timeout(options.rateLimitTimestamp - now, { signal })
  }
  options.rateLimitTimestamp = now + 1000 * options.rateLimit
  return fetch(options, { signal })
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

// Polyfill for `import { setTimeout } from 'node:timers/promises'` that works in all env
const timeout = (ms, { signal }) => {
  if (signal?.aborted) {
    return Promise.reject(new Error('Aborted', 'AbortError'))
  }
  return new Promise((resolve, reject) => {
    const abortHandler = () => {
      clearTimeout(timeout)
      reject(new Error('Aborted', 'AbortError'))
    }
    if (signal) signal.addEventListener('abort', abortHandler)
    const timeout = setTimeout(() => {
      resolve()
      if (signal) signal.removeEventListener('abort', abortHandler)
    }, ms)
  })
}

export default fetchStream
