/* global fetch */
import { setTimeout } from 'node:timers/promises'

const defaultConfig = {
  // custom
  rateLimit: 0.1, // per sec
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

export const setDefaultConfig = (config) => {
  const headers = { ...defaultConfig.headers, ...config.headers }
  Object.assign(defaultConfig, config, { headers })
}

export async function * fetchStream (configs) {
  if (!Array.isArray(configs)) configs = [configs]
  const requests = []
  for (let config of configs) {
    config = { ...defaultConfig, ...config }
    config.headers = { ...defaultConfig.headers, ...config.headers }
    if (config.qs) config.url += new URLSearchParams(config.qs)
    if (config.headers.Accept.includes('application/json')) {
      requests.push(fetchJson(config))
    } else {
      requests.push(fetchBody(config))
    }
  }
  for (const request of requests) {
    const response = await request
    for await (const chunk of response) {
      yield chunk
    }
  }
}

const fetchBody = async (config) => {
  const response = await fetchRateLimit(config)
  return response.body
}

async function * fetchJson (config) {
  const { dataPath, nextPath } = config

  while (config.url) {
    const response = await fetchRateLimit(config)
    const body = await response.json()
    config.url = nextPath && pickPath(body, nextPath)
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

const fetchRateLimit = async (config) => {
  config.rateLimitTimestamp ??= 0
  const now = Date.now()
  if (now < config.rateLimitTimestamp) {
    await setTimeout(config.rateLimitTimestamp - now)
  }
  config.rateLimitTimestamp = now + 1000 / config.rateLimit
  return fetch(config)
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

export default fetchStream
