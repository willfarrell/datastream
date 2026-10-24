---
title: fetch
description: HTTP client streams with pagination, rate limiting, and retries for 429, 5xx and network errors.
---

HTTP client streams with automatic pagination and rate limiting. Requests are retried with backoff on `429`, and on `500`/`502`/`503`/`504` or a network error when the method is idempotent.

## Install

```bash
npm install @datastream/fetch
```

## `fetchSetDefaults`

Set global defaults for all fetch streams. Mutates module-level state — not safe for concurrent multi-tenant use.

### Defaults

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `method` | `string` | `"GET"` | HTTP method |
| `headers` | `object` | `{ Accept: 'application/json', 'Accept-Encoding': 'br, gzip, deflate' }` | Request headers |
| `rateLimit` | `number` | `0.01` | Minimum seconds between requests (0.01 = 100/sec) |
| `dataPath` | `string` | — | Dot-path to data array in JSON response body |
| `nextPath` | `string` | — | Dot-path to next page URL in JSON response body |
| `qs` | `object` | `{}` | Default query string parameters |
| `offsetParam` | `string` | — | Query parameter name for offset pagination |
| `offsetAmount` | `number` | — | Increment per page for offset pagination |
| `concurrency` | `number` | `1` | Array items fetched at once |
| `maxPages` | `number \| null` | `10000` | Maximum JSON pages fetched per request config (`null` = unlimited) |
| `maxBodySize` | `number \| null` | `16777216` | Maximum bytes of a JSON response body (`null` = unlimited) |
| `retryMaxCount` | `number \| null` | `10` | Maximum attempts per request when retrying (`null` = unlimited) |
| `retryAfterMax` | `number \| null` | `60000` | Upper bound in ms for a `Retry-After` wait (`null` = no cap) |
| `timeout` | `number \| null` | — | Per-attempt timeout in ms (`null` or unset = none) |

An option left `undefined` on a request uses its default. Only `null` lifts a limit. Going over a limit throws a `RangeError`.

### Example

```javascript
import { fetchSetDefaults } from '@datastream/fetch'

fetchSetDefaults({
  headers: { Authorization: 'Bearer token123' },
  rateLimit: 0.1, // 10 requests/sec
})
```

## `fetchReadableStream` <span class="badge">Readable</span>

Fetches data from one or more URLs and emits chunks. Automatically detects JSON responses and handles pagination.

Also exported as `fetchResponseStream`.

### Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `url` | `string` | — | Request URL |
| `method` | `string` | `"GET"` | HTTP method |
| `headers` | `object` | — | Request headers (merged with defaults) |
| `rateLimit` | `number` | `0.01` | Seconds between requests |
| `dataPath` | `string` | — | Dot-path to data in JSON body |
| `nextPath` | `string` | — | Dot-path to next URL in JSON body |
| `qs` | `object` | `{}` | Query string parameters (appended to any query already on `url`) |
| `offsetParam` | `string` | — | Query param for offset-based pagination |
| `offsetAmount` | `number` | — | Offset increment per page |
| `concurrency` | `number` | `1` | Array items fetched at once (see below) |
| `maxPages` | `number \| null` | `10000` | Maximum JSON pages per request config; exceeding it throws a `RangeError`; `null` = unlimited |
| `maxBodySize` | `number \| null` | `16777216` | Maximum bytes of a JSON response body; exceeding it throws a `RangeError`; `null` = unlimited |
| `timeout` | `number \| null` | — | Per-attempt timeout in ms; see [Timeout](#timeout) |

`url` must be an absolute `http:` or `https:` URL. In a browser a relative URL resolves against the page first, as `fetch()` does. Any other scheme (`file:`, `data:`, `blob:`, `javascript:`, …), a relative URL in Node, or an unparseable one throws a `TypeError` (`fetch <method> <url> is not an http: or https: URL`) before any request is made. Pagination and redirect targets can't switch scheme either: they must stay on the first request's origin, and an origin includes the scheme.

Pass an array of option objects to fetch from multiple URLs. By default they run one at a time; set `concurrency` (see below) to fetch in parallel.

### Concurrency

Set the `concurrency` option to fetch array items in parallel. It is read from the first item of the array (or from `fetchSetDefaults`) and applies to the whole array. Results are still yielded in array order, and request starts remain spaced by `rateLimit`. Defaults to `1` (sequential). If an item fails or the consumer stops reading, requests still in flight are cancelled.

```javascript
fetchReadableStream(
  ['a', 'b', 'c'].map((name) => ({
    url: `https://api.example.com/${name}`,
    dataPath: 'data',
    concurrency: 3,
  })),
)
```

### Pagination strategies

**Link header** — the `rel="next"` link is automatically followed when present. Relative targets resolve against the current page URL. The `rel` parameter name and values are case-insensitive, and only the first `rel` of each link counts:
```
Link: <https://api.example.com/users?page=1>; rel="prev", </users?page=3>; rel="next"
```

**Body path** — use `nextPath` to extract the next URL (absolute or relative) from the JSON response:
```javascript
fetchReadableStream({
  url: 'https://api.example.com/users',
  dataPath: 'data',
  nextPath: 'pagination.next_url',
})
```

**Offset** — use `offsetParam` and `offsetAmount` for numeric pagination:
```javascript
fetchReadableStream({
  url: 'https://api.example.com/users',
  dataPath: 'results',
  offsetParam: 'offset',
  offsetAmount: 100,
})
```

Pagination stays on the origin of the first request. It stops with an error when a next URL is the same as the current page (a page linking to itself) or when more than `maxPages` pages would be fetched (a `RangeError`).

JSON responses are read in full before items are emitted, so each body is capped at `maxBodySize` bytes (a larger body throws a `RangeError`). A larger `Content-Length` is rejected before reading, and bytes are also counted while reading, since `Content-Length` can be missing.

### Redirects

Redirects are followed only within the origin of the original request. More than 20 redirects throw a `RangeError`. A redirect to another origin throws, so a server can't send the request to internal addresses. In browsers, a redirect's target is hidden from the page. The request is sent again with `redirect: "follow"` and the final URL is checked afterwards. That second request is only made for `GET` and `HEAD`. Any other method throws instead, because re-sending would submit the body twice. Set `redirect` yourself (`"follow"` or `"error"`) to turn this checking off.

### Retries

A request is retried with backoff when:

- the response is `429 Too Many Requests`, for any method (the server refused the request without processing it);
- the response is `500`, `502`, `503` or `504`, and the method is `GET`, `HEAD`, `OPTIONS`, `PUT` or `DELETE`;
- `fetch` rejects with a `TypeError` (a network error: DNS failure, connection reset, offline), and the method is one of those five.

`POST`, `PATCH` and other methods aren't retried on a 5xx or a network error, because sending them twice could apply the change twice. Their error is thrown at once. Other statuses (`4xx` except `429`, `501`, `505`, …) are never retried. Neither is an abort or a [timeout](#timeout): once the caller's `signal` has aborted, its reason is thrown as is, even when that reason is a `TypeError`. A request with a streamed body is never retried on a 5xx or a network error, and throws `cannot replay streamed request body` on a `429` (see `fetchWritableStream` below).

If a `Retry-After` header is present (on a `429` or a 5xx), its value (seconds, including `0`, or an HTTP-date) is used as the delay. Otherwise, the delay is a random value up to `min(1000 × 2^(attempt-1), 30000)` ms. A `Retry-After` delay is capped at `retryAfterMax` ms (default 60000). Without the cap, a very large value would stall the stream. With `retryAfterMax: null` the delay is still capped at 2^31-1 ms, because anything above that makes `setTimeout` fire after 1 ms, so there would be no wait at all.

Attempts are capped at `retryMaxCount` (default: 10; `null` = unlimited), counted per request. The count resets after each successful response, so every page gets the full budget. Running out on a `429` or 5xx throws a `RangeError` (`fetch <status> <method> <url> max retries (<n>) exceeded`). Running out on network errors throws the last `TypeError` from `fetch`.

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `retryMaxCount` | `number \| null` | `10` | Maximum attempts per request; `null` = unlimited |
| `retryAfterMax` | `number \| null` | `60000` | Upper bound in ms for a `Retry-After` wait; `null` = no cap (still at most 2^31-1) |

### Timeout

Set `timeout` (ms) to abort a request that takes too long. It is combined with `streamOptions.signal`, so either one cancels the request. Each attempt gets its own timeout, so a retry doesn't start out already expired. The timeout also covers reading the response body, so for a large download set it to the longest the whole transfer may take. When it fires, the request rejects with a `TimeoutError` `DOMException`, which isn't retried. Unset or `null` (the default) means no timeout.

```javascript
fetchReadableStream({ url: 'https://api.example.com/users', timeout: 30_000 })
```

### Example

```javascript
import { pipeline } from '@datastream/core'
import { fetchReadableStream } from '@datastream/fetch'
import { objectCountStream } from '@datastream/object'

const count = objectCountStream()

const result = await pipeline([
  fetchReadableStream({
    url: 'https://api.example.com/users',
    dataPath: 'data',
    nextPath: 'meta.next',
    headers: { Authorization: 'Bearer token' },
  }),
  count,
])

console.log(result)
// { objectCount: 450 }
```

### Multiple URLs

```javascript
fetchReadableStream([
  { url: 'https://api.example.com/users?status=active', dataPath: 'data' },
  { url: 'https://api.example.com/users?status=inactive', dataPath: 'data' },
])
```

## `fetchWritableStream` <span class="badge">Writable, async</span>

Streams data as the body of an HTTP request. Uses `duplex: "half"` for browser compatibility.

The request starts immediately and the stream is returned without waiting for the response, so servers that read the whole body before replying work. The response is awaited when the stream ends: a failed response rejects the pipeline, and a successful one is available as `result()` (`{ key: resultKey, value: Response }`). If the request fails before the body is finished (for example an immediate `401`), the next write throws that error.

Writes follow the server's pace. A write waits while the request body already holds `streamOptions.highWaterMark` chunks (default `1`) and continues when fetch reads the next one, so a slow server slows the upload down instead of failing it. Once the response arrives, writes stop waiting. If fetch stops reading the body early, the remaining chunks are dropped.

A streamed body can only be sent once. A `429` or a `307`/`308` redirect therefore throws `fetch <status> <method> <url> cannot replay streamed request body` instead of retrying. A 5xx or a network error throws its own error at once, even for `PUT`. This also applies to any async iterable or `ReadableStream` body passed to `fetchReadableStream`.

If the writable is torn down before it ends, the request is cancelled and its body is ended with an error, so the server never sees a complete upload. That covers `destroy()` (Node), `abort()` (browser), an upstream error in the pipeline, and `streamOptions.signal` firing.

Also exported as `fetchRequestStream`.

### Options

Takes the same request options as `fetchReadableStream`, plus:

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `resultKey` | `string` | `"output"` | Key of the response in the pipeline result |

### Example

```javascript
import { pipeline, createReadableStream } from '@datastream/core'
import { fetchWritableStream } from '@datastream/fetch'
import {
  csvObjectToArrayStream,
  csvInjectHeaderStream,
  csvFormatStream,
} from '@datastream/csv'

const headers = ['id', 'name']

await pipeline([
  createReadableStream(data), // [{ id: 1, name: 'a' }, ...]
  // csvFormatStream formats array rows: convert objects and add the header row
  csvObjectToArrayStream({ headers }),
  csvInjectHeaderStream({ header: headers }),
  csvFormatStream(),
  await fetchWritableStream({
    url: 'https://api.example.com/upload',
    method: 'PUT',
    headers: { 'Content-Type': 'text/csv' },
  }),
])
```

## `fetchRateLimit` <span class="badge">async</span>

The single request function the streams above use, exported for one-off calls. It applies the defaults, the `http:`/`https:` check, `rateLimit` spacing, same-origin redirect checks, [retries](#retries) and [`timeout`](#timeout), and resolves with the `Response` (body not read). A response that is still not ok after retries rejects with `fetch <status> <method> <url>` (the URL with its query and credentials redacted, and `{ status, url, method }` in `error.cause`).

`fetchRateLimit(options, streamOptions)` takes the same request options as `fetchReadableStream` (pagination options are ignored) and `streamOptions.signal` to cancel. It writes `rateLimitTimestamp` back onto `options`: pass the same object again to keep the next call `rateLimit` seconds after this one.

```javascript
import { fetchRateLimit } from '@datastream/fetch'

const options = { url: 'https://api.example.com/health', rateLimit: 1 }
const response = await fetchRateLimit(options)
console.log(response.status, await response.json())
await fetchRateLimit(options) // waits until 1 s after the first request
```
