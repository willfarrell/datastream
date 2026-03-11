---
title: fetch
description: HTTP client streams with automatic pagination, rate limiting, and retry.
---

HTTP client streams with automatic pagination, rate limiting, and 429 retry.

## Install

```bash
npm install @datastream/fetch
```

## `fetchSetDefaults`

Set global defaults for all fetch streams.

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
| `qs` | `object` | `{}` | Query string parameters |
| `offsetParam` | `string` | — | Query param for offset-based pagination |
| `offsetAmount` | `number` | — | Offset increment per page |

Pass an array of option objects to fetch from multiple URLs in sequence.

### Pagination strategies

**Link header** — automatically followed when present:
```
Link: <https://api.example.com/users?page=2>; rel="next"
```

**Body path** — use `nextPath` to extract the next URL from the JSON response:
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

### 429 retry

When receiving a `429 Too Many Requests` response, the request is automatically retried after respecting the rate limit delay.

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
// { count: 450 }
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

Also exported as `fetchRequestStream`.

### Example

```javascript
import { pipeline, createReadableStream } from '@datastream/core'
import { fetchWritableStream } from '@datastream/fetch'
import { csvFormatStream } from '@datastream/csv'

await pipeline([
  createReadableStream(data),
  csvFormatStream({ header: true }),
  await fetchWritableStream({
    url: 'https://api.example.com/upload',
    method: 'PUT',
    headers: { 'Content-Type': 'text/csv' },
  }),
])
```
