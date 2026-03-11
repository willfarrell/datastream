---
title: Core Concepts
description: Learn about stream types, pipeline orchestration, and naming conventions in datastream.
---

## Stream types

Datastream uses four stream types. Each package export follows a naming convention that tells you the type:

| Type | Naming pattern | Purpose |
|------|---------------|---------|
| **Readable** | `*ReadableStream`, `*ReadStream` | Injects data into a pipeline — files, databases, APIs |
| **PassThrough** | `*CountStream`, `*LengthStream`, `*DetectStream` | Observes data without modifying it, collects metrics via `.result()` |
| **Transform** | `*ParseStream`, `*FormatStream`, `*CompressStream` | Modifies data as it passes through |
| **Writable** | `*WriteStream`, `*PutItemStream` | Consumes data at the end — files, databases, APIs |

## `pipeline()` vs `pipejoin()`

### `pipeline(streams[], streamOptions)`

Connects all streams, waits for completion, and returns combined results from all PassThrough streams:

```javascript
import { pipeline, createReadableStream } from '@datastream/core'
import { objectCountStream } from '@datastream/object'
import { stringLengthStream } from '@datastream/string'

const count = objectCountStream()
const length = stringLengthStream()

const result = await pipeline([
  createReadableStream(['hello', 'world']),
  count,
  length,
])

console.log(result)
// { count: 2, length: 10 }
```

If the last stream is Readable or Transform (no Writable at the end), `pipeline` automatically appends a no-op Writable so the pipeline completes.

### `pipejoin(streams[])`

Connects streams and returns the resulting stream — use this when you want to consume the output manually:

```javascript
import { pipejoin, streamToArray, createReadableStream, createTransformStream } from '@datastream/core'

const streams = [
  createReadableStream([1, 2, 3]),
  createTransformStream((n, enqueue) => enqueue(n * 2)),
]

const river = pipejoin(streams)
const output = await streamToArray(river)
// [2, 4, 6]
```

## The `.result()` pattern

PassThrough streams collect metrics without modifying data. After the pipeline completes, retrieve results:

```javascript
import { pipeline, createReadableStream } from '@datastream/core'
import { csvParseStream } from '@datastream/csv'
import { objectCountStream } from '@datastream/object'
import { digestStream } from '@datastream/digest'

const count = objectCountStream()
const digest = await digestStream({ algorithm: 'SHA2-256' })

const result = await pipeline([
  createReadableStream(data),
  digest,
  csvParseStream(),
  count,
])

console.log(result)
// { digest: 'SHA2-256:abc123...', count: 1000 }
```

Each PassThrough stream returns `{ key, value }` from its `.result()` method. `pipeline()` combines them into a single object. You can customize the key with the `resultKey` option:

```javascript
const count = objectCountStream({ resultKey: 'rowCount' })
// result: { rowCount: 1000 }
```

## Stream options

All stream factory functions accept a `streamOptions` parameter:

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `highWaterMark` | `number` | — | Backpressure threshold — how many chunks to buffer before pausing |
| `chunkSize` | `number` | `16384` | Size hint for chunking strategies |
| `signal` | `AbortSignal` | — | Signal to abort the pipeline |

```javascript
const controller = new AbortController()

await pipeline([
  createReadableStream(data),
  csvParseStream(),
], { signal: controller.signal })

// Abort from elsewhere:
controller.abort()
```

## Error handling

### AbortSignal

Use `AbortController` to cancel pipelines:

```javascript
const controller = new AbortController()
setTimeout(() => controller.abort(), 5000) // 5s timeout

try {
  await pipeline(streams, { signal: controller.signal })
} catch (e) {
  if (e.name === 'AbortError') {
    console.log('Pipeline was cancelled')
  }
}
```

### Validation errors

Streams like `validateStream` and CSV cleaning streams collect errors in `.result()` rather than throwing, so the pipeline continues processing:

```javascript
const result = await pipeline([
  createReadableStream(data),
  csvParseStream(),
  validateStream({ schema }),
])

console.log(result.validate)
// { '#/required/name': { id: '#/required/name', keys: ['name'], message: '...', idx: [3, 7] } }
```

Invalid rows are dropped by default. Set `onErrorEnqueue: true` to keep them in the stream.

## Lazy options

Many CSV streams accept functions instead of values for options. This lets you wire up detection results that aren't available until runtime:

```javascript
const detect = csvDetectDelimitersStream()

csvParseStream({
  delimiterChar: () => detect.result().value.delimiterChar,
})
```

The function is called when the stream first needs the value, not at creation time.
