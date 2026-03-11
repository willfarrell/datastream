---
title: core
description: Pipeline orchestration, stream factories, and utility functions for datastream.
---

Foundation package providing pipeline orchestration, stream factories, and utility functions.

## Install

```bash
npm install @datastream/core
```

## Pipeline

### `pipeline(streams, streamOptions)` <span class="badge">async</span>

Connects all streams, waits for completion, and returns combined `.result()` values from all PassThrough streams. Automatically appends a no-op Writable if the last stream is Readable.

#### Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `highWaterMark` | `number` | — | Backpressure threshold |
| `chunkSize` | `number` | — | Size hint for chunking |
| `signal` | `AbortSignal` | — | Abort the pipeline |

#### Example

```javascript
import { pipeline, createReadableStream } from '@datastream/core'
import { objectCountStream } from '@datastream/object'

const count = objectCountStream()

const result = await pipeline([
  createReadableStream([{ a: 1 }, { a: 2 }, { a: 3 }]),
  count,
])

console.log(result)
// { count: 3 }
```

### `pipejoin(streams)` <span class="badge">returns stream</span>

Connects streams and returns the resulting stream. Use this when you want to consume output manually with `streamToArray`, `streamToString`, or `for await`.

#### Example

```javascript
import { pipejoin, streamToArray, createReadableStream, createTransformStream } from '@datastream/core'

const river = pipejoin([
  createReadableStream([1, 2, 3]),
  createTransformStream((n, enqueue) => enqueue(n * 2)),
])

const output = await streamToArray(river)
// [2, 4, 6]
```

### `result(streams)` <span class="badge">async</span>

Iterates over streams and combines all `.result()` return values into a single object. Called automatically by `pipeline()`.

## Consumers

### `streamToArray(stream)` <span class="badge">async</span>

Collects all chunks from a stream into an array.

```javascript
import { pipejoin, streamToArray, createReadableStream } from '@datastream/core'

const river = pipejoin([createReadableStream(['a', 'b', 'c'])])
const output = await streamToArray(river)
// ['a', 'b', 'c']
```

### `streamToString(stream)` <span class="badge">async</span>

Concatenates all chunks into a single string.

```javascript
const output = await streamToString(river)
// 'abc'
```

### `streamToObject(stream)` <span class="badge">async</span>

Merges all chunks into a single object using `Object.assign`.

```javascript
const river = pipejoin([createReadableStream([{ a: 1 }, { b: 2 }])])
const output = await streamToObject(river)
// { a: 1, b: 2 }
```

### `streamToBuffer(stream)` <span class="badge">async</span> <span class="badge">Node.js only</span>

Collects all chunks into a `Buffer`.

## Stream Factories

### `createReadableStream(input, streamOptions)` <span class="badge">Readable</span>

Creates a Readable stream from various input types.

#### Input types

| Type | Behavior |
|------|----------|
| `string` | Chunked at `chunkSize` (default 16KB) |
| `Array` | Each element emitted as a chunk |
| `AsyncIterable` / `Iterable` | Each yielded value emitted as a chunk |
| `ArrayBuffer` | Chunked at `chunkSize` (Node.js only) |

#### Example

```javascript
import { createReadableStream } from '@datastream/core'

// From string — auto-chunked
const stream = createReadableStream('hello world')

// From array — one chunk per element
const stream = createReadableStream([{ a: 1 }, { a: 2 }])

// From async generator
async function* generate() {
  yield 'chunk1'
  yield 'chunk2'
}
const stream = createReadableStream(generate())
```

### `createPassThroughStream(fn, flush?, streamOptions)` <span class="badge">Transform (PassThrough)</span>

Creates a stream that observes each chunk without modifying it. The chunk is automatically passed through.

#### Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| `fn` | `(chunk) => void` | Called for each chunk, return value ignored |
| `flush` | `() => void` | Optional, called when stream ends |
| `streamOptions` | `object` | Stream configuration |

#### Example

```javascript
import { createPassThroughStream } from '@datastream/core'

let total = 0
const counter = createPassThroughStream((chunk) => {
  total += chunk.length
})
counter.result = () => ({ key: 'total', value: total })
```

### `createTransformStream(fn, flush?, streamOptions)` <span class="badge">Transform</span>

Creates a stream that modifies chunks. Use `enqueue` to emit output — you can emit zero, one, or many chunks per input.

#### Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| `fn` | `(chunk, enqueue) => void` | Transform each chunk, call `enqueue(output)` to emit |
| `flush` | `(enqueue) => void` | Optional, emit final chunks when stream ends |
| `streamOptions` | `object` | Stream configuration |

#### Example

```javascript
import { createTransformStream } from '@datastream/core'

// Filter: emit only matching chunks
const filter = createTransformStream((chunk, enqueue) => {
  if (chunk.age > 18) enqueue(chunk)
})

// Expand: emit multiple chunks per input
const expand = createTransformStream((chunk, enqueue) => {
  for (const item of chunk.items) {
    enqueue(item)
  }
})
```

### `createWritableStream(fn, close?, streamOptions)` <span class="badge">Writable</span>

Creates a stream that consumes chunks at the end of a pipeline.

#### Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| `fn` | `(chunk) => void` | Called for each chunk |
| `close` | `() => void` | Optional, called when stream ends |
| `streamOptions` | `object` | Stream configuration |

#### Example

```javascript
import { createWritableStream } from '@datastream/core'

const rows = []
const collector = createWritableStream((chunk) => {
  rows.push(chunk)
})
```

## Utilities

### `isReadable(stream)`

Returns `true` if the stream is Readable.

### `isWritable(stream)`

Returns `true` if the stream is Writable.

### `makeOptions(options)`

Normalizes stream options for interoperability between Readable, Transform, and Writable streams.

| Option | Type | Description |
|--------|------|-------------|
| `highWaterMark` | `number` | Backpressure threshold |
| `chunkSize` | `number` | Chunking size hint |
| `signal` | `AbortSignal` | Abort signal |

### `timeout(ms, options)` <span class="badge">async</span>

Returns a promise that resolves after `ms` milliseconds. Supports `AbortSignal` cancellation.

```javascript
import { timeout } from '@datastream/core'

await timeout(1000) // wait 1 second

const controller = new AbortController()
await timeout(5000, { signal: controller.signal }) // cancellable
```

### `backpressureGuage(streams)` <span class="badge">Node.js only</span>

Measures pause/resume timing across streams. Useful for identifying bottlenecks.

```javascript
import { backpressureGuage } from '@datastream/core'

const metrics = backpressureGuage({ parse: parseStream, validate: validateStream })
// After pipeline completes:
// metrics.parse.total = { timestamp, duration }
// metrics.parse.timeline = [{ timestamp, duration }, ...]
```
