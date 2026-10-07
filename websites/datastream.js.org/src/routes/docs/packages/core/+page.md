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
| `chunkSize` | `number` | — | Slice size when `createReadableStream` chunks a string or bytes |
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
// { objectCount: 3 }
```

### `pipejoin(streams)` <span class="badge">returns stream</span>

Connects streams and returns the resulting stream. Use this when you want to consume output manually with `streamToArray`, `streamToString`, or `for await`.

If any stream in the chain fails, every stream is destroyed and the error surfaces on the returned stream, in both builds. Consume it with a helper that rejects on error, or listen for its `error` event.

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

```javascript
// Errors from any stream in the chain reject the consumer
try {
  await streamToArray(pipejoin(streams))
} catch (error) {
  console.error('pipeline failed', error)
}
```

### `result(streams)` <span class="badge">async</span>

Iterates over streams and combines all `.result()` return values into a single object. Called automatically by `pipeline()`.

## Consumers

All consumers accept an optional second argument `{ maxBufferSize }`. It is unlimited by default and when set to `null`. When the collected size exceeds it, the consumer rejects with a `RangeError` (`… buffer exceeds maxBufferSize (…)`) instead of growing without bound. Size is counted in bytes for byte chunks, characters for string chunks, and 1 per chunk for anything else (objects, numbers).

```javascript
await streamToArray(stream, { maxBufferSize: 10_000 }) // at most 10,000 object rows
```

### `streamToArray(stream, options?)` <span class="badge">async</span>

Collects all chunks from a stream into an array.

```javascript
import { pipejoin, streamToArray, createReadableStream } from '@datastream/core'

const river = pipejoin([createReadableStream(['a', 'b', 'c'])])
const output = await streamToArray(river)
// ['a', 'b', 'c']
```

### `streamToString(stream, options?)` <span class="badge">async</span>

Concatenates all chunks into a single string. Byte chunks are decoded as UTF-8 with a streaming decoder, so multi-byte characters split across chunks decode correctly.

```javascript
const output = await streamToString(river)
// 'abc'
```

### `streamToObject(stream, options?)` <span class="badge">async</span>

Merges all chunks into a single object using `Object.assign`.

```javascript
const river = pipejoin([createReadableStream([{ a: 1 }, { b: 2 }])])
const output = await streamToObject(river)
// { a: 1, b: 2 }
```

### `streamToBuffer(stream, options?)` <span class="badge">async</span>

Collects all chunks into a single byte array: a `Buffer` on Node.js, a `Uint8Array` in the browser. String chunks are encoded as UTF-8.

```javascript
const bytes = await streamToBuffer(createReadableStream(['a', 'b']))
// Node.js: <Buffer 61 62>, browser: Uint8Array [97, 98]
```

## Stream Factories

### `createReadableStream(input, streamOptions)` <span class="badge">Readable</span>

Creates a Readable stream from various input types. Call it with no input to get a stream you feed yourself (see below).

#### Input types

| Type | Behavior |
|------|----------|
| `string` | Chunked at `chunkSize` (default 16KB) |
| `Array` | Each element emitted as a chunk |
| `AsyncIterable` / `Iterable` | Each yielded value emitted as a chunk |
| `ArrayBuffer` / `SharedArrayBuffer` / any typed array or `DataView` | Raw bytes chunked into `Uint8Array`s at `chunkSize` (default 16KB) |
| none (`undefined`) | Push mode: the stream stays open until you call `stream.push(null)` |

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

#### Push mode

With no input, `createReadableStream()` returns a stream you write to with `stream.push(chunk)`, ending it with `stream.push(null)`. `push` throws once more than `highWaterMark` (default 1024) chunks are queued and unread, so a producer that outruns its consumer fails instead of buffering without limit.

```javascript
import { createReadableStream, streamToArray } from '@datastream/core'

const stream = createReadableStream()
stream.push({ id: 1 })
stream.push({ id: 2 })
stream.push(null) // end of stream

await streamToArray(stream)
// [{ id: 1 }, { id: 2 }]
```

```javascript
// Explicit chunk size for strings and bytes
createReadableStream('abcdefghij', { chunkSize: 4 })
// chunks: 'abcd', 'efgh', 'ij'
createReadableStream(new Uint8Array([1, 2, 3, 4, 5]).buffer, { chunkSize: 2 })
// chunks: Uint8Array [1, 2], [3, 4], [5]
```

### `createPassThroughStream(fn, flush?, streamOptions)` <span class="badge">Transform (PassThrough)</span>

Creates a stream that observes each chunk without modifying it. The chunk is automatically passed through.

#### Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| `fn` | `(chunk) => void` | Called for each chunk, return value ignored |
| `flush` | `() => void` | Optional, called when stream ends |
| `streamOptions` | `object` | Stream configuration (supports `signal` for abort) |

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
| `streamOptions` | `object` | Stream configuration (supports `signal` for abort) |

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
| `streamOptions` | `object` | Stream configuration (supports `signal` and `abort`) |

`streamOptions.abort(reason)` runs once when the stream is torn down before it finishes, for any reason other than its own `fn`/`close` failing: an upstream error in a pipeline, an explicit abort (`writable.abort()` in the browser, `writable.destroy()` in Node.js), or `signal` firing. Use it to cancel work in flight, such as a pending request. It may be async, and any error it throws is ignored because the stream is already failing with `reason`. It is not called after a clean finish.

#### Example

```javascript
import { createWritableStream } from '@datastream/core'

const rows = []
const collector = createWritableStream((chunk) => {
  rows.push(chunk)
})

// Cancel an in-flight upload if the pipeline fails
const controller = new AbortController()
const upload = createWritableStream(write, close, {
  abort: (reason) => controller.abort(reason),
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
| `highWaterMark` | `number` | Backpressure threshold, counted in chunks |
| `signal` | `AbortSignal` | Abort signal |

### `timeout(ms, options)` <span class="badge">async</span>

Returns a promise that resolves after `ms` milliseconds. Supports `AbortSignal` cancellation.

```javascript
import { timeout } from '@datastream/core'

await timeout(1000) // wait 1 second

const controller = new AbortController()
await timeout(5000, { signal: controller.signal }) // cancellable
```

### `resolveLazy(value)`

Returns `value()` if `value` is a function, otherwise `value`. Streams use it for options that can be given lazily, such as `csvDetectHeaderStream().result().value.header` behind an arrow function.

```javascript
import { resolveLazy } from '@datastream/core'

resolveLazy('a')       // 'a'
resolveLazy(() => 'a') // 'a'
```

### `createChunkDecoder(options?)`

Streaming UTF-8 decoder for transforms that accept both strings and bytes. Strings pass through unchanged; bytes go through one `TextDecoder` in streaming mode, so a multi-byte character split across chunks is decoded once it is complete. `flush()` returns any incomplete trailing sequence as `U+FFFD` (or `""`). `options` are `TextDecoder` options, such as `{ ignoreBOM: true }`.

```javascript
import { createChunkDecoder } from '@datastream/core'

const bytes = new TextEncoder().encode('é')
const decoder = createChunkDecoder()
decoder.decode(bytes.subarray(0, 1)) // ''
decoder.decode(bytes.subarray(1))    // 'é'
decoder.decode('plain')              // 'plain'
decoder.flush()                      // ''
```

### `concatBytes(chunks)`

Joins an array of `Uint8Array`s (views are respected) into one new `Uint8Array`.

```javascript
import { concatBytes } from '@datastream/core'

concatBytes([Uint8Array.of(1, 2), Uint8Array.of(3)]) // Uint8Array [1, 2, 3]
```

### `backpressureGauge(streams)` <span class="badge">Node.js only</span>

Measures pause/resume timing across streams. Useful for identifying bottlenecks. Web Streams have no pause/resume events, so the browser build does not export it.

```javascript
import { backpressureGauge } from '@datastream/core'

const metrics = backpressureGauge({ parse: parseStream, validate: validateStream })
// After pipeline completes:
// metrics.parse.total = { timestamp, duration }
// metrics.parse.timeline = [{ timestamp, duration }, ...]
```
