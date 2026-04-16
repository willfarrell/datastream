---
title: json
description: JSON and NDJSON (JSON Lines) parsing and formatting streams.
---

JSON and NDJSON (Newline-Delimited JSON / JSON Lines) parsing and formatting streams.

## Install

```bash
npm install @datastream/json
```

## `ndjsonParseStream` <span class="badge">Transform</span>

Parses NDJSON (one JSON value per line) into individual JavaScript values. Splits on `\n`, parses each line with `JSON.parse`, and skips empty lines.

### Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `maxBufferSize` | `number` | `16777216` (16MB) | Maximum buffer size for incomplete lines |
| `resultKey` | `string` | `"jsonErrors"` | Key in pipeline result |

### Result

Parse errors collected during processing:

```javascript
{
  ParseError: { id: 'ParseError', message: 'Invalid JSON', idx: [3, 7] }
}
```

### Example

```javascript
import { pipeline, createReadableStream } from '@datastream/core'
import { ndjsonParseStream } from '@datastream/json'

const parse = ndjsonParseStream()

const result = await pipeline([
  createReadableStream('{"name":"Alice"}\n{"name":"Bob"}\n'),
  parse,
])

console.log(result.jsonErrors)
// {}
```

## `ndjsonFormatStream` <span class="badge">Transform</span>

Formats objects as NDJSON text (one `JSON.stringify` per line). Batches output for throughput.

### Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `space` | `number \| string` | — | Passed to `JSON.stringify` for pretty-printing each line |

### Example

```javascript
import { pipeline, createReadableStream } from '@datastream/core'
import { ndjsonFormatStream } from '@datastream/json'

await pipeline([
  createReadableStream([
    { name: 'Alice', age: 30 },
    { name: 'Bob', age: 25 },
  ]),
  ndjsonFormatStream(),
])

// Output: {"name":"Alice","age":30}\n{"name":"Bob","age":25}\n
```

## `jsonParseStream` <span class="badge">Transform</span>

Parses a streaming JSON array (`[{...},{...}]`) into individual elements. Uses a state machine to track nesting depth, strings, and escapes across chunk boundaries — no external dependencies.

### Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `maxBufferSize` | `number` | `16777216` (16MB) | Maximum buffer size for incomplete elements |
| `maxValueSize` | `number` | `16777216` (16MB) | Maximum size of a single JSON element |
| `resultKey` | `string` | `"jsonErrors"` | Key in pipeline result |

#### Buffer size protection

A JSON array with deeply nested or very large objects can cause the parser to buffer significant amounts of data. Setting `maxBufferSize` caps total memory and `maxValueSize` caps per-element memory. Lower these from defaults when parsing untrusted input.

```javascript
// Parse with a 1MB buffer limit for untrusted input
jsonParseStream({ maxBufferSize: 1 * 1024 * 1024 })
```

### Result

Parse errors collected during processing:

```javascript
{
  ParseError: { id: 'ParseError', message: 'Invalid JSON', idx: [2] }
}
```

### Example

```javascript
import { pipeline, createReadableStream, streamToArray } from '@datastream/core'
import { jsonParseStream } from '@datastream/json'

const streams = [
  createReadableStream('[{"name":"Alice"},{"name":"Bob"}]'),
  jsonParseStream(),
]

const output = await streamToArray(pipejoin(streams))
// [{ name: 'Alice' }, { name: 'Bob' }]
```

## `jsonFormatStream` <span class="badge">Transform</span>

Formats objects as a JSON array string (`[{...},{...}]`). Emits `[` with the first element, `,\n` between elements, and `]` on flush. Outputs `[]` if no elements.

### Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `space` | `number \| string` | — | Passed to `JSON.stringify` for pretty-printing |

### Example

```javascript
import { pipeline, createReadableStream } from '@datastream/core'
import { jsonFormatStream } from '@datastream/json'

await pipeline([
  createReadableStream([
    { name: 'Alice', age: 30 },
    { name: 'Bob', age: 25 },
  ]),
  jsonFormatStream({ space: 2 }),
])

// Output: [{\n  "name": "Alice",\n  "age": 30\n},\n{\n  "name": "Bob",\n  "age": 25\n}\n]
```

## Full pipeline example

Fetch NDJSON API, validate, and write as JSON array:

```javascript
import { pipeline } from '@datastream/core'
import { fetchResponseStream } from '@datastream/fetch'
import { ndjsonParseStream } from '@datastream/json'
import { validateStream } from '@datastream/validate'
import { objectCountStream } from '@datastream/object'
import { jsonFormatStream } from '@datastream/json'
import { fileWriteStream } from '@datastream/file'

const count = objectCountStream()
const parse = ndjsonParseStream()

const result = await pipeline([
  fetchResponseStream({ url: 'https://api.example.com/data.ndjson' }),
  parse,
  validateStream({ schema }),
  count,
  jsonFormatStream(),
  fileWriteStream({ path: './output.json' }),
])

console.log(result.count)        // number of objects processed
console.log(result.jsonErrors)   // any parse errors
```
