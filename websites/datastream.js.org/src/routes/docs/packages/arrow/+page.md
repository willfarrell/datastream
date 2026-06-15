---
title: arrow
description: Apache Arrow record batch transform streams.
---

Apache Arrow record batch transform streams. Convert rows to and from Arrow `RecordBatch` objects and detect a schema from sampled data.

## Install

```bash
npm install @datastream/arrow apache-arrow
```

`apache-arrow` is a peer dependency.

## `arrowDetectSchemaStream` <span class="badge">PassThrough</span>

Samples the first rows of the stream and infers an Arrow `Schema`. Rows pass through unchanged. Accepts either arrays (columns become `column0`, `column1`, …) or objects (columns become object keys).

### Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `sampleSize` | `number` | `100` | Number of rows to buffer before sealing the schema |
| `resultKey` | `string` | `"arrowDetectSchema"` | Key in pipeline result |

### Result

```javascript
{ schema: Schema | null, fields: string[] | null }
```

### Type inference

| Value | Arrow type |
|-------|------------|
| `boolean` | `Bool` |
| Integer in signed 32-bit range | `Int32` |
| Integer outside 32-bit range / non-integer `number` | `Float64` |
| `Date` | `TimestampMillisecond` |
| Anything else | `Utf8` |

Integers outside the signed 32-bit range widen to `Float64` (exact up to 2^53) instead of silently wrapping inside an `Int32` builder.

### Example

```javascript
import { pipeline, createReadableStream } from '@datastream/core'
import { arrowDetectSchemaStream } from '@datastream/arrow'

const detect = arrowDetectSchemaStream()

const result = await pipeline([
  createReadableStream([
    { id: 1, name: 'Alice' },
    { id: 2, name: 'Bob' },
  ]),
  detect,
])

console.log(result.arrowDetectSchema.fields) // ['id', 'name']
```

## `arrowBatchFromArrayStream` <span class="badge">Transform</span>

Builds Arrow `RecordBatch` objects from incoming array rows. Each row is an array of column values in schema field order.

### Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `schema` | `Schema \| () => Schema` | — | Arrow schema, or a lazy function returning one (required) |
| `batchSize` | `number` | `10000` | Rows per emitted `RecordBatch` |

## `arrowBatchFromObjectStream` <span class="badge">Transform</span>

Builds Arrow `RecordBatch` objects from incoming object rows, mapping each schema field name to the matching object key.

### Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `schema` | `Schema \| () => Schema` | — | Arrow schema, or a lazy function returning one (required) |
| `batchSize` | `number` | `10000` | Rows per emitted `RecordBatch` |

### Example

```javascript
import { pipeline, createReadableStream } from '@datastream/core'
import { arrowDetectSchemaStream, arrowBatchFromObjectStream } from '@datastream/arrow'

const detect = arrowDetectSchemaStream()

await pipeline([
  createReadableStream(rows),
  detect,
  arrowBatchFromObjectStream({
    schema: () => detect.result().value.schema,
  }),
  // ... e.g. duckdbArrowInsertStream
])
```

## `arrowToArrayStream` <span class="badge">Transform</span>

Expands each incoming `RecordBatch` into array rows (one array of column values per row).

## `arrowToObjectStream` <span class="badge">Transform</span>

Expands each incoming `RecordBatch` into object rows, keyed by the batch schema's field names.

### Example

```javascript
import { pipeline } from '@datastream/core'
import { arrowToObjectStream } from '@datastream/arrow'

await pipeline([
  recordBatchReadableStream,
  arrowToObjectStream(),
  // ... each chunk is { id, name, ... }
])
```
