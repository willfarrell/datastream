---
title: arrow
description: Apache Arrow record batch transform streams.
---

Apache Arrow record batch transform streams. Convert rows to and from Arrow `RecordBatch` objects and detect a schema from sampled data.

## Install

```bash
npm install @datastream/arrow apache-arrow
```

`apache-arrow` is a peer dependency: install it alongside the package. Sharing one copy with your own code (and with `@datastream/duckdb`) keeps `instanceof` checks on Arrow types and tables working across packages.

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

Values written to a `Utf8` column are stored as text: a `Date` as its ISO 8601 string, an object as JSON, and any other value with `String()`. `null` and `undefined` stay null.

### Rows after the sample

The schema is inferred from the first `sampleSize` rows only. Later rows are not used to change it:

- A value that does not fit its column type makes `arrowBatchFromArrayStream` / `arrowBatchFromObjectStream` error instead of being silently coerced. For example, `3000000000` or `2.7` in an `Int32` column, or `"oops"` in a `Float64` column. See [Value checks](#value-checks).
- A key that first appears after the sample has no column, so `arrowBatchFromObjectStream` drops it. Raise `sampleSize`, or pass your own schema, when rows vary.

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

### Value checks

Both batch streams check each value against its field before it reaches the Arrow builder, which would otherwise coerce it silently:

| Field type | Accepted values |
|------------|-----------------|
| `Int8` … `Int32`, `Uint8` … `Uint32` | Integer `number` within the type's range |
| `Float16` / `Float32` / `Float64` | Any `number` |
| `Bool` | `boolean` |
| `Timestamp` | Valid `Date`, or integer epoch `number` |
| Other types (`Utf8`, `Int64`, …) | Not checked |

`null` and `undefined` are allowed only in nullable fields. `""` counts as null, except in `Utf8` fields. A value that fails the check errors the stream. An integer out of range throws a `RangeError`, and any other mismatch a `TypeError`. The message names the stream, the zero-based row index, and the field: `arrowBatchFromObjectStream: row 2, field "id" expects Int32, got 3000000000`.

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
