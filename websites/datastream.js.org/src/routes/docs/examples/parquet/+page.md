---
title: Parquet — read and write
description: Read and write Apache Parquet files using datastream with hyparquet and parquet-wasm.
---

### Read Parquet file into CSV

Read a Parquet file from S3, extract specific columns, and write as CSV:

```javascript
import { pipeline, createReadableStream, streamToBuffer } from '@datastream/core'
import { awsS3GetObjectStream } from '@datastream/aws'
import { objectReadableStream } from '@datastream/object'
import { csvFormatStream } from '@datastream/csv'
import { fileWriteStream } from '@datastream/file'
import { parquetRead } from 'hyparquet'

const buffer = await streamToBuffer(
  await awsS3GetObjectStream({ Bucket: 'data-lake', Key: 'users.parquet' }),
)

const rows = []
await parquetRead({
  file: {
    byteLength: buffer.byteLength,
    read: (start, end) => buffer.slice(start, end),
  },
  columns: ['id', 'name', 'email'],
  onComplete: (data) => rows.push(...data),
})

const result = await pipeline([
  objectReadableStream(rows),
  csvFormatStream({ header: true }),
  fileWriteStream({ path: './users.csv' }),
])
```

### Write CSV to Parquet

Read a CSV file, parse into objects, and write as Parquet to S3:

```javascript
import { pipeline, streamToArray } from '@datastream/core'
import { fileReadStream } from '@datastream/file'
import {
  csvDetectDelimitersStream,
  csvDetectHeaderStream,
  csvParseStream,
  csvCoerceValuesStream,
} from '@datastream/csv'
import { objectFromEntriesStream } from '@datastream/object'
import { createReadableStreamFromArrayBuffer } from '@datastream/core'
import { awsS3PutObjectStream } from '@datastream/aws'
import { tableFromJSON, tableToIPC } from 'apache-arrow'

const detectDelimiters = csvDetectDelimitersStream()
const detectHeader = csvDetectHeaderStream({
  delimiterChar: () => detectDelimiters.result().value.delimiterChar,
  newlineChar: () => detectDelimiters.result().value.newlineChar,
  quoteChar: () => detectDelimiters.result().value.quoteChar,
  escapeChar: () => detectDelimiters.result().value.escapeChar,
})

const rows = await streamToArray(
  await pipeline([
    fileReadStream({ path: './users.csv' }),
    detectDelimiters,
    detectHeader,
    csvParseStream({
      delimiterChar: () => detectDelimiters.result().value.delimiterChar,
      newlineChar: () => detectDelimiters.result().value.newlineChar,
      quoteChar: () => detectDelimiters.result().value.quoteChar,
      escapeChar: () => detectDelimiters.result().value.escapeChar,
    }),
    objectFromEntriesStream({
      keys: () => detectHeader.result().value.header,
    }),
    csvCoerceValuesStream(),
  ]),
)

const table = tableFromJSON(rows)
const { writeParquet } = await import('parquet-wasm')
const parquetBuffer = writeParquet(table)

await pipeline([
  createReadableStreamFromArrayBuffer(parquetBuffer),
  awsS3PutObjectStream({ Bucket: 'data-lake', Key: 'users.parquet' }),
])
```
