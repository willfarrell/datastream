---
title: Parquet — read and write
description: Read and write Apache Parquet files using datastream with hyparquet and parquet-wasm.
---

### Read Parquet file into CSV

Read a Parquet file from S3, extract specific columns, and write as CSV:

```javascript
import { pipeline, streamToBuffer, createReadableStream } from '@datastream/core'
import { awsS3GetObjectStream } from '@datastream/aws/s3'
import { csvInjectHeaderStream, csvFormatStream } from '@datastream/csv'
import { fileWriteStream } from '@datastream/file'
import { parquetRead } from 'hyparquet'

const buffer = await streamToBuffer(
  await awsS3GetObjectStream({ Bucket: 'data-lake', Key: 'users.parquet' }),
)

const columns = ['id', 'name', 'email']
const rows = []
await parquetRead({
  // hyparquet reads from any { byteLength, slice(start, end) }, such as an ArrayBuffer
  file: buffer.buffer.slice(buffer.byteOffset, buffer.byteOffset + buffer.byteLength),
  columns,
  onComplete: (data) => rows.push(...data), // one array per row, in `columns` order
})

await pipeline([
  createReadableStream(rows),
  csvInjectHeaderStream({ header: columns }),
  csvFormatStream(),
  await fileWriteStream({ path: './users.csv' }), // file streams are async: always await
])
```

### Write CSV to Parquet

Read a CSV file, parse into objects, and write as Parquet to S3. `pipejoin` returns the joined stream so `streamToArray` can collect the rows (`pipeline` resolves to the result object instead):

```javascript
import {
  pipejoin,
  pipeline,
  streamToArray,
  createReadableStream,
} from '@datastream/core'
import { fileReadStream } from '@datastream/file'
import {
  csvDetectDelimitersStream,
  csvDetectHeaderStream,
  csvParseStream,
  csvCoerceValuesStream,
} from '@datastream/csv'
import { objectFromEntriesStream } from '@datastream/object'
import { awsS3PutObjectStream } from '@datastream/aws/s3'
import { tableFromJSON, tableToIPC } from 'apache-arrow'
import { Table, writeParquet } from 'parquet-wasm'

const detectDelimiters = csvDetectDelimitersStream()
const detectHeader = csvDetectHeaderStream({
  delimiterChar: () => detectDelimiters.result().value.delimiterChar,
  newlineChar: () => detectDelimiters.result().value.newlineChar,
  quoteChar: () => detectDelimiters.result().value.quoteChar,
  escapeChar: () => detectDelimiters.result().value.escapeChar,
})

const rows = await streamToArray(
  pipejoin([
    await fileReadStream({ path: './users.csv' }),
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

// parquet-wasm takes its own Table, built from Arrow IPC bytes.
// In the browser, call its default export (initWasm) once before use.
const arrowTable = tableFromJSON(rows)
const parquetBuffer = writeParquet(Table.fromIPCStream(tableToIPC(arrowTable, 'stream')))

await pipeline([
  createReadableStream(parquetBuffer),
  awsS3PutObjectStream({ Bucket: 'data-lake', Key: 'users.parquet' }),
])
```
