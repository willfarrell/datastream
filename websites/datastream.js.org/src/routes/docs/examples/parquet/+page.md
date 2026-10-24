---
title: Parquet — read and write
description: Stream Apache Parquet files in and out of datastream pipelines with DuckDB.
---

[`@datastream/duckdb`](/docs/packages/duckdb) handles Parquet through SQL: `read_parquet(...)` in `duckdbQueryStream` streams rows out of a file, and `COPY ... TO '...' (FORMAT parquet)` writes one. Rows are pulled from DuckDB one result chunk at a time, so neither direction loads the whole file into memory in JavaScript. The examples below are for Node.js. In the browser, file paths refer to duckdb-wasm's virtual file system (see the [duckdb page](/docs/packages/duckdb)).

### Read Parquet file into CSV

Select columns from a Parquet file and write them as CSV:

```javascript
import { pipeline } from '@datastream/core'
import { duckdbConnect, duckdbQueryStream } from '@datastream/duckdb'
import {
  csvObjectToArrayStream,
  csvInjectHeaderStream,
  csvFormatStream,
} from '@datastream/csv'
import { fileWriteStream } from '@datastream/file'

const db = await duckdbConnect()
const columns = ['id', 'name', 'email']

await pipeline([
  duckdbQueryStream({
    db,
    sql: 'SELECT id, name, email FROM read_parquet(?) ORDER BY id',
    params: ['./users.parquet'], // bind the path; never interpolate untrusted input into SQL
  }),
  csvObjectToArrayStream({ headers: columns }),
  csvInjectHeaderStream({ header: columns }),
  csvFormatStream(),
  await fileWriteStream({ path: './users.csv' }), // file streams are async: always await
])
```

### Write CSV to Parquet

Parse a CSV file, insert the rows into a DuckDB table with `duckdbAppenderStream`, `COPY` the table to a Parquet file, then upload the file to S3:

```javascript
import { pipeline } from '@datastream/core'
import { fileReadStream } from '@datastream/file'
import {
  csvDetectDelimitersStream,
  csvDetectHeaderStream,
  csvParseStream,
  csvArrayToObjectStream,
  csvCoerceValuesStream,
} from '@datastream/csv'
import { duckdbConnect, duckdbAppenderStream } from '@datastream/duckdb'
import { awsS3PutObjectStream } from '@datastream/aws/s3'

const db = await duckdbConnect()
await db.run('CREATE TABLE users (id INTEGER, name VARCHAR, email VARCHAR)')

const detectDelimiters = csvDetectDelimitersStream()
const delimiters = {
  delimiterChar: () => detectDelimiters.result().value.delimiterChar,
  newlineChar: () => detectDelimiters.result().value.newlineChar,
  quoteChar: () => detectDelimiters.result().value.quoteChar,
  escapeChar: () => detectDelimiters.result().value.escapeChar,
}
const detectHeader = csvDetectHeaderStream(delimiters)

await pipeline([
  await fileReadStream({ path: './users.csv' }),
  detectDelimiters,
  detectHeader,
  csvParseStream(delimiters),
  csvArrayToObjectStream({ headers: () => detectHeader.result().value.header }),
  csvCoerceValuesStream({ columns: { id: 'number' } }),
  await duckdbAppenderStream({ db, table: 'users' }),
])

await db.run("COPY users TO './users.parquet' (FORMAT parquet)")

await pipeline([
  await fileReadStream({ path: './users.parquet' }),
  awsS3PutObjectStream({ Bucket: 'data-lake', Key: 'users.parquet' }),
])
```

When the input is a CSV file DuckDB can read itself and needs no stream processing, skip the pipeline: `COPY (SELECT * FROM read_csv('./users.csv')) TO './users.parquet' (FORMAT parquet)`.
