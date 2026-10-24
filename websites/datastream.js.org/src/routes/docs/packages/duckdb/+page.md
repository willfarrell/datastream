---
title: duckdb
description: DuckDB query readable and transactional writable streams for inserting rows and Arrow record batches.
---

DuckDB query and insert streams. Stream rows out of any SQL query, or insert rows or Apache Arrow `RecordBatch` objects directly into a DuckDB table. Through SQL, that covers reading and writing Parquet, CSV and JSON files.

## Install

```bash
npm install @datastream/duckdb
```

DuckDB is provided through optional peer dependencies. Install the runtime for your platform:

| Platform | Package |
|----------|---------|
| Node.js | `@duckdb/node-api` |
| Browser | `@duckdb/duckdb-wasm` |

The Arrow insert stream also requires `apache-arrow`.

## `duckdbConnect`

Opens a DuckDB connection. Defaults to an in-memory database.

| Argument | Type | Default | Description |
|----------|------|---------|-------------|
| `path` | `string` | `":memory:"` | Database file path, or `:memory:` |
| `options` | `object` | — | Node.js: DuckDB instance options. Browser: `{ bundles }` (see below) |

```javascript
import { duckdbConnect } from '@datastream/duckdb'

const db = await duckdbConnect() // in-memory
```

### Browser bundles

In the browser, `duckdbConnect` loads the duckdb-wasm module and worker. By default they come from jsDelivr (`getJsDelivrBundles()`). That code has no integrity pin, and browsers refuse to start a `Worker` from a cross-origin URL, so in production host the bundle files yourself and pass them as `bundles`:

```javascript
import { duckdbConnect } from '@datastream/duckdb'

const db = await duckdbConnect(':memory:', {
  bundles: {
    mvp: {
      mainModule: '/duckdb/duckdb-mvp.wasm',
      mainWorker: '/duckdb/duckdb-browser-mvp.worker.js',
    },
    eh: {
      mainModule: '/duckdb/duckdb-eh.wasm',
      mainWorker: '/duckdb/duckdb-browser-eh.worker.js',
    },
  },
})
```

The files ship in `@duckdb/duckdb-wasm/dist`. `bundles` is passed to duckdb-wasm's `selectBundle()`.

## `duckdbQueryStream` <span class="badge">Readable</span>

Runs a query and streams each result row as a plain object. Returns the readable stream directly (not a Promise).

### Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `db` | `Connection` | — | DuckDB connection from `duckdbConnect` (required) |
| `sql` | `string` | — | Query to run (required) |
| `params` | `array` | — | Values bound to positional `?` params in order |

Rows are pulled from DuckDB as the stream is read, one result chunk (Node.js) or Arrow record batch (browser) at a time, so large results are not loaded into memory.

Destroying the stream early (for example, `break` in a `for await` loop, a pipeline error, or an aborted `signal`) releases the result, and the browser closes the prepared statement. If the stream is waiting on DuckDB at that moment (the query is still running, or the next chunk is being fetched), the query is also stopped: Node.js calls `interrupt()` on the connection, the browser calls `cancelSent()`. A stream that was never read, or is paused between rows, is not interrupted.

`interrupt()` and `cancelSent()` act on the whole connection, not one query. Destroying a stream mid-fetch stops anything else running on that connection at the same time. Give each concurrent query its own connection.

Numbers, strings, booleans and `null` come out the same on both platforms, and `BIGINT` is a `bigint` on both. Some types differ:

| DuckDB type | Node.js | Browser |
|-------------|---------|---------|
| `DATE` / `TIMESTAMP` | `Date` | epoch-millisecond `number` |
| `DECIMAL` | `number` | Arrow's raw decimal value |

Cast in SQL (for example `price::DOUBLE`, `epoch_ms(created_at)`) when you need the same value on both.

### Example

```javascript
import { duckdbConnect, duckdbQueryStream } from '@datastream/duckdb'

const db = await duckdbConnect()

for await (const row of duckdbQueryStream({
  db,
  sql: 'SELECT id, name FROM people WHERE id > ?',
  params: [100],
})) {
  console.log(row.id, row.name)
}
```

## Parquet, CSV and JSON files

There is no separate file API: DuckDB reads and writes these formats in SQL. Read a file with `read_parquet`, `read_csv` or `read_json` in `duckdbQueryStream`:

```javascript
import { pipeline } from '@datastream/core'
import { duckdbConnect, duckdbQueryStream } from '@datastream/duckdb'

const db = await duckdbConnect()

await pipeline([
  duckdbQueryStream({
    db,
    sql: "SELECT * FROM read_parquet('events.parquet') WHERE type = ?",
    params: ['click'],
  }),
  // ...transform / write rows
])
```

Write a table or query out with `COPY` (`db.run` on Node.js, `db.query` in the browser):

```javascript
await db.run(
  "COPY (SELECT * FROM events WHERE type = 'click') TO 'clicks.parquet' (FORMAT parquet)",
)
```

To turn a stream into a Parquet file, insert it with `duckdbAppenderStream` or `duckdbArrowInsertStream` first, then `COPY` the table. In the browser, the file paths refer to duckdb-wasm's virtual file system (register files with `db.bindings.registerFileURL` / `registerFileHandle`).

## Transactions

`duckdbAppenderStream` and `duckdbArrowInsertStream` each run in their own transaction on the connection, on Node.js and in the browser:

- `BEGIN TRANSACTION` runs on the first write, before the table is created from `schema`.
- `COMMIT` runs once the stream finishes.
- `ROLLBACK` runs on any write error, upstream error, aborted `signal` or `destroy()`.

A failed pipeline commits nothing, including a table the stream created. Without the transaction, the Node.js appender would already have written its auto-flushed rows (every 204,800 rows), and the browser build would have written every insert.

The connection must not have a transaction open already. DuckDB has no nested transactions, so the stream fails with `duckdb: could not begin the write stream's transaction; use a connection with no open transaction`. DuckDB then aborts the caller's open transaction too. While a write stream is running, other statements on the same connection run inside its transaction, so use a separate connection for other work.

## `duckdbAppenderStream` <span class="badge">Writable</span>

Appends array or object rows into an existing table using the DuckDB appender. Returns a Promise resolving to the writable stream.

### Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `db` | `Connection` | — | DuckDB connection from `duckdbConnect` (required) |
| `table` | `string` | — | Target table name (required) |
| `schema` | `Schema \| () => Schema` | — | Arrow schema; when provided and the table does not exist, the table is created from it |

When no `schema` is given, the target table must already exist; its column names are read from the table. Array rows are inserted in column order, object rows by column name.

### Example

```javascript
import { pipeline, createReadableStream } from '@datastream/core'
import { duckdbConnect, duckdbAppenderStream } from '@datastream/duckdb'

const db = await duckdbConnect()
await db.run('CREATE TABLE people (id INTEGER, name VARCHAR)')

await pipeline([
  createReadableStream([
    { id: 1, name: 'Alice' },
    { id: 2, name: 'Bob' },
  ]),
  await duckdbAppenderStream({ db, table: 'people' }),
])
```

## `duckdbArrowInsertStream` <span class="badge">Writable</span>

Inserts Apache Arrow `RecordBatch` objects into a table. Returns a Promise resolving to the writable stream. Pairs with `arrowBatchFromObjectStream` / `arrowBatchFromArrayStream` from `@datastream/arrow`.

### Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `db` | `Connection` | — | DuckDB connection from `duckdbConnect` (required) |
| `table` | `string` | — | Target table name (required) |
| `schema` | `Schema \| () => Schema` | — | Arrow schema; when provided and the table does not exist, the table is created from it |
| `batchRows` | `number` | `100000` | Browser only: rows buffered before each insert. Lower it to reduce memory use |

On Node.js, batch fields are matched to table columns by name, ignoring case (a table column `ID` takes the Arrow field `id`). A batch with a missing column, or a different number of columns than the table, throws.

On Node.js, Arrow `Date` and `Timestamp` values are stored as DuckDB dates/timestamps only when the target column is `DATE` or a `TIMESTAMP` type. Other target columns get the raw epoch-millisecond number (for example, a `Timestamp` into a `BIGINT` column stores milliseconds).

### Example

```javascript
import { pipeline, createReadableStream } from '@datastream/core'
import { arrowDetectSchemaStream, arrowBatchFromObjectStream } from '@datastream/arrow'
import { duckdbConnect, duckdbArrowInsertStream } from '@datastream/duckdb'

const db = await duckdbConnect()
const detect = arrowDetectSchemaStream()

await pipeline([
  createReadableStream(rows),
  detect,
  arrowBatchFromObjectStream({ schema: () => detect.result().value.schema }),
  await duckdbArrowInsertStream({
    db,
    table: 'events',
    schema: () => detect.result().value.schema,
  }),
])
```

## Arrow type mapping

When creating a table from an Arrow schema, types map to DuckDB columns as follows:

| Arrow type | DuckDB type |
|------------|-------------|
| `Bool` | `BOOLEAN` |
| `Int8` / `Int16` / `Int32` / `Int64` | `TINYINT` / `SMALLINT` / `INTEGER` / `BIGINT` |
| `Uint8` / `Uint16` / `Uint32` / `Uint64` | `UTINYINT` / `USMALLINT` / `UINTEGER` / `UBIGINT` |
| `Float32` / `Float64` | `REAL` / `DOUBLE` |
| `DateDay` / `DateMillisecond` | `DATE` |
| `TimestampSecond` / `TimestampMillisecond` / `TimestampMicrosecond` / `TimestampNanosecond` | `TIMESTAMP_S` / `TIMESTAMP_MS` / `TIMESTAMP` / `TIMESTAMP_NS` |
| anything else | `VARCHAR` |

## Platform support

| Feature | Node.js | Browser |
|---------|---------|---------|
| `duckdbConnect` | `@duckdb/node-api` | `@duckdb/duckdb-wasm` |
| `duckdbQueryStream` | yes | yes |
| `duckdbAppenderStream` | yes | yes |
| `duckdbArrowInsertStream` | yes | yes (requires `apache-arrow`) |
