---
title: duckdb
description: DuckDB writable streams for inserting rows and Arrow record batches.
---

DuckDB writable streams. Insert rows or Apache Arrow `RecordBatch` objects directly into a DuckDB table.

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
| `options` | `object` | — | DuckDB instance options |

```javascript
import { duckdbConnect } from '@datastream/duckdb'

const db = await duckdbConnect() // in-memory
```

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
| `duckdbAppenderStream` | yes | yes |
| `duckdbArrowInsertStream` | yes | yes (requires `apache-arrow`) |
