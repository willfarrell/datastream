---
title: sqlite
description: SQLite query and insert streams built on Node's node:sqlite.
---

SQLite query and insert streams built on Node's built-in [`node:sqlite`](https://nodejs.org/api/sqlite.html). Stream rows out of a query, or stream rows into a table inside one transaction. No extra dependencies.

## Install

```bash
npm install @datastream/sqlite
```

## Opening a database

There is no connect helper. Open a `DatabaseSync` yourself and pass it as `db`:

```javascript
import { DatabaseSync } from 'node:sqlite'

const db = new DatabaseSync('data.db') // or ':memory:'
```

You own the database: close it with `db.close()` once the streams are done.

## `sqliteQueryStream` <span class="badge">Readable</span>

Runs a query and streams each result row as an object.

### Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `db` | `DatabaseSync` | — | Open database (required) |
| `sql` | `string` | — | Query to run (required) |
| `params` | `array \| object \| null` | `[]` | An array binds positional `?` params in order. An object binds named params (`$id`, `:id`, `@id`) by key. `null` means no params |

Rows are read one at a time, so large results are not loaded into memory. Destroying the stream early (for example, `break` in a `for await` loop) releases the statement.

Rows come from `node:sqlite`, so they are objects with a `null` prototype.

Array params never treat an object value as named params: `[{}, 2]` throws instead of binding `2` to the first `?`.

### Example

```javascript
import { sqliteQueryStream } from '@datastream/sqlite'

const rows = sqliteQueryStream({
  db,
  sql: 'SELECT id, name FROM people WHERE created > $since',
  params: { since: '2026-01-01' },
})

for await (const { id, name } of rows) {
  console.log(id, name)
}
```

## `sqliteInsertStream` <span class="badge">Writable</span>

Inserts object or array rows into an existing table.

### Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `db` | `DatabaseSync` | — | Open database (required) |
| `table` | `string` | — | Target table name (required) |
| `columns` | `string[]` | first row's keys | Columns to insert. Required for array rows |

Object rows are inserted by column name. A missing key, or a key set to `undefined`, is stored as `NULL`. Only a row's own keys count, so a column named `constructor` or `toString` is not read from the object's prototype.

When you pass `columns`, it picks which keys to insert and other keys are ignored. When you leave it out, the columns come from the first row's keys, and any later row with a key outside them fails with a `TypeError` that names the keys. Pass `columns` when rows can have different keys.

Array rows are inserted in `columns` order and must have exactly one value per column. A shorter or longer row fails with a `TypeError` such as `sqliteInsertStream: array row has 1 values, expected 2`.

Any of these errors rolls back the whole stream (see [Transactions](#transactions)).

`table` and each column are quoted as single SQL identifiers, so `main.people` is a table named `main.people`, not `people` in schema `main`. Empty or non-string names throw a `TypeError`.

### Transactions

All rows go into one transaction. It begins with the first row and commits when the stream finishes. If the stream errors, is destroyed, or its `signal` aborts before it finishes, the transaction is rolled back and nothing is written. A stream that receives no rows does nothing and never reads the table.

Because the stream opens its own transaction, `db` must not already be in one. If it is, the first write fails with `sqlite: connection already has an open transaction; SQLite allows one writer per database file, so run insert streams on it one after another`, and the open transaction is left as it was.

This also means one `DatabaseSync` runs one insert stream at a time. A second insert stream that starts writing while the first is still open fails with that error, and the first stream carries on. A separate `DatabaseSync` per stream only helps when each one opens a different database file (or `:memory:`). SQLite allows one writer per file, so a second connection to the same file fails with `database is locked`. To load several tables in one file, run the insert streams one after another.

Large imports hold one transaction until the end. There is no option to commit in batches.

### Reading and writing the same table

Do not pipe `sqliteQueryStream` into `sqliteInsertStream` for the same table on the same `DatabaseSync`. The query reads rows lazily, so it can see the rows the insert stream has just added, and the copy may never finish.

Use one of these instead:

- Open a second `DatabaseSync` on the same file for the insert stream. The query does not see the insert transaction's rows before it commits. If the writer reports `database is locked`, enable WAL mode (`PRAGMA journal_mode = WAL`).
- Read the whole query result first, with `db.prepare(sql).all()`, and stream that array into the insert stream.
- Do the copy in SQL: `INSERT INTO t SELECT ... FROM t`.

### Example

```javascript
import { pipeline, createReadableStream } from '@datastream/core'
import { sqliteInsertStream } from '@datastream/sqlite'

db.exec('CREATE TABLE people (id INTEGER, name TEXT)')

await pipeline([
  createReadableStream([
    { id: 1, name: 'Alice' },
    { id: 2, name: 'Bob' },
  ]),
  sqliteInsertStream({ db, table: 'people' }),
])

// Array rows need columns
await pipeline([
  createReadableStream([[3, 'Carol']]),
  sqliteInsertStream({ db, table: 'people', columns: ['id', 'name'] }),
])
```

## Platform support

Node.js only (via `node:sqlite`). The package requires Node.js 26 or later. Like `@datastream/kafka`, the package has no `browser` export condition.
