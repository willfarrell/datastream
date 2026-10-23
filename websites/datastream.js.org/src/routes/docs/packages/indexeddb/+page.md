---
title: indexeddb
description: IndexedDB read and write streams for the browser.
---

IndexedDB read and write streams for the browser.

## Install

```bash
npm install @datastream/indexeddb
```

## `indexedDBConnect`

Opens (or creates) an IndexedDB database. Re-exported from the [idb](https://www.npmjs.com/package/idb) library.

```javascript
import { indexedDBConnect } from '@datastream/indexeddb'

const db = await indexedDBConnect('my-database', 1, {
  upgrade(db) {
    db.createObjectStore('records', { keyPath: 'id' })
  },
})
```

## `indexedDBReadStream` <span class="badge">Readable</span> <span class="badge">async</span>

Reads records from an IndexedDB object store as a stream.

Records are read in batches of 100, and each batch uses its own short read-only transaction. A slow consumer can await other work between chunks without hitting `TransactionInactiveError`. The trade-off is that the read is not a single snapshot: a write or delete made between batches is reflected in the batches that follow.

Creating the stream does not touch the database. A missing store or index surfaces as a stream error (for example `NotFoundError`) on the first read, not when `indexedDBReadStream` is awaited.

### Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `db` | `IDBPDatabase` | — | idb-wrapped database from `indexedDBConnect` (a raw `IDBDatabase` does not work) |
| `store` | `string` | — | Object store name |
| `index` | `string` | — | Optional index name. Records are read in index order |
| `key` | `IDBKeyRange \| IDBValidKey` | — | Optional key or key range. Filters on the primary key, or on the index key when `index` is set |

`index` and `key` are independent: `key` alone filters by primary key, `index` alone walks the whole index, and both together filter by index key.

```javascript
// Primary keys 2 to 3
await indexedDBReadStream({ db, store: 'records', key: IDBKeyRange.bound(2, 3) })

// Every record, ordered by the 'byCity' index
await indexedDBReadStream({ db, store: 'records', index: 'byCity' })

// Records whose 'byCity' index key is 'Toronto'
await indexedDBReadStream({ db, store: 'records', index: 'byCity', key: 'Toronto' })
```

### Example

```javascript
import { pipeline } from '@datastream/core'
import { indexedDBConnect, indexedDBReadStream } from '@datastream/indexeddb'
import { objectCountStream } from '@datastream/object'

const db = await indexedDBConnect('my-database', 1)
const count = objectCountStream()

const result = await pipeline([
  await indexedDBReadStream({ db, store: 'records' }),
  count,
])

console.log(result)
// { objectCount: 100 }
```

## `indexedDBWriteStream` <span class="badge">Writable</span> <span class="badge">async</span>

Writes records to an IndexedDB object store.

Each record is inserted with `add()` in its own `readwrite` transaction, and the next record waits for that transaction to complete. There is no upsert: a record whose key already exists fails with `ConstraintError` and errors the stream. Records written before the failure stay committed, and the rest are not written. To replace existing records, delete them first or write your own stream with `put()`.

### Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `db` | `IDBPDatabase` | — | idb-wrapped database from `indexedDBConnect` (a raw `IDBDatabase` does not work) |
| `store` | `string` | — | Object store name |

### Example

```javascript
import { pipeline, createReadableStream } from '@datastream/core'
import { indexedDBConnect, indexedDBWriteStream } from '@datastream/indexeddb'

const db = await indexedDBConnect('my-database', 1)

await pipeline([
  createReadableStream([
    { id: 1, name: 'Alice' },
    { id: 2, name: 'Bob' },
  ]),
  await indexedDBWriteStream({ db, store: 'records' }),
])
```

## Platform support

Browser only. The package exports only a `browser` condition (plus the explicit `@datastream/indexeddb/browser` subpath); importing it from Node.js fails with `ERR_PACKAGE_PATH_NOT_EXPORTED` instead of loading a stub that throws "Not supported".
