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

### Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `db` | `IDBDatabase` | — | Database connection from `indexedDBConnect` |
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

### Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `db` | `IDBDatabase` | — | Database connection from `indexedDBConnect` |
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
