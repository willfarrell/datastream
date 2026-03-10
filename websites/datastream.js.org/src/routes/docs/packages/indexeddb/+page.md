---
title: indexeddb
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
| `index` | `string` | — | Optional index name |
| `key` | `IDBKeyRange` | — | Optional key range filter |

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
// { count: 100 }
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
