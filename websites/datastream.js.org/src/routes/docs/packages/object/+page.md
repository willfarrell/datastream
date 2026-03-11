---
title: object
description: Transform, reshape, filter, and aggregate object streams.
---

Transform, reshape, filter, and aggregate object streams.

## Install

```bash
npm install @datastream/object
```

## `objectReadableStream` <span class="badge">Readable</span>

Creates a Readable stream from an array of objects.

```javascript
import { objectReadableStream } from '@datastream/object'

const stream = objectReadableStream([{ a: 1 }, { a: 2 }, { a: 3 }])
```

## `objectCountStream` <span class="badge">PassThrough</span>

Counts the number of chunks that pass through.

### Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `resultKey` | `string` | `"count"` | Key in pipeline result |

### Example

```javascript
import { pipeline, createReadableStream } from '@datastream/core'
import { objectCountStream } from '@datastream/object'

const count = objectCountStream()
const result = await pipeline([
  createReadableStream([{ a: 1 }, { a: 2 }, { a: 3 }]),
  count,
])

console.log(result)
// { count: 3 }
```

## `objectFromEntriesStream` <span class="badge">Transform</span>

Converts arrays to objects using column names. Commonly used with `csvParseStream` output.

### Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `keys` | `string[] \| () => string[]` | — | Column names, or lazy function |

### Example

```javascript
import { objectFromEntriesStream } from '@datastream/object'

// Converts ['Alice', '30', 'Toronto'] → { name: 'Alice', age: '30', city: 'Toronto' }
objectFromEntriesStream({ keys: ['name', 'age', 'city'] })

// With lazy keys from csvDetectHeaderStream
objectFromEntriesStream({
  keys: () => detectHeader.result().value.header,
})
```

## `objectPickStream` <span class="badge">Transform</span>

Keeps only the specified keys on each object.

### Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `keys` | `string[]` | — | Keys to keep |

### Example

```javascript
import { objectPickStream } from '@datastream/object'

// { name: 'Alice', age: 30, city: 'Toronto' } → { name: 'Alice', age: 30 }
objectPickStream({ keys: ['name', 'age'] })
```

## `objectOmitStream` <span class="badge">Transform</span>

Removes the specified keys from each object.

### Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `keys` | `string[]` | — | Keys to remove |

### Example

```javascript
import { objectOmitStream } from '@datastream/object'

// { name: 'Alice', age: 30, city: 'Toronto' } → { name: 'Alice' }
objectOmitStream({ keys: ['age', 'city'] })
```

## `objectKeyMapStream` <span class="badge">Transform</span>

Renames keys on each object. Unmapped keys are kept as-is.

### Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `keys` | `object` | — | Map of `{ oldKey: 'newKey' }` |

### Example

```javascript
import { objectKeyMapStream } from '@datastream/object'

// { firstName: 'Alice', lastName: 'Smith' } → { first_name: 'Alice', last_name: 'Smith' }
objectKeyMapStream({ keys: { firstName: 'first_name', lastName: 'last_name' } })
```

## `objectValueMapStream` <span class="badge">Transform</span>

Maps values for a specific key using a lookup table.

### Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `key` | `string` | — | Key whose value to map |
| `values` | `object` | — | Map of `{ oldValue: newValue }` |

### Example

```javascript
import { objectValueMapStream } from '@datastream/object'

// { status: 'A' } → { status: 'Active' }
objectValueMapStream({ key: 'status', values: { A: 'Active', I: 'Inactive' } })
```

## `objectKeyJoinStream` <span class="badge">Transform</span>

Joins multiple keys into new combined keys.

### Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `keys` | `object` | — | Map of `{ newKey: ['key1', 'key2'] }` |
| `separator` | `string` | — | Separator for joined values |

### Example

```javascript
import { objectKeyJoinStream } from '@datastream/object'

// { first: 'Alice', last: 'Smith', age: 30 } → { name: 'Alice Smith', age: 30 }
objectKeyJoinStream({ keys: { name: ['first', 'last'] }, separator: ' ' })
```

## `objectKeyValueStream` <span class="badge">Transform</span>

Creates a key-value pair object from two fields.

### Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `key` | `string` | — | Field to use as the key |
| `value` | `string` | — | Field to use as the value |

### Example

```javascript
import { objectKeyValueStream } from '@datastream/object'

// { code: 'CA', country: 'Canada' } → { CA: 'Canada' }
objectKeyValueStream({ key: 'code', value: 'country' })
```

## `objectKeyValuesStream` <span class="badge">Transform</span>

Creates a key-values pair object — the key comes from a field, the value is a subset of the object.

### Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `key` | `string` | — | Field to use as the key |
| `values` | `string[]` | — | Fields to include in value object. If omitted, entire object is used |

### Example

```javascript
import { objectKeyValuesStream } from '@datastream/object'

// { id: 'u1', name: 'Alice', age: 30 } → { u1: { name: 'Alice', age: 30 } }
objectKeyValuesStream({ key: 'id', values: ['name', 'age'] })
```

## `objectBatchStream` <span class="badge">Transform</span>

Groups consecutive objects with the same key values into arrays. Use with `objectPivotLongToWideStream`.

### Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `keys` | `string[]` | — | Keys to group by |

### Example

```javascript
import { objectBatchStream } from '@datastream/object'

// Input: [{id:1,k:'a'}, {id:1,k:'b'}, {id:2,k:'c'}]
// Output: [[{id:1,k:'a'}, {id:1,k:'b'}]], [[{id:2,k:'c'}]]
objectBatchStream({ keys: ['id'] })
```

## `objectPivotLongToWideStream` <span class="badge">Transform</span>

Pivots batched arrays from long to wide format. Must be used after `objectBatchStream`.

### Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `keys` | `string[]` | — | Pivot key columns |
| `valueParam` | `string` | — | Column containing the values |
| `delimiter` | `string` | `" "` | Separator for combined key names |

### Example

```javascript
import { pipeline, createReadableStream } from '@datastream/core'
import { objectBatchStream, objectPivotLongToWideStream } from '@datastream/object'

// Input: [{id:1, metric:'temp', value:20}, {id:1, metric:'humidity', value:60}]
// Output: {id:1, temp:20, humidity:60}
await pipeline([
  createReadableStream(data),
  objectBatchStream({ keys: ['id'] }),
  objectPivotLongToWideStream({ keys: ['metric'], valueParam: 'value' }),
])
```

## `objectPivotWideToLongStream` <span class="badge">Transform</span>

Pivots wide format to long format. Emits multiple chunks per input object.

### Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `keys` | `string[]` | — | Columns to unpivot |
| `keyParam` | `string` | `"keyParam"` | Name for the new key column |
| `valueParam` | `string` | `"valueParam"` | Name for the new value column |

### Example

```javascript
import { objectPivotWideToLongStream } from '@datastream/object'

// Input: { id: 1, temp: 20, humidity: 60 }
// Output: { id: 1, keyParam: 'temp', valueParam: 20 }, { id: 1, keyParam: 'humidity', valueParam: 60 }
objectPivotWideToLongStream({
  keys: ['temp', 'humidity'],
  keyParam: 'metric',
  valueParam: 'value',
})
```

## `objectSkipConsecutiveDuplicatesStream` <span class="badge">Transform</span>

Skips consecutive duplicate objects (compared via `JSON.stringify`).

### Example

```javascript
import { objectSkipConsecutiveDuplicatesStream } from '@datastream/object'

// Input: [{a:1}, {a:1}, {a:2}, {a:1}]
// Output: [{a:1}, {a:2}, {a:1}]
```
