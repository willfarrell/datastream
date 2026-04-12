---
title: string
description: String manipulation streams for splitting, replacing, counting, and measuring text.
---

String manipulation streams — split, replace, count, and measure text data.

## Install

```bash
npm install @datastream/string
```

## `stringReadableStream` <span class="badge">Readable</span>

Creates a Readable stream from a string input.

```javascript
import { stringReadableStream } from '@datastream/string'

const stream = stringReadableStream('hello world')
```

## `stringLengthStream` <span class="badge">PassThrough</span>

Counts the total character length of all chunks.

### Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `resultKey` | `string` | `"length"` | Key in pipeline result |

### Example

```javascript
import { pipeline, createReadableStream } from '@datastream/core'
import { stringLengthStream } from '@datastream/string'

const length = stringLengthStream()
const result = await pipeline([
  createReadableStream('hello world'),
  length,
])

console.log(result)
// { length: 11 }
```

## `stringCountStream` <span class="badge">PassThrough</span>

Counts occurrences of a substring across all chunks.

### Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `substr` | `string` | — | Substring to count |
| `resultKey` | `string` | `"count"` | Key in pipeline result |

### Example

```javascript
import { pipeline, createReadableStream } from '@datastream/core'
import { stringCountStream } from '@datastream/string'

const count = stringCountStream({ substr: '\n' })
const result = await pipeline([
  createReadableStream('line1\nline2\nline3'),
  count,
])

console.log(result)
// { count: 2 }
```

## `stringSplitStream` <span class="badge">Transform</span>

Splits streaming text by a separator, emitting one chunk per segment. Handles splits that cross chunk boundaries.

### Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `separator` | `string` | — | String to split on |

### Example

```javascript
import { pipeline, createReadableStream, streamToArray, pipejoin } from '@datastream/core'
import { stringSplitStream } from '@datastream/string'

const river = pipejoin([
  createReadableStream('alice,bob,charlie'),
  stringSplitStream({ separator: ',' }),
])

const output = await streamToArray(river)
// ['alice', 'bob', 'charlie']
```

## `stringReplaceStream` <span class="badge">Transform</span>

Replaces pattern matches in streaming text. Handles replacements that span chunk boundaries.

### Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `pattern` | `string \| RegExp` | — | Pattern to search for |
| `replacement` | `string` | — | Replacement string |

### Example

```javascript
import { stringReplaceStream } from '@datastream/string'

stringReplaceStream({ pattern: /\t/g, replacement: ',' })
```

## `stringMinimumFirstChunkSize` <span class="badge">Transform</span>

Buffers data until the first chunk meets a minimum size, then passes all subsequent chunks through unchanged.

### Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `chunkSize` | `number` | `1024` (1KB) | Minimum first chunk size in characters |

## `stringMinimumChunkSize` <span class="badge">Transform</span>

Buffers data until chunks meet a minimum size before emitting. Useful when downstream requires a minimum data size for processing.

### Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `chunkSize` | `number` | `1024` (1KB) | Minimum chunk size in characters |

## `stringSkipConsecutiveDuplicates` <span class="badge">Transform</span>

Skips consecutive duplicate string chunks.

```javascript
import { stringSkipConsecutiveDuplicates } from '@datastream/string'

// Input chunks: 'a', 'a', 'b', 'a' → Output: 'a', 'b', 'a'
```
